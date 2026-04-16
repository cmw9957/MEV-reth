use std::{collections::BTreeSet, str::FromStr};

use alloy_primitives::{Address, B256, U256};
use chrono::{Months, Utc};
use eyre::{WrapErr, eyre};
use serde_json::{Value, json};

use crate::{
    db,
    models::{V2PoolMeta, V3PoolMeta, V4PoolKey, V4PoolMeta},
    topics::{V2_PAIR_CREATED_TOPIC, V3_POOL_CREATED_TOPIC, V4_INITIALIZE_TOPIC},
};

const JOB_NAME: &str = "dex_factory_backfill";

pub(crate) struct RpcClient {
    client: reqwest::Client,
    rpc_url: String,
}

impl RpcClient {
    pub(crate) fn new(rpc_url: String) -> Self {
        Self { client: reqwest::Client::new(), rpc_url }
    }

    async fn latest_block_number(&self) -> eyre::Result<u64> {
        let value = self.call("eth_blockNumber", json!([])).await?;
        parse_hex_u64(&value).wrap_err("failed to parse latest block number")
    }

    async fn block_timestamp(&self, block_number: u64) -> eyre::Result<u64> {
        let value = self
            .call(
                "eth_getBlockByNumber",
                json!([format_block_number(block_number), false]),
            )
            .await?;

        let timestamp = value
            .get("timestamp")
            .ok_or_else(|| eyre!("missing block timestamp for block {block_number}"))?;

        parse_hex_u64(timestamp)
            .wrap_err_with(|| format!("failed to parse timestamp for block {block_number}"))
    }

    async fn logs_in_range(
        &self,
        start_block: u64,
        end_block: u64,
    ) -> eyre::Result<Vec<Value>> {
        let value = self
            .call(
                "eth_getLogs",
                json!([{
                    "fromBlock": format_block_number(start_block),
                    "toBlock": format_block_number(end_block),
                    "topics": [[
                        format_b256(V2_PAIR_CREATED_TOPIC),
                        format_b256(V3_POOL_CREATED_TOPIC),
                        format_b256(V4_INITIALIZE_TOPIC),
                    ]],
                }]),
            )
            .await?;

        value
            .as_array()
            .cloned()
            .ok_or_else(|| eyre!("eth_getLogs did not return an array"))
    }

    async fn call(&self, method: &str, params: Value) -> eyre::Result<Value> {
        let response = self
            .client
            .post(&self.rpc_url)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }))
            .send()
            .await
            .wrap_err_with(|| format!("failed to call {method}"))?
            .error_for_status()
            .wrap_err_with(|| format!("rpc call {method} returned error status"))?;

        let body: Value = response
            .json()
            .await
            .wrap_err_with(|| format!("failed to decode JSON-RPC response for {method}"))?;

        if let Some(error) = body.get("error") {
            return Err(eyre!("rpc call {method} failed: {error}"))
        }

        body.get("result")
            .cloned()
            .ok_or_else(|| eyre!("rpc call {method} returned no result"))
    }
}

pub(crate) async fn run(
    rpc: &RpcClient,
    pg_client: &mut tokio_postgres::Client,
    chunk_size: u64,
    lookback_years: u32,
    from_block: Option<u64>,
    to_block: Option<u64>,
) -> eyre::Result<()> {
    if chunk_size == 0 {
        return Err(eyre!("chunk_size must be greater than 0"))
    }

    let latest_block = rpc.latest_block_number().await?;
    let target_block = to_block.unwrap_or(latest_block).min(latest_block);

    let initial_start = match from_block {
        Some(block) => block,
        None => {
            let target_ts = lookback_timestamp(lookback_years)?;
            find_first_block_at_or_after(rpc, target_ts).await?
        }
    };

    let start_block = match db::load_checkpoint(pg_client, JOB_NAME).await? {
        Some(checkpoint) => initial_start.max(checkpoint.saturating_add(1)),
        None => initial_start,
    };

    if start_block > target_block {
        tracing::info!(start_block, target_block, "nothing to backfill");
        return Ok(());
    }

    let (mut known_v2, mut known_v3, mut known_v4) = db::load_known_factories(pg_client).await?;
    tracing::info!(
        start_block,
        target_block,
        chunk_size,
        latest_block,
        known_v2 = known_v2.len(),
        known_v3 = known_v3.len(),
        known_v4 = known_v4.len(),
        "starting DEX factory backfill over RPC"
    );

    let mut chunk_start = start_block;
    while chunk_start <= target_block {
        let chunk_end = chunk_start
            .saturating_add(chunk_size.saturating_sub(1))
            .min(target_block);

        let scan = scan_chunk(rpc, chunk_start, chunk_end).await?;

        let mut new_v2 = Vec::new();
        for address in scan.v2_factories {
            if known_v2.insert(address) {
                new_v2.push(address);
            }
        }

        let mut new_v3 = Vec::new();
        for address in scan.v3_factories {
            if known_v3.insert(address) {
                new_v3.push(address);
            }
        }

        let mut new_v4 = Vec::new();
        for address in scan.v4_factories {
            if known_v4.insert(address) {
                new_v4.push(address);
            }
        }

        let stored = db::store_addresses_and_checkpoint(
            pg_client,
            &new_v2,
            &new_v3,
            &new_v4,
            &scan.v2_pools,
            &scan.v3_pools,
            &scan.v4_pools,
            JOB_NAME,
            chunk_end,
        )
        .await?;

        tracing::info!(
            chunk_start,
            chunk_end,
            new_factories_v2 = stored.factories_v2,
            new_factories_v3 = stored.factories_v3,
            new_factories_v4 = stored.factories_v4,
            pools_v2 = stored.pools_v2,
            pools_v3 = stored.pools_v3,
            pools_v4 = stored.pools_v4,
            "stored chunk results"
        );

        chunk_start = chunk_end.saturating_add(1);
    }

    Ok(())
}

fn lookback_timestamp(lookback_years: u32) -> eyre::Result<u64> {
    let now = Utc::now();
    let then = now
        .checked_sub_months(Months::new(lookback_years.saturating_mul(12)))
        .ok_or_else(|| eyre!("failed to subtract lookback period from current time"))?;
    Ok(then.timestamp() as u64)
}

async fn find_first_block_at_or_after(rpc: &RpcClient, target_timestamp: u64) -> eyre::Result<u64> {
    let mut lo = 0_u64;
    let mut hi = rpc.latest_block_number().await?;

    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let timestamp = rpc.block_timestamp(mid).await?;

        if timestamp < target_timestamp {
            lo = mid.saturating_add(1);
        } else {
            hi = mid;
        }
    }

    Ok(lo)
}

struct ChunkScan {
    v2_factories: Vec<Address>,
    v3_factories: Vec<Address>,
    v4_factories: Vec<Address>,
    v2_pools: Vec<V2PoolMeta>,
    v3_pools: Vec<V3PoolMeta>,
    v4_pools: Vec<V4PoolMeta>,
}

async fn scan_chunk(rpc: &RpcClient, start_block: u64, end_block: u64) -> eyre::Result<ChunkScan> {
    let logs = rpc.logs_in_range(start_block, end_block).await?;

    let mut v2_factories = BTreeSet::new();
    let mut v3_factories = BTreeSet::new();
    let mut v4_factories = BTreeSet::new();
    let mut v2_pools = BTreeSet::new();
    let mut v3_pools = BTreeSet::new();
    let mut v4_pools = Vec::new();

    for log in logs {
        let block_number = log
            .get("blockNumber")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let tx_hash = log
            .get("transactionHash")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let log_index = log
            .get("logIndex")
            .and_then(Value::as_str)
            .unwrap_or("unknown");

        let topic0 = match topic0(&log) {
            Ok(topic0) => topic0,
            Err(err) => {
                tracing::warn!(
                    ?err,
                    block_number,
                    tx_hash,
                    log_index,
                    "skipping malformed log without topic0"
                );
                continue;
            }
        };
        let factory = match log_address(&log) {
            Ok(factory) => factory,
            Err(err) => {
                tracing::warn!(
                    ?err,
                    block_number,
                    tx_hash,
                    log_index,
                    "skipping malformed log without address"
                );
                continue;
            }
        };

        match topic0 {
            V2_PAIR_CREATED_TOPIC => {
                v2_factories.insert(factory);
                match parse_v2_pool(&log, factory) {
                    Ok(pool) => {
                        v2_pools.insert(pool);
                    }
                    Err(err) => {
                        tracing::warn!(
                            ?err,
                            factory = %factory,
                            block_number,
                            tx_hash,
                            log_index,
                            "skipping malformed v2 pool log"
                        );
                    }
                }
            }
            V3_POOL_CREATED_TOPIC => {
                v3_factories.insert(factory);
                match parse_v3_pool(&log, factory) {
                    Ok(pool) => {
                        v3_pools.insert(pool);
                    }
                    Err(err) => {
                        tracing::warn!(
                            ?err,
                            factory = %factory,
                            block_number,
                            tx_hash,
                            log_index,
                            "skipping malformed v3 pool log"
                        );
                    }
                }
            }
            V4_INITIALIZE_TOPIC => {
                v4_factories.insert(factory);
                match parse_v4_pool(&log, factory) {
                    Ok(pool) => v4_pools.push(pool),
                    Err(err) => {
                        tracing::warn!(
                            ?err,
                            factory = %factory,
                            block_number,
                            tx_hash,
                            log_index,
                            "skipping malformed v4 pool log"
                        );
                    }
                }
            }
            _ => {}
        }
    }

    Ok(ChunkScan {
        v2_factories: v2_factories.into_iter().collect(),
        v3_factories: v3_factories.into_iter().collect(),
        v4_factories: v4_factories.into_iter().collect(),
        v2_pools: v2_pools.into_iter().collect(),
        v3_pools: v3_pools.into_iter().collect(),
        v4_pools,
    })
}

fn parse_v2_pool(log: &Value, factory: Address) -> eyre::Result<V2PoolMeta> {
    let token0 = topic_address(log, 1)?;
    let token1 = topic_address(log, 2)?;
    let words = data_words(log)?;
    let pair_address = word_address(words.first().ok_or_else(|| eyre!("v2 PairCreated missing pair address"))?)?;

    Ok(V2PoolMeta { factory, pair_address, token0, token1 })
}

fn parse_v3_pool(log: &Value, factory: Address) -> eyre::Result<V3PoolMeta> {
    let token0 = topic_address(log, 1)?;
    let token1 = topic_address(log, 2)?;
    let fee = topic_u24(log, 3)?;
    let words = data_words(log)?;
    if words.len() < 2 {
        return Err(eyre!("v3 PoolCreated missing data words"))
    }

    let tick_spacing = word_i24(&words[0])?;
    let pool_address = word_address(&words[1])?;

    Ok(V3PoolMeta { factory, pool_address, token0, token1, fee, tick_spacing })
}

fn parse_v4_pool(log: &Value, factory: Address) -> eyre::Result<V4PoolMeta> {
    let currency0 = topic_address(log, 2)?;
    let currency1 = topic_address(log, 3)?;
    let words = data_words(log)?;
    if words.len() < 5 {
        return Err(eyre!("v4 Initialize missing data words"))
    }

    let fee = word_u24(&words[0])?;
    let tick_spacing = word_i24(&words[1])?;
    let hooks = word_address(&words[2])?;
    let sqrt_price_x96 = U256::from_be_bytes(words[3].0).to_string();

    Ok(V4PoolMeta {
        factory,
        key: V4PoolKey { currency0, currency1, fee, tick_spacing, hooks },
        sqrt_price_x96,
    })
}

fn topic0(log: &Value) -> eyre::Result<B256> {
    let topic = log
        .get("topics")
        .and_then(Value::as_array)
        .and_then(|topics| topics.first())
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("log is missing topic0"))?;

    B256::from_str(topic).wrap_err_with(|| format!("failed to parse topic0 {topic}"))
}

fn topic_address(log: &Value, index: usize) -> eyre::Result<Address> {
    let topic = log
        .get("topics")
        .and_then(Value::as_array)
        .and_then(|topics| topics.get(index))
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("log is missing topic at index {index}"))?;
    let word = B256::from_str(topic).wrap_err_with(|| format!("failed to parse topic {topic}"))?;
    word_address(&word)
}

fn topic_u24(log: &Value, index: usize) -> eyre::Result<u32> {
    let topic = log
        .get("topics")
        .and_then(Value::as_array)
        .and_then(|topics| topics.get(index))
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("log is missing topic at index {index}"))?;
    let word = B256::from_str(topic).wrap_err_with(|| format!("failed to parse topic {topic}"))?;
    word_u24(&word)
}

fn log_address(log: &Value) -> eyre::Result<Address> {
    let address = log
        .get("address")
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("log is missing address"))?;

    Address::from_str(address).wrap_err_with(|| format!("failed to parse address {address}"))
}

fn data_words(log: &Value) -> eyre::Result<Vec<B256>> {
    let data = log
        .get("data")
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("log is missing data"))?;
    let data = data.strip_prefix("0x").unwrap_or(data);

    if data.len() % 64 != 0 {
        return Err(eyre!("log data is not word-aligned"))
    }

    let mut words = Vec::with_capacity(data.len() / 64);
    for i in (0..data.len()).step_by(64) {
        let chunk = &data[i..i + 64];
        let word = B256::from_str(&format!("0x{chunk}"))
            .wrap_err_with(|| format!("failed to parse data word 0x{chunk}"))?;
        words.push(word);
    }
    Ok(words)
}

fn word_address(word: &B256) -> eyre::Result<Address> {
    Address::try_from(&word.0[12..]).wrap_err("failed to parse address word")
}

fn word_u24(word: &B256) -> eyre::Result<u32> {
    let value = U256::from_be_bytes(word.0);
    let fee = u32::try_from(value).map_err(|_| eyre!("u24 value overflow"))?;
    if fee > 0x00ff_ffff {
        return Err(eyre!("u24 value exceeds 24 bits"))
    }
    Ok(fee)
}

fn word_i24(word: &B256) -> eyre::Result<i32> {
    let bytes = &word.0[29..32];
    let raw = ((bytes[0] as i32) << 16) | ((bytes[1] as i32) << 8) | (bytes[2] as i32);
    Ok(if raw & 0x80_0000 != 0 { raw - 0x1_00_0000 } else { raw })
}

fn parse_hex_u64(value: &Value) -> eyre::Result<u64> {
    let hex = value.as_str().ok_or_else(|| eyre!("expected hex string"))?;
    u64::from_str_radix(hex.trim_start_matches("0x"), 16)
        .wrap_err_with(|| format!("failed to parse hex value {hex}"))
}

fn format_block_number(block_number: u64) -> String {
    format!("0x{block_number:x}")
}

fn format_b256(value: B256) -> String {
    value.to_string()
}
