use alloy_eips::BlockHashOrNumber;
use alloy_primitives::{Log, B256, b256};
use reth_primitives_traits::Receipt;
use reth_storage_api::ReceiptProvider;
use reth_storage_errors::provider::ProviderResult;
use std::collections::HashSet;

/// Uniswap V2 `Sync` event topic.
pub const UNISWAP_V2_SYNC_TOPIC: B256 =
    b256!("1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1");

/// Uniswap V2 `Swap` event topic.
pub const UNISWAP_V2_SWAP_TOPIC: B256 =
    b256!("d78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822");

/// Uniswap V3 `Swap` event topic.
pub const UNISWAP_V3_SWAP_TOPIC: B256 =
    b256!("c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67");

/// Uniswap V4 `Swap` event topic emitted by `PoolManager`.
pub const UNISWAP_V4_SWAP_TOPIC: B256 =
    b256!("40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f");

/// DEX swap protocol identified from the event topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DexSwapProtocol {
    /// V2-style pair swap.
    V2,
    /// V3-style concentrated liquidity pool swap.
    V3,
    /// V4-style singleton `PoolManager` swap.
    V4,
}

/// Pool event kind matched from a log topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DexPoolEventKind {
    /// Uniswap V2 `Sync`.
    V2Sync,
    /// Uniswap V2 `Swap`.
    V2Swap,
    /// Uniswap V3 `Swap`.
    V3Swap,
    /// Uniswap V4 `Swap`.
    V4Swap,
}

/// Unique pool identifier used for last-event deduplication.
///
/// For V2/V3 pools this is the left-padded pool address. For V4 pools this is the `PoolId`.
pub type PoolKey = B256;

/// A pool event together with its receipt-local position.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedPoolEvent {
    /// Which event ABI matched the log.
    pub kind: DexPoolEventKind,
    /// Which swap family the log belongs to.
    pub protocol: DexSwapProtocol,
    /// Unique pool key for this log.
    pub pool_key: PoolKey,
    /// Index of the transaction receipt within the block.
    pub tx_index: usize,
    /// Index of the log within the receipt.
    pub log_index: usize,
    /// Raw log for downstream decoding.
    pub log: Log,
}

/// Collects Uniswap V2 `Sync` and Uniswap V2/V3/V4 `Swap` logs from a block via
/// [`ReceiptProvider::receipts_by_block`].
///
/// Returns `Ok(None)` if the block is not found.
pub fn collect_pool_events_by_block<P>(
    provider: &P,
    block: BlockHashOrNumber,
) -> ProviderResult<Option<Vec<IndexedPoolEvent>>>
where
    P: ReceiptProvider,
{
    let Some(receipts) = provider.receipts_by_block(block)? else {
        return Ok(None)
    };

    Ok(Some(collect_pool_events_from_receipts(&receipts)))
}

/// Collects only the latest pool event per pool from a block via
/// [`ReceiptProvider::receipts_by_block`].
///
/// Events are deduplicated by [`PoolKey`] with reverse receipt/log scanning, then returned in
/// block order.
pub fn collect_latest_pool_events_by_block<P>(
    provider: &P,
    block: BlockHashOrNumber,
) -> ProviderResult<Option<Vec<IndexedPoolEvent>>>
where
    P: ReceiptProvider,
{
    let Some(receipts) = provider.receipts_by_block(block)? else {
        return Ok(None)
    };

    Ok(Some(collect_latest_pool_events_from_receipts(&receipts)))
}

/// Collects Uniswap V2 `Sync` and Uniswap V2/V3/V4 `Swap` logs from the given receipts.
pub fn collect_pool_events_from_receipts<R>(receipts: &[R]) -> Vec<IndexedPoolEvent>
where
    R: Receipt,
{
    receipts
        .iter()
        .enumerate()
        .flat_map(|(tx_index, receipt)| {
            receipt.logs().iter().enumerate().filter_map(move |(log_index, log)| {
                classify_log(log).map(|(kind, pool_key)| IndexedPoolEvent {
                    protocol: protocol_for_event_kind(kind),
                    kind,
                    pool_key,
                    tx_index,
                    log_index,
                    log: log.clone(),
                })
            })
        })
        .collect()
}

/// Collects only the latest pool event per pool from the given receipts.
///
/// This scans receipts and logs in reverse so the first event seen for a pool is the latest one in
/// the block. The returned vector is re-ordered back into block order.
pub fn collect_latest_pool_events_from_receipts<R>(receipts: &[R]) -> Vec<IndexedPoolEvent>
where
    R: Receipt,
{
    let mut seen = HashSet::new();
    let mut latest_events = Vec::new();

    for (tx_index, receipt) in receipts.iter().enumerate().rev() {
        for (log_index, log) in receipt.logs().iter().enumerate().rev() {
            let Some((kind, pool_key)) = classify_log(log) else {
                continue;
            };

            if !seen.insert(pool_key) {
                continue;
            }

            latest_events.push(IndexedPoolEvent {
                protocol: protocol_for_event_kind(kind),
                kind,
                pool_key,
                tx_index,
                log_index,
                log: log.clone(),
            });
        }
    }

    latest_events.reverse();
    latest_events
}

fn classify_log(log: &Log) -> Option<(DexPoolEventKind, PoolKey)> {
    match log.topics().first().copied()? {
        UNISWAP_V2_SYNC_TOPIC => Some((DexPoolEventKind::V2Sync, log.address.into_word())),
        UNISWAP_V2_SWAP_TOPIC => Some((DexPoolEventKind::V2Swap, log.address.into_word())),
        UNISWAP_V3_SWAP_TOPIC => Some((DexPoolEventKind::V3Swap, log.address.into_word())),
        UNISWAP_V4_SWAP_TOPIC => Some((DexPoolEventKind::V4Swap, *log.topics().get(1)?)),
        _ => None,
    }
}

const fn protocol_for_event_kind(kind: DexPoolEventKind) -> DexSwapProtocol {
    match kind {
        DexPoolEventKind::V2Sync | DexPoolEventKind::V2Swap => DexSwapProtocol::V2,
        DexPoolEventKind::V3Swap => DexSwapProtocol::V3,
        DexPoolEventKind::V4Swap => DexSwapProtocol::V4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_eips::BlockHashOrNumber;
    use alloy_primitives::{Address, Bytes};
    use reth_ethereum_primitives::Receipt as EthReceipt;
    use reth_storage_api::ReceiptProvider;
    use reth_storage_errors::provider::ProviderResult;
    use std::collections::BTreeMap;

    #[derive(Debug, Default)]
    struct MockReceiptProvider {
        receipts: BTreeMap<u64, Vec<EthReceipt>>,
    }

    impl ReceiptProvider for MockReceiptProvider {
        type Receipt = EthReceipt;

        fn receipt(&self, _id: u64) -> ProviderResult<Option<Self::Receipt>> {
            unimplemented!("not needed for these tests")
        }

        fn receipt_by_hash(
            &self,
            _hash: alloy_primitives::TxHash,
        ) -> ProviderResult<Option<Self::Receipt>> {
            unimplemented!("not needed for these tests")
        }

        fn receipts_by_block(
            &self,
            block: BlockHashOrNumber,
        ) -> ProviderResult<Option<Vec<Self::Receipt>>> {
            match block {
                BlockHashOrNumber::Number(number) => Ok(self.receipts.get(&number).cloned()),
                BlockHashOrNumber::Hash(_) => Ok(None),
            }
        }

        fn receipts_by_tx_range(
            &self,
            _range: impl core::ops::RangeBounds<u64>,
        ) -> ProviderResult<Vec<Self::Receipt>> {
            unimplemented!("not needed for these tests")
        }

        fn receipts_by_block_range(
            &self,
            _block_range: core::ops::RangeInclusive<u64>,
        ) -> ProviderResult<Vec<Vec<Self::Receipt>>> {
            unimplemented!("not needed for these tests")
        }
    }

    #[test]
    fn filters_v2_sync_and_swap_topics_from_receipts() {
        let receipts = vec![
            receipt_with_logs(vec![
                log_with_topic(UNISWAP_V2_SWAP_TOPIC),
                log_with_topic(UNISWAP_V2_SYNC_TOPIC),
                log_with_topic(UNISWAP_V3_SWAP_TOPIC),
            ]),
            receipt_with_logs(vec![
                log_with_topic(B256::ZERO),
                log_with_topic_and_topics(
                    UNISWAP_V4_SWAP_TOPIC,
                    vec![UNISWAP_V4_SWAP_TOPIC, b256!("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")],
                ),
            ]),
        ];

        let swap_events = collect_pool_events_from_receipts(&receipts);

        assert_eq!(swap_events.len(), 4);
        assert_eq!(swap_events[0].kind, DexPoolEventKind::V2Swap);
        assert_eq!(
            swap_events[0].pool_key,
            b256!("0000000000000000000000001111111111111111111111111111111111111111")
        );
        assert_eq!(swap_events[0].tx_index, 0);
        assert_eq!(swap_events[0].log_index, 0);
        assert_eq!(swap_events[1].kind, DexPoolEventKind::V2Sync);
        assert_eq!(swap_events[1].tx_index, 0);
        assert_eq!(swap_events[1].log_index, 1);
        assert_eq!(swap_events[2].kind, DexPoolEventKind::V3Swap);
        assert_eq!(swap_events[2].tx_index, 0);
        assert_eq!(swap_events[2].log_index, 2);
        assert_eq!(swap_events[3].kind, DexPoolEventKind::V4Swap);
        assert_eq!(swap_events[3].tx_index, 1);
        assert_eq!(swap_events[3].log_index, 1);
    }

    #[test]
    fn loads_receipts_for_block_via_provider() {
        let mut provider = MockReceiptProvider::default();
        provider.receipts.insert(
            42,
            vec![receipt_with_logs(vec![
                log_with_topic(UNISWAP_V3_SWAP_TOPIC),
                log_with_topic(UNISWAP_V2_SYNC_TOPIC),
                log_with_topic(UNISWAP_V2_SWAP_TOPIC),
            ])],
        );

        let swap_events = collect_pool_events_by_block(&provider, BlockHashOrNumber::Number(42))
            .unwrap()
            .unwrap();

        assert_eq!(swap_events.len(), 3);
        assert_eq!(swap_events[0].kind, DexPoolEventKind::V3Swap);
        assert_eq!(swap_events[1].kind, DexPoolEventKind::V2Sync);
        assert_eq!(swap_events[2].kind, DexPoolEventKind::V2Swap);
    }

    #[test]
    fn returns_none_when_block_is_missing() {
        let provider = MockReceiptProvider::default();

        let swap_events = collect_pool_events_by_block(&provider, BlockHashOrNumber::Number(7))
            .unwrap();

        assert!(swap_events.is_none());
    }

    #[test]
    fn protocol_stays_v2_for_sync() {
        let events = collect_pool_events_from_receipts(&[receipt_with_logs(vec![log_with_topic(
            UNISWAP_V2_SYNC_TOPIC,
        )])]);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, DexPoolEventKind::V2Sync);
        assert_eq!(events[0].protocol, DexSwapProtocol::V2);
    }

    #[test]
    fn retains_log_indices_for_mixed_receipt() {
        let events = collect_pool_events_from_receipts(&[receipt_with_logs(vec![
            log_with_topic(B256::ZERO),
            log_with_topic(UNISWAP_V2_SYNC_TOPIC),
            log_with_topic_and_topics(
                UNISWAP_V4_SWAP_TOPIC,
                vec![UNISWAP_V4_SWAP_TOPIC, b256!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")],
            ),
        ])]);

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, DexPoolEventKind::V2Sync);
        assert_eq!(events[0].log_index, 1);
        assert_eq!(events[1].kind, DexPoolEventKind::V4Swap);
        assert_eq!(events[1].log_index, 2);
    }

    #[test]
    fn keeps_only_latest_event_per_v2_pool() {
        let pool_key =
            b256!("0000000000000000000000002222222222222222222222222222222222222222");
        let events = collect_latest_pool_events_from_receipts(&[
            receipt_with_logs(vec![log_for_address(0x22, UNISWAP_V2_SWAP_TOPIC)]),
            receipt_with_logs(vec![
                log_for_address(0x22, UNISWAP_V2_SYNC_TOPIC),
                log_for_address(0x33, UNISWAP_V3_SWAP_TOPIC),
            ]),
        ]);

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].pool_key, pool_key);
        assert_eq!(events[0].kind, DexPoolEventKind::V2Sync);
        assert_eq!(events[0].tx_index, 1);
        assert_eq!(
            events[1].pool_key,
            b256!("0000000000000000000000003333333333333333333333333333333333333333")
        );
    }

    #[test]
    fn keeps_only_latest_event_per_v4_pool_id() {
        let pool_id = b256!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let other_pool_id =
            b256!("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc");

        let events = collect_latest_pool_events_from_receipts(&[receipt_with_logs(vec![
            log_with_topic_and_topics(UNISWAP_V4_SWAP_TOPIC, vec![UNISWAP_V4_SWAP_TOPIC, pool_id]),
            log_with_topic_and_topics(
                UNISWAP_V4_SWAP_TOPIC,
                vec![UNISWAP_V4_SWAP_TOPIC, other_pool_id],
            ),
            log_with_topic_and_topics(UNISWAP_V4_SWAP_TOPIC, vec![UNISWAP_V4_SWAP_TOPIC, pool_id]),
        ])]);

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].pool_key, other_pool_id);
        assert_eq!(events[1].pool_key, pool_id);
        assert_eq!(events[1].log_index, 2);
    }

    fn receipt_with_logs(logs: Vec<Log>) -> EthReceipt {
        EthReceipt {
            tx_type: alloy_consensus::TxType::Legacy,
            success: true,
            cumulative_gas_used: 21_000,
            logs,
        }
    }

    fn log_with_topic(topic0: B256) -> Log {
        log_with_topic_and_topics(topic0, vec![topic0])
    }

    fn log_with_topic_and_topics(topic0: B256, topics: Vec<B256>) -> Log {
        let mut topics = topics;
        if topics.is_empty() {
            topics.push(topic0);
        }
        Log::new_unchecked(Address::repeat_byte(0x11), topics, Bytes::new())
    }

    fn log_for_address(byte: u8, topic0: B256) -> Log {
        Log::new_unchecked(
            alloy_primitives::Address::repeat_byte(byte),
            vec![topic0],
            Bytes::new(),
        )
    }
}
