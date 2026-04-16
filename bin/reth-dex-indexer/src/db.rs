use std::collections::HashSet;

use alloy_primitives::Address;
use eyre::WrapErr;
use tokio_postgres::Client;

use crate::models::{V2PoolMeta, V3PoolMeta, V4PoolMeta};

pub(crate) struct StoreCounts {
    pub(crate) factories_v2: usize,
    pub(crate) factories_v3: usize,
    pub(crate) factories_v4: usize,
    pub(crate) pools_v2: usize,
    pub(crate) pools_v3: usize,
    pub(crate) pools_v4: usize,
}

pub(crate) async fn connect(pg_url: &str) -> eyre::Result<Client> {
    let (client, connection) = tokio_postgres::connect(pg_url, tokio_postgres::NoTls)
        .await
        .wrap_err("failed to connect to PostgreSQL")?;

    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::error!(?err, "postgres connection terminated");
        }
    });

    Ok(client)
}

pub(crate) async fn init_schema(client: &Client) -> eyre::Result<()> {
    client
        .batch_execute(
            "
            create schema if not exists mev;

            create table if not exists mev.v2_factories (
                address bytea primary key
            );

            create table if not exists mev.v3_factories (
                address bytea primary key
            );

            create table if not exists mev.v4_factories (
                address bytea primary key
            );

            create table if not exists mev.v2_pools (
                factory bytea not null references mev.v2_factories(address),
                pair_address bytea primary key,
                token0 bytea not null,
                token1 bytea not null
            );

            create table if not exists mev.v3_pools (
                factory bytea not null references mev.v3_factories(address),
                pool_address bytea primary key,
                token0 bytea not null,
                token1 bytea not null,
                fee integer not null,
                tick_spacing integer not null
            );

            create table if not exists mev.v4_pools (
                factory bytea not null references mev.v4_factories(address),
                currency0 bytea not null,
                currency1 bytea not null,
                fee integer not null,
                tick_spacing integer not null,
                hooks bytea not null,
                sqrt_price_x96 numeric not null,
                primary key (currency0, currency1, fee, tick_spacing, hooks)
            );

            create table if not exists mev.backfill_checkpoint (
                job_name text primary key,
                last_scanned_block bigint not null
            );
            ",
        )
        .await
        .wrap_err("failed to initialize PostgreSQL schema")?;

    Ok(())
}

/// Loads known factory addresses into memory for fast deduplication.
pub(crate) async fn load_known_factories(
    client: &Client,
) -> eyre::Result<(HashSet<Address>, HashSet<Address>, HashSet<Address>)> {
    let v2_rows = client
        .query("select address from mev.v2_factories", &[])
        .await
        .wrap_err("failed to load v2 factory addresses")?;

    let v3_rows = client
        .query("select address from mev.v3_factories", &[])
        .await
        .wrap_err("failed to load v3 factory addresses")?;

    let v4_rows = client
        .query("select address from mev.v4_factories", &[])
        .await
        .wrap_err("failed to load v4 factory addresses")?;

    let v2 = v2_rows
        .into_iter()
        .map(|row| {
            let bytes: &[u8] = row.get(0);
            Address::try_from(bytes).wrap_err("invalid v2 address bytes")
        })
        .collect::<eyre::Result<HashSet<_>>>()?;

    let v3 = v3_rows
        .into_iter()
        .map(|row| {
            let bytes: &[u8] = row.get(0);
            Address::try_from(bytes).wrap_err("invalid v3 address bytes")
        })
        .collect::<eyre::Result<HashSet<_>>>()?;

    let v4 = v4_rows
        .into_iter()
        .map(|row| {
            let bytes: &[u8] = row.get(0);
            Address::try_from(bytes).wrap_err("invalid v4 address bytes")
        })
        .collect::<eyre::Result<HashSet<_>>>()?;

    Ok((v2, v3, v4))
}

pub(crate) async fn load_checkpoint(client: &Client, job_name: &str) -> eyre::Result<Option<u64>> {
    let row = client
        .query_opt(
            "select last_scanned_block from mev.backfill_checkpoint where job_name = $1",
            &[&job_name],
        )
        .await
        .wrap_err("failed to load checkpoint")?;

    Ok(row.map(|row| row.get::<_, i64>(0) as u64))
}

pub(crate) async fn store_addresses_and_checkpoint(
    client: &mut Client,
    v2_addresses: &[Address],
    v3_addresses: &[Address],
    v4_addresses: &[Address],
    v2_pools: &[V2PoolMeta],
    v3_pools: &[V3PoolMeta],
    v4_pools: &[V4PoolMeta],
    job_name: &str,
    last_scanned_block: u64,
) -> eyre::Result<StoreCounts> {
    let tx = client.transaction().await.wrap_err("failed to start PostgreSQL transaction")?;
    let mut counts = StoreCounts {
        factories_v2: 0,
        factories_v3: 0,
        factories_v4: 0,
        pools_v2: 0,
        pools_v3: 0,
        pools_v4: 0,
    };

    for address in v2_addresses {
        counts.factories_v2 += tx
            .execute(
            "insert into mev.v2_factories(address) values ($1) on conflict do nothing",
            &[&address.as_slice()],
        )
        .await
        .wrap_err("failed to insert v2 factory address")? as usize;
    }

    for address in v3_addresses {
        counts.factories_v3 += tx
            .execute(
            "insert into mev.v3_factories(address) values ($1) on conflict do nothing",
            &[&address.as_slice()],
        )
        .await
        .wrap_err("failed to insert v3 factory address")? as usize;
    }

    for address in v4_addresses {
        counts.factories_v4 += tx
            .execute(
            "insert into mev.v4_factories(address) values ($1) on conflict do nothing",
            &[&address.as_slice()],
        )
        .await
        .wrap_err("failed to insert v4 factory address")? as usize;
    }

    for pool in v2_pools {
        counts.pools_v2 += tx
            .execute(
            "
            insert into mev.v2_pools(factory, pair_address, token0, token1)
            values ($1, $2, $3, $4)
            on conflict do nothing
            ",
            &[
                &pool.factory.as_slice(),
                &pool.pair_address.as_slice(),
                &pool.token0.as_slice(),
                &pool.token1.as_slice(),
            ],
        )
        .await
        .wrap_err("failed to insert v2 pool")? as usize;
    }

    for pool in v3_pools {
        counts.pools_v3 += tx
            .execute(
            "
            insert into mev.v3_pools(factory, pool_address, token0, token1, fee, tick_spacing)
            values ($1, $2, $3, $4, $5, $6)
            on conflict do nothing
            ",
            &[
                &pool.factory.as_slice(),
                &pool.pool_address.as_slice(),
                &pool.token0.as_slice(),
                &pool.token1.as_slice(),
                &(pool.fee as i32),
                &pool.tick_spacing,
            ],
        )
        .await
        .wrap_err("failed to insert v3 pool")? as usize;
    }

    for pool in v4_pools {
        counts.pools_v4 += tx
            .execute(
            "
            insert into mev.v4_pools(
                factory, currency0, currency1, fee, tick_spacing, hooks, sqrt_price_x96
            )
            values ($1, $2, $3, $4, $5, $6, ($7::text)::numeric)
            on conflict do nothing
            ",
            &[
                &pool.factory.as_slice(),
                &pool.key.currency0.as_slice(),
                &pool.key.currency1.as_slice(),
                &(pool.key.fee as i32),
                &pool.key.tick_spacing,
                &pool.key.hooks.as_slice(),
                &pool.sqrt_price_x96,
            ],
        )
        .await
        .wrap_err("failed to insert v4 pool")? as usize;
    }

    tx.execute(
        "
        insert into mev.backfill_checkpoint(job_name, last_scanned_block)
        values ($1, $2)
        on conflict (job_name) do update
        set last_scanned_block = excluded.last_scanned_block
        ",
        &[&job_name, &(last_scanned_block as i64)],
    )
    .await
    .wrap_err("failed to store checkpoint")?;

    tx.commit().await.wrap_err("failed to commit PostgreSQL transaction")?;
    Ok(counts)
}
