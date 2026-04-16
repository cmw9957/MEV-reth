//! Backfills Uniswap V2/V3 factory addresses from an Ethereum JSON-RPC endpoint into PostgreSQL.

#![warn(unused_crate_dependencies)]

mod backfill;
mod db;
mod topics;
mod models {
    include!("models.rs");
}

use clap::Parser;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Debug, Parser)]
#[command(name = "reth-dex-indexer")]
#[command(about = "Backfills V2/V3/V4 factory addresses from a localhost JSON-RPC endpoint")]
struct Args {
    /// JSON-RPC endpoint.
    #[arg(long, env = "RPC_URL", default_value = "http://127.0.0.1:8545")]
    rpc_url: String,

    /// PostgreSQL connection string.
    #[arg(long, env = "DATABASE_URL")]
    postgres_url: String,

    /// Number of blocks to process per chunk.
    #[arg(long, default_value_t = 10_000)]
    chunk_size: u64,

    /// Override the starting block. If omitted, the indexer starts at the block closest to
    /// `lookback_years` before now.
    #[arg(long)]
    from_block: Option<u64>,

    /// Override the ending block. Defaults to the latest canonical block.
    #[arg(long)]
    to_block: Option<u64>,

    /// Number of years to look back when `from_block` is omitted.
    #[arg(long, default_value_t = 6)]
    lookback_years: u32,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let args = Args::parse();
    init_tracing()?;

    let rpc = backfill::RpcClient::new(args.rpc_url);
    let mut pg_client = db::connect(&args.postgres_url).await?;
    db::init_schema(&pg_client).await?;

    backfill::run(
        &rpc,
        &mut pg_client,
        args.chunk_size,
        args.lookback_years,
        args.from_block,
        args.to_block,
    )
    .await
}

fn init_tracing() -> eyre::Result<()> {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,reth_dex_indexer=debug"));

    fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init()
        .map_err(|err| eyre::eyre!("failed to init tracing: {err}"))
}
