//! MEV-focused helpers for Execution Extensions.

/// Receipt/log collection helpers for block-driven strategies.
pub mod collector;

pub use collector::{
    collect_latest_pool_events_by_block, collect_latest_pool_events_from_receipts,
    collect_pool_events_by_block, collect_pool_events_from_receipts, DexPoolEventKind,
    DexSwapProtocol, IndexedPoolEvent, PoolKey, UNISWAP_V2_SWAP_TOPIC, UNISWAP_V2_SYNC_TOPIC,
    UNISWAP_V3_SWAP_TOPIC, UNISWAP_V4_SWAP_TOPIC,
};
