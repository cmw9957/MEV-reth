//! MEV-focused helpers for Execution Extensions.

/// Receipt/log collection helpers for block-driven strategies.
pub mod collector;
/// Strategy entry points built on top of the collectors.
pub mod strategy;

pub use collector::{
    collect_latest_pool_events_for_chain, collect_latest_pool_events_from_receipts,
    collect_pool_events_from_receipts, BlockPoolEvents, DexPoolEventKind, DexSwapProtocol,
    IndexedPoolEvent, PoolKey, UNISWAP_V2_SWAP_TOPIC, UNISWAP_V2_SYNC_TOPIC,
    UNISWAP_V3_SWAP_TOPIC, UNISWAP_V4_SWAP_TOPIC,
};
pub use strategy::collect_block_pool_events_for_strategy;
