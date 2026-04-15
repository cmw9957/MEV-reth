use crate::{collect_latest_pool_events_for_chain, BlockPoolEvents};
use reth_execution_types::Chain;
use reth_primitives_traits::NodePrimitives;

/// Collects the latest pool events for each block in a committed chain.
///
/// This is a minimal strategy entry point intended to be called from an ExEx
/// after receiving `ChainCommitted`.
pub fn collect_block_pool_events_for_strategy<N>(committed: &Chain<N>) -> Vec<BlockPoolEvents>
where
    N: NodePrimitives,
    N::Receipt: reth_primitives_traits::Receipt + alloy_consensus::TxReceipt<Log = alloy_primitives::Log>,
{
    collect_latest_pool_events_for_chain(committed)
}
