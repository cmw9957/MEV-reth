//! Logs latest-per-pool swap events from real committed chains.

use futures::TryStreamExt;
use reth_ethereum::{
    cli::interface::Cli,
    exex::{ExExContext, ExExEvent},
    node::EthereumNode,
};
use reth_exex_mev::collect_latest_pool_events_for_chain;
use tracing::info;

async fn mev_inspect_exex<Node>(mut ctx: ExExContext<Node>) -> eyre::Result<()>
where
    Node: reth_ethereum::node::api::FullNodeComponents,
{
    while let Some(notification) = ctx.notifications.try_next().await? {
        if let Some(committed) = notification.committed_chain() {
            for block in collect_latest_pool_events_for_chain(&committed) {
                info!(
                    block_number = block.block_number,
                    block_hash = %block.block_hash,
                    latest_pool_events = block.events.len(),
                    "Collected latest pool events from committed block"
                );
            }

            ctx.events.send(ExExEvent::FinishedHeight(committed.tip().num_hash()))?;
        }

        if let Some(reverted) = notification.reverted_chain() {
            info!(reverted_chain = ?reverted.range(), "Observed canonical rollback");
        }
    }

    Ok(())
}

fn main() -> eyre::Result<()> {
    Cli::parse_args()
        .run(async move |builder, _| {
            let handle = builder
                .node(EthereumNode::default())
                .install_exex("mev-inspect", async move |ctx| Ok(mev_inspect_exex(ctx)))
                .launch()
                .await?;

            handle.wait_for_node_exit().await
        })
        .unwrap();

    Ok(())
}
