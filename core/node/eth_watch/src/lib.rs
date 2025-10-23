//! Ethereum watcher polls the Ethereum node for the relevant events, such as priority operations (aka L1 transactions),
//! protocol upgrades etc.
//! New events are accepted to the ZKsync network once they have the sufficient amount of L1 confirmations.

use std::{sync::Arc, cmp::min, collections::HashMap};
use tokio::time::{Duration, MissedTickBehavior};
use anyhow::Context as _;
use tokio::sync::watch;
use zksync_dal::{Connection, ConnectionPool, Core, CoreDal, DalError};
use zksync_mini_merkle_tree::MiniMerkleTree;
use zksync_types::{
    protocol_version::ProtocolSemanticVersion, settlement::SettlementLayer,
    api::Log, web3::BlockNumber as Web3BlockNumber, L1BatchNumber, L2ChainId, PriorityOpId,H256
};

pub use self::client::{EthClient, EthHttpQueryClient, GetLogsClient, ZkSyncExtentionEthClient};
use self::{
    client::RETRY_LIMIT,
    event_processors::{
        BatchRootProcessor, DecentralizedUpgradesEventProcessor, EventProcessor,
        EventProcessorError, EventsSource, GatewayMigrationProcessor, InteropRootProcessor,
        PriorityOpsEventProcessor,
    },
    metrics::METRICS,
};

use zksync_utils::retry::retry_with_backoff_no_state;
use rand::Rng;

const MAX_LOG_RANGE_BLOCKS: u64 = 1_000;

mod client;
mod event_processors;
mod metrics;
pub mod node;
#[cfg(test)]
mod tests;

struct PState {
    idx: usize,
    cursor: u64,
    topic1: Option<H256>,
    topic2: Option<H256>,
}

#[derive(Debug)]
struct EthWatchState {
    last_seen_protocol_version: ProtocolSemanticVersion,
    next_expected_priority_id: PriorityOpId,
    chain_batch_root_number_lower_bound: L1BatchNumber,
    batch_merkle_tree: MiniMerkleTree<[u8; 96]>,
}

/// Ethereum watcher component.
#[derive(Debug)]
pub struct EthWatch {
    l1_client: Arc<dyn EthClient>,
    sl_client: Arc<dyn EthClient>,
    poll_interval: Duration,
    event_expiration_blocks: u64,
    event_processors: Vec<Box<dyn EventProcessor>>,
    pool: ConnectionPool<Core>,
}

impl EthWatch {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        l1_client: Box<dyn EthClient>,
        sl_client: Box<dyn ZkSyncExtentionEthClient>,
        sl_layer: Option<SettlementLayer>,
        pool: ConnectionPool<Core>,
        poll_interval: Duration,
        chain_id: L2ChainId,
        event_expiration_blocks: u64,
    ) -> anyhow::Result<Self> {
        let mut storage = pool.connection_tagged("eth_watch").await?;
        let l1_client: Arc<dyn EthClient> = l1_client.into();
        let sl_client: Arc<dyn ZkSyncExtentionEthClient> = sl_client.into();
        let sl_eth_client = sl_client.clone().into_base();

        let state = Self::initialize_state(&mut storage, sl_eth_client.as_ref()).await?;
        tracing::info!("initialized state: {state:?}");

        drop(storage);

        let priority_ops_processor =
            PriorityOpsEventProcessor::new(state.next_expected_priority_id, sl_eth_client.clone())?;
        let decentralized_upgrades_processor = DecentralizedUpgradesEventProcessor::new(
            state.last_seen_protocol_version,
            sl_eth_client.clone(),
            l1_client.clone(),
        );
        let gateway_migration_processor = GatewayMigrationProcessor::new(chain_id);

        let mut event_processors: Vec<Box<dyn EventProcessor>> = vec![
            Box::new(priority_ops_processor),
            Box::new(decentralized_upgrades_processor),
            Box::new(gateway_migration_processor),
        ];

        if let Some(SettlementLayer::Gateway(_)) = sl_layer {
            let batch_root_processor = BatchRootProcessor::new(
                state.chain_batch_root_number_lower_bound,
                state.batch_merkle_tree,
                chain_id,
                sl_client.clone(),
            );
            let sl_interop_root_processor =
                InteropRootProcessor::new(EventsSource::SL, chain_id, Some(sl_client)).await;
            event_processors.push(Box::new(batch_root_processor));
            event_processors.push(Box::new(sl_interop_root_processor));
        }

        Ok(Self {
            l1_client,
            sl_client: sl_eth_client,
            poll_interval,
            event_expiration_blocks,
            event_processors,
            pool,
        })
    }

    #[tracing::instrument(name = "EthWatch::initialize_state", skip_all)]
    async fn initialize_state(
        storage: &mut Connection<'_, Core>,
        sl_client: &dyn EthClient,
    ) -> anyhow::Result<EthWatchState> {
        let next_expected_priority_id: PriorityOpId = storage
            .transactions_dal()
            .last_priority_id()
            .await?
            .map_or(PriorityOpId(0), |e| e + 1);

        let last_seen_protocol_version = storage
            .protocol_versions_dal()
            .latest_semantic_version()
            .await?
            .context("expected at least one (genesis) version to be present in DB")?;

        let sl_chain_id = sl_client.chain_id().await?;
        let batch_hashes = storage
            .blocks_dal()
            .get_executed_batch_roots_on_sl(sl_chain_id)
            .await?;

        let chain_batch_root_number_lower_bound = batch_hashes
            .last()
            .map(|(n, _)| *n + 1)
            .unwrap_or(L1BatchNumber(0));
        let tree_leaves = batch_hashes.into_iter().map(|(batch_number, batch_root)| {
            BatchRootProcessor::batch_leaf_preimage(batch_root, batch_number)
        });
        let batch_merkle_tree = MiniMerkleTree::new(tree_leaves, None);

        Ok(EthWatchState {
            next_expected_priority_id,
            last_seen_protocol_version,
            chain_batch_root_number_lower_bound,
            batch_merkle_tree,
        })
    }

    pub async fn run(mut self, mut stop_receiver: watch::Receiver<bool>) -> anyhow::Result<()> {
        let mut timer = tokio::time::interval(self.poll_interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let pool = self.pool.clone();
       
        let mut attempt: u32 = 0;
        while !*stop_receiver.borrow_and_update() {
            tokio::select! {
                _ = timer.tick() => { /* continue iterations */ }
                _ = stop_receiver.changed() => break,
            }

            let mut storage = pool.connection_tagged("eth_watch").await?;
            match self.loop_iteration(&mut storage).await {
                Ok(()) => {
                    /* everything went fine */
                    METRICS.eth_poll.inc();
                    attempt = 0; // <-- reset backoff on success
                }
                Err(EventProcessorError::Fatal(err)) => {
                    tracing::error!("Fatal error processing new blocks: {err:?}");
                    return Err(err.into());
                }
                Err(EventProcessorError::Transient(err)) => {
                    tracing::error!("Failed to process new blocks: {err}");
                    let exp = 1u64 << attempt.min(6);
                    let delay_ms = exp * 200 + rand::thread_rng().gen_range(0..200);
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    attempt = attempt.saturating_add(1);
                }
            }
        }

        tracing::info!("Stop signal received, eth_watch is shutting down");
        Ok(())
    }

    #[tracing::instrument(name = "EthWatch::loop_iteration", skip_all)]
    async fn loop_iteration(
        &mut self,
        storage: &mut Connection<'_, Core>,
    ) -> Result<(), EventProcessorError> {
        const GET_EVENTS_INTERNAL_RETRY_LIMIT: usize = 0;
        const MAX_RPCS_PER_ITER_PER_SOURCE: usize = 9;
        const INTER_CHUNK_JITTER_MS_MIN: u64 = 60;
        const INTER_CHUNK_JITTER_MS_MAX: u64 = 150;

        for source in [EventsSource::L1, EventsSource::SL].iter() {
            // Gather indices of processors for this source
            let proc_indices: Vec<usize> = self
            .event_processors
            .iter()
            .enumerate()
            .filter_map(|(i, p)| (p.event_source() == *source).then_some(i))
            .collect();
            if proc_indices.is_empty() {
                continue;
            }

            // Resolve client for this source
            let client: &dyn EthClient = match source {
                &EventsSource::L1 => self.l1_client.as_ref(),
                &EventsSource::SL => self.sl_client.as_ref(),
            };

            let chain_id = retry_with_backoff_no_state(|| async { client.chain_id().await }, 5)
                .await
                .map_err(EventProcessorError::client)?;

            // Only fetch tips we actually need for this source
            let have_confirmed = proc_indices
                .iter()
                .any(|&i| !self.event_processors[i].only_finalized_block());
            let have_finalized = proc_indices
                .iter()
                .any(|&i| self.event_processors[i].only_finalized_block());

            let to_block_confirmed = if have_confirmed {
                Some(
                    retry_with_backoff_no_state(|| async { client.confirmed_block_number().await }, 5)
                        .await
                        .map_err(EventProcessorError::client)?,
                )
            } else {
                None
            };
            let to_block_finalized = if have_finalized {
                Some(
                    retry_with_backoff_no_state(|| async { client.finalized_block_number().await }, 5)
                        .await
                        .map_err(EventProcessorError::client)?,
                )
            } else {
                None
            };

            // Shared get_events budget across BOTH buckets for this source (confirmed + finalized)
            let mut rpc_budget_total = MAX_RPCS_PER_ITER_PER_SOURCE;

            for finalized_only in [false, true] {
                if rpc_budget_total == 0 { break; }
                let to_block = if finalized_only {
                    match to_block_finalized {
                        Some(v) => v,
                        None => continue,
                    }
                } else {
                    match to_block_confirmed {
                        Some(v) => v,
                        None => continue,
                    }
                };

                let mut states: Vec<PState> = Vec::new();
                for &i in &proc_indices {
                    // Only include processors that match this bucket
                    if self.event_processors[i].only_finalized_block() != finalized_only {
                        continue;
                    }
                    let cursor = storage
                        .eth_watcher_dal()
                        .get_or_set_next_block_to_process(
                            self.event_processors[i].event_type(),
                            chain_id,
                            to_block.saturating_sub(self.event_expiration_blocks),
                        )
                        .await
                        .map_err(DalError::generalize)
                        .map_err(EventProcessorError::internal)?;
                    if cursor > to_block {
                        continue;
                    }
                    states.push(PState {
                        idx: i,
                        cursor,
                        topic1: self.event_processors[i].topic1(),
                        topic2: self.event_processors[i].topic2(),
                    });
                }
                if states.is_empty() {
                    continue;
                }

                while rpc_budget_total > 0 {
                    // Earliest outstanding cursor among processors
                    let maybe_from = states
                        .iter()
                        .filter(|s| s.cursor <= to_block)
                        .map(|s| s.cursor)
                        .min();
                    let from = match maybe_from {
                        Some(f) => f,
                        None => break, // all caught up
                    };
                    let to = min(from.saturating_add(MAX_LOG_RANGE_BLOCKS - 1), to_block);

                    // Group processors by (topic1, topic2), cap by remaining budget
                    let mut groups_map: HashMap<(Option<H256>, Option<H256>), Vec<usize>> =
                        HashMap::new();
                    for (st_idx, s) in states.iter().enumerate() {
                        if s.cursor <= to {
                            groups_map
                                .entry((s.topic1, s.topic2))
                                .or_default()
                                .push(st_idx);
                        }
                    }
                    if groups_map.is_empty() {
                        break;
                    }
                    let mut groups: Vec<((Option<H256>, Option<H256>), Vec<usize>)> =
                        groups_map.into_iter().collect();
                    if groups.len() > rpc_budget_total {
                        groups.truncate(rpc_budget_total);
                    }

                    // Fetch once per pair, reuse results and sort logs for stable cursor math
                    let mut fetched_by_pair: HashMap<(Option<H256>, Option<H256>), Vec<Log>> =
                        HashMap::with_capacity(groups.len());

                    for &((t1, t2), _) in &groups {
                        let mut logs = retry_with_backoff_no_state(
                            || async {
                                client
                                    .get_events(
                                        Web3BlockNumber::Number(from.into()),
                                        Web3BlockNumber::Number(to.into()),
                                        t1,
                                        t2,
                                        GET_EVENTS_INTERNAL_RETRY_LIMIT,
                                    )
                                    .await
                            },
                            5,
                        )
                        .await
                        .map_err(EventProcessorError::client)?;

                        logs.sort_by(|a, b| {
                            let abn = a.block_number.unwrap_or_default();
                            let bbn = b.block_number.unwrap_or_default();
                            if abn != bbn {
                                abn.cmp(&bbn)
                            } else {
                                a.log_index
                                    .unwrap_or_default()
                                    .cmp(&b.log_index.unwrap_or_default())
                            }
                        });

                        fetched_by_pair.insert((t1, t2), logs);
                    }

                    // Distribute logs to each processor; borrow &mut just-in-time
                    for ((pair_t1, pair_t2), st_indices) in groups {
                        let pair_logs = fetched_by_pair
                            .get(&(pair_t1, pair_t2))
                            .expect("logs fetched");
                        for st_i in st_indices {
                            // Read fields with explicit type so the compiler knows this is PState
                            let (idx, cursor_before) = {
                                let s: &PState = &states[st_i];
                                (s.idx, s.cursor)
                            };

                            // Short-lived &mut borrow for processing
                            let processed = {
                                let proc = &mut *self.event_processors[idx];
                                proc.process_events(storage, pair_logs.clone()).await?
                            };

                            let next_block_to_process = if processed == pair_logs.len() {
                                to.saturating_add(1)
                            } else if processed == 0 {
                                from
                            } else {
                                pair_logs[processed - 1]
                                    .block_number
                                    .expect("Event block number is missing")
                                    .try_into()
                                    .unwrap()
                            };

                            storage
                                .eth_watcher_dal()
                                .update_next_block_to_process(
                                    self.event_processors[idx].event_type(),
                                    chain_id,
                                    next_block_to_process,
                                )
                                .await
                                .map_err(DalError::generalize)
                                .map_err(EventProcessorError::internal)?;

                            // Update local state; avoid no-progress
                            let s: &mut PState = &mut states[st_i];
                            if next_block_to_process <= cursor_before {
                                let delay_ms: u64 =
                                    rand::thread_rng().gen_range(150..=350);
                                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                                s.cursor = cursor_before.saturating_add(1);
                            } else {
                                s.cursor = next_block_to_process;
                            }
                        }
                    }

                    // Decrement shared per-source budget by #pairs we fetched this chunk
                    let fetched_count = fetched_by_pair.len();
                    rpc_budget_total = rpc_budget_total.saturating_sub(fetched_count);
                    
                    let inter_ms: u64 = rand::thread_rng().gen_range(INTER_CHUNK_JITTER_MS_MIN..=INTER_CHUNK_JITTER_MS_MAX);
                    tokio::time::sleep(Duration::from_millis(inter_ms)).await;
                }

                // Optional: small tail jitter if still behind
                if states.iter().any(|s| s.cursor <= to_block) {
                    let tail_ms: u64 = rand::thread_rng().gen_range(200..=600);
                    tokio::time::sleep(Duration::from_millis(tail_ms)).await;
                }
            }
        }

        Ok(())
    }
}
