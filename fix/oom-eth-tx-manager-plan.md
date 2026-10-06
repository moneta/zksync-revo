# Fix Plan: `eth_tx_manager` OOM on prolonged L1↔L2 sync gap

Status: **FIX IMPLEMENTED LOCALLY — compile/config checks green; DB tests and canary rollout pending**
Owner: TBD
Files affected:
- `core/lib/config/src/configs/eth_sender.rs`
- `core/lib/db_connection/src/instrument.rs`
- `core/lib/dal/src/eth_sender_dal.rs`
- `core/lib/dal/src/models/storage_eth_tx.rs`
- `core/lib/dal/migrations/20260922150000_eth_tx_history_pagination.{up,down}.sql`
- `core/node/eth_sender/src/eth_tx_manager.rs`
- `core/node/eth_sender/src/metrics.rs`
- `core/node/eth_sender/src/tester.rs`
- `core/node/eth_sender/src/tests.rs`

---

## 0. Background: the eth_tx lifecycle (why `eth_tx_manager` pulls these rows at all)

Two components share the `eth_txs` / `eth_txs_history` tables; understanding
the hand-off between them is what explains why the backlog accumulates on
the `eth_tx_manager` side specifically.

**Before `eth_tx_manager` touches anything — `eth_tx_aggregator` / `aggregator.rs`:**
- "Watches sealed L1 batches" = polls Postgres for rows in `l1_batches` that
  the **state keeper / sequencer** (a separate, upstream component, not part
  of this investigation) has already sealed. An L1 batch is a container of
  many **L2 transactions**; it gets sealed (closed off, given a batch number)
  by the state keeper once *any* of several independent limits is hit first —
  see `core/node/state_keeper/src/seal_criteria/`:
  - `SlotsCriterion`: max L2 tx count per batch (`transaction_slots`, capped
    by the bootloader's `MAX_TXS_IN_BLOCK`).
  - `PubDataBytesCriterion`: max pubdata bytes (compressed state diffs +
    L2→L1 messages) that will have to go into the `Commit` tx's L1
    calldata/blob — `config.max_pubdata_per_batch`.
  - `CircuitsCriterion` (geometry): max proof-circuit capacity used, so the
    batch remains provable.
  - `TxEncodingSizeCriterion`, `L2L1LogsCriterion`, `GasForBatchTipCriterion`,
    `InteropRootsCriterion`: similar hard caps on encoded size / L2→L1 log
    count / gas / interop roots.
  - `TimeoutSealer`: unconditionally seals a batch after
    `l1_batch_commit_deadline` even if no other limit was hit, so a
    low-traffic chain still produces batches regularly.
  - Whichever limit is hit first seals the batch; the L2 txs that were
    *already included* stay in that batch, the next L2 tx starts a new one.
  - Once sealed, the batch itself still isn't an L1 tx yet — its L2 txs get
    proven, and only *then* does `eth_tx_aggregator`'s own, separate limit
    (`max_aggregated_blocks_to_commit`/`_execute`, from §2) decide how many
    *already-sealed L1 batches* get bundled together into a single `Commit`/
    `Execute` **L1** transaction.
- Once a batch satisfies its publish criteria (size/gas/timestamp limits),
  builds the L1 calldata for `Commit` / `PublishProofOnchain` / `Execute` (or
  `Precommit` for L2 blocks).
- Calls `save_eth_tx(...)`, which **inserts one new row into `eth_txs`**
  (`raw_tx`, `nonce`, `tx_type`, optional `blob_sidecar`) and links it back to
  the batch via `l1_batches.eth_commit_tx_id` / `eth_prove_tx_id` /
  `eth_execute_tx_id` (or `miniblocks.eth_precommit_tx_id` for precommits).
- At this point the row is **unsent**: no `eth_txs_history` row exists yet,
  and `eth_txs.confirmed_eth_tx_history_id IS NULL`.

**`eth_tx_manager` (this investigation's file) then owns getting each
`eth_txs` row from "created" to "finalized on L1":**
1. `send_new_eth_txs`: picks up `eth_txs` rows that have never been
   broadcast, signs them, sends via the L1 client, and **inserts a row into
   `eth_txs_history`** per send attempt (`tx_hash`, fee params, `sent_at_block`,
   `finality_status = 'pending'`). If accepted by the L1 node, that history
   row's `sent_successfully` is set to `TRUE`.
2. `update_statuses_and_resend_if_needed` /
   `monitor_inflight_transactions_single_operator`: every poll, for each
   still-unconfirmed `eth_tx` (`confirmed_eth_tx_history_id IS NULL`), it
   compares the on-chain operator nonce to the tx's nonce:
   - **Not yet mined** (operator nonce still `<=` tx nonce): may **resend**
     with a bumped fee, which **adds another row to `eth_txs_history`** for
     the *same* `eth_tx_id`. This is exactly where the per-tx history count
     (`M` in §2) grows — the longer a tx is stuck (e.g. fees spiking or the
     node catching up after downtime), the more resend rows accumulate.
   - **Nonce surpassed** (operator nonce `>` tx nonce, meaning *something*
     with that nonce landed on L1): walks `get_tx_history_to_check` (all the
     resend attempts, newest first) and calls the L1 client's
     `get_tx_status` per attempt's `tx_hash` to find out **which specific
     attempt** actually got mined (only one of the N fee-bumped attempts for
     a given nonce can land).
3. Once the winning attempt is found, `apply_tx_status` does one of:
   - **`confirm_tx`**: sets `eth_txs.confirmed_eth_tx_history_id` (pointer to
     the winning `eth_txs_history` row) + `gas_used`, and updates that
     history row's `finality_status` to `fast_finalized`/`finalized` and
     `confirmed_at`. This is the terminal "success" state.
   - **`fail_tx`**: sets `eth_txs.has_failed = TRUE` and **panics the
     process** — a reverted L1 tx here is treated as an unrecoverable/fatal
     condition (this is by design, not a bug being investigated here).

**After `eth_tx_manager` confirms a tx:** downstream consumers read
`eth_txs.confirmed_eth_tx_history_id` / `l1_batches.eth_*_tx_id` to mark the
corresponding batch as committed/proven/executed (API responses, block
explorer, `BlocksEthSenderStats`, and — for external nodes —
`transaction_finality_updater`, which mirrors finality status using
`get_unfinalized_transactions`, already `LIMIT`-bounded by a
`processing_batch_size` param). The `eth_tx_aggregator` also only advances to
the next stage (e.g. won't build the `Execute` tx) once the prior stage's tx
is confirmed, so a stuck `Commit` tx blocks the whole pipeline behind it.

**Why this specific backlog forms:** if L1 connectivity/finality has been
stalled for a while (e.g. against Sepolia, RPC hiccups, fee spikes, or the
pod being down), both axes grow at once:
- `N` (rows returned by `get_non_final_txs`/`get_inflight_txs`) grows because
  more `eth_txs` pile up with `confirmed_eth_tx_history_id IS NULL`.
- `M` (rows returned by `get_tx_history_to_check` per tx) grows because each
  stuck tx accumulates more fee-bump resend attempts over time.

Every poll iteration then re-reads **all** of `N` and, for surpassed-nonce
txs, **all** of `M`'s history in one shot — unlike `eth_tx_aggregator`'s
creation side, which already pages via `limit: config.max_aggregated_blocks_to_commit/execute`.

## 1. Problem statement

The `eth_tx_manager` component OOM-crashes on k8s. It is more likely to happen
after the node has been unable to keep its L1 (Sepolia) txs confirmed for a
while — i.e. a backlog builds up between "txs we know about" and "txs L1 has
finalized".

## 2. Root cause (from investigation)

Three DAL queries load **unbounded** result sets instead of paging, and one
of them additionally **duplicates a large column per row**:

| Query | File | Issue |
|---|---|---|
| `get_non_final_txs` | `eth_sender_dal.rs` (~L28) | `SELECT eth_txs.* ... ORDER BY eth_txs.id` — no `LIMIT`. Returns every not-yet-finalized `eth_tx` in one shot. |
| `get_inflight_txs` | `eth_sender_dal.rs` (~L104) | Same pattern — no `LIMIT`, returns every unconfirmed tx up to the last-seen-in-flight id. |
| `get_tx_history_to_check` | `eth_sender_dal.rs` (~L992) | `SELECT eth_txs_history.*, eth_txs.blob_sidecar ... WHERE eth_tx_id = $1 ORDER BY created_at DESC` — no `LIMIT`. Returns **every resend attempt** for a single tx, and the `LEFT JOIN eth_txs` re-attaches the tx's full `blob_sidecar` (and it's `raw_tx`-sized sibling `signed_raw_tx` per row) to **every single history row**. |

Call chain that amplifies this (`eth_tx_manager.rs`):

```
monitor_inflight_transactions_single_operator()
  -> get_non_final_txs() / get_inflight_txs()      // O(N) unconfirmed txs, unbounded
  -> apply_inflight_txs_statuses_and_get_first_to_resend()
       for tx in inflight_txs (all N in memory):
         -> check_all_sending_attempts(tx)
              -> get_tx_history_to_check(tx.id)     // O(M) resend attempts, unbounded,
                                                     // each row carrying a full blob_sidecar copy
```

Peak memory is roughly the unconfirmed-tx vector plus one history vector,
`O(N × eth_tx_row_size + M × history_row_size)`, where:
- `N` = number of eth_txs that are not yet finalized (grows if L1 confirmations
  stall or the operator has been down for a while).
- `M` = number of resend/fee-bump attempts per tx (grows with congestion / gas
  volatility on Sepolia, or while the node is catching up).
- `row_size` = `signed_raw_tx` (can be sizeable for `Commit` txs with pubdata)
  **plus a duplicated `blob_sidecar`** on every row.

The vectors are not multiplied in memory simultaneously, but the entire `N`
vector remains alive while each tx is processed, and the current tx's entire
`M` history is then materialized alongside it. This matches the reported
trigger: after a long L1↔L2 sync gap, both are elevated and loaded "in one
shot" rather than page-by-page.

### 2.1 Production evidence (2026-09-22)

The metrics-only PR added preflight `COUNT(*)` gauges before each heavy
`fetch_all()`, plus start/finish tracing and post-fetch row/payload metrics.
On the Sepolia k8s deployment it recorded:

```text
tx_scan_expected_rows{query="non_final",operator="blob"} 0
tx_scan_expected_rows{query="inflight",operator="blob"} 190
tx_scan_rows_sum{query="inflight",operator="blob"} 190
tx_scan_payload_bytes_sum{query="inflight",operator="blob"} 26476120
tx_scan_expected_rows{query="history",operator="blob"} 839848
```

After the history preflight, no `history` post-fetch row or payload metric was
emitted, so `get_tx_history_to_check()` never returned. During that fetch,
container memory rose monotonically from **0.07 GiB to 3.99 GiB in about 33
seconds**. Kubernetes then reported:

```text
eth-tx-manager reason=OOMKilled exit=137
```

This is direct evidence that the immediate OOM occurs while the Blob history
query materializes **839,848 full `StorageTxHistory` rows**. Each row includes
`signed_raw_tx`, and the join duplicates the parent tx's `blob_sidecar` into
every row even though `TxHistory` discards it during conversion. The 190-row
inflight scan (about 25.25 MiB measured payload) is unbounded and should still
be capped, but it is not the immediate 4-GiB failure observed here.

## 3. Goals / non-goals

**Goals**
- Bound memory usage of `eth_tx_manager`'s status-check path regardless of
  backlog size.
- Preserve correctness: no tx status/finalization logic should change,
  only how much data is loaded per iteration/query.
- Make the fix low-risk / easy to review and roll back.

**Non-goals**
- Not touching `eth_watch` (L1 event ingestion) in this pass — it already has
  chunked `get_events` + budget-per-iteration logic. Can revisit separately if
  needed.
- Not changing transaction lifecycle semantics or deleting historical resend
  attempts. One supporting index may be added for efficient keyset pagination;
  see §4.1.

## 4. Implemented changes

### 4.0 Implementation status

| Area | Status | Implementation |
|---|---|---|
| Configured bound | Implemented | `SenderConfig::status_scan_batch_size: NonZeroU64`, default `100`; ENV/YAML parsing fixtures updated. |
| History OOM path | Implemented | New slim `(id, tx_hash)` DAL page query; no `signed_raw_tx`, fee fields, or joined blob data. |
| History progress | Implemented | One page per poll, one active cursor per operator, active tx resumed directly by ID before outer scans. |
| Pacing / ordering | Implemented | `MoreHistory` bypasses block backoff and stops later nonce processing; found/exhausted/reorg paths clear cursor state. |
| Outer row scans | Implemented | `non_final` and `inflight` use bounded oldest-first keyset pages with independent per-operator cursors. |
| Reorg handling | Implemented | `unfinalize_txs` clears history, non-final, and inflight cursors before rescanning lower IDs. |
| Blob deduplication | Implemented | Removed unused `blob_sidecar` projection / storage field from all full `StorageTxHistory` mappings. |
| Pagination index | Implemented | SQLx no-transaction migration creates/drops `(eth_tx_id, id DESC)` concurrently. |
| Diagnostics | Implemented | Counts run only when a scan starts; row/payload metrics and start/finish tracing remain. |
| Unit/config validation | Complete | All-target eth-sender compilation passes; three sender config tests pass. |
| DB-backed tests | Added, not executed locally | Tests compile, but local execution requires `TEST_DATABASE_URL` / the repository test harness. |
| Production validation | Pending | Apply migration, deploy canary to Sepolia, verify bounded rows/RSS and complete backlog progress. |

### 4.1 Replace the history fetch with a slim, cursor-paginated query (first fix)

`get_tx_history_to_check` (and the sibling `get_eth_tx_history_by_id`,
`get_last_sent_successfully_eth_tx`, `get_unfinalized_transactions`) all
join `eth_txs.blob_sidecar` onto every `eth_txs_history` row even though the
blob sidecar belongs to the `eth_tx`, not to each attempt.

**Note on `get_unfinalized_transactions` specifically**: unlike the other
three queries, this one *already* takes a `limit: NonZeroU64` and applies
`LIMIT $1` in SQL (its only caller, `transaction_finality_updater`, passes a
configured `processing_batch_size`). So it does **not** need the row-count
cap from §4.2 — it only needs the same blob-dedup fix as the others, since it
still joins the full `eth_txs.blob_sidecar` onto each of the (already capped)
rows for no reason (see §6.1).

`check_all_sending_attempts` only needs a history row's ID and `tx_hash`; it
does not need fee fields, `signed_raw_tx`, `blob_sidecar`, `tx_type`, or
`chain_id`.

Implementation:
- Replace this call site with a dedicated query returning only `(id, tx_hash)`.
- Page newest-first using a stable ID cursor:
  `WHERE eth_tx_id = $1 AND id < $cursor ORDER BY id DESC LIMIT $page_size`.
  The first page uses no cursor / `i32::MAX`; each next page uses the last ID
  from the previous page.
- Check L1 status for each hash in the page and stop immediately when an
  executed attempt is found.
- Process **at most one page for a given tx per manager iteration**. Store one
  active history cursor per operator in `EthTxManager`, e.g.
  `HashMap<OperatorType, HistoryScanCursor { eth_tx_id, before_id }>`. Since
  nonce ordering allows only the oldest eligible tx to block an operator,
  there is no need to retain an unbounded map of cursors. If the eligible tx
  ID changes, reset that operator's cursor to the newest page. While a cursor
  is active, the manager loads that exact `eth_tx` by ID before any outer page
  scan; an earlier outer-page tx therefore cannot overwrite or starve it.
- Return an explicit internal outcome from the history check:
  - `Found(status)`: remove the cursor and apply the receipt.
  - `MoreHistory`: retain the cursor and stop processing later nonces in this
    operator iteration; this is not an error or a reorg.
  - `Exhausted`: remove the cursor and only then emit the existing "possible
    block reorg / no receipt found" error log.
- Propagate `MoreHistory` through
  `monitor_inflight_transactions_single_operator` into
  `update_statuses_and_resend_if_needed` as a distinct progress outcome. It
  must not be treated as the current `Ok(None)` "no action" result, because
  that path exponentially backs off by L1 block number. On `MoreHistory`, keep
  `next_scan_at_block` at the current block and reset status backoff so the
  next poll can process another page even if no new L1 block arrives.
- Clear cursor state when a tx is found/exhausted, failed, unfinalized due to
  reorg handling, or replaced as the active eligible tx for that operator.
  Cursor state is an optimization only; a process restart safely begins again
  at the newest page.
- Use `config.status_scan_batch_size` (default 100) as both the history page
  size and the outer-query row limit. A future tuning split can be added if
  Sepolia RPC throughput and DB row-size limits require different values.
- Do **not** use a plain `ORDER BY ... DESC LIMIT 10` on every poll. That would
  repeatedly check the same newest attempts and could permanently miss an
  older attempt that was actually mined. Cursor pagination preserves complete
  coverage while bounding memory.
- Add a composite index supporting the access pattern:
  `eth_txs_history (eth_tx_id, id DESC)`. The existing index on only
  `eth_tx_id` does not guarantee efficient `ORDER BY id DESC LIMIT ...` pages
  and may repeatedly sort a very large per-tx history. For production, create
  the index concurrently before deploying the code (or use the repository's
  approved online-migration mechanism), then record it in migrations. Do not
  perform a blocking index build on the live write path without operational
  review. The migration uses `-- no-transaction` plus
  `CREATE/DROP INDEX CONCURRENTLY`.
- Keep existing full `TxHistory` queries for call sites that require fee data;
  remove the unused `blob_sidecar` projection from those shared queries only
  after their SQLx mapping is split from `StorageTxHistory` or otherwise made
  explicit.

### 4.2 Add `LIMIT`/pagination to the three unbounded queries

All batch sizes are driven by a **single new `SenderConfig` field**, added
the same way as the existing `max_aggregated_blocks_to_commit` /
`max_aggregated_blocks_to_execute` fields in
`core/lib/config/src/configs/eth_sender.rs`:

```rust
/// Max number of unconfirmed/history rows scanned per status-check DB call.
#[config(default_t = NonZeroU64::new(100).unwrap())]
pub status_scan_batch_size: NonZeroU64,
```

- This uses the same `DescribeConfig`/`DeserializeConfig` derive as every
  other `SenderConfig` field, so the env var is auto-derived exactly like
  `ETH_SENDER_SENDER_MAX_TXS_IN_FLIGHT` today (i.e.
  `ETH_SENDER_SENDER_STATUS_SCAN_BATCH_SIZE`) — no manual env var plumbing.
- **Default: `100`** via the `NonZeroU64` config default shown above.
- Zero is rejected during config parsing via `NonZeroU64`; it cannot be
  misinterpreted as an exhausted history scan.
- Call sites pass it straight through the same way `aggregator.rs` already
  does for its own limits, e.g.:
  ```rust
  limit: config.status_scan_batch_size.get(),
  ```
- Used to cap all three queries below (one knob, not three), so the memory
  ceiling is a single, easy-to-reason-about number. If we later find the
  three call sites need different tuning we can split them, but starting with
  one shared knob keeps the change small and reviewable.

- `get_tx_history_hashes_to_check(eth_tx_id, before_id, limit)`
  - Apply `LIMIT` to each cursor page, not to the overall search.
  - Preserve newest-first ordering and eventual inspection of every attempt.
  - Return rows ordered by `id DESC`; derive the next cursor from the final
    row's ID. An empty page means `Exhausted`.
- `get_non_final_txs(operator_address, is_gateway, after_id, limit)` /
  `get_inflight_txs(operator_address, is_gateway, after_id, limit)`
  - Add `LIMIT $n ... ORDER BY eth_txs.id` plus an `id > after_id` keyset
    cursor (oldest first, since we must resend/confirm in nonce order anyway),
    `limit` = `status_scan_batch_size`.
  - `eth_tx_manager.rs` already only actually resends the *first* tx whose
    nonce is `>= operator_nonce.latest` and only walks forward from there, so
    it never needed the *entire* tail of the queue at once — capping to
    `status_scan_batch_size` per call is enough per iteration.
  - Retain one non-final cursor and one inflight cursor per operator. A full
    page with no action advances its cursor and returns `MoreHistory`; a short
    page resets it. This prevents a fixed oldest page from starving later txs.
  - Reorg/unfinalize handling clears both outer cursors before the newly
    unfinalized lower IDs are scanned again.

Note: `status_scan_batch_size` only caps how much is read/scanned per DB call
(memory bound). It is intentionally kept separate from `max_txs_in_flight`
(which caps how many *new* txs get sent), since 10 is meant to be a small,
safe default for the read-path regardless of what `max_txs_in_flight` is
configured to on a given deployment.

### 4.3 Deduplicate shared full-history queries

Remove the unused `blob_sidecar` projection from
`get_unfinalized_transactions`, `get_eth_tx_history_by_id`, and
`get_last_sent_successfully_eth_tx`, and the legacy
`get_tx_history_to_check` test/helper query without changing domain behavior.
`get_unfinalized_transactions` already has a caller-provided SQL `LIMIT`, so
only payload deduplication is required there.

### 4.4 Guardrail metrics/logging

Already implemented in the evidence PR:
- `tx_scan_expected_rows`: exact-predicate preflight count emitted before
  materialization, so a query that OOMs still exposes its expected size.
- `tx_scan_rows`: rows returned after a successful materialization.
- `tx_scan_payload_bytes`: approximate transaction payload bytes returned.
- Start/finish tracing labeled by query and operator type.

The fix retains post-fetch row/payload metrics and start/finish tracing.
Run the history `COUNT(*)` only when initializing a new operator cursor (not
for every page), so it can report backlog size without rescanning 839,848 index
entries per poll. Outer-query preflight counts likewise run only when starting
a new keyset scan, not on continuation pages. They can be removed after canary
validation if their production diagnostic value no longer justifies the query.

## 5. Concrete step-by-step execution order

1. **Metrics/evidence PR — complete and deployed.** Production evidence in
  §2.1 confirms the history materialization OOM.
2. **History OOM fix — implemented.** Added nonzero default-100 config,
  concurrent composite index migration, slim ID/hash pages, direct active-tx
  resume, and one-page-per-poll history cursor state.
3. **Outer scan caps — implemented.** Added bounded, resumable oldest-first
  keyset cursors to `get_non_final_txs` and `get_inflight_txs`.
4. **Full-history payload deduplication — implemented.** Removed unused blob
  projection from all `StorageTxHistory` mappings, including
  `get_unfinalized_transactions`.
5. **Call-site / pacing / reorg behavior — implemented.** Added explicit
  `Resend`, `MoreHistory`, and `NoAction` outcomes; `MoreHistory` advances on
  the next poll at the same L1 block; reorg resets all cursor state.
6. **Tests and static validation — partially complete.** Added tests intended
  to assert:
   - each history page contains at most `limit` rows,
   - cursors advance without duplicates or gaps until history is exhausted,
   - resend/confirmation logic finds the right result in both the first page
     and a later page,
   - `MoreHistory` does not emit the reorg error and prevents later nonces from
     being processed out of order,
   - `MoreHistory` bypasses block-based status backoff and advances on the next
     poll even when the observed L1 block number has not changed,
   - cursor state is cleared on `Found`, `Exhausted`, and reorg/unfinalize
     paths, and restarting without cursor state remains correct,
   - outer inflight/non-final queries return at most `limit` oldest rows,
   - no behavior change for the common case (few attempts).
  Current validation evidence:
  - `cargo check -p zksync_eth_sender --all-targets`: passes.
  - `cargo test -p zksync_config configs::eth_sender::tests`: 3/3 pass.
  - Editor diagnostics and `git diff --check`: clean.
  - Final blocker-focused code review: pass, no blocking findings.
  - DB-backed eth-sender tests: compile, but local execution is blocked until
    `TEST_DATABASE_URL` is provided (normally via `zk test rust` or equivalent).
7. **Database validation — pending**: run the added pagination / later-page
  confirmation tests with the repository DB test harness; verify migration
  up/down and confirm `EXPLAIN` uses `eth_txs_history_eth_tx_id_id_idx`.
8. **Canary rollout — pending**: deploy to one Sepolia node, verify history
  pages stay at or below 100, RSS stays well below 4 GiB, cursors progress
  through the 839,848-row backlog, and the mined attempt is eventually found.
9. **Full rollout — pending**: after one complete backlog-recovery cycle,
  deploy broadly and consider removing preflight count diagnostics.
10. **Follow-up (separate ticket, not in this plan)**: apply the same
   "no unbounded `fetch_all` on multiplying joins" audit to `eth_watch` and
  `eth_proof_manager` if similar patterns exist there. Also investigate why a
  single tx accumulated 839,848 attempts and whether resend-rate controls or
  history retention/archival are needed; pagination prevents OOM but does not
  address abnormal table growth.

**Decision**: the metrics PR was deployed and confirmed the root cause in
§2.1. `get_unfinalized_transactions` remains in scope for payload deduplication
only; its row count is already bounded by `processing_batch_size`.

## 6. Risks / mitigations

| Risk | Mitigation |
|---|---|
| Capping `get_non_final_txs`/`get_inflight_txs` repeatedly returns the same oldest rows and starves later txs. | Use per-operator `id > after_id` keyset cursors; advance after full no-action pages and clear on short pages, actions, or reorg/unfinalize. |
| Limiting history to only the newest page misses an older mined attempt. | Never use a latest-only cap. Walk stable ID-cursor pages newest-first until a receipt is found or history is exhausted. |
| Paging all 839,848 hashes in one iteration avoids OOM but blocks the manager and floods L1 RPC. | Process one bounded page per iteration, retain a per-tx cursor, and stop later nonce processing until the current tx resolves. |
| Keyset pages repeatedly scan/sort a huge history. | Add `(eth_tx_id, id DESC)` and verify the production query plan uses it before rollout. Build it online / concurrently according to operations policy. |
| In-memory cursor is lost on pod restart. | Safe by design: restart from the newest page. This may repeat work but cannot skip attempts or corrupt DB status. |
| Existing block-based status pacing stalls a multi-page scan. | Treat `MoreHistory` as progress, reset backoff, and allow the next poll at the same L1 block. |
| Diagnostic preflight counts become permanent duplicate work. | Count history once when initializing a cursor; remove outer counts after validating limits, while retaining post-fetch metrics. |
| Removing `blob_sidecar` from the check-path query breaks some other consumer of `TxHistory` that expects it populated. | **Verified not an issue** — see §6.1 below: `TxHistory` (the domain type returned from these DAL calls) has no `blob_sidecar` field at all; it's already discarded during `StorageTxHistory -> TxHistory` conversion for every existing caller. |
| Removing `signed_raw_tx` breaks a consumer that reads `TxHistory.signed_raw_tx`. | **Verified not an issue for the new lean query** — grep found no current reader. The implementation adds a separate slim `(id, tx_hash)` query for `check_all_sending_attempts` and preserves `signed_raw_tx` in the shared full-history model for existing callers. |
| Changed SQL cannot use stale SQLx offline macro metadata. | Modified projections / dynamic cursor queries use typed runtime `query_as`; `StorageEthTx` and `StorageTxHistory` derive `FromRow`. No offline-cache regeneration is required for these queries. |

### 6.1 What could break if we drop `blob_sidecar` / `signed_raw_tx`? (investigated)

- **`blob_sidecar`**: The domain struct `TxHistory` (`core/lib/types/src/eth_sender.rs`)
  does **not have a `blob_sidecar` field at all**. Before this fix, the
  `eth_txs.blob_sidecar` column was read into `StorageTxHistory.blob_sidecar`
  and then silently dropped by the `StorageTxHistory -> TxHistory`
  conversion. The implementation removes that projection and storage-model
  field from all full-history mappings.
  (The real `blob_sidecar` used for signing/resending lives on `EthTx`, not
  `TxHistory`, and is fetched separately via `get_eth_tx`/`get_inflight_txs`/etc.
  — that path is untouched by this plan.)
- **`signed_raw_tx`**: This field still exists on `TxHistory` and the
  conversion panics (`.expect("Should rely only on the new txs")`) if the
  column is `NULL`. However, a repo-wide search found **no code that reads
  `TxHistory.signed_raw_tx`** after it's constructed — not in
  `eth_tx_manager.rs`, `eth_fees_oracle.rs`, `eth_tx_aggregator.rs`, tests, or
  anywhere else. It's loaded and then never used by any current caller.
  - The implementation leaves shared `TxHistory` behavior intact and adds a
    separate slim `(id, tx_hash)` query used only by
    `check_all_sending_attempts`. The hot status path therefore does not fetch
    `signed_raw_tx`, while fee-related callers continue receiving it.

## 7. Open questions for reviewer

1. ~~What default should the batch size have?~~ **Resolved**: single
  `status_scan_batch_size` config field, env var
  `ETH_SENDER_SENDER_STATUS_SCAN_BATCH_SIZE`, default `100`.
2. ~~What could break by dropping `blob_sidecar`/`signed_raw_tx`?~~
   **Resolved** — see §6.1: nothing, `blob_sidecar` is already discarded by
   the existing conversion for all callers, and `signed_raw_tx` on
   `TxHistory` has no readers anywhere in the codebase today.
3. ~~Do we want metrics landed as a separate PR first?~~ **Resolved: yes** —
   see the "Decision" note at the end of §5.
4. ~~Should this also cover `get_unfinalized_transactions`?~~ **Resolved:
   yes**, for the blob-dedup fix only (it's already row-count-limited); see
   the note in §4.1 and the "Decision" note at the end of §5.

No design questions remain. Local implementation is complete; the remaining
gates are DB-backed tests, migration/query-plan validation, and canary rollout.

---
*Diagnostics are deployed in PR #1. The query-shape fix is implemented locally
on `fix/eth-tx-manager-oom-history-pagination` and is not yet committed or deployed.*
