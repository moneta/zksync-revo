# Fix Plan: `eth_tx_manager` OOM on prolonged L1↔L2 sync gap

Status: **DRAFT — for review, no code changed yet**
Owner: TBD
Files affected:
- `core/lib/dal/src/eth_sender_dal.rs`
- `core/node/eth_sender/src/eth_tx_manager.rs`

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

Memory cost is roughly `O(N × M × row_size)`, where:
- `N` = number of eth_txs that are not yet finalized (grows if L1 confirmations
  stall or the operator has been down for a while).
- `M` = number of resend/fee-bump attempts per tx (grows with congestion / gas
  volatility on Sepolia, or while the node is catching up).
- `row_size` = `signed_raw_tx` (can be sizeable for `Commit` txs with pubdata)
  **plus a duplicated `blob_sidecar`** on every row.

This matches the reported trigger: after a long L1↔L2 sync gap, both `N` and
`M` are elevated at once, and the queries load everything "in one shot"
instead of batch-by-batch (unlike `aggregator.rs`, which already pages L1
batches via `limit: config.max_aggregated_blocks_to_commit/execute` +
SQL `LIMIT`).

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
- Not changing the DB schema.

## 4. Proposed changes

### 4.1 Stop duplicating `blob_sidecar` per history row (highest ROI, lowest risk)

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

Plan:
- Drop `eth_txs.blob_sidecar` (and `tx_type`, `chain_id` if unused per-row)
  from the per-history-row queries.
- Fetch `blob_sidecar` once per `eth_tx` (single row lookup, already have
  `get_eth_tx`) and attach it in Rust code only where actually needed (i.e.
  when building the tx to resend), instead of on every history row used just
  to check status/receipts.
- `check_all_sending_attempts` only needs `tx_hash` per history item to call
  `get_tx_status` — it doesn't need `signed_raw_tx` or `blob_sidecar` at all
  for that loop. Consider a slimmer query/struct for this specific call site
  (e.g. `get_tx_hashes_to_check` returning just `(tx_hash)` ordered
  newest-first) instead of reusing the full `TxHistory` struct.

### 4.2 Add `LIMIT`/pagination to the three unbounded queries

All batch sizes are driven by a **single new `SenderConfig` field**, added
the same way as the existing `max_aggregated_blocks_to_commit` /
`max_aggregated_blocks_to_execute` fields in
`core/lib/config/src/configs/eth_sender.rs`:

```rust
/// Max number of unconfirmed/history rows scanned per status-check DB call.
#[config(default_t = 10)]
pub status_scan_batch_size: u64,
```

- This uses the same `DescribeConfig`/`DeserializeConfig` derive as every
  other `SenderConfig` field, so the env var is auto-derived exactly like
  `ETH_SENDER_SENDER_MAX_TXS_IN_FLIGHT` today (i.e.
  `ETH_SENDER_SENDER_STATUS_SCAN_BATCH_SIZE`) — no manual env var plumbing.
- **Default: `10`** via `#[config(default_t = 10)]`.
- Call sites pass it straight through the same way `aggregator.rs` already
  does for its own limits, e.g.:
  ```rust
  limit: config.status_scan_batch_size,
  ```
- Used to cap all three queries below (one knob, not three), so the memory
  ceiling is a single, easy-to-reason-about number. If we later find the
  three call sites need different tuning we can split them, but starting with
  one shared knob keeps the change small and reviewable.

- `get_tx_history_to_check(eth_tx_id, limit)`
  - Add `limit: u64` (defaults to `status_scan_batch_size` = 10 via config).
  - Since we only need to find *a* successful/failed receipt among attempts,
    and old attempts with low gas are increasingly unlikely to ever confirm,
    checking only the most recent `limit` attempts (already `ORDER BY
    created_at DESC`) is safe and sufficient in practice.
- `get_non_final_txs(operator_address, is_gateway, limit)` /
  `get_inflight_txs(operator_address, is_gateway, limit)`
  - Add `LIMIT $n ... ORDER BY eth_txs.id` (oldest first, since we must
    resend/confirm in nonce order anyway), `limit` = `status_scan_batch_size`.
  - `eth_tx_manager.rs` already only actually resends the *first* tx whose
    nonce is `>= operator_nonce.latest` and only walks forward from there, so
    it never needed the *entire* tail of the queue at once — capping to
    `status_scan_batch_size` per call is enough per iteration.
  - Because processing is already nonce-ordered and sequential (a resend
    halts on first failure, confirmations happen oldest-nonce-first), capping
    to N per call does not skip any tx — it will simply be picked up on a
    later loop iteration once earlier ones clear.

Note: `status_scan_batch_size` only caps how much is read/scanned per DB call
(memory bound). It is intentionally kept separate from `max_txs_in_flight`
(which caps how many *new* txs get sent), since 10 is meant to be a small,
safe default for the read-path regardless of what `max_txs_in_flight` is
configured to on a given deployment.

### 4.3 (Optional, if 4.1+4.2 aren't sufficient) Stream/paginate at the DB layer

If profiling after 4.1/4.2 still shows spikes, consider replacing `fetch_all`
with a `fetch()` stream and processing history rows one at a time in
`check_all_sending_attempts`, so only one row (not the whole `Vec`) is held
in memory during the RPC round-trip to the L1 client. This is a larger change
and only needed if the simpler cap doesn't fully resolve it.

### 4.4 Guardrail metrics/logging

- Emit a gauge/histogram for:
  - number of rows returned by `get_non_final_txs` / `get_inflight_txs` per
    call,
  - number of resend attempts returned by `get_tx_history_to_check` per call,
  - total bytes of `signed_raw_tx`/`blob_sidecar` loaded per call (approx, via
    `.len()` sum).
- This lets us catch the next backlog buildup from dashboards before it OOMs
  again, and validates the fix under real load.

## 5. Concrete step-by-step execution order

1. **Add metrics first** (4.4) on the current (unpatched) code, deploy to a
   non-critical env if possible, and confirm the hypothesis by observing row
   counts / byte sums during a simulated or real backlog. *(Optional but
   recommended if we want hard evidence before changing query shape.)*
2. **Fix `get_tx_history_to_check` blob duplication** (4.1): introduce a
   lean query/struct used only by `check_all_sending_attempts` that selects
   `tx_hash` (+ whatever `ExecutedTxStatus`/ordering fields are actually
   required) without `blob_sidecar`/`signed_raw_tx`. Keep the existing
   `TxHistory`-returning function for callers that truly need the full
   payload (e.g. `send_eth_tx`'s "get previous sent tx" path uses
   `get_last_sent_successfully_eth_tx`, which does need fee fields, but not
   necessarily the blob).
3. **Add `limit` to `get_tx_history_to_check`** (4.2) with a config knob
   (default e.g. 50), plumbed through `EthSenderDal` → `eth_tx_manager.rs`.
4. **Add `limit` to `get_non_final_txs` / `get_inflight_txs`** (4.2) with a
   config knob (reuse `max_txs_in_flight` or add
   `status_scan_batch_size`), plumbed through the same call sites.
5. **Update `eth_tx_manager.rs` call sites** to pass the new limits; no
   behavioral branching needed since processing was already
   sequential/nonce-ordered.
6. **Local/unit tests**: extend `eth_sender/src/tests.rs` /
   `dal` tests to seed a tx with N history rows / M in-flight txs (N, M >
   limit) and assert:
   - only `limit` rows are returned,
   - resend/confirmation logic still finds the right result when it exists
     within the capped window,
   - no behavior change for the common case (few attempts).
7. **Manual verification**: run against a local/dev chain with an
   artificially large backlog (e.g. pause the sender for a while against a
   testnet, or seed the DB directly) and observe RSS before/after the fix.
8. **Rollout**: deploy to a canary node tracking Sepolia first, watch the new
   metrics + k8s memory graphs for a full backlog-recovery cycle, then roll
   out broadly.
9. **Follow-up (separate ticket, not in this plan)**: apply the same
   "no unbounded `fetch_all` on multiplying joins" audit to `eth_watch` and
   `eth_proof_manager` if similar patterns exist there.

**Decision**: metrics (step 1) will be landed as its **own PR first**, ahead
of the query-shape changes, so we have production evidence of the backlog
before/after. `get_unfinalized_transactions` **is in scope** for the
blob-dedup fix (step 2/4.1) alongside the other three queries — see the note
added to §4.1.

## 6. Risks / mitigations

| Risk | Mitigation |
|---|---|
| Capping `get_non_final_txs`/`get_inflight_txs` causes some tx to never be scanned. | Processing is nonce-ordered and sequential; capping just spreads work over more loop iterations, doesn't skip any tx. Add a test that proves eventual full coverage across iterations. |
| Capping `get_tx_history_to_check` misses the one attempt that actually got mined (e.g. a very old low-gas attempt got included by a builder). | Very unlikely in practice (only the latest fee-bumped attempt is expected to land), but flag as an accepted tradeoff; log a warning if `apply_inflight_txs_statuses_and_get_first_to_resend` ever hits the "possible reorg" branch so we can raise the limit if this ever triggers. |
| Removing `blob_sidecar` from the check-path query breaks some other consumer of `TxHistory` that expects it populated. | **Verified not an issue** — see §6.1 below: `TxHistory` (the domain type returned from these DAL calls) has no `blob_sidecar` field at all; it's already discarded during `StorageTxHistory -> TxHistory` conversion for every existing caller. |
| Removing `signed_raw_tx` breaks a consumer that reads `TxHistory.signed_raw_tx`. | **Verified not an issue for the new lean query** — grep shows no call site anywhere in the codebase reads `TxHistory.signed_raw_tx` after construction; see §6.1. We will still add a *new*, separate slim query/struct for `check_all_sending_attempts` rather than remove the column from the existing `TxHistory`/`StorageTxHistory` types, since those are shared by other call sites and the field is `NOT NULL`-enforced (`.expect(...)`) in the existing conversion. |
| SQLx offline query cache (`sqlx-data.json` / `.sqlx/`) needs regeneration for changed queries. | Run `cargo sqlx prepare` (or project's equivalent) against a live dev DB as part of the change, per repo conventions. |

### 6.1 What could break if we drop `blob_sidecar` / `signed_raw_tx`? (investigated)

- **`blob_sidecar`**: The domain struct `TxHistory` (`core/lib/types/src/eth_sender.rs`)
  does **not have a `blob_sidecar` field at all**. The `eth_txs.blob_sidecar`
  column joined in `get_tx_history_to_check` / `get_eth_tx_history_by_id` /
  `get_last_sent_successfully_eth_tx` / `get_unfinalized_transactions` is read
  into `StorageTxHistory.blob_sidecar` and then **silently dropped** by the
  `From<StorageTxHistory> for TxHistory` conversion
  (`core/lib/dal/src/models/storage_eth_tx.rs`). So today, this join is pure
  wasted I/O and memory (duplicated per history row) with **zero functional
  purpose** — removing it from the `SELECT` in these queries is risk-free.
  (The real `blob_sidecar` used for signing/resending lives on `EthTx`, not
  `TxHistory`, and is fetched separately via `get_eth_tx`/`get_inflight_txs`/etc.
  — that path is untouched by this plan.)
- **`signed_raw_tx`**: This field *does* exist on `TxHistory` and the
  conversion panics (`.expect("Should rely only on the new txs")`) if the
  column is `NULL`. However, a repo-wide search found **no code that reads
  `TxHistory.signed_raw_tx`** after it's constructed — not in
  `eth_tx_manager.rs`, `eth_fees_oracle.rs`, `eth_tx_aggregator.rs`, tests, or
  anywhere else. It's loaded and then never used by any current caller.
  - Because it's still schema-mandatory (`NOT NULL` assumption baked into the
    shared conversion), we will **not** modify the existing `TxHistory`/
    `StorageTxHistory` types or their shared queries. Instead, `4.1`
    introduces a *new*, narrower struct/query used **only** by
    `check_all_sending_attempts` (which only needs `tx_hash`), so the shared
    types and their other call sites (`send_eth_tx`'s previous-tx lookup,
    aggregator's health checks, tests) are completely unaffected.
  - Net effect: no behavior changes anywhere; we simply stop fetching two
    columns worth of bytes (per-row duplicated, in the `blob_sidecar` case)
    on the hot status-scanning path that never used them.

## 7. Open questions for reviewer

1. ~~What default should the batch size have?~~ **Resolved**: single
   `status_scan_batch_size` config field, env var
   `ETH_SENDER_SENDER_STATUS_SCAN_BATCH_SIZE`, default `10`.
2. ~~What could break by dropping `blob_sidecar`/`signed_raw_tx`?~~
   **Resolved** — see §6.1: nothing, `blob_sidecar` is already discarded by
   the existing conversion for all callers, and `signed_raw_tx` on
   `TxHistory` has no readers anywhere in the codebase today.
3. ~~Do we want metrics landed as a separate PR first?~~ **Resolved: yes** —
   see the "Decision" note at the end of §5.
4. ~~Should this also cover `get_unfinalized_transactions`?~~ **Resolved:
   yes**, for the blob-dedup fix only (it's already row-count-limited); see
   the note in §4.1 and the "Decision" note at the end of §5.

No open questions remain — plan is ready to move to implementation, starting
with the metrics-only PR.

---
*No code has been modified as part of authoring this plan.*
