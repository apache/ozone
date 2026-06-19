---
title: Leader-Planned State Transition Execution
summary: Leader computes DB changes once and sends them to followers, instead of every node repeating the same work
date: 2026-06-12
jira: HDDS-11898
status: proposed
author: Abhishek Pal
---
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Leader-Planned State Transition Execution

## Table of Contents

1. [Motivation](#1-motivation)
2. [Industry Precedent](#2-industry-precedent)
3. [Proposal](#3-proposal)
   * [3.1 High-Level Architecture](#31-high-level-architecture)
   * [3.2 Proto Format](#32-proto-format-replicatedstatetransition)
   * [3.3 Managed Index Service](#33-managed-index-service)
   * [3.4 Leader Planning Framework](#34-leader-planning-framework)
   * [3.5 Apply Engine](#35-apply-engine)
   * [3.6 State Machine Integration](#36-state-machine-integration)
   * [3.7 Granular Locking](#37-granular-locking)
   * [3.8 Lock Hold Span](#38-lock-hold-span)
4. [Detailed Concurrency Model](#4-detailed-concurrency-model)
5. [Correctness Arguments](#5-correctness-arguments)
6. [Testing Strategy](#6-testing-strategy)
7. [Comparison: Legacy vs Planned Path](#7-comparison-legacy-vs-planned-path)
8. [Migration Strategy](#8-migration-strategy)
9. [FAQs](#9-faqs)
10. [Future Work and Open Items](#10-future-work-and-open-items)

---

## 1. Motivation

### Problem with the current OM HA write flow

In the current architecture, every write request follows this path:

```
Client -> OM Leader (preExecute) -> Ratis replication -> All OMs (validateAndUpdateCache)
```

Every OM node (leader and followers) runs the full business logic again in
`validateAndUpdateCache()`. This causes several problems:

| Problem | Impact |
|:--------|:-------|
| **Wasted compute on followers** | Every follower repeats the full request processing — lock acquisition, validation, key building, quota computation |
| **Risk of inconsistency** | `validateAndUpdateCache` reads from a table cache that may have different temporary state on leader vs follower when requests run at the same time |
| **Double buffer complexity** | Results are queued in `OzoneManagerDoubleBuffer`, which adds batching delay and a separate flush thread. Snapshot barriers add more coordination |
| **Tight coupling** | External consumers (such as Recon) must understand all command logic to read state changes |
| **Lock contention** | OBS key operations hold a bucket-level write lock even though most creates/commits do not conflict with each other |

### What we want

1. Leader computes a **fixed DB patch** (a list of table puts and deletes) once.
2. The patch is sent to all nodes via Ratis as a self-contained payload.
3. All nodes (leader, followers, listeners) apply the patch directly to RocksDB — no business logic runs again.
4. Locks are made more fine-grained so independent key operations can run in parallel.

---

## 2. Industry Precedent

Before we detail the proposal, it is useful to see how other production
Raft-based systems solve the same problem. There are two models used in
practice:

| Model | Who runs the logic? | What goes through Raft? | Used by |
|-------|--------------------|-----------------------|---------|
| **State Machine Replication (SMR)** | All replicas | Commands (the operation) | TiKV, older CockroachDB, existing Ozone replication model |
| **Leader Execution (Primary-Backup)** | Leader only | Computed results (WriteBatch) | YugabyteDB, newer CockroachDB, **this proposal** |

### 2.1 CockroachDB — Proposer-Evaluated KV

CockroachDB originally used classic state machine replication (all replicas
evaluate commands independently). They later moved to **Proposer-Evaluated KV
(PEVK)** — the same pattern we propose here.

**How it works:**
1. The lease-holder (leader) evaluates the request and produces a `WriteBatch`
   (the exact bytes to write to storage).
2. The `WriteBatch` + computed response are proposed to Raft as a single entry.
3. Followers apply the pre-computed `WriteBatch` directly — they do not
   re-execute command logic.

**Why they moved to this model** (from their design RFC):
- Removes duplicate execution across replicas
- Makes online migrations simpler (only leader needs new evaluation code)
- Puts all complex logic in one place (the proposer)

**Their concurrency control:**
- **Latches** (fine-grained per-key read/write locks) are acquired **before**
  evaluation and released **after** the WriteBatch is built — but **before**
  Raft replication. This is the same pattern as our striped locks.
- An in-memory **lock table** handles transaction-level conflict queueing.
- MVCC timestamps provide version ordering.

**Key insight:** CockroachDB holds latches only during evaluation, NOT during
replication:
```
acquire_latch → evaluate(request) → build WriteBatch → release_latch → propose to Raft
```
This matches our design: `acquireLock → plan() → releaseLock → submit to Ratis`.

Source: `pkg/kv/kvserver/replica_proposal.go`, `pkg/kv/kvserver/concurrency/`

### 2.2 YugabyteDB — Leader-Computed Write Batches

YugabyteDB also uses leader execution:

1. The tablet leader computes the write batch (exact RocksDB key-value pairs).
2. The batch is replicated via Raft to followers.
3. Followers apply the **pre-computed byte-level changes** to their local
   RocksDB — no re-execution.

Their concurrency uses "provisional records" (intents) written to a separate
RocksDB instance as persistent locks, plus hybrid timestamps for ordering.

### 2.3 TiKV — Classic State Machine Replication

TiKV uses the traditional approach: all replicas execute the same Raft log
entries. However, TiKV's "commands" are already low-level Put/Delete operations
on key-value pairs. There is no complex business logic to re-execute.

This works for TiKV because the transaction coordination layer (Percolator 2PC)
runs **above** Raft. By the time something enters the Raft log, it is already a
simple key-value mutation.

---

## 3. Proposal

### 3.1 High-Level Architecture

```
                         LEADER                                   FOLLOWER
                   +------------------+
  Client Request   | startTransaction |
  ------------->   |  (leader only)   |
                   |                  |
                   |  1. Create       |
                   |     PlannedReq   |
                   |  2. preProcess   |
                   |  3. authorize    |
                   |  4. acquireLock  |
                   |  5. plan()       |
                   |     -> deltas    |
                   |  6. releaseLock  |
                   |  7. Build proto  |
                   +--------+---------+
                            |
                            | Ratis log entry
                            | (OMRequest with embedded
                            |  BatchedStateTransitions)
                            |
                   +--------v---------+                     +------------------+
                   | applyTransaction |                     | applyTransaction |
                   |  (leader)        |                     |  (follower)      |
                   |                  |                     |                  |
                   |  1. Parse proto  |                     |  1. Parse proto  |
                   |  2. For each     |                     |  2. For each     |
                   |     Operation:   |                     |     Operation:   |
                   |     table.put/   |                     |     table.put/   |
                   |     delete/merge |                     |     delete/merge |
                   |  3. Commit batch |                     |  3. Commit batch |
                   |                  |                     |                  |
                   |  (zero business  |                     |  (zero business  |
                   |   logic)         |                     |   logic)         |
                   +------------------+                     +------------------+
```

**Important:** The leader does NOT write to RocksDB during `startTransaction`.
It only computes the deltas (the list of puts and deletes). The actual DB write
happens later in `applyTransaction`, which runs on **both leader and followers**
after Ratis commits the log entry. This ensures all nodes have the same DB state.

The result of planning is a list of raw `{table, key_bytes, value_bytes,
PUT/DELETE}` operations packed into a `ReplicatedStateTransition` proto. The
apply step on all nodes opens a RocksDB batch, loops through the deltas, and
commits.

### 3.2 Proto Format: ReplicatedStateTransition

The proto has two layers:

**Inner layer (domain-agnostic, in `hadoop-hdds/framework`):**
```protobuf
message Operation {
  enum Kind { PUT = 1; DELETE = 2; MERGE = 3; CHECKPOINT = 4; }
  Kind kind = 1;
  bytes column_family = 2;
  bytes key = 3;
  bytes value = 4;
  bytes merge_operator_id = 5;   // selects the merge resolver (for MERGE kind)
}

message Batch {
  repeated Operation ops = 1;
}
```

**Outer layer (OM envelope, in `hadoop-ozone/interface-client`):**
```protobuf
message ReplicatedStateTransition {
  required int64 managedIndex = 1;         // OM-managed monotonic ID
  required Batch batch = 2;                // the inner domain-agnostic patch
  required OMResponse response = 3;        // pre-computed client response
  required Type cmdType = 4;               // for barrier detection
}

message BatchedStateTransitions {
  repeated ReplicatedStateTransition transitions = 1;
}
```

**Key decisions:**
- **Raw bytes, not domain objects.** Each operation carries the exact bytes
  that will be written to the RocksDB column family. Followers do no
  deserialization of `OmKeyInfo`, `OmBucketInfo`, etc.
- **Four operation kinds.** `MERGE` is needed for commutative quota updates
  (see [OBS quota handling](./obs-design.md#3-quota-handling-decision-required)).
  `CHECKPOINT` triggers the snapshot barrier.
- **Inner layer is domain-agnostic.** The `Batch`/`Operation` proto and its
  apply engine live in `hadoop-hdds/framework` and import no Ozone OM types.
  This makes the apply engine reusable by Recon-as-listener and future
  consumers without importing OM command classes.
- **The inner layer never deserializes a domain object.** Apply sees bytes,
  writes bytes. This is what makes followers business-logic-free.

The `BatchedStateTransitions` wrapper is placed on the `OMRequest` at field
number 200. The state machine checks `hasBatchedStateTransitions()` to decide
whether to use the new apply path.

### 3.3 Managed Index Service

The OM-managed `AtomicLong` counter that mints objectIDs independently of the
Ratis log index. Survives leader switchover, is persisted atomically with each
DB batch, and ensures old-path/new-path objectID ranges are disjoint during
mixed mode.

→ [Full component details](./components.md#1-managed-index-service)

### 3.4 Leader Planning Framework

Three classes form the planning framework: `PlannedRequest` (abstract base
with a step-iterator model that generalizes single-step and multi-step
operations), `ChangeRecorder` (accumulates a `Batch` proto instead of staging
into the table cache), and `LeaderPlanner` (the orchestrator that drives the
step-iterator to completion, continuation-driven to free worker threads during
Ratis awaits).

→ [Full component details](./components.md#2-leader-planning-framework)

### 3.5 Apply Engine

The `OperationApplier` (in `hadoop-hdds/framework`) writes a `Batch` to
RocksDB. Domain-agnostic — imports no Ozone OM types. Handles barrier splitting
for snapshot operations. A follower that cannot apply a committed patch must
crash and re-sync (fail-stop), same as today.

→ [Full component details](./components.md#3-apply-engine)

### 3.6 State Machine Integration

The `OzoneManagerStateMachine` runs a dual-path model: planned commands
(registered via `registerPlannedCommand`) use the new apply engine; legacy
commands use the existing `runCommand()` + double-buffer path. This allows
step-by-step migration one command at a time.

→ [Full component details](./components.md#4-state-machine-integration)

### 3.7 Granular Locking

Fine-grained, objectID-keyed locking that is rename-stable and ABA-free.
Two lock types: **container locks** (keyed by directory objectID, governs
children) and **slot locks** (keyed by parentObjectID+name, governs one entry).
Implemented as a fixed striped array of non-thread-affine semaphores (~2^20
stripes). Deadlock-free via uniform total ordering of lock acquisition.

→ [Full locking details](./locking-and-concurrency.md#1-granular-locking)

### 3.8 Lock Hold Span

An open decision: release locks before Ratis submit (Option A — higher
parallelism, matches CockroachDB) or hold through commit+apply (Option B —
removes table cache, trivially correct for multi-step operations).
Recommendation leans toward Option B for FSO and a possible hybrid for OBS.

→ [Full analysis](./locking-and-concurrency.md#2-lock-hold-span-decision-required)

---

## 4. Detailed Concurrency Model

The detailed concurrency model covers the life of a request, concurrent
scenarios (parallel creates, same-key commits, CreateKey vs DeleteBucket,
FSO createFile vs rm-rf, crossing renames), quota handling, multi-step
operations, known limitations, and retry strategy.

→ [OBS concurrency scenarios](./obs-design.md#2-concurrent-scenarios)
→ [FSO concurrency scenarios](./fso-design.md#2-concurrent-scenarios)
→ [Multi-step operations (FSO)](./fso-design.md#3-multi-step-operations)
→ [Quota handling decision](./obs-design.md#3-quota-handling-decision-required)
→ [Known limitations](./locking-and-concurrency.md#5-known-limitations-and-accepted-tradeoffs)
→ [Retry and idempotency](./locking-and-concurrency.md#6-retry-and-idempotency-deferred-decision)

---

## 5. Correctness Arguments

This section explains why the design is correct — i.e., it produces the same
results as the current serial execution model.

### 5.1 Argument 1: Ratis Total Order Guarantees Linearizability

All committed transitions are applied in the same order on all nodes. Ratis
ensures this: each entry has a unique `(term, index)` and entries are applied in
index order. This means:
- If request A is submitted before request B (from Ratis's perspective), A is
  applied before B on every node
- The DB state after applying N entries is the same on all nodes

This is the same guarantee as today. The difference is only in what gets applied
(raw bytes vs re-executed commands).

### 5.2 Argument 2: Locks Serialize Conflicting Plans

Two requests that touch the same data will acquire the same striped lock. Since
the lock is exclusive (write lock), only one can plan at a time. This ensures:
- The second request reads committed state that is consistent with what the
  first request planned against
- No two plans can produce conflicting deltas for the same key

**Exception:** CreateKey uses only bucket READ locks and no key lock. Multiple
creates to the same bucket plan in parallel. This is safe because each open key
entry has a unique key (`/vol/bucket/key/clientID`), so parallel creates write
to different rows.

### 5.3 Argument 3: The Plan-to-Apply Gap is Safe

Between planning (lock released) and applying (Ratis commits), the leader's DB
does not reflect the planned changes. Could a subsequent request read stale
state and produce wrong results?

**For non-conflicting requests:** They touch different keys or different tables.
The stale state does not affect them.

**For conflicting requests:** They need the same lock. Since the first request
still holds the lock during its planning phase, the second request cannot start
planning until the first finishes and releases the lock. By that point, the
first request has already submitted its transition to Ratis (sequentially after
releasing the lock). Ratis log ordering then guarantees the first transition is
applied before the second.

**Remaining risk — quota drift:** If two requests both read quota=100 and both
plan quota=101, the final value is 101 (not 102). For CreateKey, this is
acceptable (see [OBS Scenario A](./obs-design.md#scenario-a-two-createkey-requests-to-different-keys-in-the-same-bucket)).
For CommitKey, the key write lock prevents this because commits to the same
bucket-and-key are serialized.

If strict per-create quota accounting is needed, we can fall back to bucket
WRITE lock for CreateKey (same as today). This decision is a tunable tradeoff:
- Bucket READ lock = higher parallelism, slightly relaxed quota at create time
- Bucket WRITE lock = lower parallelism, strict quota at create time

In both cases, CommitKey enforces final quota correctly because it holds the key
write lock.

### 5.4 Argument 4: Atomic Apply Prevents Partial State

Each `ReplicatedStateTransition` is applied as a single RocksDB `WriteBatch`.
RocksDB guarantees that a WriteBatch is atomic — either all writes succeed or
none do. If the process crashes mid-apply, the batch is not committed, and on
restart the entry is replayed from the Ratis log.

### 5.5 Argument 5: Leader Failure is Handled

If the leader fails after planning but before Ratis commits:
- The Ratis entry was not committed (no quorum ack). It is lost.
- The new leader's `ManagedIndexService.onBecomeLeader(lastCommitted)` resets
  the index counter to `max(currentIndex, lastCommittedIndex)`.
- The client times out and retries. The new leader plans the request fresh.

If the leader fails after Ratis commits but before apply:
- On restart, the Ratis log replays all committed-but-not-applied entries.
- The apply engine processes the pre-computed deltas from the log.
- No re-planning is needed.

### 5.6 What Could Go Wrong (and How We Prevent It)

| Risk | Mitigation |
|------|-----------|
| Two threads plan for same key with different base state | Striped key write lock serializes them |
| Quota drift on parallel CreateKey | Acceptable for reservations; enforced at CommitKey. Can use bucket WRITE lock if stricter |
| Bucket deleted while key create is in Ratis log | Bucket WRITE lock + Ratis ordering prevents this (see [Scenario C](./obs-design.md#scenario-c-createkey-and-deletebucket-for-the-same-bucket)) |
| ManagedIndex duplicates after leader change | `onBecomeLeader` calls `max(current, lastCommitted)` — always moves forward |
| Crash during RocksDB WriteBatch | Atomic batch — either fully committed or not. Ratis replays on restart |
| Follower apply fails | Fatal error — follower must catch up via Ratis snapshot (same as today) |
| Snapshot barrier missed | Apply engine checks `cmdType` — CreateSnapshot/SnapshotPurge always get own batch |

---

## 6. Testing Strategy

Tests at multiple levels ensure correctness: unit tests for each component,
multi-threaded concurrency tests for lock interaction, 3-node HA integration
tests, a determinism verification test (planned path produces byte-identical
DB state to legacy path), a Wing-Gong/Lincheck linearizability harness, and
stress/chaos tests with random node kills.

→ [Full testing strategy](./testing-strategy.md)

---

## 7. Comparison: Legacy vs Planned Path

| Aspect | Legacy Path | Planned Path |
|:-------|:------------|:-------------|
| **Who executes logic** | All nodes | Leader only |
| **What is replicated** | Raw `OMRequest` bytes | DB patch (`ReplicatedStateTransition`) |
| **Follower work** | Full `validateAndUpdateCache` | Write raw bytes to column families |
| **Locking** | Bucket write lock (coarse) | Bucket read + striped key/dir (fine) |
| **ID generation** | From Ratis `transactionLogIndex` | From `ManagedIndexService` |
| **Response available** | After `applyTransaction` | Right after planning |
| **Double buffer** | Required (batches + flushes) | Not used |
| **Table cache** | Maintained per-table | Not needed (reads from DB directly) |

---

## 8. Migration Strategy

Commands are migrated hardest-first across 8 phases (P-0 through P-7). Phase-0
establishes 3 interlocked prerequisites (shared objectID counter, dual-path
applied-index durability, OMLayoutFeature finalization gate). Each command
migration is self-contained and independently revertible via a dual activation
gate (finalization + per-command runtime flag).

→ [Full migration playbook](./migration-playbook.md)

---

## 9. FAQs

### Q: Why not batch multiple planned transitions into one Ratis entry (like the double buffer does)?

The double buffer batches to spread out the cost of Ratis round-trips. But
Ratis already does its own batching at the replication layer (AppendEntries
groups multiple log entries in one RPC). Batching before Ratis brings back the
same problems as the double buffer:
- Queue management complexity
- Actual batch gain is small (~1.2x in practice)
- Adds delay for the first item in the batch
- One slow item blocks everything behind it

Each planned request goes to Ratis individually. Ratis's built-in batching at
the log replication layer gives enough throughput.

### Q: What happens between planning and apply on the leader? Can another request read stale state?

Between planning (in `startTransaction`) and apply (in `applyTransaction`),
the leader's DB has **not yet been updated** with the planned writes. Another
request that touches the same key would:
- Try to acquire the same striped lock in its own `startTransaction` call
- Wait until the first request releases the lock after planning

So the lock orders the *planning phase*, not the apply phase. This is safe
because:
- The DB is only changed during `applyTransaction` (after Ratis consensus)
- The second request cannot plan until the first has finished planning and
  released the lock
- Between planning and apply, no reader can see a half-written state

### Q: Why use `ManagedIndexService` instead of just using Ratis index?

In `startTransaction()`, the Ratis log index for the current entry has not been
assigned yet (it is assigned during log append, which happens after
`startTransaction` returns). We need a unique ID at planning time for
`objectID` generation. The managed index provides this.

Also, if we ever want to batch in the future, multiple transitions in one log
entry would share a Ratis index but need different object IDs.

### Q: Why raw bytes in the proto instead of structured domain objects?

Three reasons:
1. **Performance:** Followers skip all deserialization and codec work.
   They write raw bytes directly to column families.
2. **Simplicity:** The apply engine is ~30 lines of code with no command-specific
   logic.
3. **Reusability:** Any system that can open the same RocksDB (Recon, backup
   tools, external audit systems) can apply the same payload without needing
   OM command processing classes.

### Q: How are snapshots handled?

`CreateSnapshot` and `SnapshotPurge` are "barrier" operations. The apply engine
detects these by `cmdType` and makes sure they are applied in their own separate
RocksDB batch. This keeps the same guarantee as the current double buffer's
`splitReadyBufferAtCreateSnapshot()`.

### Q: What about the table cache?

The legacy path keeps per-table caches (in `TypedTable`) so that
`validateAndUpdateCache` on the leader can see not-yet-committed writes from
earlier requests in the same double-buffer batch. In the planned path:
- There is no double buffer.
- Each request plans on its own, reading from the committed DB state.
- The striped lock ensures requests to the same key run one after another, so
  there is no "uncommitted earlier write" to worry about.

Therefore, **table caches are not needed** for planned commands. They remain
in use for legacy (unmigrated) commands.

### Q: Can the planned path and legacy path coexist?

Yes. The state machine routes by command type. Planned commands bypass the
double buffer entirely. Legacy commands use the existing `runCommand` +
double-buffer path unchanged. Both update `lastAppliedTermIndex` correctly.

### Q: What if planning fails (e.g., key not found, quota exceeded)?

`LeaderPlanner` catches all exceptions, builds an error `OMResponse`, and
returns a `ReplicatedStateTransition` with **empty deltas** and the error
response. This is still sent through Ratis so all nodes see the same
outcome (no DB changes, index still moves forward). The client receives
the error response.

### Q: Why bucket read lock instead of no lock for CreateKey?

The bucket read lock stops the bucket from being deleted or having its
properties changed (e.g., quota, replication policy) while a key operation
is running. It does not block other key operations within the same bucket
(read locks are shared — multiple readers are allowed).

### Q: How does this relate to removing the double buffer?

Once all commands are migrated to the planned path, the double buffer is no
longer needed and can be removed entirely. During the migration period, both
paths coexist. The double buffer only serves unmigrated commands.

### Q: What about audit logging and metrics?

Not yet connected in the planned path. This is a TODO. Audit events and
OM metrics (`incNumKeyAllocates`, `incNumKeyCommits`, etc.) need to be
called during the planning phase (on the leader) or read from the response
(on followers).

### Q: How does this compare to other production systems?

See Section 2 (Industry Precedent). Our design follows the same model as
CockroachDB's Proposer-Evaluated KV and YugabyteDB's leader-computed write
batches. Both are proven at scale in production. The key difference: those
systems also need full MVCC and 2PC because they have multiple Raft groups
(sharded data). We do not — all OM metadata is in one Raft group, so our
concurrency needs are simpler (just latching, no multi-shard transactions).

### Q: Does this work with Zero-Downtime Upgrade (ZDU)?

Yes. See the [migration playbook](./migration-playbook.md#5-zero-downtime-upgrade-zdu-compatibility)
for full details. In short:
- The planned execution path is gated behind a component version check.
- Pre-finalization: OMs use the legacy path (compatible with old software).
- Post-finalization: all OMs switch to the planned path together.
- The legacy path stays in the code for one version after introduction, then
  can be removed. Users upgrading across multiple versions must pass through
  the intermediate version if using ZDU.

### Q: Do we need to keep the old `validateAndUpdateCache` code forever?

No. The ZDU versioning policy allows removing old internal APIs after one
version. For example:
- Version N introduces the planned path (legacy path still used pre-finalization)
- Version N+1 keeps the legacy path for pre-finalization only
- Version N+2 removes the legacy path entirely

Users doing ZDU from version N to N+2 must pass through N+1. Users doing
offline (downtime) upgrades can jump directly.

### Q: Why not reuse OzoneManagerLock?

Two reasons: (1) It is built on `ReentrantReadWriteLock` which is thread-affine
— cannot be released on a different thread than acquired. Under Option B of the
lock hold span, the lock is released on the continuation thread after the Ratis
await. (2) It carries eight leveled resource types, per-type striped maps,
trackers, and reentrancy — machinery this design does not need. The replacement
is a single fixed striped array with no resource hierarchy.

### Q: Why objectID-keyed locks instead of path-keyed?

A directory rename in FSO is O(1): the directory keeps its objectID, only its
entry changes parent. Children are never touched because they key on the
parent's unchanged objectID. A path-keyed lock would be invalidated by rename;
an objectID-keyed lock survives it. Additionally, objectIDs are never reused
(monotonic from ManagedIndex), so there is no ABA problem where a deleted
node's lock could be confused with a new node's lock.

### Q: What happens to quota if the Merge operator is chosen?

The quota counter (`usedBytes`/`usedNamespace`) is always exact — it never
loses an update. Deltas are summed in Ratis order at apply time. However, the
quota **limit check** is still best-effort: two parallel commits can both pass
the check (reading the same pre-increment value) and then both apply. This
means the bucket can transiently exceed its quota limit by up to the number of
in-flight commits. The existing `QuotaRepair` background service reconciles any
drift. This soft-limit behavior is an accepted tradeoff for parallelism.

### Q: How does FSO createFile with missing parents work as multi-step?

The leader decomposes `createFile /a/b/c/file` (with `/b` and `/c` missing)
into a chain: create-dir-b → await commit → create-dir-c → await commit →
create-open-file. Each step runs under its own lock set, revalidates the locked
nodes, plans one transition, and submits to Ratis. The worker thread is freed
during each Ratis await (continuation-driven). If the leader crashes mid-chain,
the client retries the whole request on the new leader — already-created
directories are skipped idempotently.

---

## 10. Future Work and Open Items

These items are scoped out of the current design but are known to need
resolution as the implementation progresses. They are listed here so that
future work does not re-derive them from scratch.

### 10.1 Exact Quota Enforcement (Open Decision)

The current design enforces quota limits best-effort (see
[known limitations](./locking-and-concurrency.md#5-known-limitations-and-accepted-tradeoffs)).
The open question: should admission be **exact** (leader-local atomic
reservation) or **approximate** (Merge-only, rely on `QuotaRepair`)?

Leading candidate for exact enforcement:
- Atomic check-and-reserve in memory on the leader before Ratis submit.
- DB Merge remains the durable truth (no change to the replicated patch).
- Decrement-on-abort if the Ratis submit fails.
- Rebuild-from-DB on failover (the in-memory reservation is leader-local).

This is layered on top of the commutative Merge without re-migrating any
command. The Merge lands unconditionally in P-1; the enforcement mode is
added later if exact admission is needed.

### 10.2 Retry/Idempotency Mechanism (Deferred Decision)

See [retry and idempotency](./locking-and-concurrency.md#6-retry-and-idempotency-deferred-decision)
for the full analysis. The mechanism is deferred pending the per-operation
idempotency audit. The ~10 non-idempotent operations need a durable retry
entry written atomically with the data batch; the ~37 idempotent operations
likely need nothing beyond their natural idempotency.

### 10.3 Multipart Upload Lock Placement and Large-Value Concern

MPU (FSO and OBS) lock placement is not yet specified at the same detail as
key operations. Specific open questions:
- Does `CompleteMultiPartUpload` (which assembles N parts) need a multi-step
  chain, or is it a single large transition?
- The large-value concern (HDDS-8238): a completed multi-GB MPU object's
  `OmKeyInfo` with all block locations is replicated as raw bytes in the
  Ratis log. For objects with thousands of blocks, this can be tens of KB per
  log entry. Is this acceptable, or does the design need a block-list delta
  optimization for MPU complete?

This is addressed in Phase P-4 of the migration plan.

### 10.4 hsync / Lease-Recovery Interaction

`hsync` and lease-recovery (`RecoverLease`) interact with the slot lock
`X(parent, file)` that commit takes. Specific questions:
- Does `RecoverLease` need the same slot lock as commit?
- How does an in-progress hsync stream interact with the planned-execution
  model where each `AllocateBlock` is a separate planned transition?
- Does the lease-recovery path need to read the open-key state and plan a
  forced-commit transition?

### 10.5 Snapshot Checkpoint Ordering vs In-Flight Operations

`CreateSnapshot` emits a `CHECKPOINT` operation that triggers a snapshot
barrier. The open question: how does this interact with fine-grained locks
introduced in P-2?

If multiple operations are in-flight (planned but not yet applied) when a
`CreateSnapshot` is submitted, the snapshot must capture a consistent point.
The barrier semantics (Checkpoint gets its own batch, applied in isolation)
handle this at the apply layer. But the planning-layer question is: must
`CreateSnapshot` acquire exclusive bucket locks to drain in-flight operations,
or is the Ratis log ordering sufficient to define the snapshot boundary?

The current design relies on the Ratis log index as the snapshot boundary
(same as today's `splitReadyBufferAtCreateSnapshot`). This is likely
sufficient but needs validation against the fine-grained locking model.

### 10.6 FSO Directory mtime on Cross-Parent Rename

When renaming a file from directory A to directory B, both directories'
`mtime` should be updated. Under the current design, this is straightforward
(both parent directories are locked via `S(P1) + S(P2)` and the Batch can
include mtime updates for both). However, if mtime updates ever use a Merge
operator (to allow parallel mtime updates without serialization), the
interaction with the quota Merge operator needs specification.

Current expectation: mtime is a last-writer-wins property, not a commutative
counter, so a standard PUT of the updated directory entry suffices. No Merge
needed for mtime.

### 10.7 SetAcl / SetTimes / AllocateBlock Lock Placement

These operations are expected to follow the pattern:
`S(bucket) + S(parent) + X(parent, name)` — shared bucket, shared container
on the parent, exclusive slot on the target entry. This matches the
deleteFile/commitFile pattern.

`AllocateBlock` is a special case: it mutates the open-key entry (appending
a block). Since open keys include `clientID` in their key, concurrent
`AllocateBlock` calls from different clients write different rows and do not
conflict. Same-client `AllocateBlock` calls are serialized by the client
(one at a time). So `AllocateBlock` likely needs only `S(bucket)` — same as
`createKey/createFile`.

### 10.8 Pre-Ratis Batching

The current design sends each planned request to Ratis individually. Ratis's
built-in log-replication batching (AppendEntries groups multiple entries in
one RPC) provides baseline throughput.

A future optimization: accumulate multiple independently-planned transitions
in a queue and submit them as a single `BatchedStateTransitions` Ratis entry.
This amortizes the per-entry Ratis overhead (fsync, quorum ack) across
multiple requests. The proto already supports this via
`BatchedStateTransitions { repeated ReplicatedStateTransition }`.

Trade-offs:
- Adds queue management and flush-interval complexity.
- The first item in the batch incurs the wait time for the batch to fill.
- One slow item does NOT block others (each plans independently; only the
  Ratis submit is batched).
- Estimated throughput gain: 2-4x for small operations under high load.

This is a post-P-1 optimization. The design supports it without structural
changes.

---

## Appendix: File Layout

```
hadoop-hdds/framework/src/main/proto/
  ReplicatedDbProtocol.proto          - Inner Batch/Operation proto (domain-agnostic)

hadoop-hdds/framework/src/main/java/org/apache/hadoop/hdds/utils/db/replicated/
  OperationApplier.java               - Domain-agnostic apply engine
  MergeOperatorRegistry.java          - Registry for merge resolvers
  MergeOperator.java                  - Interface for merge resolvers

hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/
  execution/
    ManagedIndexService.java          - Monotonic index management
    LeaderPlanner.java                - Orchestrator (drives step-iterator)
    ChangeRecorder.java               - Accumulates Batch per step
    lock/
      StripedSemaphoreLockManager.java - Non-thread-affine striped RW locks
      LockHandle.java                 - AutoCloseable, any-thread release
      LockRequest.java                - (stripeIndex, mode) pair
  execution/request/
    PlannedRequest.java               - Abstract base (step-iterator)
    key/
      CreateKeyPlannedRequest.java    - OBS CreateKey
      CommitKeyPlannedRequest.java    - OBS CommitKey
      CreateFilePlannedRequest.java   - FSO CreateFile (multi-step)
      CreateDirectoryPlannedRequest.java - FSO CreateDirectory (multi-step)
  execution/merge/
    QuotaMergeOperator.java           - Commutative quota delta resolver
  ratis/
    OzoneManagerStateMachine.java     - Dual-path routing (modified)

hadoop-ozone/interface-client/src/main/proto/
  OmClientProtocol.proto              - Outer OM envelope (ReplicatedStateTransition)
```
