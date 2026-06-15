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
   * [2.1 CockroachDB — Proposer-Evaluated KV](#21-cockroachdb--proposer-evaluated-kv)
   * [2.2 YugabyteDB — Leader-Computed Write Batches](#22-yugabytedb--leader-computed-write-batches)
   * [2.3 TiKV — Classic State Machine Replication](#23-tikv--classic-state-machine-replication)
   * [2.4 Why Ozone Does NOT Need Full RDBMS Concurrency](#24-why-ozone-does-not-need-full-rdbms-concurrency)
3. [Proposal](#3-proposal)
   * [3.1 High-Level Architecture](#31-high-level-architecture)
   * [3.2 Proto Format: ReplicatedStateTransition](#32-proto-format-replicatedstatetransition)
   * [3.3 Managed Index Service](#33-managed-index-service)
   * [3.4 Leader Planning Framework](#34-leader-planning-framework)
   * [3.5 Apply Engine](#35-apply-engine)
   * [3.6 State Machine Integration](#36-state-machine-integration)
   * [3.7 Granular Locking](#37-granular-locking)
4. [Detailed Concurrency Model](#4-detailed-concurrency-model)
   * [4.1 Life of a Request](#41-life-of-a-request-detailed)
   * [4.2 Concurrent Scenarios](#42-concurrent-scenarios)
   * [4.3 Lock Granularity Summary](#43-lock-granularity-summary)
5. [Correctness Arguments](#5-correctness-arguments)
6. [Testing Strategy](#6-testing-strategy)
   * [6.1 Unit Tests](#61-unit-tests)
   * [6.2 Concurrency Tests](#62-concurrency-tests-critical-for-correctness)
   * [6.3 Integration Tests](#63-integration-tests-3-node-ha-cluster)
   * [6.4 Determinism Verification Test](#64-determinism-verification-test)
   * [6.5 Stress/Chaos Tests](#65-stresschaos-tests)
7. [Comparison: Legacy vs Planned Path](#7-comparison-legacy-vs-planned-path)
8. [Migration Strategy](#8-migration-strategy)
9. [FAQs](#9-faqs)

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
| **Tight coupling** | External consumers (Recon) must understand all command logic to read state changes |
| **Lock contention** | OBS key operations hold a bucket-level write lock even though most creates/commits do not conflict with each other |

### What we want

1. Leader computes a **fixed DB patch** (a list of table puts and deletes) once.
2. The patch is sent to all nodes via Ratis as a self-contained payload.
3. All nodes (leader, followers, future Recon follower) apply the patch directly to RocksDB — no business logic runs again.
4. Locks are made more fine-grained so independent key operations can run in parallel.

---

## 2. Industry Precedent

Before we detail the proposal, it is useful to see how other production
Raft-based systems solve the same problem. There are two models used in
practice:

| Model | Who runs the logic? | What goes through Raft? | Used by |
|-------|--------------------|-----------------------|---------|
| **State Machine Replication (SMR)** | All replicas | Commands (the operation) | TiKV, older CockroachDB |
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

### 2.4 Why Ozone Does NOT Need Full RDBMS Concurrency

The reviewer raised a valid concern: does leader-side execution require us to
implement RDBMS-style MVCC, transaction rollback, or two-phase commit?

**The answer is no.** Here is why:

| RDBMS concern | Why Ozone does not need it |
|---------------|---------------------------|
| Transaction rollback | Each request produces one atomic RocksDB `WriteBatch`. It either commits fully or not at all. Nothing to roll back. |
| MVCC (multiple versions) | All OM metadata lives in one Ratis group. No multi-shard transactions that need versioned visibility. |
| Two-phase commit (2PC) | All state is in one RocksDB. No distributed commit across multiple storage nodes needed. |
| Deadlock detection | Fixed lock ordering (bucket read → key/dir write) prevents deadlocks by design. |
| Write-ahead log for crash recovery | The Ratis log IS the write-ahead log. Apply is a replay of committed entries. |

**What we DO need (and have):**
- Fine-grained latching (striped locks) — same concept as CockroachDB's `latchManager`
- Atomic apply (RocksDB WriteBatch) — same as CockroachDB and YugabyteDB
- Leader-computed patches — same as CockroachDB PEVK and YugabyteDB

The systems that need full MVCC/rollback (CockroachDB, TiKV, YugabyteDB) do so
because their transactions can span **multiple Raft groups** (multiple shards).
A write may touch data on different nodes and must be atomic across all of them.
This requires provisional writes, 2PC, and rollback.

Ozone OM is different: all metadata lives in **one Ratis group**. Each request
is self-contained — it reads from the DB, computes changes, and produces one
atomic batch. There are no multi-entry transactions that span multiple log
entries. The complexity level we need is closer to CockroachDB's **latch
manager** (which is a simple concurrency primitive), not their full transaction
engine.

---

## 3. Proposal

### 3.1 High-Level Architecture

```
                         LEADER                                   FOLLOWER
                   +------------------+                     +------------------+
  Client Request   | startTransaction |                     | applyTransaction |
  ------------->   |                  |                     |                  |
                   |  1. Create       |   Ratis log entry   |  1. Parse proto  |
                   |     PlannedReq   |   (serialized       |  2. For each     |
                   |  2. preProcess   |    OMRequest with   |     DBDelta:     |
                   |  3. authorize    |    embedded         |     table.put/   |
                   |  4. acquireLock  |    BatchedState     |     delete       |
                   |  5. plan()       |    Transitions)     |  3. Commit batch |
                   |     -> deltas    | ------------------> |                  |
                   |  6. releaseLock  |                     |  (zero business  |
                   |  7. Build proto  |                     |   logic)         |
                   +------------------+                     +------------------+
```

The leader runs the full execution steps. The result is a list of raw
`{table, key_bytes, value_bytes, PUT/DELETE}` operations packed into a
`ReplicatedStateTransition` proto. Followers simply open a RocksDB batch,
loop through the deltas, and commit.

### 3.2 Proto Format: ReplicatedStateTransition

```protobuf
enum DeltaType {
  PUT = 0;
  DELETE = 1;
}

message DBDelta {
  required string tableName = 1;           // e.g. "keyTable", "openKeyTable"
  required bytes key = 2;                  // raw key bytes as stored in RocksDB
  optional bytes value = 3;                // raw value bytes (absent for DELETE like ops)
  required DeltaType type = 4;
}

message ReplicatedStateTransition {
  required int64 managedIndex = 1;         // OM-managed monotonic ID
  repeated DBDelta deltas = 2;             // the DB patch from above proto
  required OMResponse response = 3;        // pre-computed client response
  required Type cmdType = 4;               // for barrier detection
}

message BatchedStateTransitions {
  repeated ReplicatedStateTransition transitions = 1;
}
```

**Key decision: raw bytes, not domain objects.** Each delta carries the exact
bytes that will be written to the RocksDB column family. This means:
- Followers do **no deserialization** of `OmKeyInfo`, `OmBucketInfo`, etc.
- Any system with RocksDB access (e.g. Recon as a Ratis listener) can apply
  the same payload without knowing how OM commands work.
- The payload is a self-contained "DB patch" that does not depend on Java types.

The `BatchedStateTransitions` wrapper is placed on the `OMRequest` at field
number 200. The state machine checks `hasBatchedStateTransitions()` to decide
whether to use the new apply path.

### 3.3 Managed Index Service

**Problem:** With the current architecture, `objectID` for new keys/volumes
comes from the Ratis `transactionLogIndex`. In the new model, planning happens
in `startTransaction()` (before the log entry is written), so the Ratis index
is not yet available.

**Solution:** An OM-managed `AtomicLong` counter (`ManagedIndexService`) that:
- Returns a unique, always-increasing value via `getAndIncrement()`.
- Is saved together with each DB batch (stored in `TransactionInfoTable`
  under key `#MANAGED_INDEX`).
- Survives leader switchover: `onBecomeLeader(lastCommitted)` calls
  `max(currentIndex, lastCommittedIndex)`.
- Is recovered on restart from the saved value.

```java
public final class ManagedIndexService {
  private final AtomicLong currentIndex;

  public long getAndIncrement() { return currentIndex.incrementAndGet(); }
  public void updateCommitIndex(long committed) {
    currentIndex.updateAndGet(c -> Math.max(c, committed));
  }
  public void onBecomeLeader(long lastCommitted) {
    currentIndex.updateAndGet(c -> Math.max(c, lastCommitted));
  }
}
```

### 3.4 Leader Planning Framework

Three classes form the planning framework:

#### PlannedRequest (abstract template)

```java
public abstract class PlannedRequest {
  void preProcess(OzoneManager om);           // validation, normalization
  void authorize(OzoneManager om);            // ACL checks
  void acquireLocks(OzoneManager om);         // granular lock acquisition
  void plan(OzoneManager om, TransitionBuilder builder);  // core logic
  void releaseLocks(OzoneManager om);         // unlock
  OMResponse buildErrorResponse(Exception ex);
}
```

Each concrete `PlannedRequest` implements these methods. The template keeps
each step separate: validation is separate from authorization, which is
separate from the locked section (lock + plan).

#### TransitionBuilder (request-scoped)

```java
public final class TransitionBuilder {
  <V> void put(String tableName, String key, V value, Codec<V> codec);
  void delete(String tableName, String key);
  void setResponse(OMResponse response);
  ReplicatedStateTransition build(long managedIndex, Type cmdType);
}
```

The builder uses existing `Codec<V>` to convert values to raw bytes at
planning time. This ensures the bytes written during apply are the same as
what `TypedTable` would produce.

#### LeaderPlanner (orchestrator)

```java
public final class LeaderPlanner {
  ReplicatedStateTransition plan(PlannedRequest request) {
    long managedIndex = indexService.getAndIncrement();
    TransitionBuilder builder = new TransitionBuilder(managedIndex);
    try {
      request.preProcess(om);
      request.authorize(om);
      try {
        request.acquireLocks(om);
        request.plan(om, builder);
      } finally {
        request.releaseLocks(om);
      }
    } catch (Exception ex) {
      // build error response, return transition with empty deltas
    }
    return builder.build(managedIndex, request.getCmdType());
  }
}
```

### 3.5 Apply Engine

`StateTransitionApplyEngine` writes a `BatchedStateTransitions` proto to
RocksDB:

```java
public void applyBatch(BatchedStateTransitions batched, TermIndex termIndex) {
  List<Queue<ReplicatedStateTransition>> splits = splitAtBarriers(batched);
  for (Queue<ReplicatedStateTransition> segment : splits) {
    try (BatchOperation batch = store.initBatchOperation()) {
      for (ReplicatedStateTransition transition : segment) {
        for (DBDelta delta : transition.getDeltasList()) {
          Table table = store.getTable(delta.getTableName());
          if (delta.getType() == PUT)
            table.putWithBatch(batch, delta.getKey(), delta.getValue());
          else
            table.deleteWithBatch(batch, delta.getKey());
        }
      }
      // Also write TransactionInfo and ManagedIndex in same batch
      store.commitBatchOperation(batch);
    }
  }
}
```

**Barrier detection:** `CreateSnapshot` and `SnapshotPurge` operations are
applied alone (in their own separate batch). This keeps the same behavior as
`OzoneManagerDoubleBuffer.splitReadyBufferAtCreateSnapshot()`.

### 3.6 State Machine Integration

The `OzoneManagerStateMachine` runs a **dual-path** model:

- **Planned commands** (registered via `registerPlannedCommand(Type, creator)`):
  - In `startTransaction()`: call `LeaderPlanner.plan()`, embed the result as
    `BatchedStateTransitions` in the `OMRequest`, set as log data.
  - In `applyTransaction()`: detect `hasBatchedStateTransitions()`, route to
    `StateTransitionApplyEngine.applyBatch()`, return pre-computed `OMResponse`.

- **Legacy commands** (everything not registered): existing `runCommand()` path
  via `validateAndUpdateCache()` + `OzoneManagerDoubleBuffer`.

This dual-path allows step-by-step migration: commands are moved one at a time.

### 3.7 Granular Locking

A major benefit of leader-only execution is that locks only need to protect
the planning phase, not Ratis replication. This allows more fine-grained locking.

#### OBS Bucket Layout

| Operation | Locks Acquired |
|:----------|:---------------|
| CreateKey | Bucket **read** lock only |
| CommitKey | Bucket **read** lock + striped key **write** lock |

**Why CreateKey needs no key lock:** The open key table entry uses
`/volume/bucket/key/clientID` as its key, which is unique per client session.
Parallel creates to different keys (or even the same key name from different
clients) write to different rows and cannot conflict.

**Why CommitKey needs key write lock:** Commit changes the committed key entry.
Two commits to the *same* key must run one at a time to handle overwrite
correctly (move old key to deleted table, update quota).

Implementation uses Guava `Striped<ReadWriteLock>` (1024 stripes by default):
```java
public final class OBSKeyLockManager {
  private final Striped<ReadWriteLock> keyStripedLocks;
  private final IOzoneManagerLock omLock;  // for bucket read lock
}
```

#### FSO Bucket Layout

| Operation | Locks Acquired |
|:----------|:---------------|
| CreateKey/File | Bucket **read** lock + parent directory **write** lock |
| CommitKey/File | Bucket **read** lock + parent directory **write** lock |

FSO operations are ordered at the parent-directory level. Operations
in different directories run at the same time. The lock key is
`volumeId/bucketId/parentObjectId`.

```java
public final class FSOKeyLockManager {
  private final Striped<ReadWriteLock> parentDirStripedLocks;
  private final IOzoneManagerLock omLock;
}
```

**Lock ordering:** Always acquire the bucket read lock first, then the striped
lock. This prevents deadlocks because bucket read locks are shared (multiple
readers allowed) and the order is fixed.

---

## 4. Detailed Concurrency Model

This section explains step-by-step how concurrent requests interact. The goal is
to show that correctness is maintained without needing MVCC or rollback.

### 4.1 Life of a Request (Detailed)

```
Thread A (CreateKey /vol/bucket/key1):
  1. preProcess()  — validate key name format
  2. authorize()   — check ACLs
  3. acquireLock() — acquire bucket READ lock for /vol/bucket
  4. plan()        — read bucket info from DB, build OmKeyInfo,
                     write deltas to TransitionBuilder:
                       PUT openKeyTable /vol/bucket/key1/clientA → omKeyInfo bytes
                       PUT bucketTable /vol/bucket → updated bucket bytes (quota+1)
  5. releaseLock() — release bucket READ lock
  6. Submit ReplicatedStateTransition to Ratis
  7. Ratis replicates to quorum
  8. applyTransaction() on all nodes:
     — open RocksDB WriteBatch
     — write PUT openKeyTable /vol/bucket/key1/clientA → bytes
     — write PUT bucketTable /vol/bucket → bytes
     — commit batch atomically
  9. Return OMResponse to client
```

### 4.2 Concurrent Scenarios

#### Scenario A: Two CreateKey requests to different keys in the same bucket

```
Thread A: CreateKey /vol/bucket/key1     Thread B: CreateKey /vol/bucket/key2
  acquireLock(bucket READ)                 acquireLock(bucket READ)
  — Both succeed (READ locks are shared)
  plan() — reads bucket, builds key1       plan() — reads bucket, builds key2
  releaseLock()                            releaseLock()
  submit to Ratis                          submit to Ratis
```

**Why this is safe:** Both threads read the same bucket quota from DB. Both
produce a delta that increments quota by 1. When applied in sequence (Ratis
guarantees total ordering of log entries), each apply writes its own open key
entry (different rows: key1/clientA vs key2/clientB). The bucket quota updates
are also applied in order.

**Quota correctness:** Each planning thread reads the current committed quota
from DB (say quota=100). Thread A plans quota=101. Thread B also plans
quota=101. After apply, the DB has quota=101, not 102. This is acceptable for
CreateKey because:
- CreateKey allocates an open key — it is a reservation, not a final commit
- The actual quota enforcement happens at CommitKey time
- If needed, we can make CreateKey also acquire a striped key lock to serialize
  quota updates — but this reduces parallelism for a small benefit

**Alternative (stricter quota):** If exact quota tracking at CreateKey is needed,
we can use a bucket WRITE lock for CreateKey (same as today). This serializes
all creates within a bucket but keeps the other benefits (no follower execution,
no double buffer). The granular locking is an optimization that can be tuned.

#### Scenario B: Two CommitKey requests to the same key

```
Thread A: CommitKey /vol/bucket/key1     Thread B: CommitKey /vol/bucket/key1
  acquireLock(bucket READ)                 acquireLock(bucket READ)
  acquireLock(key WRITE for key1)          acquireLock(key WRITE for key1)
                                             — BLOCKS (write lock is exclusive)
  plan():
    read openKey from DB
    read existing key from keyTable (for overwrite)
    build deltas:
      DELETE openKeyTable/key1/clientA
      PUT keyTable/key1 → committed key bytes
      PUT deletedTable/old-key1 (if overwrite)
      PUT bucketTable → updated quota
  releaseLock(key WRITE)                   — Thread B now acquires key WRITE
  releaseLock(bucket READ)
  submit to Ratis                          plan():
                                             read openKey from DB
                                             ...
                                           releaseLock(key WRITE)
                                           releaseLock(bucket READ)
                                           submit to Ratis
```

**Why this is safe:** The key write lock ensures only one thread plans for the
same key at a time. Thread B cannot read stale state because it waits until
Thread A finishes planning and releases the lock.

**Important detail:** Between Thread A releasing the lock and its transition
being applied, Thread B reads from DB. At this point Thread A's changes are NOT
yet in the DB (they are in the Ratis log, waiting to be applied). Is this a
problem?

**Answer: No.** Thread B's open key entry is different from Thread A's (different
clientID). Thread B's commit produces a different set of deltas based on what it
reads from the DB. When both are applied in Ratis order, the last one wins —
which is the correct overwrite behavior.

However, there is a subtlety: if Thread A's commit would move the existing key
to the deleted table, and Thread B's commit also tries to move the same existing
key to the deleted table (because it reads the same "current key" from DB), we
would get a duplicate entry in the deleted table.

**Solution:** The apply engine processes entries in Ratis log order. Thread A's
transition applies first (moves old key to deleted table, writes new key).
Thread B's transition applies second (again reads the old key — but now from
Thread A's write — and moves it to deleted table). Since deletedTable keys
include a unique suffix (objectID or transaction index), both entries are
distinct. The background key deletion service handles cleanup.

#### Scenario C: CreateKey and DeleteBucket for the same bucket

```
Thread A: CreateKey /vol/bucket/key1     Thread B: DeleteBucket /vol/bucket
  acquireLock(bucket READ)                 acquireLock(bucket WRITE)
  — Thread A succeeds                       — BLOCKS (write lock waits for all
  plan()                                      readers to finish)
  releaseLock(bucket READ)
                                           — Thread B now acquires bucket WRITE
  submit to Ratis                          plan():
                                             check bucket is empty
                                             — Reads from DB; Thread A's new key
                                               is NOT yet applied. Bucket appears
                                               empty in DB.
                                             build delta: DELETE bucketTable
                                           releaseLock()
                                           submit to Ratis
```

**Problem:** Thread B's delete succeeds because the DB does not yet show Thread
A's key. When both apply, we have a key in a deleted bucket.

**Solution:** This is the same problem that exists today — bucket delete checks
the committed DB, and a concurrent create may have a pending entry in the double
buffer. The current solution is that bucket delete checks both the DB and the
open key table. In the planned path, we handle this the same way:
- DeleteBucket acquires a bucket WRITE lock (exclusive), which blocks all
  concurrent key creates (they need bucket READ lock)
- By the time DeleteBucket acquires the write lock, all prior key creates have
  finished planning and submitted to Ratis
- DeleteBucket then reads from DB. If a prior key was created (and already
  applied), it sees the key and fails the delete
- If the prior key has not yet been applied, the Ratis log ordering ensures:
  the CreateKey entry was submitted first (before the delete could acquire the
  write lock), so it appears before DeleteBucket in the log, and is applied first

**Key guarantee:** The bucket WRITE lock ensures no new keys can start planning
while the delete is planning. Combined with Ratis log ordering, this prevents
the race.

### 4.3 Lock Granularity Summary

| Operation | Lock(s) | Conflicts with |
|-----------|---------|----------------|
| OBS CreateKey | Bucket READ | DeleteBucket, SetBucketProperty |
| OBS CommitKey | Bucket READ + Key WRITE (striped) | Same key CommitKey, DeleteBucket |
| OBS DeleteKey | Bucket READ + Key WRITE (striped) | Same key CommitKey/DeleteKey |
| FSO CreateKey | Bucket READ + ParentDir WRITE (striped) | Same dir creates, DeleteBucket |
| FSO CommitKey | Bucket READ + ParentDir WRITE (striped) | Same dir operations, DeleteBucket |
| DeleteBucket | Bucket WRITE | All key operations in this bucket |
| SetBucketProperty | Bucket WRITE | All key operations in this bucket |
| CreateVolume | Volume WRITE | DeleteVolume |
| CreateSnapshot | None (barrier in apply) | N/A (applied in isolation) |

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
acceptable (see Scenario A above). For CommitKey, the key write lock prevents
this because commits to the same bucket-and-key are serialized.

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
| Bucket deleted while key create is in Ratis log | Bucket WRITE lock + Ratis ordering prevents this (see Scenario C) |
| ManagedIndex duplicates after leader change | `onBecomeLeader` calls `max(current, lastCommitted)` — always moves forward |
| Crash during RocksDB WriteBatch | Atomic batch — either fully committed or not. Ratis replays on restart |
| Follower apply fails | Fatal error — follower must catch up via Ratis snapshot (same as today) |
| Snapshot barrier missed | Apply engine checks `cmdType` — CreateSnapshot/SnapshotPurge always get own batch |

---

## 6. Testing Strategy

To ensure correctness and catch concurrency bugs, we need tests at multiple
levels.

### 6.1 Unit Tests

| Test class | What it validates |
|-----------|-------------------|
| `TestManagedIndexService` | Monotonic increments, leader switchover, crash recovery |
| `TestTransitionBuilder` | Correct proto generation from put/delete calls |
| `TestLeaderPlanner` | Template ordering (preProcess → authorize → lock → plan → unlock), error handling |
| `TestStateTransitionApplyEngine` | Atomic apply, barrier splitting, TransactionInfo update |
| `TestOBSKeyLockManager` | Lock ordering, stripe distribution, timeout behavior |
| `TestFSOKeyLockManager` | Parent-dir lock ordering, stripe distribution |

### 6.2 Concurrency Tests (Critical for Correctness)

These tests use multiple threads to stress the locking and planning path:

**Test: Parallel CreateKey same bucket**
```java
// 50 threads create keys in the same bucket simultaneously
// Assert: all keys exist in openKeyTable after apply
// Assert: no exceptions, no lost writes
ExecutorService pool = Executors.newFixedThreadPool(50);
for (int i = 0; i < 50; i++) {
  pool.submit(() -> createKey("/vol/bucket/key" + threadId));
}
// Verify all 50 keys in openKeyTable
```

**Test: Parallel CommitKey same key (overwrite race)**
```java
// 10 threads commit the same key name simultaneously
// Assert: exactly one wins (the rest get errors or overwrite correctly)
// Assert: deleted table has correct number of entries
// Assert: final key in keyTable is from the last applied commit
```

**Test: CreateKey + DeleteBucket race**
```java
// Thread A creates keys in rapid succession
// Thread B tries to delete the bucket
// Assert: either the bucket is deleted and no keys exist,
//         OR the bucket still exists with the created keys
// Assert: never a key in a deleted bucket
```

**Test: Leader failover during planning**
```java
// Start planning a request, then trigger leader change
// Assert: ManagedIndex moves forward on new leader
// Assert: retried request gets a new managedIndex
// Assert: no duplicate objectIDs
```

**Test: Quota consistency under concurrency**
```java
// 100 threads create + commit keys in parallel
// Assert: final bucket quota equals number of committed keys
// Assert: no negative quota, no over-count
```

### 6.3 Integration Tests (3-Node HA Cluster)

These run on a `MiniOzoneHAClusterImpl` with 3 OMs:

| Test | What it validates |
|------|-------------------|
| Write key, read from follower | All nodes have the same state after apply |
| Write key, kill leader, read from new leader | Failover preserves committed state |
| Write key, kill follower, bring back, verify sync | Ratis log replay works with new format |
| Mixed planned + legacy commands | Both paths coexist correctly |
| CreateSnapshot between planned commands | Barrier semantics preserved |
| Bulk write (10k keys), verify all 3 nodes | No lost writes under load |

### 6.4 Determinism Verification Test

A special test that validates the planned path produces the same DB state as the
legacy path:

```java
// For each migrated command:
// 1. Run it through the legacy path (validateAndUpdateCache) → capture DB state
// 2. Run the same request through the planned path (plan + apply) → capture DB state
// 3. Assert: both DB states are byte-for-byte identical
```

This test gives high confidence that the migration does not change behavior.

### 6.5 Stress/Chaos Tests

- **Thread starvation test:** Run with more concurrent requests than available
  lock stripes (>1024). Verify no deadlocks and eventual progress.
- **Slow Ratis test:** Add artificial delay in Ratis replication. Verify that
  planning still proceeds correctly (locks are not held during replication).
- **Random kill test:** Randomly kill OM nodes during key operations. Verify
  all committed operations survive, no partial state.

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
| **Recon compatibility** | Must understand all commands | Can apply the raw patch directly |

---

## 8. Migration Strategy

Commands are migrated one at a time:

1. Implement a `PlannedRequest` subclass (e.g. `OBSCreateKeyPlannedRequest`).
2. Register it via `stateMachine.registerPlannedCommand(Type.CreateKey, creator)`.
3. The state machine handles routing automatically.

**No backwards compatibility is required.** This is a clean-cut migration within
a release boundary. All nodes in the cluster run the same code, so there is no
mixed-version case to handle.

**Migration order (proposed):**
1. OBS CreateKey + CommitKey (highest throughput benefit)
2. OBS DeleteKey, RenameKey
3. FSO key operations
4. Volume/Bucket operations
5. Snapshot operations (barrier handling already in place)
6. Remaining commands

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

---

## Appendix: File Layout

```
hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/
  execution/
    ManagedIndexService.java          - Monotonic index management
    TransitionBuilder.java            - Request-scoped delta builder
    LeaderPlanner.java                - Planning orchestrator
  ratis/
    StateTransitionApplyEngine.java   - Raw byte apply engine
    OzoneManagerStateMachine.java     - Dual-path routing (modified)
  request/
    PlannedRequest.java               - Abstract execution template
    PlannedRequestCreator.java        - Factory interface
    key/
      OBSCreateKeyPlannedRequest.java - OBS CreateKey implementation
      OBSCommitKeyPlannedRequest.java - OBS CommitKey implementation
  lock/
    OBSKeyLockManager.java            - Striped key-level locking
    FSOKeyLockManager.java            - Striped parent-dir-level locking

hadoop-ozone/interface-client/src/main/proto/
  OmClientProtocol.proto              - ReplicatedStateTransition messages
```

## Appendix: References

- CockroachDB Proposer-Evaluated KV RFC:
  `github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20160420_proposer_evaluated_kv.md`
- CockroachDB concurrency control source:
  `pkg/kv/kvserver/concurrency/` (latch manager, lock table)
- YugabyteDB transaction architecture:
  `docs.yugabyte.com/preview/architecture/transactions/distributed-txns/`
- TiKV deep-dive:
  `tikv.org/deep-dive/`
- ScyllaDB Raft integration:
  `github.com/scylladb/scylladb/tree/master/raft/`
