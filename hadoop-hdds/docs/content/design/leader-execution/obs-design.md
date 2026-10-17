---
title: Leader-Planned Execution — OBS Design
summary: OBS key operations under leader-planned execution — lock usage, concurrency scenarios, and quota handling
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

# OBS Design

This document describes how Object Store (OBS) bucket-layout key operations
work under leader-planned execution. For the general locking model, see
[locking and concurrency](./locking-and-concurrency.md). For the high-level
architecture, see the [overview](./overview.md).

## Table of Contents

1. [OBS Lock Matrix](#1-obs-lock-matrix)
2. [Concurrent Scenarios](#2-concurrent-scenarios)
3. [Quota Handling (Decision Required)](#3-quota-handling-decision-required)
4. [OBS Operation Inventory](#4-obs-operation-inventory)

---

## 1. OBS Lock Matrix

OBS operations use a flat key namespace within a bucket. There is no directory
hierarchy, so container locks (which protect parent-child relationships) are
not used. The bucket itself serves as the root container.

| Operation | Bucket | Container | Slot | Notes |
|:----------|:-------|:----------|:-----|:------|
| CreateKey | S | — | — | open key carries `clientID`; creates never collide |
| CommitKey | S | — | X(bucket, key) | two commits of same key serialize |
| DeleteKey | S | — | X(bucket, key) | same-key Commit/Delete serialize |
| DeleteBucket | X | — | — | exclusive bucket lock blocks all operations |
| SetBucketProperty | X | — | — | exclusive bucket lock blocks all operations |

**Key design choices:**

- **CreateKey takes no slot lock** (only bucket `S`): the open key table key
  includes `clientID`, so concurrent creates of the same key name by different
  clients write distinct rows and cannot collide. Same-name resolution is
  deferred to commit where `X(bucket, key)` is taken.
- **CommitKey/DeleteKey take a slot lock** `X(bucket, key)`: two commits or a
  commit and a delete of the same key must serialize to prevent conflicting
  overwrites.

---

## 2. Concurrent Scenarios

### Scenario A: Two CreateKey requests to different keys in the same bucket

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

**Quota correctness:** Each planning thread reads the current committed quota from DB (say quota=100). Thread A plans quota=101. Thread B also plans quota=101. After apply, the DB has quota=101, not 102. This is acceptable for CreateKey because:
- CreateKey allocates an open key — it is a reservation, not a final commit
- The actual quota enforcement happens at CommitKey time
- If needed, we can make CreateKey also acquire a striped key lock to serialize quota updates — but this reduces parallelism for a small benefit

**Alternative (stricter quota):** If exact quota tracking at CreateKey is needed, we can use a bucket WRITE lock for CreateKey (same as today). This serializes all creates within a bucket but keeps the other benefits (no follower execution, no double buffer). The granular locking is an optimization that can be tuned.

### Scenario B: Two CommitKey requests to the same key

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

**Important detail:** Between Thread A releasing the lock and its transition being applied, Thread B reads from DB. At this point Thread A's changes are NOT yet in the DB (they are in the Ratis log, waiting to be applied). Is this a problem?

**Answer: No.** Thread B's open key entry is different from Thread A's (different clientID). Thread B's commit produces a different set of deltas based on what it reads from the DB. When both are applied in Ratis order, the last one wins — which is the correct overwrite behavior.

However, there is a subtlety: if Thread A's commit would move the existing key to the deleted table, and Thread B's commit also tries to move the same existing key to the deleted table (because it reads the same "current key" from DB), we would get a duplicate entry in the deleted table.

**Solution:** The apply engine processes entries in Ratis log order. Thread A's transition applies first (moves old key to deleted table, writes new key). Thread B's transition applies second (again reads the old key — but now from Thread A's write — and moves it to deleted table). Since deletedTable keys include a unique suffix (objectID or transaction index), both entries are distinct. The background key deletion service handles cleanup.

### Scenario C: CreateKey and DeleteBucket for the same bucket

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

**Problem:** Thread B's delete succeeds because the DB does not yet show Thread A's key. When both apply, we have a key in a deleted bucket.

**Solution:** This is the same problem that exists today — bucket delete checks the committed DB, and a concurrent create may have a pending entry in the double buffer. The current solution is that bucket delete checks both the DB and the open key table. In the planned path, we handle this the same way:
- DeleteBucket acquires a bucket WRITE lock (exclusive), which blocks all concurrent key creates (they need bucket READ lock)
- By the time DeleteBucket acquires the write lock, all prior key creates have finished planning and submitted to Ratis
- DeleteBucket then reads from DB. If a prior key was created (and already applied), it sees the key and fails the delete
- If the prior key has not yet been applied, the Ratis log ordering ensures: the CreateKey entry was submitted first (before the delete could acquire the write lock), so it appears before DeleteBucket in the log, and is applied first

**Key guarantee:** The bucket WRITE lock ensures no new keys can start planning while the delete is planning. Combined with Ratis log ordering, this prevents the race.

---

## 3. Quota Handling (Decision Required)

Bucket quota (`usedBytes`/`usedNamespace`) is a counter that parallel commits
race on. If updating quota requires an exclusive bucket lock, commits serialize
and the throughput benefit of fine-grained locking is lost.

### Option A — Direct PUT under bucket shared lock (simpler, soft quota limit)

Each planned commit reads the current `OmBucketInfo` from RocksDB, increments `usedBytes`/`usedNamespace` locally, and emits a whole-row `PUT` of the updated `OmBucketInfo`. Because commits hold only a shared bucket lock, two parallel commits can both read the same `usedBytes=100`, both write `usedBytes=101`, and the second apply overwrites the first — losing one increment.

- **Quota counter accuracy:** The `usedBytes` counter can drift under parallel commits. The existing `QuotaRepair` background service reconciles it.
- **Quota limit enforcement:** The limit check is best-effort (soft). Two commits near the limit can both pass the check and both apply, transiently exceeding the quota by up to the in-flight commit count.
- **Simplicity:** No new primitive needed. Standard whole-row PUT.

### Option B — Commutative RocksDB Merge operator (more complex, exact counter)

Each planned commit records the quota change as a `Merge` operation in the replicated batch: `merge(BUCKET_TABLE, bucketKey, "quota", encode(+delta))`. At apply time on every node, a registered `QuotaMergeOperator` runs in Ratis order: it reads the current `OmBucketInfo` row, applies the delta, and writes the whole row back.

- **Quota counter accuracy:** The `usedBytes` counter is always exact (never loses an update). Deltas are summed in Ratis order at apply time.
- **Quota limit enforcement:** Still best-effort (soft) because the limit check runs at planning time, before the Merge is applied. Two parallel commits can both pass the check. The counter is exact; only the limit gate is soft.
- **No on-disk format change:** The `bucketTable` value remains the same `OmBucketInfo` bytes — the Merge operator writes the whole row, not an operand list. No RocksDB-native merge operator is needed.
- **Registration invariant:** The Merge operator must be registered on every node before any node can receive a Merge operation. Otherwise a follower hits an unknown operator ID and must crash.

### Trade-offs

| Aspect | Option A (direct PUT) | Option B (Merge operator) |
|--------|----------------------|--------------------------|
| Counter accuracy | Can drift under parallel commits | Always exact |
| Limit enforcement | Soft (same as Option B) | Soft (same as Option A) |
| Complexity | None — standard PUT | New Merge framework; operator per counter |
| Recovery | QuotaRepair reconciles drift | No reconciliation needed |
| Bucket serialization | Last-writer-wins on parallel quota PUTs | Commutative — parallel commits safe |

**Recommendation:** Option B ensures the quota counter is always exact, which eliminates a class of silent-drift bugs and reduces reliance on the `QuotaRepair` background reconciliation. The implementation cost is bounded (one Merge operator for quota, registered at startup on all nodes). The quota **limit** remains soft under both options, which is an accepted tradeoff for parallelism.

---

## 4. OBS Operation Inventory

OBS operations in the migration plan (from the
[full operation inventory](./migration-playbook.md#3-full-operation-inventory)):

| Operation | Tables | Quota? | SCM? | Phase |
|-----------|--------|--------|------|-------|
| CreateKey (OBS) | openKeyTable | usage | yes | P-1 |
| CommitKey (OBS) | keyTable, openKeyTable, deletedTable | usage | no | P-1 |
| AllocateBlock (OBS) | openKeyTable | no | yes | P-1 |
| DeleteKey (OBS) | keyTable, deletedTable | usage | no | P-1 |

All four are single-step operations. They are migrated in Phase P-1 as the
hardest single-step operations (they involve SCM block allocation and/or
commutative quota updates).
