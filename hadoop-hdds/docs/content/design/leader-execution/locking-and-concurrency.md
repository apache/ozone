---
title: Leader-Planned Execution — Locking and Concurrency
summary: Granular locking model, lock manager, hold span decision, life of a request, limitations, and retry strategy
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

# Locking and Concurrency

This document describes the fine-grained locking model that underpins
leader-planned execution. For operation-specific lock usage, see the
[OBS design](./obs-design.md) and [FSO design](./fso-design.md) documents.
For the high-level architecture, see the [overview](./overview.md).

## Table of Contents

1. [Granular Locking](#1-granular-locking)
2. [Lock Hold Span (Decision Required)](#2-lock-hold-span-decision-required)
3. [Life of a Request](#3-life-of-a-request)
4. [Lock Granularity Summary](#4-lock-granularity-summary)
5. [Known Limitations and Accepted Tradeoffs](#5-known-limitations-and-accepted-tradeoffs)
6. [Retry and Idempotency (Deferred Decision)](#6-retry-and-idempotency-deferred-decision)

---

## 1. Granular Locking

A major benefit of leader-only execution is that locks only need to protect
the planning phase on the leader. This allows fine-grained, objectID-keyed
locking that is rename-stable and ABA-free.

### 1.1 The lock model: containers and slots

Every namespace node plays two roles, each with its own lock namespace:

- **Container lock** — keyed by a directory's **objectID**. Governs "what
  children exist under me." Modes: **S** (shared) = "I am adding/touching one
  child"; **X** (exclusive) = "I am emptying/removing this directory."
- **Slot lock** — keyed by **(parentObjectID, name)** (the DB key minus the
  vol/bucket prefix). Governs "the existence/identity of this one entry."
  Taken **X** by whoever creates/commits/deletes/renames that specific entry.

The **bucket** is the root container. It additionally carries bucket-level
properties (quota limits, default ACLs, replication, encryption) that every
key operation reads, so the bucket lock is taken **S** by every key operation
and **X** by `SetBucketProperty`/`DeleteBucket`.

**Why objectID-keyed, not path-keyed:** A directory rename is O(1) in FSO —
the directory keeps its objectID; only its own entry changes parent. Children
are never touched because they key on the parent's (unchanged) objectID. A
path-keyed lock would be invalidated by rename; an objectID-keyed lock
survives it. ObjectIDs are never reused (monotonic from ManagedIndex), so
there is no ABA problem.

### 1.2 Per-operation lock matrix

`P` = immediate parent directory; `D` = the directory being deleted;
`P1/P2` = source/dest parent of a rename. All locks are acquired in the
total order defined in [Section 1.4](#14-deadlock-avoidance).

| Operation | Bucket | Container | Slot | Notes |
|:----------|:-------|:----------|:-----|:------|
| createKey (OBS) | S | — | — | open key carries `clientID`; creates never collide |
| createFile (FSO) | S | S(P) | — | open file carries `clientID`; same as OBS |
| createDir (FSO) | S | S(P) | X(P, dirName) | dir name has no `clientID` so uniqueness needed |
| commit (OBS) | S | — | X(bucket, key) | two commits of same key serialize |
| commit (FSO) | S | S(P) | X(P, fileName) | |
| deleteFile | S | S(P) | X(P, fileName) | |
| deleteDir (empty) | S | S(P) | X(P, dirName) + **X(D)** | X(D) blocks new children during the empty-check |
| rename | S | S(P1) + S(P2) | X(P1, srcName) + X(P2, dstName) | no X on the moved node — children untouched |
| SetBucketProperty / DeleteBucket | X | — | — | exclusive bucket lock |

**Key design choices:**
- **createKey/createFile take no slot lock** (only `S(parent)`): the open
  key/file table key includes `clientID`, so concurrent creates of the same
  name by different clients write distinct rows and cannot collide. Same-name
  resolution is deferred to commit where `X(parent, name)` is taken.
- **rename takes no container lock on the moved node.** Children key on the
  moved node's unchanged objectID and are untouched, so a concurrent create
  under the moved directory does not conflict with the rename.

### 1.3 Lock manager implementation

The existing `OzoneManagerLock` is not reused for this design. It is built on
`ReentrantReadWriteLock` which is thread-affine (cannot be released on a
different thread than acquired). Under Option B of the lock hold span (see
[Section 2](#2-lock-hold-span-decision-required)), the lock must be held
across the Ratis await and released on the continuation thread.

The new lock manager is a **fixed striped array** of non-thread-affine RW
primitives:

- **Striped, not per-key.** A key hashes to a stripe index. With a
  generously-sized array (~2^20 stripes, ~64 MB), false-contention collisions
  are in the low tens at peak and cost only latency, never correctness.
- **Non-thread-affine primitive:** a fair counting `Semaphore` per stripe used
  as an RW lock — **S** = acquire 1 permit, **X** = acquire N permits (N = the
  per-stripe permit ceiling). Releasable on any thread; fairness prevents
  writer starvation.
- **Handle-owned release.** `acquire(sortedReqs)` returns a `LockHandle`
  (`AutoCloseable`); the operation releases it in its completion path, possibly
  on the continuation thread.
- **No lock timeout.** The timeout lives on the Ratis request (existing). When
  the Ratis operation times out or errors, the holder fails and releases the
  lock in its `finally`. A genuinely hung OM is handled by failover (locks are
  leader-local and discarded on leader change), not by a lock timer.

```java
public final class StripedSemaphoreLockManager {
  private final Semaphore[] stripes;  // ~2^20 entries, fair
  private final int permitsPerStripe; // N; X drains all, S takes 1

  public LockHandle acquire(List<LockRequest> sortedDedupedReqs) {
    // reqs pre-sorted by stripe index; deduped to strongest mode
    for (LockRequest req : sortedDedupedReqs) {
      stripes[req.stripeIndex()].acquire(req.isExclusive() ? permitsPerStripe : 1);
    }
    return new LockHandle(sortedDedupedReqs);
  }
}

public interface LockHandle extends AutoCloseable {
  void release();  // any thread; reverse order; idempotent
}
```

### 1.4 Deadlock avoidance

All locks for an operation are sorted by a single total order and acquired in
that order; released in reverse. The comparator over lock identities:

1. By the lock's primary ID — `objectID` for a container, `parentObjectID` for
   a slot. The bucket (lowest objectID) sorts first.
2. Tiebreak: container-before-slot when IDs coincide.
3. Tiebreak: slots by `name`.

This is deadlock-free purely by uniform total ordering — not by an
ancestor-first rule, which rename can invert.

---

## 2. Lock Hold Span (Decision Required)

There are two valid approaches to when locks are released. **This is a
performance-vs-correctness tradeoff that needs a decision.**

**Option A — Release before Ratis submit:**
```
acquireLock → plan() → releaseLock → submit to Ratis → ... → applyTransaction
```
The lock protects only the planning phase. After planning, the lock is released
and Ratis replication happens without holding any lock.

**Option B — Hold through Ratis commit and apply:**
```
acquireLock → plan() → submit to Ratis → ... → applyTransaction → releaseLock
```
The lock is held until the data is durable in RocksDB. A second request on the
same key blocks until the first request's data is visible in the DB.

**Trade-offs:**

| Aspect | Option A (release before) | Option B (hold through apply) |
|--------|--------------------------|-------------------------------|
| Parallelism | Higher — lock held only during CPU work (~microseconds) | Lower — lock held during Ratis round-trip (~2-5ms) |
| Read-your-writes | Requires a cache or conditional ordering for the plan-to-apply gap | Guaranteed — successor reads committed state directly from RocksDB |
| Table cache removal | Cannot fully remove the table cache; reads during the gap may see stale data | Table cache can be fully removed; reads always go to RocksDB |
| Lock primitive | Standard `ReadWriteLock` works (thread-affine OK) | Needs non-thread-affine semaphore (release on continuation thread) |
| Multi-step FSO ops | Each step must handle the gap; createFile chain needs predecessor's objectID from DB but it may not be applied yet | Each step's commit is visible before the next step locks; chain "just works" |
| Complexity | Simpler lock primitive; more complex gap-handling logic | More complex lock primitive; simpler correctness reasoning |
| Quota correctness | Parallel creates can drift (read same base, both increment by 1, final = base+1 not base+2) | No drift — each successor reads committed predecessor state |

**Arguments for Option A:**
- CockroachDB uses this model (latches released before Raft propose).
- Higher throughput for independent operations.
- Standard Java `ReentrantReadWriteLock` suffices.

**Arguments for Option B:**
- Removes the table cache entirely (a known source of OM bugs). Read-your-writes is structural, not cache-dependent.
- Multi-step FSO operations (createFile with missing parents) are trivially correct — each step sees the previous step's committed state.
- Eliminates an entire class of plan-to-apply gap bugs.
- The non-thread-affine semaphore is a one-time implementation cost.
- Locks are held only during Ratis round-trip (~2-5ms), not during network I/O to external services. The throughput impact is bounded.

**Note on multi-step operations:** Under Option A, a `createFile /a/b/c/file` where `/a/b/c` is missing must create `/b` → submit → wait for apply → create `/c` → submit → wait for apply → create open-file. Each intermediate step must wait for its predecessor to be applied before the next step can read the committed objectID. Under Option B, the lock hold span itself provides this guarantee — the next step blocks on the lock until the predecessor's bytes are in RocksDB.

**Recommendation:** Option B provides stronger guarantees for the same operations that are hardest to get right (multi-step FSO, recursive delete, quota). The non-thread-affine semaphore is a bounded implementation cost. The table cache removal enabled by Option B eliminates a known bug class. However, if throughput testing shows Option B is unacceptable for OBS workloads (which are single-step and do not need read-your-writes across requests), a hybrid approach is possible: Option B for FSO/multi-step commands, Option A for simple OBS commands.

---

## 3. Life of a Request

This section traces a single request through the planned-execution pipeline.

```
Thread A (CreateKey /vol/bucket/key1):
  1. preProcess()  — validate key name format
  2. authorize()   — check ACLs
  3. acquireLock() — acquire bucket READ lock for /vol/bucket
  4. plan()        — read bucket info from DB, build OmKeyInfo,
                     write deltas to ChangeRecorder:
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

---

## 4. Lock Granularity Summary

Restated in container/slot terminology (see [Section 1](#1-granular-locking)):

| Operation | Bucket | Container | Slot | Conflicts with |
|-----------|--------|-----------|------|----------------|
| OBS CreateKey | S | — | — | DeleteBucket, SetBucketProperty |
| OBS CommitKey | S | — | X(bucket, key) | Same-key Commit/Delete, DeleteBucket |
| OBS DeleteKey | S | — | X(bucket, key) | Same-key Commit/Delete, DeleteBucket |
| FSO createFile | S | S(parent) | — | DeleteBucket (creates never conflict) |
| FSO createDir | S | S(parent) | X(parent, name) | Same-name createDir, DeleteBucket |
| FSO commitFile | S | S(parent) | X(parent, name) | Same-name operations, DeleteBucket |
| FSO deleteFile | S | S(parent) | X(parent, name) | Same-name operations, DeleteBucket |
| FSO deleteDir (empty) | S | S(parent) | X(parent, name) + X(dir) | Children creates, same-name ops |
| FSO rename | S | S(srcParent) + S(dstParent) | X(srcParent, src) + X(dstParent, dst) | Same-name ops at either end |
| FSO recursive delete | S | S(parent) | X(parent, name) + X(root) | All descendant operations |
| DeleteBucket | X | — | — | All key operations in this bucket |
| SetBucketProperty | X | — | — | All key operations in this bucket |
| CreateSnapshot | — | — | — | Barrier in apply (own batch) |

---

## 5. Known Limitations and Accepted Tradeoffs

These are deliberate design choices, not gaps. They are stated explicitly so
reviewers read them as accepted tradeoffs.

**Soft quota limit enforcement (accepted):**

Because key commits take only a shared bucket lock (so commits to different
keys run in parallel — the core throughput goal), and `usedBytes` is updated
by a commutative Merge operator rather than read-modify-written under an
exclusive lock, the quota *limit* is enforced best-effort, not exactly. Two
or more commits in flight can each pass the quota check against the same
pre-increment `usedBytes` and then both apply, transiently over-committing
the limit by up to the in-flight commit count.

What IS guaranteed:
- The `usedBytes`/`usedNamespace` counter never loses an update — it always
  equals the true committed size. No double-count, no lost decrement.
- Only the *limit gate* is soft. The counter itself is exact.

Mitigation:
- The existing `QuotaRepair` background service reconciles any drift.
- A future leader-local atomic reservation (check-and-reserve in memory,
  DB Merge remains the durable truth, decrement-on-abort, rebuild-from-DB
  on failover) can enforce exact admission if needed. This is a separate
  enhancement tracked under the open quota-enforcement decision.

**Non-atomic multi-step createFile (accepted):**

A failed multi-step `createFile` may leave empty intermediate directories.
This matches `mkdir -p` semantics, is idempotent on retry (create-dir on
an existing dir is a no-op), and is low-harm. Not cleaned eagerly.

**Lazy quota release on recursive delete (accepted):**

`rm -rf` releases quota as the background purge drains each node, not
synchronously with the root tombstone. The namespace root disappears
synchronously (fast UX) but `usedBytes`/`usedNamespace` converge over time
as nodes are purged. Synchronous release would require maintaining a
subtree-size aggregate on every create/commit/delete up the chain — a
separate enhancement if ever needed.

**Eventual subtree purge (accepted):**

Recursive-delete subtree reclamation is asynchronous. Descendants are
reclaimed over time by the `DirectoryDeletingService`. The root tombstone
provides immediate user-visible deletion semantics.

**No lock timeout (by design):**

Locks have no timer. The timeout that bounds a wait lives on the Ratis
request (existing). When the Ratis operation times out or errors, the holder
fails and releases the lock in its `finally`. A genuinely hung OM is handled
by failover (locks are leader-local and discarded on leader change), not by
a lock timer. A holder-lease that expired a held lock during an in-flight
Ratis operation would violate mutual exclusion.

---

## 6. Retry and Idempotency (Deferred Decision)

This section documents the retry/idempotency mechanism as a **deferred
decision** — not omitted by oversight. The framework reserves the seam for
it, and a reader should not mistake its absence for a gap.

### 6.1 Why it is deferred

The choice of retry mechanism depends on a per-operation classification that
does not yet exist: which operations are idempotent on re-execution (safe to
re-plan from scratch on failover retry) and which are not (would
double-apply). That classification gates the design.

### 6.2 The two idempotency questions

There are two different idempotency questions with opposite answers:

1. **Is the DB patch idempotent?** Almost always YES. The patch is
   whole-object Put/Delete over deterministic keys. Applying the same bytes
   twice is a no-op. This is why a follower can crash-and-resync and re-apply
   committed entries safely.

2. **Is re-execution (re-planning from scratch) idempotent?** NO for any
   operation that, during planning, consumes a non-idempotent external
   resource or emits a commutative Merge. Specifically:

| Category | Operations | Why non-idempotent on re-exec |
|----------|-----------|-------------------------------|
| SCM block allocation | CreateKey, AllocateBlock, CreateFile | Fresh `allocateBlock` returns new block IDs each call |
| Quota Merge | CommitKey, DeleteKey, CommitMPUPart, CompleteMPU, DeleteKeys | A re-planned `+delta` Merge double-counts |
| Table move | SnapshotMoveDeletedKeys, SnapshotMoveTableKeys | Re-moving already-moved keys double-counts reclaim |

That is ~10 operations out of ~47 total. The naturally-idempotent pure
Put/Delete operations (all of Group A, plus renames) tolerate weaker
handling.

### 6.3 Two candidate mechanisms (neither is chosen)

**(a) Leader-local in-flight registry + durable replicated table:**
- `(clientId, callId) → CompletableFuture<response>` in memory on the leader
  deduplicates concurrent retries cheaply.
- `(clientId, callId) → response` table in RocksDB, written **atomically with
  the data batch**, survives failover. This is the likely invariant for
  non-idempotent operations.

**(b) In-memory-only retry state:**
- Weaker — loses deduplication on failover.
- Tolerable only for naturally-idempotent operations.

The atomic-with-data-batch property is the leading candidate for the ~10
non-idempotent operations. The purely-idempotent operations (Group A) may
need nothing beyond their natural idempotency.

### 6.4 What IS fixed (regardless of mechanism)

- The retry-cache entry is written by the **terminal step only** of a
  multi-step request. Intermediate sub-steps are idempotent by structure
  (create-dir on an existing dir is a no-op) and need no entry.
- The name is `retryCache` (aligned with Ratis terminology).

### 6.5 Phasing consequence

Each phase that introduces a non-idempotent operation (P-1: SCM + quota,
P-3: snapshot moves, P-4: MPU, P-5: batch DeleteKeys) leaves the
terminal-step retry-cache write point as a seam. When the retry decision
lands, the durable atomic-with-batch entry is wired in at that seam for the
~10 non-idempotent operations without touching the idempotent ones.

### 6.6 Items needing clarity before this decision can be taken

- **Per-operation idempotency audit:** Each of the ~47 write operations must
  be classified as idempotent or non-idempotent under re-execution. The
  inventory in the [migration playbook](./migration-playbook.md) provides the
  structural classification; the audit must verify edge cases (e.g., is a
  re-planned DeleteKey that soft-deletes an already-deleted key truly safe,
  or does it double-decrement quota?).
- **Retry entry lifetime and eviction:** How long does a durable retry entry
  live? Is it scoped to the client session or time-bounded? The existing
  Ratis retry cache uses configurable expiry — should the durable table
  follow the same model?
- **Interaction with batching:** If multiple transitions are ever batched
  into one Ratis entry, each carries its own `(clientId, callId)`. The retry
  table must handle per-transition dedup within a batch, not just per-entry.
- **SCM block allocation on retry:** A re-planned CreateKey gets fresh block
  IDs from SCM. The old block IDs (from the first plan attempt that was lost)
  are now orphaned SCM allocations. How are they reclaimed? Today Ozone has
  block-GC for open keys that expire — does the same path cover this, or does
  it need explicit cleanup?
