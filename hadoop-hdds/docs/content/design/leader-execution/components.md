---
title: Leader-Planned Execution — Components
summary: ManagedIndex, LeaderPlanner, ChangeRecorder, ApplyEngine, and State Machine integration
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

# Components

This document describes the core components that implement leader-planned execution. For the high-level architecture and motivation, see the [overview](./overview.md).

## Table of Contents

1. [Managed Index Service](#1-managed-index-service)
2. [Leader Planning Framework](#2-leader-planning-framework)
3. [Apply Engine](#3-apply-engine)
4. [State Machine Integration](#4-state-machine-integration)

---

## 1. Managed Index Service

**Problem:** With the current architecture, `objectID` for new keys/volumes comes from the Ratis `transactionLogIndex`. In the new model, planning happens in `startTransaction()` (before the log entry is written), so the Ratis index is not yet available.

**Why must objectId be decided during planning?**

The `objectID` is part of the value that gets serialized into the DB delta. For example, when we create a new key:
1. We build an `OmKeyInfo` object — this object contains the `objectID` field
2. We serialize this `OmKeyInfo` to raw bytes via the codec
3. These raw bytes go into an `Operation` message in the `Batch` proto

The raw bytes are frozen at planning time. They get written as-is to RocksDB during apply. We cannot "fill in" the objectId later during apply because the bytes are already serialized and embedded in the Ratis log entry.

In the current architecture, this is not a problem: `validateAndUpdateCache` runs during `applyTransaction` where the Ratis log index is already known. So it uses `transactionLogIndex` directly as the objectId.

In the new architecture, planning runs in `startTransaction` which happens **before** Ratis assigns a log index. So we need our own counter.

**Solution:** An OM-managed `AtomicLong` counter (`ManagedIndexService`) that:
- Returns a unique, always-increasing value via `getAndIncrement()`.
- Is saved together with each DB batch (stored in `TransactionInfoTable` under key `#MANAGED_INDEX`).
- Survives leader switchover: `onBecomeLeader(lastCommitted)` calls `max(currentIndex, lastCommittedIndex)`.
- Is recovered on restart from the saved value.

```java
public final class ManagedIndexService {
  private final AtomicLong currentIndex;

  public long next() { return currentIndex.getAndIncrement(); }
  public void seedFromFinalization(long ratisIndexAtFinalize) {
    currentIndex.updateAndGet(c -> Math.max(c, ratisIndexAtFinalize + 1));
  }
  public void onBecomeLeader(long lastAppliedRatisIndex) {
    currentIndex.updateAndGet(c -> Math.max(c, lastAppliedRatisIndex + 1));
  }
  public void persist(BatchOperation batch) {
    // write #MANAGED_INDEX into the same atomic batch as the data patch
  }
}
```

**Mixed-mode objectID safety:** During the rolling-upgrade window, both the
legacy path and the new path must draw objectIDs from this single counter.
The legacy `OzoneManager.getObjectIdFromTxId(trxnId)` is retrofitted to call
`ManagedIndexService.next()` instead of using the raw Ratis transaction index.
This ensures old-path and new-path objectID ranges are disjoint by
construction. The objectID encoding format (`(epoch<<62)|(index<<8)`) is
unchanged — only the source of `index` changes. The low 8 bits go dead-zero
(the 256-wide recursive-directory window is retired for migrated commands).

**Persistence:** The counter's durable value is written into the same
`BatchOperation` as the data patch and the `#TRANSACTIONINFO` advance. The
triple (patch + transaction info + managed index) lands atomically or not at
all.

---

## 2. Leader Planning Framework

Three classes form the planning framework:

### 2.1 PlannedRequest (abstract base with step-iterator)

```java
public abstract class PlannedRequest {
  OMRequest preProcess(OzoneManager om);         // validation, normalization, auth-prep
  List<LockRequest> stepLocks(StepContext ctx);  // which stripes this step needs
  boolean hasNextStep();                         // dynamic iterator (single-step = one step)
  StepPlan nextStep();                           // returns the step to execute
  void advance(CommitResult committed);          // consume commit, possibly re-plan next step
}
```

The step-iterator model generalizes from "one request → one transition" to
"one request → an ordered chain of transitions." A single-step operation is
the degenerate N=1 case. Multi-step operations (FSO createFile with missing
parents, recursive delete) produce multiple steps dynamically — each step is
planned only after the previous step commits, because the committed state may
affect the next step.

**Why dynamic, not pre-computed:** A concurrent `rm -rf` can invalidate a
statically-planned chain. The planner must re-resolve per step, under the
lock, and revalidate that the target node still exists.

### 2.2 ChangeRecorder (request-scoped, replaces TransitionBuilder)

```java
public final class ChangeRecorder {
  void put(String columnFamily, byte[] key, byte[] value);
  void delete(String columnFamily, byte[] key);
  void merge(String columnFamily, byte[] key, String operatorId, byte[] operand);
  void checkpoint(long index);
  Batch toBatch();
}
```

The `ChangeRecorder` accumulates a `Batch` (from the inner proto layer)
instead of staging changes into the OM table cache. This is the structural
break from the legacy path: `plan()` records operations into a `Batch`, and
reads go straight to RocksDB. No table cache interaction.

**How raw bytes are computed:** Ozone already has `Codec<V>` classes for every
value type stored in RocksDB. The recorder uses these codecs to serialize
domain objects to bytes at planning time — the same serialization that
`TypedTable.put()` uses today.

**How raw bytes flow through the system:**

1. During planning on the leader: the `ChangeRecorder` uses existing `Codec<V>`
   classes (e.g., `OmKeyInfo.getCodec()`) to serialize domain objects to the
   exact `byte[]` that would be stored in RocksDB — the same serialization
   `TypedTable.put()` uses today.
2. These raw bytes go into `Operation` messages in the `Batch` proto.
3. The `Batch` is embedded in the Ratis log entry.
4. At apply time on all nodes: the `OperationApplier` writes the raw bytes
   directly to RocksDB column families — no deserialization needed.

### 2.3 LeaderPlanner (orchestrator)

The `LeaderPlanner` drives the request's step-iterator to completion. For
single-step operations, this is one iteration. For multi-step operations (FSO
createFile with missing parents), it loops until the iterator is exhausted.

```java
public final class LeaderPlanner {
  CompletableFuture<OMResponse> execute(PlannedRequest request) {
    request.preProcess(om);
    return driveSteps(request);
  }

  private CompletableFuture<OMResponse> driveSteps(PlannedRequest req) {
    if (!req.hasNextStep()) return completedFuture(req.buildResponse());

    StepPlan step = req.nextStep();
    List<LockRequest> locks = req.stepLocks(step.context());
    LockHandle handle = lockManager.acquire(sorted(locks));
    // Revalidate locked nodes (close resolve-to-lock window)
    step.revalidate(om);
    long index = indexService.next();
    Batch batch = step.plan(new ChangeRecorder(), om);
    // Submit to Ratis (lock still held under Option B)
    return submitToRatis(batch, index).thenCompose(commit -> {
      handle.release();  // released on continuation thread (non-thread-affine)
      req.advance(commit);
      return driveSteps(req);  // next step, if any
    });
  }
}
```

**Continuation-driven:** The worker thread is freed during each Ratis await.
The lock is held across the await (under Option B) and released on the
continuation thread — this is why the lock primitive must be non-thread-affine.
Under Option A, the lock is released before `submitToRatis`.

---

## 3. Apply Engine

The `OperationApplier` (in `hadoop-hdds/framework`) writes a `Batch` to
RocksDB. It is domain-agnostic — it imports no Ozone OM types.

```java
public final class OperationApplier {
  void apply(Batch batch, DBStore store, BatchOperation rocksBatch,
             MergeOperatorRegistry registry) {
    for (Operation op : batch.getOpsList()) {
      switch (op.getKind()) {
        case PUT:
          store.getTable(op.getColumnFamily())
               .putWithBatch(rocksBatch, op.getKey(), op.getValue());
          break;
        case DELETE:
          store.getTable(op.getColumnFamily())
               .deleteWithBatch(rocksBatch, op.getKey());
          break;
        case MERGE:
          MergeOperator resolver = registry.get(op.getMergeOperatorId());
          byte[] current = store.getTable(op.getColumnFamily()).get(op.getKey());
          byte[] merged = resolver.apply(current, op.getValue());
          store.getTable(op.getColumnFamily())
               .putWithBatch(rocksBatch, op.getKey(), merged);
          break;
        case CHECKPOINT:
          // flush current batch, take RocksDB checkpoint at this index
          break;
      }
    }
  }
}
```

The OM-level apply wrapper handles:
- Splitting at snapshot barriers (`CHECKPOINT` operations get their own batch)
- Writing `#TRANSACTIONINFO` and `#MANAGED_INDEX` in the same atomic batch
- Committing the batch

**Barrier detection:** `CreateSnapshot` emits a `CHECKPOINT` operation. The
apply wrapper detects this and applies it in its own separate RocksDB batch,
preserving the same semantics as `splitReadyBufferAtCreateSnapshot()`.

**Failure handling:** A follower that cannot apply a committed patch must crash
and re-sync (fail-stop). The state machine already has this discipline for
`INTERNAL_ERROR` / `METADATA_ERROR`. The planned-apply path inherits it.

---

## 4. State Machine Integration

The `OzoneManagerStateMachine` runs a **dual-path** model:

- **Planned commands** (registered via `registerPlannedCommand(Type, creator)`):
  - In `startTransaction()`: call `LeaderPlanner.plan()`, embed the result as
    `BatchedStateTransitions` in the `OMRequest`, set as log data.
  - In `applyTransaction()`: detect `hasBatchedStateTransitions()`, route to
    `StateTransitionApplyEngine.applyBatch()`, return pre-computed `OMResponse`.

- **Legacy commands** (everything not registered): existing `runCommand()` path
  via `validateAndUpdateCache()` + `OzoneManagerDoubleBuffer`.

This dual-path allows step-by-step migration: commands are moved one at a time.
See the [migration playbook](./migration-playbook.md) for the phased migration
plan.
