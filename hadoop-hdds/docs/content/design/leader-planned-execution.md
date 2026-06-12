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
2. [Proposal](#2-proposal)
   * [2.1 High-Level Architecture](#21-high-level-architecture)
   * [2.2 Proto Format: ReplicatedStateTransition](#22-proto-format-replicatedstatetransition)
   * [2.3 Managed Index Service](#23-managed-index-service)
   * [2.4 Leader Planning Framework](#24-leader-planning-framework)
   * [2.5 Apply Engine](#25-apply-engine)
   * [2.6 State Machine Integration](#26-state-machine-integration)
   * [2.7 Granular Locking](#27-granular-locking)
3. [Migration Strategy](#4-migration-strategy)

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

## 2. Proposal

### 2.1 High-Level Architecture

```
                         LEADER                                   FOLLOWER
                    +-----------------+                     +------------------+
  Client Request   |  startTransaction|                     | applyTransaction |
  ------------->   |                  |                     |                  |
                   |  1. Create       |   Ratis log entry   |  1. Parse proto  |
                   |     PlannedReq   |   (serialized       |  2. For each     |
                   |  2. preProcess   |    OMRequest with    |     DBDelta:     |
                   |  3. authorize    |    embedded          |     table.put/   |
                   |  4. acquireLock  |    BatchedState      |     delete       |
                   |  5. plan()       |    Transitions)      |  3. Commit batch |
                   |     -> deltas    | ------------------> |                  |
                   |  6. releaseLock  |                     |  (zero business  |
                   |  7. Build proto  |                     |   logic)         |
                   +-----------------+                     +------------------+
```

The leader runs the full execution steps. The result is a list of raw
`{table, key_bytes, value_bytes, PUT/DELETE}` operations packed into a
`ReplicatedStateTransition` proto. Followers simply open a RocksDB batch,
loop through the deltas, and commit.

### 2.2 Proto Format: ReplicatedStateTransition

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

### 2.3 Managed Index Service

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

### 2.4 Leader Planning Framework

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

### 2.5 Apply Engine

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

### 2.6 State Machine Integration

The `OzoneManagerStateMachine` runs a **dual-path** model:

- **Planned commands** (registered via `registerPlannedCommand(Type, creator)`):
  - In `startTransaction()`: call `LeaderPlanner.plan()`, embed the result as
    `BatchedStateTransitions` in the `OMRequest`, set as log data.
  - In `applyTransaction()`: detect `hasBatchedStateTransitions()`, route to
    `StateTransitionApplyEngine.applyBatch()`, return pre-computed `OMResponse`.

- **Legacy commands** (everything not registered): existing `runCommand()` path
  via `validateAndUpdateCache()` + `OzoneManagerDoubleBuffer`.

This dual-path allows step-by-step migration: commands are moved one at a time.

### 2.7 Granular Locking

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

## 3. Migration Strategy

Commands are migrated one at a time:

1. Implement a `PlannedRequest` subclass (e.g. `OBSCreateKeyPlannedRequest`).
2. Register it via `stateMachine.registerPlannedCommand(Type.CreateKey, creator)`.
3. The state machine handles routing automatically.

**No backwards compatibility is required.** This is a clean-cut migration within
a release boundary. All nodes in the cluster run the same code, so there is no
mixed-version case to handle.
