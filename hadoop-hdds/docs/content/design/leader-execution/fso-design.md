---
title: Leader-Planned Execution — FSO Design
summary: FSO operations under leader-planned execution — multi-step chains, recursive delete, revalidation, and recovery
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

# FSO Design

This document describes how File System Optimized (FSO) bucket-layout
operations work under leader-planned execution. FSO operations are the hardest
to migrate because they involve multi-step chains, recursive deletes, and
directory-tree consistency. For the general locking model, see
[locking and concurrency](./locking-and-concurrency.md). For the high-level
architecture, see the [overview](./overview.md).

## Table of Contents

1. [FSO Lock Matrix](#1-fso-lock-matrix)
2. [Concurrent Scenarios](#2-concurrent-scenarios)
3. [Multi-Step Operations](#3-multi-step-operations)
4. [Revalidation After Lock Acquisition](#4-revalidation-after-lock-acquisition)
5. [Recovery](#5-recovery)
6. [FSO Operation Inventory](#6-fso-operation-inventory)

---

## 1. FSO Lock Matrix

FSO operations use a hierarchical directory namespace. Container locks protect
the parent-child relationship (what children exist under a directory). Slot
locks protect the existence of individual entries.

| Operation | Bucket | Container | Slot | Notes |
|:----------|:-------|:----------|:-----|:------|
| createFile | S | S(P) | — | open file carries `clientID`; same as OBS |
| createDir | S | S(P) | X(P, dirName) | dir name has no `clientID` so uniqueness needed |
| commitFile | S | S(P) | X(P, fileName) | |
| deleteFile | S | S(P) | X(P, fileName) | |
| deleteDir (empty) | S | S(P) | X(P, dirName) + **X(D)** | X(D) blocks new children during empty-check |
| rename | S | S(P1) + S(P2) | X(P1, srcName) + X(P2, dstName) | no X on the moved node — children untouched |
| recursive delete | S | S(P) | X(P, name) + X(root) | All descendant operations blocked |

`P` = immediate parent directory; `D` = the directory being deleted;
`P1/P2` = source/dest parent of a rename.

**Key design choices:**

- **createFile takes no slot lock** (only `S(parent)`): the open file table
  key includes `clientID`, so concurrent creates of the same filename by
  different clients write distinct rows. Uniqueness is resolved at commit.
- **rename takes no container lock on the moved node.** FSO rename is O(1) —
  children key on the moved node's unchanged objectID and are untouched.
  A concurrent create under the moved directory does not conflict with rename.
- **deleteDir takes X(D) (exclusive container lock on the directory being
  deleted).** This blocks any new child operations from starting while the
  empty-check runs.

---

## 2. Concurrent Scenarios

### Scenario D: FSO createFile with missing parents vs concurrent rm-rf

```
Thread A: createFile /vol/bucket/a/b/c/file   Thread B: rm -rf /vol/bucket/a
  resolve path: a exists (objectID=10),         acquireLock: S(bucket), S(parent-of-a),
    b exists (objectID=20), c missing              X(parent-of-a, "a"), X(a-container)
  Step 1: create dir c under b                  plan(): tombstone dir a → deletedDirTable
    acquireLock: S(bucket), S(b-container=20)   releaseLock, submit to Ratis
    revalidate: b still exists? YES             ... Ratis commits, a is tombstoned ...
    plan(): PUT directoryTable c
    releaseLock, submit to Ratis
  ... Ratis commits dir c ...
  Step 2: create open-file under c
    acquireLock: S(bucket), S(c-container=30)
    revalidate: c still exists? YES (just created)
    plan(): PUT openKeyTable
    releaseLock, submit to Ratis
```

**What happens:** Thread A's step 1 succeeds because it acquired locks before Thread B's tombstone was visible. After Thread B's tombstone commits, the background purge will eventually reach dir `b` and dir `c`. When it tries to purge `c`, it finds the open file and cannot remove `c` until that file is committed/aborted and cleaned up (per the no-orphan guarantee).

**If Thread A starts step 1 AFTER the tombstone commits:** Path resolution of `/vol/bucket/a/b/c/file` traverses `a`. Since `a` is tombstoned, resolution fails with `DIRECTORY_NOT_FOUND`. Thread A's request fails immediately.

This is linearizable: either Thread A's operation logically preceded the delete (and completed), or it came after (and failed).

### Scenario E: FSO two crossing renames

```
Thread A: rename /vol/bucket/a/x → /vol/bucket/b/y
Thread B: rename /vol/bucket/b/p → /vol/bucket/a/q
```

Both operations need slot locks in directories `a` and `b`. The deadlock
avoidance total order (sort by objectID) ensures both threads acquire locks in
the same order. If `objectID(a) < objectID(b)`, both acquire `a`'s locks first,
then `b`'s. No deadlock is possible.

---

## 3. Multi-Step Operations

Two operation types are not single transitions. The leader orchestrates them as
an ordered chain of dependent Ratis transitions, each with its own per-step
lock acquisition and release.

### 3.1 Implicit directory creation (createFile with missing parents)

`createFile /a/b/c/file` with `/a/b/c` missing is decomposed, OM-internally,
into: `create b` → await commit → `create c` → await commit → `create
open-file`. The client sends one request with the full path; the OM orchestrates
the sub-creates internally. Each sub-create needs the previous one's committed
objectID to resolve the next parent.

**Dynamic step-iterator, not pre-computed chain.** The planner pulls one step at a time from the request's iterator, acquires that step's locks, revalidates the locked nodes by `(parentObjectID, name)` (confirming they still exist with the expected objectID), plans that step, submits to Ratis, and on commit releases the locks and advances the iterator. The next step may differ from what a static pre-computation would have produced, because a concurrent operation could have changed the state.

**Non-atomic (accepted).** A failure after creating `b` and `c` leaves empty directories. This matches `mkdir -p` semantics, is idempotent on retry (create dir on an existing dir is a no-op), and is low-harm.

**ObjectID window retirement.** Because each created object is now its own Ratis transition, each gets exactly one managed index. The 256-wide recursive-dir objectID window (`(epoch<<62)|(txId<<8)|offset`) is retired for migrated commands.

**Asynchronous orchestration.** The leader registers a continuation on each sub-operation's commit future and frees the worker thread during the Ratis await. It holds the client RPC open, not a worker thread.

### 3.2 Recursive directory delete

`rm -rf /a/b` is handled in two phases:

1. **Synchronous root tombstone:** The root directory `/a/b` is tombstoned immediately (moved to `deletedDirTable`). This makes any new path resolution that traverses `/a/b` fail with `DIRECTORY_NOT_FOUND` immediately — providing fast UX and preventing new descendant operations from starting.

2. **Asynchronous per-node purge:** A redesigned `DirectoryDeletingService` walks the subtree in the background. Each node removal takes that node's `X(container)` lock + its slot lock, enumerates current children under the lock, and removes the node only when childless. This guarantees no orphan: a late child (from an in-flight create that resolved before the tombstone) is processed before the parent node is removed.

**Quota release is lazy:** Subtree bytes and namespace are freed as the purge drains each node, not synchronously with the root tombstone. This is eventually consistent.

---

## 4. Revalidation After Lock Acquisition

After acquiring its locks, every operation re-reads each locked node by `(parentObjectID, name)` using the parent objectIDs captured during path resolution. It confirms the node still exists with the expected objectID before mutating. This closes the window between path resolution and lock acquisition where a concurrent delete could have removed the node. ABA-safe because objectIDs are never reused.

---

## 5. Recovery

Recovery is client-retry-driven, with no persisted orchestration or saga state. A mid-orchestration leader crash leaves committed sub-steps durable on the quorum and the client's RPC unanswered. The failover retry re-runs the whole request, idempotently skipping already-created directories and completing. The retry-cache entry (`clientId#callId → response`) is written by the terminal step only; intermediate sub-steps are idempotent by structure.

---

## 6. FSO Operation Inventory

FSO operations in the migration plan (from the
[full operation inventory](./migration-playbook.md#3-full-operation-inventory)):

| Operation | Tables | Quota? | SCM? | Multi-step? | Phase |
|-----------|--------|--------|------|-------------|-------|
| CreateKey (FSO) | openKeyTable, directoryTable | usage | yes | implicit parents | P-2 |
| CommitKey (FSO) | keyTable, openKeyTable, deletedTable | usage | no | no | P-2 |
| AllocateBlock (FSO) | openKeyTable | no | yes | no | P-2 |
| DeleteKey (FSO) | keyTable, deletedTable | usage | no | recursive | P-2 |
| CreateFile | openKeyTable, directoryTable | usage | yes | yes (mkdir-p) | P-2 |
| CreateDirectory | directoryTable | usage | no | yes (mkdir-p) | P-2 |
| PurgeDirectories | directoryTable, fileTable, deletedDirTable | usage (lazy) | no | yes (background) | P-2 |

All are migrated in Phase P-2 — the hardest multi-step operations. This phase
retires the showstopper risks (multi-step chains, recursive delete, revalidation
under concurrent modifications).
