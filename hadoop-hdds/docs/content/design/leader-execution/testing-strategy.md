---
title: Leader-Planned Execution — Testing Strategy
summary: Unit, concurrency, integration, determinism, linearizability, and stress/chaos tests
date: 2026-06-12
jira: HDDS-11898
status: proposed
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

# Testing Strategy

To ensure correctness and catch concurrency bugs, we need tests at multiple
levels. For the high-level architecture, see the [overview](./overview.md).
For the locking model being tested, see
[locking and concurrency](./locking-and-concurrency.md).

## Table of Contents

1. [Unit Tests](#1-unit-tests)
2. [Concurrency Tests](#2-concurrency-tests)
3. [Integration Tests](#3-integration-tests)
4. [Determinism Verification Test](#4-determinism-verification-test)
5. [Linearizability Harness](#5-linearizability-harness)
6. [Stress/Chaos Tests](#6-stresschaos-tests)

---

## 1. Unit Tests

| Test class | What it validates |
|-----------|-------------------|
| `TestManagedIndexService` | Monotonic increments, leader switchover, crash recovery |
| `TestChangeRecorder` | Correct proto generation from put/delete/merge calls |
| `TestLeaderPlanner` | Template ordering (preProcess → authorize → lock → plan → unlock), error handling |
| `TestStateTransitionApplyEngine` | Atomic apply, barrier splitting, TransactionInfo update |
| `TestOBSKeyLockManager` | Lock ordering, stripe distribution, timeout behavior |
| `TestFSOKeyLockManager` | Parent-dir lock ordering, stripe distribution |

---

## 2. Concurrency Tests

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

---

## 3. Integration Tests

These run on a `MiniOzoneHAClusterImpl` with 3 OMs:

| Test | What it validates |
|------|-------------------|
| Write key, read from follower | All nodes have the same state after apply |
| Write key, kill leader, read from new leader | Failover preserves committed state |
| Write key, kill follower, bring back, verify sync | Ratis log replay works with new format |
| Mixed planned + legacy commands | Both paths coexist correctly |
| CreateSnapshot between planned commands | Barrier semantics preserved |
| Bulk write (10k keys), verify all 3 nodes | No lost writes under load |

---

## 4. Determinism Verification Test

A special test that validates the planned path produces the same DB state as the
legacy path:

```java
// For each migrated command:
// 1. Run it through the legacy path (validateAndUpdateCache) → capture DB state
// 2. Run the same request through the planned path (plan + apply) → capture DB state
// 3. Assert: both DB states are byte-for-byte identical
```

This test gives high confidence that the migration does not change behavior.

---

## 5. Linearizability Harness

The design allows many interleavings and treats "an error under one ordering"
as a legal outcome. Example-based "operation X always succeeds" tests are not
sufficient. The bar is **linearizability**: every observed concurrent history is
equivalent to some legal sequential order consistent with real-time precedence.

**Architecture:**

1. **Sequential reference model** — a single-threaded in-memory filesystem
   (path → node with objectIDs) implementing identical operation semantics and
   error conditions. The oracle of "what is legal."
2. **Concurrent harness** — randomized concurrent operation mixes against the
   real lock + execution path, recording each operation's invocation/response
   interval and the final DB state. Generation is weighted toward adversarial
   pairs (see scenarios below).
3. **Linearizability checker** — a Wing-Gong / Lincheck-style search verifying
   the recorded history is linearizable against the reference model. Accepts
   any legal order, including orders in which an operation legitimately errors.
4. **Invariant assertions** layered on top: no orphan, no objectID collision,
   quota counter exact (even if limit is soft), deadlock-free completion.

**Adversarial test scenarios:**

| Scenario | What it validates |
|----------|-------------------|
| Create under ancestor being rm-rf'd | No orphan; outcome linearizable |
| Already-purged parent | Revalidation fails the operation cleanly |
| Two crossing renames | Deadlock-free (bounded completion) and linearizable |
| Delete-empty vs create-child | X(dir)/S(dir) rendezvous yields linearizable order |
| Deep mkdirs vs ancestor rename | Children follow renamed objectID; chain completes or fails via revalidation |
| Rename vs delete of same node | X(parent, name) slot rendezvous yields exactly one winner |
| Hot-parent throughput | S(parent) allows concurrent creates; no false serialization |
| Leader failover mid-orchestration | Client retry completes idempotently; no double-apply |

**Scope:** Namespace operations are strictly linearizable. Quota usage and
subtree reclamation are eventually consistent — the checker treats them as
"converges after background work drains," not "correct at every linearization
point."

---

## 6. Stress/Chaos Tests

- **Thread starvation test:** Run with more concurrent requests than available
  lock stripes. Verify no deadlocks and eventual progress.
- **Slow Ratis test:** Add artificial delay in Ratis replication. Verify
  correctness under both lock hold span options.
- **Random kill test:** Randomly kill OM nodes during key operations. Verify
  all committed operations survive, no partial state.
- **Mixed-mode test:** Run some commands on the planned path and others on the
  legacy path simultaneously. Verify objectID disjointness and applied-index
  correctness.
