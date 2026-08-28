---
title: Deterministic IO Fault Injection POC
summary: Replayable harness for validating deterministic IO faults at Ozone client boundaries.
date: 2026-06-16
jira: HDDS-0000
status: draft
author: Apache Ozone Contributors
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

# Deterministic IO Fault Injection POC

## Problem

Ozone has several useful failure tools today: MiniOzone daemon restarts,
Blockade network partition tests, a Ratis client error injector, and a FUSE
filesystem fault service. These tools are valuable, but their failures are not
coordinated by one deterministic scenario runner. When a random failure exposes
a bug, reproducing the exact workload and fault placement is hard.

This POC tests the core idea before touching production IO paths:

1. Place a wrapper around each important IO boundary.
2. Classify injected failures by the recovery behavior expected from Ozone.
3. Record generated operations and injected faults as JSON lines.
4. Replay the same logical scenario as a regression fixture.

## Scope

The first implementation is intentionally test-only and belongs under
`hadoop-ozone/fault-injection-test/mini-chaos-tests`.

The POC has two layers:

1. An in-memory model/oracle layer that proves deterministic record/replay and
   ambiguous commit classification without a cluster.
2. A MiniOzoneCluster client-boundary layer that wraps real
   `OzoneBucket.createKey`, `OzoneOutputStream.write`,
   `OzoneOutputStream.close`, and later `OzoneBucket.readKey`.

This is not production instrumentation. It is a repeatable test harness for
finding unsafe retry behavior and then preserving the failing scenario as a
JSONL replay fixture.

The POC is useful if it can demonstrate this class of bug:

```text
operation succeeds at the wrapped Ozone IO boundary
ack/response is lost before the caller sees success
caller retries
retry may repeat a side effect or overwrite an already committed result
harness reports AMBIGUOUS_COMMIT instead of treating it as ordinary retryable IO
```

This is the "ambiguous commit" case. It is more important than a plain timeout
because the caller cannot tell whether the side effect happened.

## Failure Classes

The POC uses three failure classes:

`RECOVERABLE`: The side effect did not happen, or the operation is safe to retry.
Ozone should retry or fail over.

`AMBIGUOUS_COMMIT`: The side effect may have happened, but the client did not
receive the response. Ozone retries must be idempotent or deduplicated.

`NON_RECOVERABLE`: The local component cannot safely continue the same
operation. Ozone should fail fast, contain, or repair from another replica.

The POC treats `AMBIGUOUS_COMMIT` separately because retrying a non-idempotent
operation can create a real correctness bug.

## POC Architecture

The in-memory layer keeps the harness small enough to reason about:

```text
ScenarioRunner
  generates or replays Operation events
  owns the simple model/oracle
  retries recoverable failures
  validates model state against the wrapped IO implementation

FaultInjectedKeyValueIo
  wraps the real key-value IO object
  calls FaultPolicy before and after IO
  records any injected FaultEvent

FaultPolicy
  seedable random policy for exploration
  scripted policy for replay

Trace
  JSONL metadata, operations, and fault events
  can reproduce the same logical workload and fault placement
```

The MiniOzoneCluster layer uses the same idea at a real Ozone client boundary:

```text
TestDeterministicOzoneClientIo
  starts a one-datanode MiniOzoneCluster
  creates a volume and bucket
  runs deterministic write scenarios with RATIS/ONE replication
  verifies both client-visible result and OM-visible key state

DeterministicOzoneClientIo
  wraps OzoneBucket and OzoneOutputStream calls
  injects faults before or after selected client IO points
  records operation and fault events as JSONL
  replays from a saved trace without mutating the trace file

FaultPolicy
  ScriptedFaultPolicy consumes selected FaultEvent entries once
  later seeded policies can generate broader deterministic exploration
```

The first concrete Ozone IO points are:

| IO point | Initial phases | Why it matters |
| --- | --- | --- |
| `CREATE_KEY` | before, after success | create/open failures are common client retry boundaries |
| `WRITE` | before, after success | exercises stream write behavior before close |
| `CLOSE_KEY` | before, after success | close commits the key and is the first ambiguous-commit target |
| `READ_KEY` | before, after success | needed next for read-after-fault and data validation scenarios |

## Trace Format

Each trace record is a JSON object. The file stores one record per line; the
fault event is expanded below for readability. The in-memory layer records a
logical operation name; the Ozone client layer records the client IO point,
attempt number, replication type, and replication factor.

```json
{"type":"meta","seed":7,"operationCount":4}
{"type":"operation","index":0,"operation":"APPEND","key":"k0","value":"v0"}
{
  "type": "fault",
  "operationIndex": 0,
  "operation": "APPEND",
  "phase": "AFTER_SUCCESS",
  "point": "APPEND",
  "action": "DROP_RESPONSE_AFTER_SUCCESS",
  "failureClass": "AMBIGUOUS_COMMIT"
}
```

The replay driver uses the operation index, phase, and fault point to inject
the same fault during replay.

For the Ozone client layer, a trace is expected to look like this:

```json
{"type":"meta","version":1}
{"type":"operation","index":0,"key":"k0","data":"dmFsdWU=","replicationType":"RATIS","replicationFactor":"ONE"}
{
  "type": "fault",
  "operationIndex": 0,
  "attempt": 1,
  "point": "CLOSE_KEY",
  "phase": "AFTER_SUCCESS",
  "action": "DROP_RESPONSE_AFTER_SUCCESS",
  "failureClass": "AMBIGUOUS_COMMIT"
}
```

Replay must read the trace and execute the same logical operation against a new
bucket or cluster state. Replay must not rewrite the original trace, because the
trace is the regression evidence.

## What This Proves

The in-memory layer should show three things:

1. A recoverable pre-IO fault can be retried without changing state.
2. An ambiguous post-success fault can expose duplicate side effects in a
   non-idempotent implementation.
3. The JSONL trace can replay the same failure, and the same trace can pass
   when the IO implementation deduplicates operation IDs.

The MiniOzoneCluster client-boundary layer should show two additional things:

1. The same deterministic fault/replay model can wrap real Ozone client IO
   without changing production code.
2. A dropped response after a successful `close()` is reported as
   `AMBIGUOUS_COMMIT` while the key remains readable and OM metadata confirms
   the committed size.

This is a harness finding, not a confirmed Ozone production bug by itself. The
bug signal is that this boundary must not be treated as blindly recoverable
without an idempotency or deduplication argument.

## Current POC File Set

The current source files for the POC are:

| File | Purpose |
| --- | --- |
| `DeterministicIoFaultScenario.java` | in-memory deterministic fault model and oracle |
| `TestDeterministicIoFaultScenario.java` | unit tests for recoverable, ambiguous, replay, and dedup cases |
| `DeterministicOzoneClientIo.java` | MiniOzoneCluster client IO wrapper and JSONL replay helper |
| `TestDeterministicOzoneClientIo.java` | real Ozone client-boundary tests |

The focused validation command is:

```bash
mvn -pl hadoop-ozone/fault-injection-test/mini-chaos-tests -am \
  -Dtest=TestDeterministicIoFaultScenario,TestDeterministicOzoneClientIo \
  -DskipShade -DskipRecon -DskipDocs test
```

The hygiene checks for this POC are:

```bash
mvn -pl hadoop-ozone/fault-injection-test/mini-chaos-tests \
  -DskipDocs -DskipRecon -Dcheckstyle.failOnViolation=false \
  --no-transfer-progress checkstyle:check

mvn -pl hadoop-hdds/docs,hadoop-ozone/fault-injection-test/mini-chaos-tests \
  -DskipDocs -DskipRecon --no-transfer-progress apache-rat:check
```

## What Is Left

The remaining work should stay incremental:

1. Decide the landing scope: keep this as local POC work, or create a real
   HDDS Jira and commit the design plus the four source files together.
2. Extend `DeterministicOzoneClientIo` to wrap `readKey` instead of only using
   direct reads from the test for validation.
3. Add seeded exploration to the real Ozone client wrapper. The in-memory layer
   already has seeded exploration; the MiniOzoneCluster wrapper is still
   scripted.
4. Add operation IDs or another deduplication oracle to the real Ozone path so
   ambiguous retries can be tested as both unsafe and fixed.
5. Move from one-key write scenarios to small workloads: create, write, close,
   read, overwrite, and delete.
6. Add service-side injection points only after the client-boundary harness is
   stable: OM metadata response loss, datanode chunk IO, and Ratis pipeline
   commit/watch behavior.
7. Define a small always-on CI subset and keep broader seeded exploration as an
   opt-in stress profile.
8. Replace `HDDS-0000` with a real Jira before proposing this outside local POC
   work.

## Later Integration Path

If the POC is useful, the next step is not to copy the in-memory store. The
next step is to move the same interfaces deeper into real Ozone IO boundaries:

| Ozone boundary | First useful fault points |
| --- | --- |
| Client to Ratis pipeline | send request, watch commit, lost response after commit |
| Datanode chunk IO | write chunk, read chunk, fsync, delete chunk |
| OM metadata commit | RocksDB batch write, response lost after commit |
| SCM metadata commit | pipeline/container state transition persistence |
| MiniOzone lifecycle | OM/SCM/DN stop, restart, delayed restart |

The scenario runner can then call real `LoadBucket` or Freon operations and
record the same JSONL operations and fault events.
