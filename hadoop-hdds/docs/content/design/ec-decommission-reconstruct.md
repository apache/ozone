---
title: Speed up EC container decommission
summary: Transition from single-source replication to multi-source reconstruction for EC container decommission.
date: 2026-08-18
jira: HDDS-15014
status: draft
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

[HDDS-15014](https://issues.apache.org/jira/browse/HDDS-15014) Speed up EC container decommission

# Implementation status (2026-08-18)

| Phase | Jira | Status | Notes |
| --- | --- | --- | --- |
| Phase 1 — SCM config + global reconstruction cap | [HDDS-15071](https://issues.apache.org/jira/browse/HDDS-15071) | Patch available, not merged | [#11054](https://github.com/apache/ozone/pull/11054) (supersedes closed #10122) |
| Phase 2 — Dynamic load-based replication→reconstruction switch | [HDDS-15072](https://issues.apache.org/jira/browse/HDDS-15072) | Patch available, not merged | [PR #10123](https://github.com/apache/ozone/pull/10123); depends on Phase 1 |
| Phase 3 — Datanode disk-level fairness | [HDDS-15412](https://issues.apache.org/jira/browse/HDDS-15412) | **Merged** | [PR #10693](https://github.com/apache/ozone/pull/10693); opt-in via `hdds.datanode.replication.per.volume.enabled` |
| Phase 4 — Observability | [HDDS-15075](https://issues.apache.org/jira/browse/HDDS-15075) | Open | [HDDS-15352](https://issues.apache.org/jira/browse/HDDS-15352) Grafana dashboard merged; EC-switch metrics not yet implemented |
| Prerequisite — Failed replication cleanup | [HDDS-15327](https://issues.apache.org/jira/browse/HDDS-15327) | Patch available, not merged | [PR #10540](https://github.com/apache/ozone/pull/10540); SCM inflight quota leak slows decommission today |
| Superseded — Per-volume outbound counter + lookahead dispatcher | [HDDS-15073](https://issues.apache.org/jira/browse/HDDS-15073), [HDDS-15074](https://issues.apache.org/jira/browse/HDDS-15074) | **Resolved (Won't Do)** — superseded by HDDS-15412 | Per-volume thread pools ([#10693](https://github.com/apache/ozone/pull/10693)) replace outbound counters ([#10128](https://github.com/apache/ozone/pull/10128)) and lookahead dispatch ([#10129](https://github.com/apache/ozone/pull/10129)) |

**Current production behavior:** EC decommission still uses 1-1 push replication from the decommissioning datanode. The reconstruction switch (Phases 1–2) is the core speedup and is not yet in `master`.

# Problem:

Decommissioning of dense datanode is, especially the datanodes of mostly EC containers, is very slow:

[https://ozone.apache.org/docs/next/administrator-guide/operations/node-decommissioning-and-maintenance/datanodes/datanode-decommission](https://ozone.apache.org/docs/next/administrator-guide/operations/node-decommissioning-and-maintenance/datanodes/datanode-decommission)

```
When we initiate the process of decommissioning, first we check the current state of the node, ideally it should be IN_SERVICE, then we change it's state to DECOMMISSIONING and start the process of decommissioning, it goes through a workflow where the following happens:

1. First an event is fired to close any pipelines on the node, which will also close any containers.  
2. Next the containers on the node are obtained and checked to see if new replicas are needed. If so, the new replicas are scheduled.  
3. After scheduling replication, the node remains pending until replication has completed.  
4. At this stage the node will complete the decommission process and the state of the node will be changed to DECOMMISSIONED.
```

EC container decommissioning is bottlenecked by the transfer speed of a single source datanode.

[https://ozone.apache.org/docs/next/system-internals/replication/data/containers/replication#scenario-1-decommissioning](https://ozone.apache.org/docs/next/system-internals/replication/data/containers/replication#scenario-1-decommissioning)

```
**How it works:**

1. **Detection**: ECUnderReplicationHandler detects containers with replicas on decommissioning Datanodes  
2. **Index Identification**: The handler identifies which EC indexes are only present on decommissioning Datanodes (decommissioningOnlyIndexes())  
3. **One-to-One Replication**: For each decommissioning index, a replication command is created to copy that specific index to a new Datanode  
4. **Target Selection**: New target Datanodes are selected based on placement policies  
5. **Replication Execution**: Each index is replicated independently via push replication (`ReplicateContainerCommand`). Pull replication has been removed upstream.
```

[https://ozone.apache.org/docs/system-internals/replication/data/replication-manager/#ec-and-decommissioning](https://ozone.apache.org/docs/system-internals/replication/data/replication-manager/#ec-and-decommissioning)  

```
For an EC container, the decommissioning host is likely the only source of the replica which needs to be copied and hence the decommission will be slower.
```

If the majority of the containers on the decommissioning datanode are EC containers, the network bandwidth or the aggregate disk speed of the datanode determines how long the decommission will require.

# Node-level congestion

Let’s say a cluster’s SLA dictates the decommission must complete within 8 hours. 

* Decommission of a 100TB datanode full of EC containers:  
  * Implying 3.45GB/s replication/reconstruction rate.  
  * 100TB datanode implies 12 x 8TB disks.  
  * Each disk has 150MB/s maximum throughput so 1.8GB/s throughput per datanode, and at least 25Gbps network bandwidth.  
  * Therefore, EC decommission cannot just replicate from source. It needs at least 2 target nodes too.

* For 400TB datanodes:  
  * Implying 13.8GB/s replication/reconstruction rate.  
  * 400TB datanode implies 24 x 16TB disks.  
  * Each disk has 150MB/s maximum throughput so 3.6GB/s throughput per datanode, and at least 40Gbps network bandwidth.  
  * Therefore, EC decommission cannot just replicate from source. It needs at least 4 target nodes too.  
  * Assuming RS(3,2), to reconstruct a container requires reading 3x. Implying 13.8*3 = 41.4 GB/s bisectional bandwidth, which is at least 12 datanodes aggregated disk throughput.  
  * Assuming RS(6,3), to reconstruct a container requires reading 6x. Implying 13.8*6 = 82.8 GB/s bisectional bandwidth, which is at least 23 datanodes aggregated disk throughput.

# Disk-level congestion

In fact, because Ozone replication manager does not control I/O at disk level (only node level), multiple tasks may land at the same disk at the same time, and the single disk becomes the bottleneck for the entire datanode, and thus the enter cluster.

* Enqueue more tasks at the same DN; isolate push replication concurrency per disk so one slow volume does not block others (implemented in HDDS-15412 via per-volume thread pools).  

Reconstruction is CPU intensive, network intensive and disk I/O intensive. It is therefore less efficient than re-replication, but reconstruction in a larger cluster improves parallelism and throughput.

# Solutions

## Solution 1 (baseline, not implemented)

Simply force EC decommission to perform reconstruction, similar to under-constructed EC containers. Useful as a baseline for benchmarking the dynamic switch in Solution 3.

## Solution 2 (superseded)

An early prototype ([PR #10082](https://github.com/apache/ozone/pull/10082)) used a fixed in-flight command count threshold (`hdds.scm.replication.decommission.ec.reconstruction.threshold`) and a separate global cap (`hdds.scm.replication.decommission.concurrency`). This approach was replaced by Solution 3, which uses a load-factor threshold relative to each datanode's effective replication limit (see Phase 2 below).

## Solution 3 (chosen approach)

This implementation plan outlines the transition from single-source replication to multi-source reconstruction for EC container decommission. The plan focuses on SCM-side dynamic switching and Datanode-side disk-level fairness. Solution 1 remains a possible future baseline; Solution 2 is not being pursued.

### Phase 1: SCM Configuration and Global Capacity
**Status:** Patch available in [HDDS-15071](https://issues.apache.org/jira/browse/HDDS-15071) ([PR #10122](https://github.com/apache/ozone/pull/10122)); not yet merged.

We introduce configuration properties in `ReplicationManagerConfiguration` to control the behavior and protect cluster resources.

1.  **New Configuration Keys:**
    *   `hdds.scm.replication.decommission.ec.reconstruction.enabled` (Boolean, default: false): Feature flag to enable/disable the switch to reconstruction during decommission (used by Phase 2).
    *   `hdds.scm.replication.decommission.ec.reconstruction.load.factor` (Double, default: 0.9): The threshold of a node's replication limit at which SCM switches to reconstruction (used by Phase 2).
    *   `hdds.scm.replication.reconstruction.global.limit` (Int, default: 0): Cluster-wide cap on concurrent reconstruction commands. A value of zero disables global limit checking (backward compatible). A positive value (e.g. 50) is recommended when enabling EC decommission reconstruction.
2.  **Global Throttling Implementation:**
    *   `ReplicationManager` tracks active `ReconstructECContainersCommand` tasks with an atomic counter and a per-command fragment map.
    *   The limit is enforced in `sendThrottledReconstructionCommand`, not by stopping the under-replicated processor loop. When the cap is reached, new reconstruction commands are deferred via `CommandTargetOverloadedException` and the container is re-queued.
    *   Other under-replication recovery continues: 1-1 `ReplicateContainerCommand` work (EC decommission copies, Ratis copies) is unaffected by the reconstruction cap.
    *   On SCM leader transition, reconstruction counters are cleared alongside `ContainerReplicaPendingOps` because `clear()` does not fire `opCompleted` callbacks.
    *   Deferred reconstruction commands increment the existing `ec_reconstruction_cmds_deferred_total` metric.

### Phase 2: SCM Logic - The Dynamic Switch
**Status:** Patch available in [HDDS-15072](https://issues.apache.org/jira/browse/HDDS-15072) ([PR #10123](https://github.com/apache/ozone/pull/10123)); not yet merged; depends on Phase 1.

The SCM will monitor the load on decommissioning Datanodes and dynamically shift to reconstruction to offload the source node.

1.  **Load Factor Calculation:**
    *   SCM will calculate a node's load using: $\frac{\text{queued replication} + (\text{queued reconstruction} \times \text{weight})}{\text{effective replication limit}}$.
    *   The "effective replication limit" correctly accounts for the `hdds.datanode.replication.outofservice.limit.factor` for decommissioning/maintenance nodes.
2.  **ECUnderReplicationHandler Enhancement:**
    *   In `processDecommissioningIndexes`, if a source Datanode's load factor exceeds the configured threshold (default 90%):
        *   **Action:** Generate a `ReconstructECContainersCommand` instead of individual `ReplicateContainerCommands`.
        *   **Optimization:** When selecting source Datanodes for this reconstruction, SCM will exclude the decommissioning node if $k$ other replicas are available. This transforms a node-level bottleneck into a parallelized cluster-wide task.

### Phase 3: Datanode Logic - Disk-Level Fairness
**Status:** Merged in [HDDS-15412](https://issues.apache.org/jira/browse/HDDS-15412) ([PR #10693](https://github.com/apache/ozone/pull/10693)).

The original draft proposed a leapfrog queue-dispatch algorithm with per-volume outbound counters on `HddsVolume` ([HDDS-15073](https://issues.apache.org/jira/browse/HDDS-15073), [HDDS-15074](https://issues.apache.org/jira/browse/HDDS-15074)). The merged solution uses dedicated per-volume `ThreadPoolExecutor` instances in `VolumeReplicationThreadPools`, which isolates push replication concurrency per disk without SCM protocol changes.

1.  **Configuration:**
    *   `hdds.datanode.replication.per.volume.enabled` (Boolean, default: `false`): Enables per-volume replication thread pools. **Requires DataNode restart** (not reconfigurable).
    *   `hdds.datanode.replication.per.volume.streams.limit` (Int, default: `1`): Maximum concurrent push replication streams per data volume. **Reconfigurable** at runtime via the DataNode reconfiguration handler.
2.  **Task routing in `ReplicationSupervisor`:**
    *   Push replication (`ReplicateContainerCommand` with a target datanode) is dispatched to the thread pool for the container's source volume.
    *   EC reconstruction (`ReconstructECContainersCommand`) and container reconciliation tasks continue to use the global `ReplicationSupervisor` thread pool.
    *   Volume is resolved on the source DataNode via `ContainerSet`.
    *   When `per.volume.enabled` is `false` (default), all tasks use the global pool for backward compatibility.
3.  **Per-volume thread pools:**
    *   On DataNode startup, `VolumeReplicationThreadPools` creates one fixed-size `ThreadPoolExecutor` per healthy data volume, each with its own `PriorityBlockingQueue`.
    *   Pool size follows `per.volume.streams.limit` and scales with `hdds.datanode.replication.outofservice.limit.factor` when the node enters maintenance or decommissioning state.
4.  **Volume failure handling:**
    *   When a volume fails, the failed-volume listener shuts down and removes that volume's thread pool via `ReplicationSupervisor.shutdownFailedVolumePools()`.
    *   Push replication tasks for containers on a failed or missing volume are rejected (logged as warnings); SCM retries on the next replication cycle.
5.  **Operational notes:**
    *   No SCM protocol or Replication Manager changes are required.
    *   Pull replication has been removed upstream; `ReplicateContainerCommand` is push-only.
    *   Reconfiguration of `per.volume.streams.limit` is ignored when `per.volume.enabled` is `false`.
    *   For EC decommission speedup, operators should enable `per.volume.enabled=true` on datanodes in addition to enabling the SCM reconstruction switch (Phases 1–2).
6.  **Known follow-ups:**
    *   Reconfiguration parity with out-of-service scaling.
    *   Rejection metrics for tasks dropped due to unavailable volume pools.
    *   Integration test coverage ([HDDS-15412](https://issues.apache.org/jira/browse/HDDS-15412) sub-tasks).

### Prerequisite: SCM failed-replication cleanup
**Status:** Patch available in [HDDS-15327](https://issues.apache.org/jira/browse/HDDS-15327) ([PR #10540](https://github.com/apache/ozone/pull/10540)); not yet merged.

During large-scale decommission, SCM can accumulate stale in-flight replication entries when datanodes report command failures. Those entries are not cleared until the event timeout (default 12 minutes), which blocks new commands and makes decommission appear stuck even when datanodes have spare capacity. This fix should land before or alongside Phase 2 to make load-factor switching and throughput testing reliable.

### Phase 4: Observability and Robustness
**Status:** Partially complete. [HDDS-15352](https://issues.apache.org/jira/browse/HDDS-15352) Grafana dashboard merged; [HDDS-15075](https://issues.apache.org/jira/browse/HDDS-15075) open.

1.  **SCM Metrics:**
    *   `ec_reconstruction_decommission_triggered_total`: Counter for switches triggered by the load factor (Phase 2; **not yet implemented**).
    *   `ec_reconstruction_cmds_deferred_total`: **Available today**; will also include reconstruction commands deferred due to global reconstruction limit once Phase 1 merges.
2.  **Datanode Metrics:**
    *   `volume_outbound_concurrency_wait_total`: Count of times a task was skipped due to volume load (**not yet implemented**; HDDS-15412 follow-up).
    *   Existing `ReplicationSupervisor` counters continue to track queued, success, failure, and timeout counts per task type.
3.  **Grafana:**
    *   [HDDS-15352](https://issues.apache.org/jira/browse/HDDS-15352) decommission/maintenance dashboard merged; covers general RM and decommission metrics, not EC-switch-specific counters.
4.  **Fault Tolerance:**
    *   If reconstruction fails due to source/target issues, SCM will automatically retry in the next cycle, re-evaluating the best strategy (replication vs. reconstruction) based on the latest node load.

### Phase 5: Verification Strategy
**Status:** Not yet executed end-to-end. Blocked on merging Phases 1–2 and [HDDS-15327](https://issues.apache.org/jira/browse/HDDS-15327).

1.  **Simulation:** Decommission a dense node and verify SCM switching behavior at the 90% load mark.
2.  **Disk Fairness:** With `hdds.datanode.replication.per.volume.enabled=true`, stress test a single DataNode volume with 10+ push replication commands and verify that only `per.volume.streams.limit` (default 1) are active on that volume, while tasks targeting other volumes proceed on their own per-volume pools.
3.  **Global Cap:** Verify that the cluster-wide reconstruction limit effectively throttles background traffic during simultaneous decommission of multiple large nodes.
4.  **Failed-command recovery:** Verify that [HDDS-15327](https://issues.apache.org/jira/browse/HDDS-15327) prevents SCM inflight quota exhaustion under injected replication failures during decommission.

# Expected result:

1. Increase the parallelism of transfer to meet SLAs.  
2. transition from a "single-source replication" model to a "multi-source reconstruction"

# Requirements:

* **Fairness and Impact on User I/O:** ensure that reconstruction/replication tasks don't starve foreground client requests. A throttle mechanism is necessary to rate-limit reconstruction. Can be a fixed, dumb throttling mechanism, or something smarter. We’ll need a quick fix now. Let’s worry about a smart throttling mechanism later.  
* **Bisectional Bandwidth "Storm" Prevention** If multiple large nodes are decommissioned simultaneously, it risks a cluster-wide "storm". A Global Concurrency Limit in the SCM is required which caps the total number of simultaneous reconstruction fragments across the entire cluster, not just per-node.  
  * A global limit is good, but 82.8 GB/s can saturate specific rack-level switches even if the cluster-wide limit isn't reached.  
  * The algorithm may need to be rack-aware. **Not yet planned as a Jira; future work.**
* **Placement Policy:** The RM should choose the target node based on the existing placement policy. It should judiciously pick datanode destinations that have the most network bandwidth or disk I/O bandwidth.   
* Optionally, SCM Replication Manager and Datanodes should maximize the reconstruction speed as high as possible.  
* SCM RM should prioritize re-replication from source, but once the in-transit re-replication reaches a threshold, it should schedule reconstruction.  
  * Threshold: load factor relative to effective replication limit (Phase 2); RS(3,2) vs RS(6,3) differentiated thresholds remain **future work**.  
* **Disk-Aware Scheduling:** The Datanode (DN) ensures push replication tasks do not overload a single physical disk. HDDS-15412 implements this at the DN via per-volume replication thread pools (no SCM disk-level status reporting). EC reconstruction and reconciliation remain on the global pool.  
* **Bisectional Bandwidth Management:** For RS(6,3), you need to read 6x the data. The specification needs a plan for how the RM selects "source" nodes for reconstruction to avoid creating new bottlenecks in other parts of the cluster. Phase 2 source offloading addresses the decommissioning node; rack-aware source selection is **future work**.  
* **Memory Overhead:** increasing parallelism and adding more re-construction tasks could increase memory overhead. That is expected, but it shouldn’t consume so much it becomes infeasible to deploy. Say, ideally contain the datanode heap to under 31GB, and total process memory (including direct memory, native memory) should not exceed 64GB at any point in time.  
* **Error handling:** decommission should complete eventually even if the decommissioning data node crashes. The reconstruction mechanism for EC containers is fault-tolerant. If a container at the decommissioning datanode becomes corrupt or missing during re-replication, it should be able to retry and finally fallback to reconstruction automatically without human intervention.  
* **Observability**  
  * CLI to monitor decommission status (**available today**)  
  * Metrics that show if a decommission is slow due to "Source Disk I/O," "Network Bisectional Bandwidth," or "Target DN I/O" (**partial; HDDS-15075**)  
  * Grafana dashboard (**HDDS-15352 merged**)  
* **Feature flag:** enable/disable this feature (prioritize throughput than efficiency for EC decommission) with a feature flag; can be turned on/off at runtime (`hdds.scm.replication.decommission.ec.reconstruction.enabled`, Phase 1).

# Non-Goals
* Re-architecting the Storage Container Manager (SCM) heartbeat mechanism.
* Implementing dynamic network congestion sensing (will use configurable static limits).


Sidenote:  
Reached out to the Uber Engineering team to seek their solution (presumably battle tested), but they did not respond.

