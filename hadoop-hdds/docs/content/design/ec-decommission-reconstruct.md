---
title: Speed up EC container decommission
summary: Transition from single-source replication to multi-source reconstruction for EC container decommission.
date: 2026-04-17
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
5. **Replication Execution**: Each index is replicated independently using the configured replication mode (push by default, configurable to pull via hdds.scm.replication.push)
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

* Enqueue more tasks at the same DN; change task pick up algorithm so Datanode create a X-way queue,  

Reconstruction is CPU intensive, network intensive and disk I/O intensive. It is therefore less efficient than re-replication, but reconstruction in a larger cluster improves parallelism and throughput.

# Solutions

## Solution 1

Simply force EC decommission to perform reconstruction, similar to under-constructed EC containers.

## Solution 2

Start EC decommission with 1-1 replication. Once the number of in-flight decommission exceeds a pre-configured threshold (`hdds.scm.replication.decommission.ec.reconstruction.threshold`), Replication Manager schedules reconstruction tasks instead.

This behavior can be enabled/disabled with a feature flag `hdds.scm.replication.decommission.ec.reconstruction.enabled`

We will implement both, use solution 1 as a baseline. The solution 2 is more sophsticated and should balance between overhead and parallelism.

# Expected result:

1. Increase the parallelism of transfer to meet SLAs.  
2. transition from a "single-source replication" model to a "multi-source reconstruction"

# Requirements:

* **Fairness and Impact on User I/O:** ensure that reconstruction/replication tasks don't starve foreground client requests. A throttle mechanism is necessary to rate-limit reconstruction. Can be a fixed, dumb throttling mechanism, or something smarter. We’ll need a quick fix now. Let’s worry about a smart throttling mechanism later.  
* **Bisectional Bandwidth "Storm" Prevention** If multiple large nodes are decommissioned simultaneously, it risks a cluster-wide "storm". A Global Concurrency Limit in the SCM is required which caps the total number of simultaneous reconstruction fragments across the entire cluster, not just per-node.  
  * A global limit is good, but 82.8 GB/s can saturate specific rack-level switches even if the cluster-wide limit isn't reached.  
  * The algorithm may need to be rack-aware.  
* **Placement Policy:** The RM should choose the target node based on the existing placement policy. It should judiciously pick datanode destinations that have the most network bandwidth or disk I/O bandwidth.   
* Optionally, SCM Replication Manager and Datanodes should maximize the reconstruction speed as high as possible.  
* SCM RM should prioritize re-replication from source, but once the in-transit re-replication reaches a threshold, it should schedule reconstruction.  
  * Threshold: for example, number of in-transit re-replication tasks reaches number of disks, throughput speed.  
  * **RS(3,2) vs. RS(6,3) Differentiation:** Because RS(6,3) requires 6x the bisectional bandwidth (82.8 GB/s vs 41.4 GB/s), the threshold for switching should likely be different for different EC policies?  
* **Disk-Aware Scheduling:** The Datanode (DN) ensures tasks in the task queue don't land on the same physical disk simultaneously. A mechanism for the DN to report disk-level busy status to the SCM or for the DN to reorder its internal queue based on disk availability. Note: Ozone datanode does not report disk level status to SCM, so this scheduling algorithm must be design and implemented at Datanode not SCM.  
* **Bisectional Bandwidth Management:** For RS(6,3), you need to read 6x the data. The specification needs a plan for how the RM selects "source" nodes for reconstruction to avoid creating new bottlenecks in other parts of the cluster.  
* **Memory Overhead:** increasing parallelism and adding more re-construction tasks could increase memory overhead. That is expected, but it shouldn’t consume so much it becomes infeasible to deploy. Say, ideally contain the datanode heap to under 31GB, and total process memory (including direct memory, native memory) should not exceed 64GB at any point in time.  
* **Error handling:** decommission should complete eventually even if the decommissioning data node crashes. The reconstruction mechanism for EC containers is fault-tolerant. If a container at the decommissioning datanode becomes corrupt or missing during re-replication, it should be able to retry and finally fallback to reconstruction automatically without human intervention.  
* **Observability**  
  * CLI to monitor decommission status  
  * Metrics that show if a decommission is slow due to "Source Disk I/O," "Network Bisectional Bandwidth," or "Target DN I/O"  
  * Grafana dashboard  
* **Feature flag:** enable/disable this feature (prioritize throughput than efficiency for EC decommission) with a feature flag; can be turned on/off at runtime.

# Non-Goals
* Re-architecting the Storage Container Manager (SCM) heartbeat mechanism.
* Implementing dynamic network congestion sensing (will use configurable static limits).


Sidenote:  
Reached out to the Uber Engineering team to seek their solution (presumably battle tested), but they did not respond.

Related: [BofA Ozone decommisson plan of actions](https://docs.google.com/document/d/19LJfIuB6wZ0EHByZcEUKYlqdqLUnWOTbNn3mSX_TBAg/edit?tab=t.0)  
[Ozone Datanode Decommission and Maintenance Guide](https://docs.google.com/document/d/13roWQ1TyCcz6RzvV2dcrbsYkBrrGQTKxYlOBsuTSpkc/edit?tab=t.0)
