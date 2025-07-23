---
title: "DiskBalancer for Datanode"
summary: "DiskBalancer is a feature to evenly distribute data across all disks within a Datanode for even disk utilisation."
date: 2025-07-21
jira: HDDS-5713
status: implementing
author: Janus Chow, Sammi Chen, Gargi Jaiswal, Stephen O' Donnell
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
# [HDDS-5713](https://issues.apache.org/jira/browse/HDDS-5713) DiskBalancer for Datanode (implementing)

## Background
**Apache Ozone** works well to distribute all containers evenly
across all multiple disks on each Datanode. This initial spread
ensures that I/O load is balanced from the start. However,
over the operational lifetime of a cluster **disk imbalance** can
occur due to the following reasons:
- **Adding new disks** to expand datanode storage space.
- **Replacing old broken disks** with new disks.
- Massive **block** or **replica deletions**.

This uneven utilisation of disks can create performance bottlenecks, as
**over-utilised disks** become **hotspots** limiting the overall throughput of the
Datanode. As a result, this new feature, **DiskBalancer**, is introduced to
ensure even data distribution across disks within a Datanode.

## Proposed Solution
The DiskBalancer is a feature which evenly distributes data across
different disks of a Datanode.

It detects an imbalance within datanode, using the term from
[HDFS DiskBalancer](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSDiskbalancer.html)
metric called **Volume Data Density**. This metric is calculated for
each disk using following formula:

```
AverageUtilization = TotalUsed / TotalCapacity
VolumeUtilization = diskUsedSpace / diskCapacity
VolumeDensity = | VolumeUtilization - AverageUtilization |
```
Here, **VolumeUtilization** is each disk's individual utilization and
**AverageUtilization** is the ideal utilization for all disks to maintain
eveness.

A disk is considered a candidate for balancing if its `VolumeDataDensity` exceeds a configurable
`threshold`. The DiskBalancer then moves the containers from most
utilised disk to the least utilised disk. DiskBalancer can be triggered manually by **CLI commands**.

## High-Level DiskBalancer Implementation

The general view of this design consists of 3 parts as follows:

**Client & SCM -:**

Administrators use the `ozone admin datanode diskbalancer` CLI to manage and monitor the feature.
* Clients can control the DiskBalancer job by sending requests to SCM, like start,
stop, update configuration and can query for diskBalancer status.
* Clients get storageReport from SCM to decide which datanode to balance.

**SCM & DN -:**

SCM acts as a **control plane** and information hub but remains **stateless** 
regarding the balancing process.
* SCM retrieves storageReport and balance status via heartbeat from DN.

**DN -:**

All balance operations are done in dataNodes. 

A daemon thread, the **Scheduler**, runs periodically on each Datanode.
1.  It calculates the `VolumeDataDensity` for all volumes.
2.  If an imbalance is detected (i.e., density > threshold), it moves a set of closed containers
from the most over-utilized disk (source) to the least utilized disk (destination).
3.  The scheduler dispatches these move tasks to a pool of **Worker** threads for parallel execution.

## Container Move Process

Suppose, we are moving container C1 **(CLOSED state)** from Source Disk d1 to Destination disk d2 :
1. A temporary copy, `Temp C1-CLOSED`, is created in the `temp directory` of the destination disk D2.

2. `Temp C1-CLOSED` is transitioned to `Temp C1-RECOVERING` state. This **Temp C1-RECOVERING** container is now
atomically moved to the **final destination** directory of D2 as `C1-RECOVERING`.
3. Now **new container** import is initiated for `C1-RECOVERING` container.
4. Once the import is successful, all the metadata updates are done for this new container created on D2.
5. Finally, the original container `C1-CLOSED` on D1 is deleted.

```
D1     ----> C1-CLOSED  --- (5) ---> C1-DELETED
        |
        |
       (1)
        |
D2      ----> Temp C1-CLOSED  --- (2) ---> Temp C1-RECOVERING --- (3) ---> C1-RECOVERING --- (4) ---> C1-CLOSED
```
## DiskBalancing Policies

By default, the DiskBalancer uses specific policies to decide which disks to balance and which containers to move. These
are configurable, but the default implementations provide robust and safe behavior.

*   **`DefaultVolumeChoosingPolicy`**: This is the default policy for selecting the source and destination volumes. It 
identifies the most over-utilized volume as the source and the most under-utilized volume as the destination by comparing
each volume's utilization against the Datanode's average. The calculation is smart enough to account for data that is 
already in the process of being moved, ensuring it makes accurate decisions based on the future state of the volumes.

*   **`DefaultContainerChoosingPolicy`**: This is the default policy for selecting which container to move from a source
volume. It iterates through the containers on the source disk and picks the first one that is in a **CLOSED** state 
and is not already being moved by another balancing operation. To optimize performance and avoid re-scanning the same 
containers repeatedly, it caches the list of containers for each volume which auto expires after one hour of its last 
used time or if the container iterator for that is invalidated on full utilisation.

## DiskBalancer Metrics

The DiskBalancer service exposes JMX metrics on each Datanode for real-time monitoring. These metrics provide insights
into the balancer's activity, progress, and overall health.

| DiskBalancer Service Metrics             | Description                                                                                                        |                                                                                                                                                             
|------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| `SuccessCount`                           | The number  of successful balance jobs.                                                                            | 
| `SuccessBytes`                           | The total bytes for successfully balanced jobs.                                                                    | 
| `FailureCount`                           | The number of failed balance jobs.                                                                                 |
| `moveSuccessTime`                        | The time spent on successful container moves.                                                                      |
| `moveFailureTime`                        | The time spent on failed container moves.                                                                          |
| `runningLoopCount`                       | The total number of times the balancer's main loop has run.                                                        |
| `idleLoopNoAvailableVolumePairCount `    | The number of loops where balancing did not run because no suitable source/destination volume pair could be found. |
| `idleLoopExceedsBandwidthCount`          | The number of loops where balancing did not run due to bandwidth limits.                                           |

