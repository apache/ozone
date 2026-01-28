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

The general view of this design consists of 2 parts as follows:

**Client & DN - Direct Communication:**

Administrators use the `ozone admin datanode diskbalancer` CLI to manage and monitor the feature.
* Clients communicate **directly** with datanodes via RPC using the `DiskBalancerProtocol` interface, bypassing SCM.
* Clients can control the DiskBalancer job by sending requests directly to datanodes, including:
  - Start/stop DiskBalancer operations
  - Update configuration parameters
  - Query DiskBalancer status and volume density reports
* Each datanode performs its own **authentication** (via RPC) and **authorization** checks (using `OzoneAdmins` based on `ozone.administrators` configuration).
* For batch operations, clients can use the `--in-service-datanodes` flag to automatically query SCM for all IN_SERVICE datanodes and execute commands on all of them.

**DN - DiskBalancer Service:**

All balance operations are done in dataNodes. 

A daemon thread, the **Scheduler**, runs periodically on each Datanode.
1.  It calculates the `VolumeDataDensity` for all volumes.
2.  If an imbalance is detected (i.e., density > threshold), it moves a set of closed containers
from the most over-utilized disk (source) to the least utilized disk (destination).
3.  The scheduler dispatches these move tasks to a pool of **Worker** threads for parallel execution.

**Note:** SCM is used **only** for datanode discovery when using the `--in-service-datanodes` flag. SCM provides a list of IN_SERVICE datanodes for batch operations but
does **not** participate in DiskBalancer control operations (start/stop/update/status/report). All DiskBalancer operations are performed directly between client and datanode.

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

## Security Design
DiskBalancer follows the same security model as other services:

* **Authentication**: Clients communicate directly with datanodes via RPC. In secure clusters, RPC authentication is required (Kerberos).

* **Authorization**: After successful authentication, each datanode performs authorization checks using `OzoneAdmins` based on the `ozone.administrators` configuration:
  - **Admin operations** (start, stop, update): Require the authenticated user to be in `ozone.administrators` or belong to a group in `ozone.administrators.groups`
  - **Read-only operations** (status, report): Do not require admin privileges - any authenticated user can query status and reports
  
By default, if `ozone.administrators` is not configured, only the user who launched the datanode service has admin privileges. This ensures that DiskBalancer operations are restricted to authorized administrators while allowing read-only access for monitoring purposes.

## CLI Interface Design

The DiskBalancer CLI provides five main commands that communicate directly with datanodes:

1. **start** - Initiates DiskBalancer on `specified datanodes` or all `in-service-datanodes` with optional configuration parameters
2. **stop** - Stops DiskBalancer operations on specified datanodes.
3. **update** - Updates DiskBalancer configuration.
4. **status** - Retrieves current DiskBalancer status including running state, metrics, and configuration.
5. **report** - Retrieves volume density report showing imbalance analysis.

The CLI supports:
- **Direct datanode addressing**: Commands can target specific datanodes by hostname or IP address
- **Batch operations**: The `--in-service-datanodes` flag queries SCM for all IN_SERVICE and HEALTHY datanodes and executes commands on all of them
- **Flexible input**: Datanode addresses can be provided as positional arguments or read from stdin
- **Output formats**: Results can be displayed in human-readable format or JSON for programmatic access

### Operational State Awareness

DiskBalancer automatically responds to datanode operational state changes:
* When a datanode enters **DECOMMISSIONING** or **MAINTENANCE** state, DiskBalancer automatically **pauses** (transitions to `PAUSED` state).
* When a datanode returns to **IN_SERVICE** state, DiskBalancer automatically **resumes** (if it was previously `RUNNING`).
* If DiskBalancer was explicitly **stopped** (via CLI), it remains `STOPPED` even after the datanode returns to `IN_SERVICE` state.

This ensures DiskBalancer respects datanode lifecycle management and does not interfere with maintenance or decommissioning operations.

## Feature Flag

The DiskBalancer feature is gated behind a feature flag (`hdds.datanode.disk.balancer.enabled`) to allow controlled rollout. By default, the feature is disabled. When disabled, the DiskBalancer service is not initialized on datanodes, and the CLI commands are hidden from the main help output to prevent accidental usage.

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

