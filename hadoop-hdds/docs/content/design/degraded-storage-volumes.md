---
title: Improved Storage Volume Handling for Ozone Datanodes
summary: Proposal to add a degraded storage volume health state in datanodes.
date: 2025-05-06
jira: HDDS-8387
status: draft
author: Ethan Rose, Rishabh Patel
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

# Improved Storage Volume Handling for Ozone Datanodes

## Background

Currently Ozone uses two health states for storage volumes: **healthy** and **failed**. A volume scanner runs on each datanode to determine whether a volume should be moved from a **healthy** to a **failed** state. Once a volume is failed, all container replicas on that volume are removed from tracking by the datanode and considered lost. Volumes cannot return to a healthy state after failure without a datanode restart.

This model only works for hard failures in volumes, but in practice most volume failures are soft failures. Disk issues manifest in a variety of ways and minor problems usually appear before a drive fails completely. The current approach to volume scanning and health classification does not account for this. If a volume is starting to exhibit signs of failure, the datanode only has two options:
- Fail the volume
    - In many cases the volume may still be mostly or partially readable. Containers on this volume that were still readable would be removed by the system and have their redundancy reduced unecessarily. This is not a safe operation.
- Keep the volume healthy
    - Containers on this volume will not have extra copies made until the container scanner finds corruption and marks them unhealthy, after which we have already lost redundancy.

For the common case of soft volume failures, neither of these are good options. This document outlines a proposal to classify and handle soft volume failures in datanodes.

## Proposal

This document proposes adding a new volume state called **degraded**, which will correspond to partially failed volumes. Handling degraded volumes can be broken into two problems:
- **Identification**: Detecting degraded volumes and alerting via metrics and reports to SCM and Recon
- **Remediation**: Proactively making copies of data on degraded volumes and preventing new writes before the volume completely fails

This document is primarily focused on identification, and proposes handling remediation with a volume decommissioning feature that can be implemented independently of volume health state. 

### Identification of Degraded Volumes

Ozone has access to the following checks from the volume scanner to determine volume health. Most of these checks are already present.

#### Directory Check

This check verifies that a directory exists at the specified location for the volume, and that the datanode has read, write, and execute permissions on the directory.

#### Database Check

This check only applies to container data volumes (called `HddsVolumes` in the code). It checks that a new read handle can be acquired for the RocksDB instance on that volume, in addition to the write handle the process is currently holding. It does not use any RocksDB APIs that do individual SST file checksum validation, like paranoid checks. corruption within individual SST files will only affect the keys in those files, and RocksDB verifies checksums for individual keys on each read. This makes SST file checksum errors isolated to a per-container level and they will be detected by the container scanner and cause the container to be marked unhealthy.

#### File Check

This check runs the following steps:
1. Generates a fixed amount of data and keeps it in memory
2. Writes the data to a file on the disk
3. Syncs the file to the disk to touch the hardware
4. Reads the file back to ensure the contents match what was in memory
5. Deletes the file

Of these, the file sync is the most important check, because it ensures that the disk is still reachable. This detects a dangerous condition where the disk is no longer present, but data remains readable and even writeable (if sync is not used) due to in-memory caching by the OS and file system. The cached data may cease to be reachable at any time, and should not be counted as valid replicas of the data.

#### IO Error Count

This would be a new check that can be used as part of this feature. Currently each time datanode IO encounters an error, we request an on-demand volume scan. This should include every time the container scanner marks a container unhealthy. We can keep a counter of how many IO errors have been reported on a volume over a given time frame, regardless of whether the corresponding volume scan passed or failed. This accounts for cases that may show up on the main IO path but may otherwise not be detected by the volume scanner. For example, numerous old sectors with existing container data may be unreadable. The volume scanner's **File Check** will only utilize new disk sectors so it will still pass with these errors present, but the container scanner may be hitting many bad sectors across containers, which this check will account for.

#### Sliding Window

Most checks will encounter intermittent issues, even on overall healthy drives, so we should not downgrade volume health state after just one error. The current volume scanner uses a counter based sliding window for intermittent failues, meaning the volume will be failed if `x` out of the last `y` checks failed, regardless of when they occurred. This approach works for background volume scans, because `y` is the number of times the check ran, and `x` is the number of times it failed. It does not work if we want to apply a sliding window to on-demand checks like IO error count that do not care if the corresponding volume scan passed or failed.
To handle this, we can switch to time based sliding windows to determine when a threshold of tolerable errors is crossed. For example, if this check has failed `x` times in the last `y` minutes, we should consider the volume degraded.

We can use one time based sliding window to track errors that would cause a volume to be degraded, and a second one for errors that would cause a volume to be failed. When a check fails, it can add the result to whichever sliding window it corresponds to. We can create the following assignments of checks:

- **Directory Check**: No sliding window required. If the volume is not present based on filesystem metadata it should be failed immediately.
- **Database Check**: On failure, add an entry to the **failed health sliding window**
- **File Check**:
    - If the sync portion of the check fails, add an entry to the **failed health sliding window**
    - If any other part of this check fails, add an entry to the **degraded health sliding window**
- **IO Error Count**: When an on-demand volume scan is requested, add an entry to the **degraded health sliding window**

Once a sliding window crosses the threshold of errors over a period of time, the volume is moved to that health state.

#### Summary of Volume Health States

Above we have defined each type of check the volume scanner can do, mapped each one to a sliding window that corresponds to a volume health state. We can use this information to finish defining the reuqirements for each volume health state. This document does not propose adding persistence for volume health states, so all volumes will return to healthy on restart until checks move them to a different state.

##### Healthy 

Healthy is the default state for a volume. Volumes remain in this state until health tests fail and move them to a lower health state.

- **Enter**:
    - **On Startup**: If permissions are valid and a RocksDB write handle is successsfully acquired.
    - **While Running**: N/A. Volumes cannot transition into the healthy state after startup.
- **Exit**: When tests for one of the other states fail.
- **Actions**: None

##### Degraded

Degraded volumes are still reachable, but are reporting numerous IO errors when interacting with them. For identification purposes, we can escalate this with SCM, Recon, and metrics which will allow admins to decide whether to leave the volume or remove it. Identification of degraded volumes will be based on errors reported by ongoing datanode IO. This includes the container scanner to ensure that degraded volumes can still be detected even on idle nodes. The container scanner will continue to determine whether individual containers are healthy or not, and the volume scanner will still run to determine if the volume needs to be moved to **failed**. **Degraded** volumes may move back to **healthy** if IO errors are no longer being reported. The historical moves of a volume from healthy to degraded and back can be tracked in a metrics database like Prometheus.

- **Enter**:
    - **On Startup**: N/A. Degraded state is not persisted to disk.
    - **While Running**:
        - Failure threshold of the **degraded volume sliding window** is crossed.
- **Exit**:
    - **On Startup**:
        - The volume will return to **healthy** until re-evaluation is complete
    - **While Running**:
        - When tests for **failed** state indicate the volume should be failed.
        - When the failure threshold of the **degraded volume sliding window** is no longer crossed, the volume will be returned to **healthy**.
- **Actions**:
    - Datanode publishes metrics indicating the degraded volume
    - Volume state in storage report to SCM and Recon updated
    - Container scanner continues to identify containers that are unhealthy individually
    - Volume scanner continues to run all checks to determine whether the volume should be failed

##### Failed

Failed volumes are completely inaccessible. We are confident that there is no way to retrieve any container data from this volume, and all replicas on the volume should be reported as missing.

- **Enter**:
    - **On Startup**:
        - A RocksDB write handle cannot be acquired. This check is a superset of checking that the volume directory exists.
    - **While Running**:
        - Failure threshold of the **failed volume sliding window** is crossed.
- **Exit**:
    - **On Startup**:
        - Entry conditions will be re-evaluated and the volume may become **healthy** or be **failed** again.
    - **While Running**:
        - N/A. **failed** is a terminal state while the node is running.
- **Actions**:
    - Container scanner stops running
    - Volume scanner stops running
    - All containers on the volume are removed from Datanode memory
    - All containers on the volume are reported missing to SCM

#### Volume Health and Disk Usage

Ideally, volumes that are completely full should not be considered degraded or failed. Just like the rest of the datanode, the scanners will depend on the [VolumeUsage](https://github.com/apache/ozone/blob/2a1a6bf124007821eb68779663bbaad371ea668f/hadoop-hdds/container-service/src/main/java/org/apache/hadoop/ozone/container/common/volume/VolumeUsage.java#L97) API returning reasonably accurate values to assess whether there is enough space to perform checks. **File Check**, for example, must be skipped if there is not enough space to write the file. Additionally, volume checks will be dependent on the space reservation feature to ensure that the RocksDB instance has enough room to write files needed to be opened in write mode and do compactions.

In the future, we may want to handle completely full volumes by opening the RocksDB instance in read-only mode and only allowing read operations on the volume. While this may be a nice feature to have, it is more complicated to implement due to its effects on the delete path. This could have a ripple effect because SCM depends on replicas to ack deletes before it can proceed with operations like deleting empty containers. For simplicity the current proposal considers failure to obtain a DB write handle on startup a full volume failure and delegates read-only volumes as a separate feature.

#### Identifying Volume Health with Metrics

To identify degraded and failed volumes through metrics which can be plugged into an alerting system, we can expose counters for each volume health state per datanode: `NumHealthyVolumes`, `NumDegradedVolumes`, and `NumFailedVolumes`. When the counter for degraded or failed volumes goes above 0, it can be used to trigger an alert about an unhealthy volume on a datanode. This saves us from having to expose health state (which does not easily map to a numeric counter or gauge) as a per-volume metric in [VolumeInfoMetrics](https://github.com/apache/ozone/blob/536701649e1a3d5fa94e95888a566e213febb6ff/hadoop-hdds/container-service/src/main/java/org/apache/hadoop/ozone/container/common/volume/VolumeInfoMetrics.java#L64). Once the alert is raised, admins can use the command line to determine which volumes are concerning.

#### Identifying Volume Health with the Command Line

The `ozone admin datanode` command is currently lacking any specifc information about volumes. It is not clear where to put this information since the command's layout is atypical of other Ozone commands. Most commands follow the pattern of a `list` subcommand which gives a summary of each item, followed by an `info` command to get more detailed information. See `ozone admin container {info,list}` and `ozone sh {volume,bucket,key} {info,list}`. The datanode CLI instead provides two relevant subcommands:
- `ozone admin datanode list` which provides very verbose information about each datanode, some of which could be saved for an `info` command
- `ozone admin datanode usageinfo` which provides only information about total node capacity, in addition to verbose information about the original node when `--json` is used.
    - This command adds further confusion because it seems to be intended to provide info on one node, but then provides options to obtain a list of most and least used nodes, changing it to a list type command.

 This current CLI layout splits a dedicated `info` command between two commands that are each `list/info` hybrids. This makes it difficult to add new datanode information like we want for volumes. To improve the CLI in a compatible way we can take the following steps:
 1. Add sorting flags to `ozone admin datanode list` that allow sorting by node capacity.
 2. Add a counter for `totalVolumes` and `healthyVolumes` to each entry in `ozone admin datanode list`.
     - This allows filtering for datanodes that have `healthyVolumes < totalVolumes` from a single place using `jq`.
     - We could optionally add a flag to the `list` command to only give nodes with unhealthy volumes.
 3. Add an `ozone admin datanode info` command that gives all information about a datanode in a verbose json format.
    - This includes its total usage info, per volume usage info, and volume health.
 4. Deprecate `ozone admin datanode usageinfo`.
     - All its functionality should now be covered by `ozone admin datanode {list,info}`.
 
### Remediating Degraded Volumes

Once degraded volumes can be reported to SCM, it is possible to take action for the affected data on that volume. When a degraded volume is encountered, we should:
- Stop writing new data onto the volume
- Make additional copies of the data on that volume, ideally using other nodes as sources if possible
- Notify the admin when all data from the volume has been fully replicated so it is safe to remove

This summary is very similar to the process of datanode decommissioning, just at the disk level instead of the node level. Having decommissioning for each failure domain (full node or disk) in the system is generally useful, so we can implement this as a volume decommissioning feature that is completely separate from disk health. Designing this feature comes with its own challenges and is not in the scope of this document.

If automated decommissioning of degraded volumes is desired, the two features could be joined with a config key that enables SCM to automatically decommission degraded volumes.

## Task Breakdown

This is an approximate task breakdown of the work required to implement this feature. Tasks are listed in order and grouped by focus area.

### Improve Volume Scanner

- Create a generic timer-based sliding window class that can be used for checks with intermittent failures.
- Create a **degraded** volume health state which can be used by the volume scanner.
    - Add volume health state to `StorageReportProto` so it is received at SCM
    - Currently we only have a boolean that says whether the volume is healthy or not.
- Determine default configurations for sliding windows and scan intervals
    - Sliding window timeouts and scan intervals need to be set accordingly so that background scanners alone have a chance to cross the check threshold even if no foreground load is triggering on-demand scans.
    - Minimum volume scan gap is currently 15 minutes. This value can likely be reduced since volume scans are cheap, which will give us a more accurate view of the volume's health.
- Migrate existing volume scanner checks to put entries in either the **failed** or **degraded** sliding windows and move health state accordingly.
- Add IO error count to on-demand volume scanner, which counts towards the **degraded** sliding window.
- Trigger on-demand volume scan when the container scanner finds an unhealthy container.

### Improve CLI

- Improve `ozone admin datanode volume list/info` for identifying volumes based on health state.
- (optional) Add container to volume mapping in SCM to see which containers are affected by volume health.
    - This would not work for **failed** volumes, because datanodes would not report the replicas or volume anymore.

### Improve Metrics

- Add metrics for volume scanners. They currently don't have any.
    - This includes metrics for the current counts in each health state sliding window.
- Add metrics for volume health state (including the new degraded state), which can be used for alerting.

### Improve Recon Volume Overview

- Support showing information about individual volumes within a node from Recon based on the existing storage reports received.
    - This would include capacity and health information.
