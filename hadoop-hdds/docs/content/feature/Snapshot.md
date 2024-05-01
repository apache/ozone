---
title: "Ozone Snapshot"
weight: 1
menu:
   main:
      parent: Features
summary: Ozone Snapshot
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

## Introduction

Snapshot feature for Apache Ozone object store allows users to take point-in-time consistent image of a given bucket. Snapshot feature enables various use cases, including:
 * Backup and Restore: Create hourly, daily, weekly, monthly snapshots for backup and recovery when needed.
 * Archival and Compliance: Take snapshots for compliance purpose and archive them as required.
 * Replication and Disaster Recovery (DR): Snapshots provide frozen immutable images of the bucket on the source Ozone cluster. Snapshots can be used for replicating these immutable bucket images to remote DR sites.
 * Incremental Replication: DistCp with SnapshotDiff offers an efficient way to incrementally sync up source and destination buckets.

## Snapshot APIs

Snapshot feature is available through 'ozone fs' and 'ozone sh' CLI. This feature can also be programmatically accessed from Ozone `ObjectStore` Java client. The feature provides following functionalities:
* Create Snapshot: Create an instantenous snapshot for a given bucket
```shell
ozone sh snapshot create [-hV] <bucket> [<snapshotName>]
```
* List Snapshots: List all snapshots of a given bucket
```shell
ozone sh snapshot list [-hV] <bucket>
```
* Delete snapshot: Delete a given snapshot for a given bucket
```shell
ozone sh snapshot delete [-hV] <bucket> <snapshotName>
```
* Snapshot Diff: Given two snapshots, list all the keys that are different between the them.
```shell
ozone sh snapshot diff [-chV] [-p=<pageSize>] [-t=<continuation-token>] <bucket> <fromSnapshot> <toSnapshot>
```
SnapshotDiff CLI/API is asynchronous. The first time the API is invoked, OM starts a background thread to calculate the SnapshotDiff, and returns "Retry" with suggested duration for the retry operation. Once the SnapshotDiff is computed, this API returns the diffs in multiple Pages. Within each Diff response, OM also returns a continuation token for the client to continue from the last batch of Diff results.  This API is safe to be called multiple times for a given snapshot source and destination pair. Internally each Ozone Manager computes Snapdiff only once and stores it for future invocations of the same Snapshot Diff API.

* List SnapshotDiff Jobs: List all snapshotDiff jobs of a given bucket 
```shell
ozone sh snapshot listDiff [-ahV] [-s=<jobStatus>] <bucket>
```

* Snapshot Info: Returns information about an existing snapshot
```shell
ozone sh snapshot info [-hV] <bucket> <snapshotName>
```

## Architecture

Ozone Snapshot architecture leverages the fact that data blocks once written, remain immutable for their lifetime. These data blocks are reclaimed only when the object key metadata that references them, is deleted from the Ozone namespace. All of this Ozone metadata is stored on the OM nodes in the Ozone cluster. When a user takes a snapshot of an Ozone bucket, internally the system takes snapshot of the Ozone metadata in OM nodes. Since Ozone doesn't allow updates to datanode blocks, integrity of data blocks referenced by Ozone metadata snapshot in OM nodes remains intact. Ozone key deletion service is also aware of Ozone snapshots.  Key deletion service does not reclaim any key as long as it is referenced by the active object store bucket or any of its snapshot. When the snapshots are deleted, a background garbage collection service reclaims any key that will not be part of any snapshot or active object store.
Ozone also provides SnapshotDiff API. Whenever a user issues a SnapshotDiff between two snapshots, it efficiently calculates all the keys that are different between these two snapshots and returns paginated diff list result.

## Deployment
----------
### Cluster and Hardware Configuration

Snapshot feature places additional demands on the cluster in terms of CPU, memory and storage. Cluster nodes running Ozone Managers and Ozone Datanodes should be configured with extra storage capacity depending on the number of active snapshots that the user wants to keep. Ozone Snapshots consume incremental amount of space per snapshot. e.g. if the active object store has 100 GB data (before replication) and a snapshot is taken, then the 100 GB of space will be locked in that snapshot. If the active object store consumes another 10 GB of space (before replication) subsequently then overall space requirement would be 100 GB + 10 GB = 110 GB in total (before replication). This is because common keys between Ozone snapshots and the active object store will share the storage space.

Similarly, nodes running Ozone Manager should be configured with extra memory depending on how many snapshots are concurrently read from. This also depends on how many concurrent SnapshotDiff jobs are expected in the cluster. By default, an Ozone Manager allows 10 concurrent SnapshotDiff jobs at a time, which can be increased in config.

