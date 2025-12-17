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

Ozone Snapshots let you create point-in-time, consistent, read-only images of a bucket. Key uses include:

*   **Backup and Restore**: For regular data protection and recovery.
*   **Archival and Compliance**: For long-term data retention.
*   **Replication and Disaster Recovery (DR)**: For copying bucket images to remote DR sites.
*   **Incremental Replication**: `DistCp` with `SnapshotDiff` efficiently syncs buckets.

## Architecture

Ozone Snapshots provide point-in-time, read-only copies of buckets. This relies on Ozone's immutable data blocks. When a snapshot is taken, Ozone Manager (OM) copies the bucket's metadata (key namespace) using its RocksDB store. Data blocks aren't duplicated; they are preserved as long as any snapshot or the live bucket references them. Background services reclaim unreferenced blocks.

The SnapshotDiff feature compares two snapshots (or a snapshot and the live bucket) to identify changes like added, deleted, modified, or renamed keys, caching results for speed.

## System Architecture Deep Dive

Ozone snapshots version bucket metadata within the OM. A dedicated snapshot metadata table in RocksDB records the key directory tree at snapshot creation. This is an instant operation as it involves metadata pointers (via RocksDB checkpoints) rather than data copying. Each snapshot has a unique ID and name.

When keys are changed or deleted in the live bucket, their data blocks are retained if a snapshot references them. Deleting a snapshot makes its exclusively referenced blocks reclaimable by background cleanup processes.

**SnapshotDiff Implementation:** Differences are computed using RocksDB key comparisons and a compaction DAG for recent changes (default: 30 days). For older snapshots or if DAG data is compacted, a full metadata scan is used. Diff results (`+` add, `-` delete, `M` modify, `R` rename) are cached.

**Snapshot Data Storage:** Snapshot metadata resides in OM's RocksDB. Diff job data is stored in `ozone.om.snapshot.diff.db.dir` (defaults to OM metadata directory).

For more details, see Prashant Pogde’s [Introducing Apache Ozone Snapshots](https://medium.com/@prashantpogde/introducing-apache-ozone-snapshots-af82e976142f).

## User Tutorial

This guide shows how to use Ozone snapshots via CLI and Java.

### Using Snapshots via CLI

Manage snapshots using `ozone sh` or `ozone fs` (Hadoop-compatible) commands:

*   **Create Snapshot:**
    ```shell
    ozone sh snapshot create /vol1/bucket1 [snapshotName]
    # Or via Hadoop FS interface:
    # ozone fs -createSnapshot ofs://om-service/vol1/bucket1 [snapshotName]
    ```
    Requires bucket owner or admin privilege. If `snapshotName` is omitted, it's auto-generated (e.g., `s20250530-005848.163`). Custom names must be unique, valid DNS names.

*   **Delete Snapshot:**
    ```shell
    ozone sh snapshot delete /vol1/bucket1 <snapshotName>
    # Or via Hadoop FS interface:
    # ozone fs -deleteSnapshot ofs://om-service/vol1/bucket1 <snapshotName>
    ```

*   **List Snapshots:**
    ```shell
    ozone sh snapshot list /vol1/bucket1
    # Or via Hadoop FS interface (list .snapshot directory):
    # ozone fs -ls /vol1/bucket1/.snapshot
    ```
    Snapshots appear in the bucket's read-only `.snapshot` directory.

*   **Read from Snapshot:**
    List keys:
    ```shell
    ozone sh key list /vol1/bucket1/.snapshot/<snapshotName>
    # Or: ozone fs -ls /vol1/bucket1/.snapshot/<snapshotName>
    ```
    Get a key:
    ```shell
    ozone sh key get /vol1/bucket1/.snapshot/<snapshotName>/reports/Q1.csv ./Q1_snapshot.csv
    ```
    Requires read privileges on the bucket.

*   **Snapshot Diff:** Shows changes between two snapshots or a snapshot and the live bucket.
    ```shell
    ozone sh snapshot diff /vol1/bucket1 <snap1> <snap2_or_live_bucket>
    ```
    Output prefixes: `+` (add), `-` (delete), `M` (modify), `R` (rename). Use `-p`, `-t` for pagination.
    Manage diff jobs: `ozone sh snapshot listDiff /vol1/bucket1`, `ozone sh snapshot cancelDiff <jobId>`.

*   **List Snapshot Diff Jobs:** Lists snapshot diff jobs for a bucket.
    ```shell
    ozone sh snapshot listDiff /vol1/bucket1
    ```
    By default, lists jobs with `in_progress` status. Use `--job-status` to filter by specific status:
    ```shell
    # List jobs with specific status (queued, in_progress, done, failed, rejected)
    ozone sh snapshot listDiff /vol1/bucket1 --job-status done
    ```
    Use `--all-status` to list all jobs regardless of status:
    ```shell
    # List all snapshot diff jobs regardless of status
    ozone sh snapshot listDiff /vol1/bucket1 --all-status
    ```
    **Note:** The difference between `--all-status` and `-all` (or `-a`):
    * `--all-status`: Controls which jobs to show based on status (lists all jobs regardless of status)
    * `-all` (or `-a`): Controls the number of results returned (pagination option, removes pagination limit, **not related to snapshot diff job status**)
    
    For example:
    ```shell
    # List all jobs regardless of status, with pagination limit removed
    ozone sh snapshot listDiff /vol1/bucket1 --all-status -all
    # Or limit results to 10 items
    ozone sh snapshot listDiff /vol1/bucket1 --all-status -l 10
    ```

*   **Rename Snapshot:**
    ```shell
    ozone sh snapshot rename /vol1/bucket1 <oldName> <newName>
    ```
    Requires bucket owner or admin.

*   **Snapshot Info:**
    ```shell
    ozone sh snapshot info /vol1/bucket1 <snapshotName>
    ```
    Shows ID, creation time, status, and space usage (Reference and Exclusive Size).

CLI operations call Ozone Manager RPCs and enforce authorization.

### Programmatic Access via Java

Manage and access snapshots using Java APIs:

**Hadoop Compatible FileSystem (HCFS) Interface:** Use Ozone FileSystem (OFS) API (Hadoop `FileSystem`).

```java
// Example: Create, list, read, rename, delete snapshots
Configuration conf = new OzoneConfiguration();
FileSystem fs = FileSystem.get(new Path("ofs://om-service/vol1/bucket1").toUri(), conf);
Path bucketPath = new Path("/vol1/bucket1");

// fs.createSnapshot(bucketPath, "snapshotName");
// fs.listStatus(new Path(bucketPath, ".snapshot"));
// fs.open(new Path(bucketPath, ".snapshot/snapshotName/key"));
// fs.rename(new Path(bucketPath, ".snapshot/oldName"), new Path(bucketPath, ".snapshot/newName"));
// fs.deleteSnapshot(bucketPath, "snapshotName");
```
Handle `OMException` or `IOException`. Snapshots are in the bucket's `.snapshot` directory.
Refer to the [Ozone File System API guide](https://ozone.apache.org/docs/edge/interface/ofs.html).

**Ozone ObjectStore Client API:** Use `OzoneClient` and `ObjectStore` API.

```Java
// Example: Create, list, get info, rename, delete snapshots
OzoneClient ozClient = OzoneClientFactory.getRpcClient(conf);
ObjectStore store = ozClient.getObjectStore();

// store.createSnapshot("vol1", "bucket1", "snapshotName");
// store.listSnapshot("vol1", "bucket1", null, null);
// store.getSnapshotInfo("vol1", "bucket1", "snapshotName");
// store.renameSnapshot("vol1", "bucket1", "oldName", "newName");
// store.deleteSnapshot("vol1", "bucket1", "snapshotName");
```
Handle exceptions for privilege or non-existent snapshot issues.

**HTTP REST API Access:** Use HttpFS Gateway (WebHDFS-compatible REST API) for filesystem operations on snapshots (e.g., reading from `.snapshot` paths). Create/delete/rename are supported; `getSnapshotDiff` is not yet.

## System Administration How-To

This section covers key configurations and monitoring for Ozone snapshots.

### Configuration Properties

See [Snapshot Configuration Properties]({{< ref "Snapshot-Configuration-Properties.md" >}}).

Note: Snapshot configuration may change over time. Check `ozone-default.xml` for the most up-to-date settings.

### Monitoring

Monitor OM heap usage with many snapshots or large diffs. Enable Ozone Native ACLs or Ranger for access control.

**Monitoring Snapshots:** Use OM metrics (Prometheus, RPC) for snapshot counts, diff operations, etc. Check OM logs for snapshot-related messages.

## Authorization

Snapshot operations require specific privileges:

*   **Create, Delete, Rename Snapshot:** Require admin or bucket owner privileges. Access is denied otherwise.
*   **List Snapshots, Get Snapshot Info:** Require read/list access to the bucket. Users who can list bucket contents can typically list its snapshots.
*   **SnapshotDiff, Cancel/List SnapshotDiff Jobs:** Require read access to the bucket, as diffs reveal key information.

Ozone supports native ACLs and optional Ranger policies for snapshot authorization. The behavior described assumes native ACLs. If using Ranger, ensure appropriate permissions are configured for snapshot operations.

## Comparison to HDFS Snapshots

Ozone and HDFS snapshots are conceptually similar but differ in key aspects:

*   **Granularity:** Ozone snapshots are bucket-level; HDFS snapshots can be taken at any directory level (if snapshottable).
*   **Metadata vs. Data Changes:** Both track key/file changes. Ozone snapshots don't version bucket metadata changes (e.g., quotas, ACLs).
*   **Access and Restore:** Both use a `.snapshot` path for read-only access. Restoring in Ozone is a manual copy process (e.g., using DistCp); no automatic rollback.
*   **Implementation:** Ozone uses OM's key-value store (RocksDB) for O(1) metadata-pointer-based snapshots. HDFS also uses metadata manipulation but Ozone's object-store nature means no DataNode-level block tracking for snapshots; all intelligence is in OM.

## Known Issues and Limitations

Key limitations for Ozone snapshots include:

*   **S3 Interface Support:** Snapshot operations (create, list, delete) are not available via the S3 API or `s3a` connector. Manage snapshots using Ozone RPC (shell, `ozone fs`, Java API). Snapshotted data can be read via S3 using the `.snapshot/snapshotName/keyName` path.
*   **Ratis & EC Buckets:** Snapshots work for both Ratis and EC buckets, managed via Ozone interfaces.
*   **Snapshots During Ongoing Deletes:** Taking a snapshot during a large delete operation will prevent space reclamation for the deleted keys until the snapshot is removed.
*   **Reserved Namespace Name `.snapshot`:** `.snapshot` is a reserved name at the root of a bucket and cannot be used for user-created keys or directories.
*   **Snapshot Performance and Scale:** While creating snapshots is fast, a very large number of snapshots per bucket (thousands) can increase OM metadata size and potentially slow down listing operations. Snapshot diff performance is generally not affected by the number of snapshots but by concurrent diff operations.
*   **Space Utilization Reporting:** Space used by snapshots (data no longer in the active bucket but retained by snapshots) is reported in `ozone sh snapshot info`. Deleting a snapshot frees space asynchronously.
*   **Hard Link Upper Limit:** RocksDB checkpoints use hard links, limited to 65,535 per file by the filesystem. This restricts the number of snapshots per bucket.

Refer to project release notes for updates on these limitations.

## Linux System Configuration

For optimal performance and stability when using Ozone snapshots, especially in production, consider the following system configurations:

*   **Open File Descriptors:** Increase the `nofile` ulimit (e.g., to 64,000 or higher) for the Ozone Manager (OM) process to handle numerous RocksDB files and snapshot operations.
*   **Metadata Storage:** Use high-performance storage like NVMe SSDs for OM metadata directories (`ozone.metadata.dirs`) to improve I/O for snapshot and diff operations.
*   **OM Resources:** Allocate sufficient RAM (e.g., 16-32GB+, monitor GC) and CPU to the Ozone Manager, particularly for clusters with many snapshots or concurrent diff jobs.
*   **DataNode Disk Space:** Account for increased disk usage on DataNodes, as snapshots retain data blocks that would otherwise be deleted. Plan capacity based on snapshot retention policies and change rates.
*   **Filesystem & Kernel:** Use filesystems like ext4 or xfs (often preferred for RocksDB) for OM metadata. Ensure disk schedulers and RAID configurations are optimized for low latency.
*   **Networking:** Ensure robust network connectivity to the OM, as snapshot diffs or HttpFS access can involve significant data transfer.

Always test snapshot operations under your expected load to fine-tune these configurations.
