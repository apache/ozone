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

Snapshot feature for Apache Ozone object store allows users to take a point-in-time consistent image of a given bucket. The snapshot is a read-only, frozen image of the bucket’s state at the time of snapshot creation. Snapshot feature enables various use cases, including:

* **Backup and Restore** – Create hourly, daily, weekly, monthly snapshots for backup and recovery when needed.
* **Archival and Compliance** – Take snapshots for compliance purposes and archive them as required.
* **Replication and Disaster Recovery (DR)** – Snapshots provide frozen, immutable images of the bucket on the source Ozone cluster. These can be used for replicating bucket images to remote DR sites.
* **Incremental Replication** – DistCp with SnapshotDiff offers an efficient way to incrementally sync up source and destination buckets.

## **Architecture**

Ozone Snapshot architecture leverages the immutability of data blocks in Ozone. Data blocks, once written, remain immutable for their lifetime and are only reclaimed when the corresponding key metadata is removed from the namespace. All Ozone metadata (volume, bucket, keys, directories) is stored in the Ozone Manager (OM) metadata store (RocksDB). When a user takes a snapshot of a bucket, the system internally creates a point-in-time copy of the bucket’s namespace metadata on the OM. Since Ozone doesn’t allow in-place updates to DataNode blocks, the integrity of data referenced by the snapshot is preserved. The OM’s key deletion service is aware of snapshots: it will not permanently delete any key as long as that key is still referenced by the active bucket or any existing snapshot. A background KeyDeletingService and DirectoryDeleting Service (garbage collectors) identify keys that are no longer referenced by any snapshot or the live bucket, and reclaim those blocks.

Ozone also provides a SnapshotDiff feature. When a user issues a SnapshotDiff between two snapshots, the OM efficiently computes all the differences (added, deleted, modified, or renamed keys) between the two snapshots and returns a paginated list of changes. Snapshot diff results are cached to speed up subsequent requests for the same snapshot pair.

## **System Architecture Deep Dive**

Internally, Ozone implements snapshots by **versioning the OM metadata for each bucket** snapshot. The OM maintains a snapshot metadata table that records the state of the bucket’s key directory tree at the moment of snapshot creation. No data is physically copied at snapshot creation – the operation simply marks a consistent snapshot of the OM’s RocksDB state (hence snapshots are created instantaneously). Under the hood, Ozone relies on RocksDB’s abilities (like checkpoint) to preserve point-in-time views of the metadata. Each snapshot is identified by a unique ID and name, and each key entry in the OM DB carries information about which snapshots (if any) it belongs to. This approach ensures that **common data is not duplicated** across snapshots: if a key has not changed between two snapshots, both snapshots reference the same underlying data blocks.

When keys are modified or deleted in the active bucket, Ozone checks for snapshots: if a snapshot exists that references the old version, the key’s data blocks are retained until the snapshot is deleted. The reference counting in metadata ensures that deleting a snapshot will mark any keys that were exclusively held by that snapshot as reclaimable, triggering block cleanup in the background.

**SnapshotDiff Implementation:** Ozone computes snapshot diffs efficiently by leveraging RocksDB key range comparisons and a directed acyclic graph (DAG) of compaction history. For a configurable time window after snapshot creation (by default 30 days), OM maintains a *compaction DAG* that allows computing diffs in time proportional to the number of changed keys. If snapshots are relatively recent, OM can determine differences by examining only the metadata changes captured in the DAG (e.g., RocksDB SST files differences) rather than scanning the entire key space. The parameter `ozone.om.snapshot.compaction.dag.max.time.allowed` controls this window (default 30 days). For older snapshots beyond this window (or if the efficient diff data has been compacted away), OM falls back to a full metadata scan to compute the diff. In the worst case, the cost of diff is proportional to iterating over all keys in the bucket (if nearly the entire namespace changed or if using full scan). The SnapshotDiff results (a list of keys with indicators for created (`+`), deleted (`-`), modified (`M`), or renamed (`R`)) are stored in a temporary on-disk cache so that subsequent requests for the same diff can be served quickly without re-computation.

**Snapshot Data Storage:** Snapshot metadata resides on the OM in the same RocksDB as the live metadata, but separated by snapshot-specific prefixes or tables. The OM persists snapshots such that each snapshot’s metadata can be treated as a read-only view. Additionally, OM stores snapshot-related info such as the mapping of snapshot names to snapshot IDs and the list of snapshot diff jobs. By default, temporary data for snapshot diff computations is stored under the OM metadata directory, but this location can be configured via `ozone.om.snapshot.diff.db.dir` (a dedicated directory for snapshot diff scratch space).

For a more in-depth discussion of the snapshot design and its evolution, refer to Prashant Pogde’s introduction of Apache Ozone snapshots (the first in a series of blog posts). This Medium post covers the motivation and high-level design of Ozone snapshots, and subsequent posts delve further into the technical implementation.

## **User Tutorial**

In this section, we demonstrate how to create and use Ozone snapshots via command-line and programmatically.

### **Using Snapshots via CLI**

The Ozone shell provides convenient commands to manage snapshots. Snapshots can be created and manipulated either through the **`ozone sh`** subcommands or the **`ozone fs`** Hadoop-compatible filesystem commands:

* **Creating a Snapshot:** As shown earlier, use `ozone sh snapshot create <bucket> [snapshotName]` to create a snapshot. For example, to snapshot a bucket named `bucket1` in volume `vol1` with an optional name:
```shell
ozone sh snapshot create /vol1/bucket1 finance_backup_2025
```
  This captures an instantaneous image of all keys under `/vol1/bucket1`. The operation requires you to be the bucket owner or volume owner (admin privilege). If no snapshot name is provided, Ozone will auto-generate a name (often using a timestamp).
  Alternatively, you can create a snapshot using the Hadoop FS interface:
```shell
ozone fs -createSnapshot ofs://om-service/vol1/bucket1 finance_backup_2025
```
  (Here, `ofs://om-service/vol1/bucket1` is the URI of the bucket in the Ozone FileSystem; see the *Ozone File System API* reference.)
* **Listing Snapshots:** To list all snapshots of a bucket, use:
```shell
ozone sh snapshot list /vol1/bucket1
```
  This will display all snapshot names for that bucket and their creation times. You can achieve the same via the filesystem interface by listing the hidden `.snapshot` directory:
```shell
ozone fs -ls /vol1/bucket1/.snapshot
```
  As soon as a snapshot is created, it appears under the bucket’s `.snapshot` directory. (Note: The `.snapshot` path is a reserved, read-only namespace that exposes snapshots; you cannot create or modify files inside `.snapshot`.)
* **Reading from a Snapshot:** A snapshot presents a read-only view of the bucket’s data at the snapshot time. You can list keys within a snapshot or read a specific key’s content:
  *List keys in snapshot:*
```shell
ozone sh key list /vol1/bucket1/.snapshot/finance_backup_2025
```
  or equivalently
```shell
ozone fs -ls /vol1/bucket1/.snapshot/finance_backup_2025

```
  This will list all keys and directories that were in the bucket at the moment of that snapshot. To read a file from the snapshot, for example:
```shell
ozone sh key get /vol1/bucket1/.snapshot/finance_backup_2025/reports/Q1.csv ./Q1_snapshot.csv
```
  which downloads the key as it existed at snapshot time. (Using `ozone fs`, one could likewise use `-get` on the `.snapshot/<snapshotName>/<key>` path.) **Note:** Accessing data in a snapshot requires read privileges on the bucket, just as reading the current data does.

**Snapshot Diff operations:** Snapshot diff allows you to see what changed between two snapshots (or between a snapshot and the live bucket). For example:
```shell
ozone sh snapshot diff /vol1/bucket1 finance_backup_2025 finance_backup_2026
```
This will initiate or retrieve a diff report between the two snapshots `finance_backup_2025` and `finance_backup_2026`. The output will list keys with a prefix indicating addition (`+`), deletion (`-`), modification (`M`), or rename (`R`). For instance, you might see:
```shell
+ Q2.csv
- temp/dataset_old.csv
M reports/summary.txt
R projectX -> projectY
```
  indicating a new file `Q2.csv` added, an old file deleted, a file modified, and a directory renamed from `projectX` to `projectY`. SnapshotDiff results may span multiple pages; use the `-p` and `-t` options for pagination if the result is large. While a SnapshotDiff job is running, you can list active jobs with `ozone sh snapshot listDiff /vol1/bucket1` and even cancel one with `ozone sh snapshot cancelDiff <jobId>` (if needed for administrative reasons).
* **Renaming a Snapshot:** To rename an existing snapshot (e.g., give it a more meaningful name):
```shell
ozone sh snapshot rename /vol1/bucket1 finance_backup_2025 finance_backup_Q1
```
  This requires the bucket owner or admin privilege, since it changes snapshot metadata. After renaming, the snapshot will appear under the new name in `.snapshot` (e.g., `.snapshot/finance_backup_Q1`). Any references to the old snapshot name (such as for diff or info) should use the new name going forward.
* **Snapshot Info:** You can fetch detailed metadata about a snapshot:
```shell
ozone sh snapshot info /vol1/bucket1 finance_backup_Q1
```
  The output will include information like the snapshot ID, creation time, the status (whether the snapshot is active or scheduled for deletion), and space usage metrics such as **Reference Size** and **Exclusive Size**. *Reference size* is the total size of data that the snapshot can see (i.e., all keys in the snapshot, counting shared data), whereas *Exclusive size* is the size of data unique to that snapshot (data that would be freed if this snapshot were deleted).

All the above CLI operations correspond to underlying RPC calls to Ozone Manager. They enforce the same security checks described in the **Authorization** section.

### **Programmatic Access via Java**

In addition to the CLI, Ozone snapshots can be managed and accessed using Java APIs:

**Using the Hadoop Compatible FileSystem (HCFS) Interface:** The Ozone FileSystem (OFS) API presents Ozone storage as a Hadoop `FileSystem`. For example:

```java
Configuration conf = new OzoneConfiguration();
FileSystem fs = FileSystem.get(new Path("ofs://om-service/vol1/bucket1").toUri(), conf);

Path bucketPath = new Path("/vol1/bucket1");
String snapshotName = "finance_backup_2025";

// Create a snapshot
fs.createSnapshot(bucketPath, snapshotName);

// List snapshots (by listing .snapshot directory)
FileStatus[] snapshots = fs.listStatus(new Path(bucketPath, ".snapshot"));
for (FileStatus snap : snapshots) {
System.out.println("Snapshot: " + snap.getPath().getName());
}

// Read from snapshot (e.g., list files inside the snapshot)
Path snapshotPath = new Path(bucketPath, ".snapshot/" + snapshotName);
FileStatus[] files = fs.listStatus(snapshotPath);
for (FileStatus file : files) {
System.out.println("Snapshot content: " + file.getPath().toString());
}

// Rename snapshot (rename directory under .snapshot)
Path oldSnap = new Path(bucketPath, ".snapshot/" + snapshotName);
Path newSnap = new Path(bucketPath, ".snapshot/finance_backup_Q1");
fs.rename(oldSnap, newSnap);

// Delete snapshot
fs.deleteSnapshot(bucketPath, "finance_backup_Q1");
```

The above code demonstrates the Hadoop Compatible File System Snapshot APIs. Under the hood, these calls translate to the same OM actions as the CLI. The Java API will throw exceptions if the user lacks privileges or if the snapshot does not exist, etc. Always wrap these calls in try-catch and handle `OMException` or `IOException` as appropriate.

Snapshots are visible in the File System interface under the special `.snapshot` directory of each bucket. For example, an application can list and read snapshot contents using standard Hadoop file system calls:

```java
OzoneConfiguration conf = new OzoneConfiguration();
FileSystem fs = FileSystem.get(new URI("ofs://ozone1.vol1.bucket1"), conf);

// List all snapshots in the bucket by listing the .snapshot directory
FileStatus[] snapshots = fs.listStatus(new Path("/vol1/bucket1/.snapshot"));
for (FileStatus snap : snapshots) {
    System.out.println("Snapshot name: " + snap.getPath().getName());
}

// Read a file from a snapshot
FSDataInputStream in = fs.open(new Path("/vol1/bucket1/.snapshot/finance_backup_Q1/reports/Q1.csv"));
// ... (read from 'in' as usual, then close it)
in.close();

```

In the code above, the `ofs://ozone1.vol1.bucket1` URI is using the **Ozone File System (OFS)** scheme. The `.snapshot/finance_backup_Q1` path behaves like a read-only directory. Standard file system operations (e.g., `open`, `listStatus`) can be used on snapshot paths. For reference, see the [Ozone File System API guide](https://ozone.apache.org/docs/edge/interface/ofs.html) for details on the `ofs://` scheme.

**Using the Ozone ObjectStore Client API:** Ozone also provides a native RPC client interface. Applications can use `OzoneClient` and the `ObjectStore` API to programmatically create, list, and delete snapshots:

```Java
// Initialize Ozone RPC client
OzoneClient ozClient = OzoneClientFactory.getRpcClient(conf);
ObjectStore store = ozClient.getObjectStore();

// Create a snapshot
store.createSnapshot("vol1", "bucket1", "finance_backup_2025");

// List snapshots
Iterator<OzoneSnapshot> snapshotIter = store.listSnapshot("vol1", "bucket1", null, null);
while (snapshotIter.hasNext()) {
    OzoneSnapshot snap = snapshotIter.next();
    System.out.printf("Snapshot %s (ID=%s) created at %s%n",
      snap.getName(), snap.getSnapshotId(), snap.getCreationTime());
}

// Get snapshot info
OzoneSnapshot snapInfo = store.getSnapshotInfo("vol1", "bucket1", "finance_backup_2025");
long refBytes = snapInfo.getReferencedSize();
long exclBytes = snapInfo.getExclusiveSize();
System.out.println("Snapshot exclusive size = " + exclBytes + " bytes, referenced size = " + refBytes + " bytes");

// Rename snapshot
store.renameSnapshot("vol1", "bucket1", "finance_backup_2025", "finance_backup_Q1");

// Delete snapshot
store.deleteSnapshot("vol1", "bucket1", "finance_backup_Q1");
```

The above code snippet demonstrates the Ozone ObjectStore Client Java API for snapshot operations. Again under the hood, these calls translate to the same OM actions as the CLI. The Java API will throw exceptions if the user lacks privileges or if the snapshot does not exist, etc. Always wrap these calls in try-catch and handle `OMException` or `IOException` as appropriate.

**HTTP REST API Access:** For applications that cannot use the Java API directly, Ozone offers an HttpFS Gateway which exposes a WebHDFS-compatible REST API. Through HttpFS, you can perform filesystem operations (including reading from `.snapshot` paths) over HTTP. This can be useful for remote access to snapshot data. Snapshot management operations like create/delete/rename via HttpFS are supported, but getSnapshotDiff is not yet supported.

## **System Administration How-To**

This section summarizes the configuration parameters and administrative considerations for Ozone snapshots. Administrators can tune these settings in **ozone-site.xml** to control snapshot behavior and resource usage.

**Snapshot-Related Configuration Parameters:**

| Configuration Key | Default Value | Description |
| ----- | ----- | ----- |
| **ozone.om.fs.snapshot.max.limit** | **10000** | Maximum number of snapshots that Ozone Manager (OM) will allow per bucket (or overall). This is a safety limit on how many snapshots can be created. Earlier versions defaulted to 1000, but it has been increased to 10,000 in Ozone 2.1.0. |
| **ozone.om.snapshot.compaction.dag.max.time.allowed** | **2592000000 ms** (30 days) | Time window for which OM retains compaction DAG information to enable efficient SnapshotDiff calculations. Snapshots older than this age will still support diff, but the diff will fall back to a full metadata scan (slower). Administrators can increase this if longer retention of fast-diff data is needed (at the cost of more metadata storage). |
| **ozone.om.snapshot.diff.db.dir** | *(empty)* | Directory on the OM node for storing SnapshotDiff job data and scratch files. By default, if not set, it uses the same location as OM metadata (`ozone.metadata.dirs`). An admin may point this to a location with ample space, especially if large diffs are expected. |
| **ozone.om.snapshot.rocksdb.metrics.enabled** | **false** | Controls whether to collect detailed RocksDB metrics for the snapshot metadata store. By default this is disabled to avoid overhead. Enable (`true`) only if you need detailed RocksDB performance stats for snapshots (for debugging or monitoring). |
| **ozone.om.snapshot.load.native.lib** | **true** | When true, Ozone will use the native RocksDB library for certain snapshot operations (like loading native RocksDB checkpoints for OM). In some environments, this may cause issues. Setting this to false forces a pure-Java path for loading snapshot data, which can be used as a workaround if you encounter RocksDB native library problems. |
| **ozone.om.snapshot.diff.concurrent.max** | **10** | The maximum number of SnapshotDiff jobs that can run concurrently on one OM node (default is 10). If users request more diffs at once, additional requests will be queued or rejected until running jobs complete. An administrator can increase this limit to allow more parallel diff computations if OM has sufficient memory and CPU. (The config property name for this setting may be `ozone.om.snapshot.diff.thread.pool.size` in some versions.) |

In addition to the above, snapshot operations will consume memory proportional to the number of active snapshots and ongoing diff jobs. Administrators should monitor OM heap usage if many snapshots are kept or many large diffs are run simultaneously. It is also advisable to enable **Ozone Native ACLs or Ranger** for fine-grained access control on snapshot operations (see **Authorization** below).

**Monitoring Snapshots:** Ozone exposes metrics and JMX information for snapshot operations. For example, there are OM metrics for the number of snapshots, SnapshotDiff operation counts, etc., and these can be observed via the OM’s Prometheus metrics or RPC metrics. Administrators should also pay attention to the OM logs for snapshot-related messages – e.g., when a SnapshotDiff job starts or finishes, or if a snapshot delete triggers a large number of keys to be purged, etc.

## **Authorization**

Snapshot operations in Ozone are subject to access control checks. Not all users can create or delete snapshots by default, to prevent abuse or unintended usage. Below are the requirements and ACLs for each operation:

* **Create Snapshot / Delete Snapshot / Rename Snapshot:** These operations **require admin or owner privileges** on the bucket. For example, only the bucket’s owner (or a cluster admin) can create a snapshot on that bucket. Similarly, deleting or renaming a snapshot is restricted to the bucket owner or admin. If a non-owner user attempts these operations, OM will reject it with an **Access Denied** error.
* **List Snapshots / Get Snapshot Info:** These read-only operations perform ACL checks to ensure the user has **read/list access** on the bucket. In general, a user who has permission to list the bucket’s contents can also list its snapshots. The Ozone native ACL type that governs listing child objects (like snapshots) is “l” (list). In practice, having *READ* access on the bucket implicitly grants the ability to see snapshot listings and info. The documentation often phrases this as requiring *read privilege on the bucket*.
* **SnapshotDiff / Cancel SnapshotDiff / List SnapshotDiff Jobs:** These operations also perform ACL checks on the bucket. Since a snapshot diff reveals information about keys (names and whether they changed), the user must have **read access** on the bucket to run a diff or view diff jobs. In Ozone’s native ACLs, that means the user needs either the “l” (list) permission to list the keys or “r” (read) to read the key metadata. In practice, if a user can read the bucket’s data, they can compare its snapshots. The `listSnapshotDiffJobs` and `cancelSnapshotDiff` administrative calls typically require the same level of access (or owner/admin privileges if attempting to cancel someone else’s job). Generally, a non-admin user will only see and manage their own snapshot diff jobs for buckets they have access to.

It’s worth noting that Ozone supports two kinds of authorization models for snapshots: **Ozone native ACLs** and optionally **Ranger policies** (if Ranger is integrated). The above describes native ACL behavior. If using Ranger or a similar authorizer, ensure that appropriate permissions are granted for the `snapshot` operations (Ranger defines separate permissions for volume, bucket, and key actions, and snapshot create/delete would fall under bucket admin operations).

## **Comparison to HDFS Snapshots**

Apache Ozone’s snapshot feature is conceptually similar to HDFS snapshots, but there are important differences:

* **Granularity:** Ozone snapshots are **bucket-level only**. You can snapshot an entire bucket at once (which may contain a hierarchy of directories and keys), but you cannot snapshot a sub-directory of a bucket in isolation. By contrast, HDFS supports snapshots at any directory level (you must designate a directory as *snapshottable* and then you can snapshot that directory and its subtree). In Ozone, the bucket is the smallest unit of snapshot; if you need snapshot-like behavior on a subset of data, you would need to organize that data into its own bucket.
* **Metadata vs. Data Changes:** Ozone snapshots primarily track **key-level changes** (file creations, deletions, modifications, and renames) within the bucket. They do not capture changes to bucket metadata or properties. For example, if you change a bucket’s quota or ACLs, those changes are not versioned via snapshots – a snapshot only reflects the state of the filesystem namespace (keys and directories). HDFS snapshots are similar in that they track file/directory changes and not higher-level constructs (like no snapshot of an HDFS mount point’s permission outside the snapshotted dir).
* **Access and Restore:** Reading from an Ozone snapshot is done through the special `.snapshot` path in the bucket, analogous to HDFS where snapshots appear in a `.snapshot` directory under the snapshottable directory. Restoring data from a snapshot in Ozone is a manual process of copying data out of the `.snapshot` directory back into the active area if needed (or using DistCp for large-scale copy). There is no automatic “rollback” of a bucket to a snapshot, since Ozone treats snapshots as read-only backups (similar approach in HDFS – HDFS snapshots can be used to restore files but not instant revert of the live state without manual intervention).
* **Implementation:** Internally, Ozone snapshots leverage the OM’s key-value store and share data blocks between snapshots, which is analogous to HDFS’s copy-on-write approach for metadata. One notable difference is that Ozone being object-store, does not have an equivalent of HDFS block tracking for snapshots at the DataNode level – all the intelligence is in the OM. HDFS NameNode also handles snapshot metadata, but Ozone’s design (with its RocksDB metadata store) means snapshot creation is O(1) and implemented via metadata pointers rather than duplication. Both systems do not copy actual file data on snapshot – they rely on not deleting underlying blocks.

In summary, Ozone snapshots give similar benefits for Ozone buckets as HDFS snapshots do for directories, but Ozone’s approach is bucket-scoped and oriented toward object store semantics. If coming from HDFS, just note that you cannot snapshot volumes or the entire namespace at once (you snapshot each bucket individually), and you cannot snapshot sub-folders of a bucket.

## **Known Issues and Limitations**

While Apache Ozone’s snapshot feature is powerful, there are a few current limitations and caveats to be aware of:

* **S3 Interface Support:** Snapshot operations are *not* supported via the S3 API (`s3://` or `s3a://` access). Ozone’s S3 Gateway does not expose snapshot functionality, since Amazon S3 has no direct concept of snapshots. This means you cannot create or list snapshots through the S3 Gateway or the `s3a` Hadoop FileSystem connector. Snapshots must be managed via the Ozone RPC interface (ozone shell, ozone fs, or Ozone Client API). However, snapshotted data can be read via the S3 Gateway using the special `.snapshot` key prefix. For example, a key `key1` in the snapshot `snap1` can be read using the path `.snapshot/snap1/key1`.
* **Ratis & EC Buckets:** Snapshots are supported on both replication (Ratis) and erasure-coded buckets in Ozone. There is no limitation in using snapshots on an EC-enabled bucket – the snapshot will preserve the keys regardless of the underlying replication scheme. However, as noted above, you must use the Ozone interfaces (RPC/HCFS) to manage these snapshots, not S3. All snapshots behave consistently in terms of data retention whether the data is Ratis-replicated or EC-coded.
* **Snapshots During Ongoing Deletes:** If a large recursive delete is executed on a bucket and a snapshot is taken concurrently (or just before the delete), it’s possible that the space will not be reclaimed until the snapshot is removed. In simpler terms, if you delete a large directory (with many keys) from the active bucket but you had created a snapshot right before that delete (or during the deletion process), all those deleted keys remain protected by the snapshot. They will consume space on disk until the snapshot that references them is deleted. This is expected (since snapshots preserve data), but the caveat is that if an admin is not aware a snapshot was taken, it may appear that a delete did not free up space. The space will be freed only after deleting the snapshot and allowing the background cleaner to run. In practice, avoid scheduling snapshots at exactly the same time as massive deletes, or be mindful that snapshots will “lock” data from deletion.
* **Reserved Namespace Name `.snapshot`:** The name `.snapshot` is reserved by Ozone. Users cannot create keys or directories with the exact name `.snapshot` at the root of a bucket (and similarly, `.snapshot` cannot be used as a volume or bucket name). This is because `.snapshot` is used as the special entry to access snapshots. For example, if a user tries to create a directory literally named `.snapshot` in a bucket, the system will prevent it or treat it as the snapshot namespace. This is analogous to HDFS where `.snapshot` is a reserved directory name in snapshottable directories. The reservation is case-sensitive (only `.snapshot` in all lowercase is reserved). As a best practice, avoid naming any key or folder in Ozone starting with “.snapshot” to prevent any ambiguity.
* **Snapshot Performance and Scale:** Creating snapshots is instantaneous and cheap in Ozone; however, maintaining a very large number of snapshots (thousands per bucket) can increase the size of OM metadata and slow down operations like listing snapshots or performing snapshot diff (especially if many snapshots are being diffed). Recent improvements have raised the supported snapshot limit (default max 10k per OM as noted above) and optimized diff performance. Nevertheless, if you have an extremely high snapshot count, monitor the OM for any memory or performance impact. Likewise, snapshot diff operations that produce extremely large outputs (e.g., millions of differences) may be resource-intensive to serve; they are paginated and cached to mitigate this.
* **Space Utilization Reporting:** The space occupied by snapshots is essentially the space used by data that is no longer in the active bucket but still referenced by snapshots. Ozone currently provides metrics for snapshot “exclusive” vs “shared” size in the snapshot info. However, tools and commands that report bucket usage might not immediately make it obvious how much space is tied up in snapshots. Admins should use `ozone sh snapshot info` to see how much data each snapshot is holding (exclusively) to understand storage usage. Additionally, deleting a snapshot does not instantly free space; it queues keys for deletion and actual block deletion happens asynchronously via the OM and SCM’s normal cleanup processes.

By keeping these limitations and behaviors in mind, administrators and users can effectively use snapshots while avoiding surprises. Ozone’s snapshot feature is evolving, and future versions may address some of these limitations or add new capabilities (refer to the project release notes for updates).

## **Linux System Configuration**

Proper Linux system configuration and hardware provisioning are important for running Ozone with snapshots in production. The snapshot feature, due to its additional metadata and potential large number of file handles, may require tuning the operating system limits and using high-performance storage for metadata:

* **Open File Descriptors Limit:** It is recommended to raise the maximum number of open file descriptors (ulimit) for the Ozone Manager process. Each RocksDB instance (and each snapshot and diff job) can open many files. In production deployments, consider setting the per-process file descriptor limit to **at least 32,000**, and in many cases 64k or 128k is advisable. Ensure the Linux `nofile` ulimit is configured accordingly (via `/etc/security/limits.conf` or systemd service unit limits for ozone).
* **Use NVMe SSDs for Metadata:** Ozone Manager’s metadata (which includes the RocksDB key tables and snapshot info) should be placed on fast storage. It is highly recommended to use NVMe SSD drives for the OM metadata directories. Snapshots put additional I/O load on OM’s RocksDB (for maintaining multiple snapshot tables and for scanning during diffs). Fast disk (NVMe) improves OM throughput significantly. If NVMe is not available, use the fastest disks possible (SSD over HDD). Additionally, consider RAID1 or RAID10 for OM metadata for reliability, but prioritize random read/write performance.
* **Memory and CPU for Ozone Manager:** Snapshots consume OM memory proportional to how many are being accessed concurrently. Each active SnapshotDiff job, for example, will load snapshot metadata into memory. By default, OM allows 10 concurrent diff jobs – if each diff is large, this can use considerable memory. Ensure the Ozone Manager is allocated sufficient heap. For a production cluster with snapshots, a larger heap (e.g., 16-32 GB or more) for OM may be warranted, depending on the number of keys and snapshots. Monitor GC and adjust the heap size as needed.
* **Disk Space for Snapshots:** Plan for additional disk usage on DataNodes. Each snapshot retains data blocks that might otherwise have been deleted. For example, if a bucket has 100 GB of data and you take a snapshot, then later an additional 10 GB is added to the bucket, the total physical storage used will be \~110 GB (before replication). Common data between the active bucket and snapshots is not duplicated, but any data that gets deleted from the active bucket remains stored as long as a snapshot references it. Therefore, if users keep many snapshots or long retention, the storage can grow significantly. Administrators should factor this into capacity planning – e.g., if expecting N days of snapshots with X change rate per day, ensure DataNodes have X\*N extra space (plus replication overhead).
* **Kernel and Filesystem Settings:** There are no Ozone-specific kernel tweaks required beyond what HDFS would typically need. Ensure the filesystem hosting OM metadata is formatted with a filesystem that can handle a large number of small files efficiently (ext4, xfs are common choices – xfs is often preferred for RocksDB workloads). Ensure disk scheduler and RAID controller settings favor low latency.
* **Networking:** Snapshot operations (like diff) can result in large data transfers between OM and clients (for diff results). Ensure the network link to OM is robust. If using HttpFS to retrieve snapshot data, consider that as well in network planning.

In summary, for production use of snapshots: **raise the open file limits**, **use high-performance storage (NVMe)** for metadata, and **allocate ample memory to OM**. These steps will help Ozone handle snapshots (and their metadata overhead in OM’s RocksDB) smoothly. Always test snapshot operations under load to verify that the system is well-tuned. Many of these considerations are similar to tuning a NameNode for HDFS snapshots, but adjusted for Ozone’s architecture.

