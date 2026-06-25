---
title: "RocksDB in Apache Ozone"
menu:
  main:
    parent: Architecture
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

> Note: This page covers advanced topics. Ozone administrators typically do not need to tinker with these settings.

RocksDB is a critical component of Apache Ozone, providing a high-performance embedded key-value store. It is used by various Ozone services to persist metadata and state.

## 1. Introduction to RocksDB

RocksDB is a log-structured merge-tree (LSM-tree) based key-value store developed by Facebook. It is optimized for fast storage environments like SSDs and offers high write throughput and efficient point lookups. For more details, refer to the [RocksDB GitHub project](https://github.com/facebook/rocksdb) and the [RocksDB Wiki](https://github.com/facebook/rocksdb/wiki).

## 2. How Ozone uses RocksDB

RocksDB is utilized in the following Ozone components to store critical metadata:

*   **Ozone Manager (OM):** The OM uses RocksDB as its primary metadata store, holding the entire namespace and related information. As defined in `OMDBDefinition.java`, this includes tables for:
    *   **Namespace:** `volumeTable`, `bucketTable`, `keyTable` (for object store layout), `directoryTable`, and `fileTable` (for file system layout).
    *   **Security:** `userTable`, `dTokenTable` (delegation tokens), and `s3SecretTable`.
    *   **State Management:** `transactionInfoTable` for tracking transactions, `deletedTable` for pending key deletions, and `snapshotInfoTable` for managing Ozone snapshots.

*   **Storage Container Manager (SCM):** The SCM persists the state of the storage layer in RocksDB. The structure, defined in `SCMDBDefinition.java`, includes tables for:
    *   `pipelines`: Manages the state and composition of data pipelines.
    *   `containers`: Stores information about all storage containers in the cluster.
    *   `deletedBlocks`: Tracks blocks that are marked for deletion and awaiting garbage collection.
    *   `move`: Coordinates container movements for data rebalancing.
    *   `validCerts`: Stores certificates for validating datanodes.
    *   `validSCMCerts`: Stores certificates for validating SCMs.
    *   `scmTransactionInfos`: Tracks SCM transactions.
    *   `sequenceId`: Manages sequence IDs for various SCM operations.
    *   `meta`: Stores miscellaneous SCM metadata, like upgrade status.
    *   `statefulServiceConfig`: Stores configurations for stateful services.

*   **Datanode:** A Datanode utilizes RocksDB for two main purposes:
    1.  **Per-Volume Metadata:** It maintains one RocksDB instance per storage volume. Each of these instances manages metadata for the containers and blocks stored on that specific volume. As specified in `DatanodeSchemaThreeDBDefinition.java`, this database is structured with column families for `block_data`, `metadata`, `delete_txns`, `finalize_blocks`, and `last_chunk_info`. To optimize performance, it uses a fixed-length prefix based on the container ID, enabling efficient lookups with RocksDB's prefix seek feature.
    2.  **Global Container Tracking:** Additionally, each Datanode has a single, separate RocksDB instance to record the set of all containers it manages. This database, defined in `WitnessedContainerDBDefinition.java`, contains a `ContainerCreateInfoTable` table that provides a complete index of the containers hosted on that Datanode.

*   **Recon:** Ozone's administration and monitoring tool, Recon, maintains its own RocksDB database to store aggregated and historical data for analysis. The `ReconDBDefinition.java` outlines tables for:
    *   `containerKeyTable`: Maps containers to the keys they contain.
    *   `namespaceSummaryTable`: Stores aggregated namespace information for quick reporting.
    *   `replica_history`: Tracks the historical locations of container replicas, which is essential for auditing and diagnostics.
    *   `keyContainerTable`: Maps keys to the containers they are in.
    *   `containerKeyCountTable`: Stores the number of keys in each container.
    *   `replica_history_v2`: Tracks the historical locations of container replicas with BCSID, which is essential for auditing and diagnostics.

## 3. Tunings applicable to RocksDB

Effective tuning of RocksDB can significantly impact Ozone's performance. Ozone exposes several configuration properties to tune RocksDB behavior. These properties are typically found in `ozone-default.xml` and can be overridden in `ozone-site.xml`.

### General Settings

Ozone provides a set of general RocksDB configurations that apply to all services (OM, SCM, and Datanodes) unless overridden by more specific settings. With the exception of `hdds.db.profile` and `ozone.metastore.rocksdb.cf.write.buffer.size`, these properties are defined in `RocksDBConfiguration.java`.

*   `hdds.db.profile`: Specifies the RocksDB profile to use, which determines the default `DBOptions` and `ColumnFamilyOptions`. Default value: `DISK`.
    *   Possible values include `SSD` and `DISK`.
    *   For example, setting this to `SSD` will apply tunings optimized for SSD storage.

*   **Write Options:**
    *   `hadoop.hdds.db.rocksdb.writeoption.sync`: If set to `true`, writes are synchronized to persistent storage, ensuring durability at the cost of performance. If `false`, writes are flushed asynchronously. Default: `false`.

*   `ozone.metastore.rocksdb.cf.write.buffer.size`: The write buffer (memtable) size for each column family of the rocksdb store. Default: `128MB`.

*   **Write-Ahead Log (WAL) Management:**
    *   `hadoop.hdds.db.rocksdb.WAL_ttl_seconds`: The time-to-live for WAL files in seconds. Default: `1200`.
    *   `hadoop.hdds.db.rocksdb.WAL_size_limit_MB`: The total size limit for WAL files in megabytes. When this limit is exceeded, the oldest WAL files are deleted. A value of `0` means no limit. Default: `0`.

*   **Logging:**
    *   `hadoop.hdds.db.rocksdb.logging.enabled`: Enables or disables RocksDB's own logging. Default: `false`.
    *   `hadoop.hdds.db.rocksdb.logging.level`: The logging level for RocksDB (INFO, DEBUG, WARN, ERROR, FATAL). Default: `INFO`.
    *   `hadoop.hdds.db.rocksdb.max.log.file.size`: The maximum size of a single RocksDB log file. Default: `100MB`.
    *   `hadoop.hdds.db.rocksdb.keep.log.file.num`: The maximum number of RocksDB log files to retain. Default: `10`.

### Ozone Manager (OM) Specific Settings

These settings, defined in `ozone-default.xml`, apply specifically to the Ozone Manager.

*   `ozone.om.db.max.open.files`: The total number of files that a RocksDB can open in the OM. Default: `-1` (unlimited).
*   `ozone.om.compaction.service.enabled`: Enable or disable a background job that periodically compacts rocksdb tables flagged for compaction. Default: `false`.
*   `ozone.om.compaction.service.run.interval`: The interval for the OM's compaction service. Default: `6h`.
*   `ozone.om.compaction.service.timeout`: Timeout for the OM's compaction service. Default: `10m`.
*   `ozone.om.compaction.service.columnfamilies`: A comma-separated list of column families to be compacted by the service. Default: `keyTable,fileTable,directoryTable,deletedTable,deletedDirectoryTable,multipartInfoTable`.

### DataNode-Specific Settings

These settings, defined in `DatanodeConfiguration.java`, apply specifically to Datanodes and will override the general settings where applicable.

Key tuning parameters for the DataNode often involve:

*   **Memory usage:** Configuring block cache, write buffer manager, and other memory-related settings.
    *   `hdds.datanode.metadata.rocksdb.cache.size`: Configures the block cache size for RocksDB instances on Datanodes. Default value: `1GB`.
*   **Compaction strategies:** Optimizing how data is merged and organized on disk. For more details, refer to the [Merge Container RocksDB in DN Documentation]({{< ref "feature/dn-merge-rocksdb.md" >}}).
    *   `hdds.datanode.rocksdb.auto-compaction-small-sst-file`: Enables or disables auto-compaction for small SST files. Default value: `true`.
    *   `hdds.datanode.rocksdb.auto-compaction-small-sst-file-size-threshold`: Threshold for small SST file size for auto-compaction. Default value: `1MB`.
    *   `hdds.datanode.rocksdb.auto-compaction-small-sst-file-num-threshold`: Threshold for the number of small SST files for auto-compaction. Default value: `512`.
    *   `hdds.datanode.rocksdb.auto-compaction-small-sst-file.interval.minutes`: Auto compact small SST files interval in minutes. Default value: `120`.
    *   `hdds.datanode.rocksdb.auto-compaction-small-sst-file.threads`: Auto compact small SST files threads. Default value: `1`.
*   **Write-ahead log (WAL) settings:** Balancing durability and write performance.
    *   `hdds.datanode.rocksdb.log.max-file-size`: The max size of each user log file of RocksDB. O means no size limit. Default value: `32MB`.
    *   `hdds.datanode.rocksdb.log.max-file-num`: The max user log file number to keep for each RocksDB. Default value: `64`.
*   **Logging:**
    *   `hdds.datanode.rocksdb.log.level`: The user log level of RocksDB(DEBUG/INFO/WARN/ERROR/FATAL)). Default: `INFO`.
*   **Other Settings:**
    *   `hdds.datanode.rocksdb.delete-obsolete-files-period`: Periodicity when obsolete files get deleted. Default is 1h.
    *   `hdds.datanode.rocksdb.max-open-files`: The total number of files that a RocksDB can open. Default: `1024`.

## 4. Troubleshooting and repair tools relevant to RocksDB

Troubleshooting RocksDB issues in Ozone often involves:

*   Analyzing RocksDB logs for errors and warnings.
*   Using RocksDB's built-in tools for inspecting database files:
    *   [**ldb**](https://github.com/facebook/rocksdb/wiki/Administration-and-Data-Access-Tool#ldb-tool): A command-line tool for inspecting and manipulating the contents of a RocksDB database.
    *   [**sst_dump**](https://github.com/facebook/rocksdb/wiki/Administration-and-Data-Access-Tool#sst-dump-tool): A command-line tool for inspecting the contents of SST (Static Table) files, which are the files that store the data in RocksDB.
*   Understanding common RocksDB error codes and their implications.

## 5. Version Compatibility

Apache Ozone uses RocksDB version 7.7.3. It is recommended to use RocksDB tools of this version to ensure compatibility and avoid any potential issues.

## 6. Monitoring and Metrics

Monitoring RocksDB performance is crucial for maintaining a healthy Ozone cluster.

*   **RocksDB Statistics:** Ozone can expose detailed RocksDB statistics. Enable this by setting `ozone.metastore.rocksdb.statistics` to `ALL` or `EXCEPT_DETAILED_TIMERS` in `ozone-site.xml`. Be aware that enabling detailed statistics can incur a performance penalty (5-10%).
*   **Grafana Dashboards:** Ozone provides Grafana dashboards that visualize low-level RocksDB statistics. Refer to the [Ozone Monitoring Documentation]({{< ref "feature/Observability.md" >}}) for details on setting up monitoring and using these dashboards.

## 7. Storage Sizing

Properly sizing the storage for RocksDB instances is essential to prevent performance bottlenecks and out-of-disk errors. The requirements vary significantly for each Ozone component, and using dedicated, fast storage (SSDs) is highly recommended.

*   **Ozone Manager (OM):**
  *   **Baseline:** A minimum of **100 GB** should be reserved for the OM's RocksDB instance. The OM stores the entire namespace metadata (volumes, buckets, keys), so this is the most critical database in the cluster.
  *   **With Snapshots:** Enabling Ozone Snapshots will substantially increase storage needs. Each snapshot preserves a view of the metadata, and the underlying data files (SSTs) cannot be deleted by compaction until a snapshot is removed. The exact requirement depends on the number of retained snapshots and the rate of change (creations/deletions) in the namespace. Monitor disk usage closely after enabling snapshots. For more details, refer to the [Ozone Snapshot Documentation]({{< ref "feature/Snapshot.md" >}}).

*   **Storage Container Manager (SCM):**
  *   SCM's metadata footprint (pipelines, containers, Datanode heartbeats) is much smaller than the OM's. A baseline of **20-50 GB** is typically sufficient for its RocksDB instance.

*   **Datanode:**
  *   The Datanode's RocksDB stores metadata for all containers and their blocks. Its size grows proportionally with the number of containers and blocks hosted on that Datanode.
  *   **Rule of Thumb:** A good starting point is to reserve **0.1% to 0.5%** of the total data disk capacity for RocksDB metadata. For example, a Datanode with 100 TB of data disks should reserve between 100 GB and 500 GB for its RocksDB metadata.
  *   Workloads with many small files will result in a higher block count and will require space on the higher end of this range.
