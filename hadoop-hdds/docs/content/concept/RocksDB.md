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

RocksDB is a critical component of Apache Ozone, providing a high-performance embedded key-value store. It is used by various Ozone services to persist metadata and state.

## 1. Introduction to RocksDB

RocksDB is a log-structured merge-tree (LSM-tree) based key-value store developed by Facebook. It is optimized for fast storage environments like SSDs and offers high write throughput and efficient point lookups. For more details, refer to the [RocksDB GitHub project](https://github.com/facebook/rocksdb) and the [RocksDB Wiki](https://github.com/facebook/rocksdb/wiki).

## 2. How Ozone uses RocksDB

RocksDB is utilized in the following Ozone components:

*   **Ozone Manager (OM):** Stores metadata related to volumes, buckets, keys, and directory structures.
*   **Storage Container Manager (SCM):** Persists information about containers, datanodes, and pipelines.
*   **Datanode:** Used for block metadata and other internal state.

## 3. Tunings applicable to RocksDB

Effective tuning of RocksDB can significantly impact Ozone's performance. Ozone exposes several configuration properties to tune RocksDB behavior. These properties are typically found in `ozone-default.xml` and can be overridden in `ozone-site.xml`.

Key tuning parameters often involve:

*   **Memory usage:** Configuring block cache, write buffer manager, and other memory-related settings.
    *   `hdds.datanode.metadata.rocksdb.cache.size`: Configures the block cache size for RocksDB instances on Datanodes. Default value: `1GB`.
*   **Compaction strategies:** Optimizing how data is merged and organized on disk.
    *   `hdds.datanode.rocksdb.auto-compaction-small-sst-file`: Enables or disables auto-compaction for small SST files. Default value: `true`.
    *   `hdds.datanode.rocksdb.auto-compaction-small-sst-file-size-threshold`: Threshold for small SST file size for auto-compaction. Default value: `1MB`.
    *   `hdds.datanode.rocksdb.auto-compaction-small-sst-file-num-threshold`: Threshold for the number of small SST files for auto-compaction. Default value: `512`.
*   **Write-ahead log (WAL) settings:** Balancing durability and write performance.
    *   `hdds.datanode.rocksdb.log.max-file-size`: Maximum size of each RocksDB log file. Default value: `32MB`.
    *   `hdds.datanode.rocksdb.log.max-file-num`: Maximum number of RocksDB log files. Default value: `64`.
    *   `ozone.metastore.rocksdb.writeoption.sync`: Whether to sync RocksDB writes to disk. Default value: `false` (based on common RocksDB usage for performance).

Ozone also manages RocksDB's `DBOptions` and `ColumnFamilyOptions` through `DBProfile`s (e.g., `DatanodeDBProfile`), which can be configured based on storage types (SSD, HDD). This can be configured using the `hdds.db.profile` property.

*   `hdds.db.profile`: Specifies the RocksDB profile to use, which determines the default `DBOptions` and `ColumnFamilyOptions`. Default value: `DISK`.
    *   Possible values include `SSD`, `DISK`, and `TEST`.
    *   For example, setting this to `SSD` will apply tunings optimized for SSD storage.

## 4. Troubleshooting and repair tools relevant to RocksDB

Troubleshooting RocksDB issues in Ozone often involves:

*   Analyzing RocksDB logs for errors and warnings.
*   Using RocksDB's built-in tools (e.g., `ldb`, `sst_dump`) for inspecting database files.
*   Understanding common RocksDB error codes and their implications.

## 5. Version Compatibility

This section will detail the specific RocksDB versions that are compatible with different Apache Ozone releases, including any known issues or recommended versions.

## 6. Monitoring and Metrics

Monitoring RocksDB performance is crucial for maintaining a healthy Ozone cluster.

*   **RocksDB Statistics:** Ozone can expose detailed RocksDB statistics. Enable this by setting `ozone.metastore.rocksdb.statistics` to `ALL` or `EXCEPT_DETAILED_TIMERS` in `ozone-site.xml`. Be aware that enabling detailed statistics can incur a performance penalty (5-10%).
*   **Grafana Dashboards:** Ozone provides Grafana dashboards that visualize low-level RocksDB statistics. Refer to the [Ozone Monitoring Documentation]({{< ref "feature/Observability.md" >}}) for details on setting up monitoring and using these dashboards.

## 7. Storage Sizing

Properly sizing the storage for RocksDB instances is essential to prevent performance bottlenecks and out-of-disk errors. The requirements vary significantly for each Ozone component, and using dedicated, fast storage (SSDs) is highly recommended.

*   **Ozone Manager (OM):**
  *   **Baseline:** A minimum of **100 GB** should be reserved for the OM's RocksDB instance. The OM stores the entire namespace metadata (volumes, buckets, keys), so this is the most critical database in the cluster.
  *   **With Snapshots:** Enabling Ozone Snapshots will substantially increase storage needs. Each snapshot preserves a view of the metadata, and the underlying data files (SSTs) cannot be deleted by compaction until a snapshot is removed. The exact requirement depends on the number of retained snapshots and the rate of change (creations/deletions) in the namespace. Monitor disk usage closely after enabling snapshots.

*   **Storage Container Manager (SCM):**
  *   SCM's metadata footprint (pipelines, containers, Datanode heartbeats) is much smaller than the OM's. A baseline of **20-50 GB** is typically sufficient for its RocksDB instance.

*   **Datanode:**
  *   The Datanode's RocksDB stores metadata for all containers and their blocks. Its size grows proportionally with the number of containers and blocks hosted on that Datanode.
  *   **Rule of Thumb:** A good starting point is to reserve **0.1% to 0.5%** of the total data disk capacity for RocksDB metadata. For example, a Datanode with 100 TB of data disks should reserve between 100 GB and 500 GB for its RocksDB metadata.
  *   Workloads with many small files will result in a higher block count and will require space on the higher end of this range.
