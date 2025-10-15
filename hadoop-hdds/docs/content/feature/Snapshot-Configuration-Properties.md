---
title: "Snapshot Configuration Properties"
weight: 2
menu:
  main:
    parent: "Ozone Snapshot"
summary: Snapshot configuration properties overview
hideFromSectionPage: true
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

Key configurations for Ozone snapshots.

## Snapshot-Related Configuration Parameters

These parameters, defined in `ozone-site.xml`, control how Ozone manages snapshots.

*   **General Snapshot Management**
    *   `ozone.om.fs.snapshot.max.limit`: Max snapshots per bucket (Default: 10000). Safety limit.
    *   `ozone.om.ratis.snapshot.dir`: The directory where OM Ratis snapshots are stored (Default: ratis-snapshot under OM DB dir).
    *   `ozone.om.ratis.snapshot.max.total.sst.size`: The maximum total size of SST files to be included in a Ratis snapshot (Default: 100000000).
    *   `ozone.om.snapshot.load.native.lib`: Use native RocksDB library for snapshot operations (Default: true). Set to false as a workaround for native library issues.
    *   `ozone.om.snapshot.checkpoint.dir.creation.poll.timeout`: Timeout for polling the creation of the snapshot checkpoint directory (Default: 20s).

*   **SnapshotDiff Service**
    *   `ozone.om.snapshot.diff.db.dir`: Directory for SnapshotDiff job data. Defaults to OM metadata dir. Use a spacious location for large diffs.
    *   `ozone.om.snapshot.force.full.diff`: Force a full diff for all snapshot diff jobs (Default: false).
    *   `ozone.om.snapshot.diff.disable.native.libs`: Disable native libraries for snapshot diff (Default: false).
    *   `ozone.om.snapshot.diff.max.page.size`: Maximum page size for snapshot diff (Default: 1000).
    *   `ozone.om.snapshot.diff.thread.pool.size`: Thread pool size for snapshot diff (Default: 10).
    *   `ozone.om.snapshot.diff.job.default.wait.time`: Default wait time for a snapshot diff job (Default: 1m).
    *   `ozone.om.snapshot.diff.max.allowed.keys.changed.per.job`: Maximum number of keys allowed to be changed per snapshot diff job (Default: 10000000).

*   **Snapshot Compaction and Cleanup**
    *   `ozone.snapshot.key.deleting.limit.per.task`: The maximum number of keys scanned by the snapshot deleting service in a single run (Default: 20000).
    *   `ozone.om.snapshot.compact.non.snapshot.diff.tables`: When enabled, allows compaction of tables not tracked by snapshot diffs after snapshots are evicted from the cache (Default: false).
    *   `ozone.om.snapshot.compaction.dag.max.time.allowed`: Window for efficient SnapshotDiff (Default: 30 days). Older diffs may be slower.
    *   `ozone.om.snapshot.prune.compaction.backup.batch.size`: Batch size for pruning compaction backups (Default: 2000).
    *   `ozone.om.snapshot.compaction.dag.prune.daemon.run.interval`: Interval for the compaction DAG pruning daemon (Default: 1h).
    *   `ozone.om.snapshot.diff.max.jobs.purge.per.task`: Maximum number of snapshot diff jobs to purge per task (Default: 100).
    *   `ozone.om.snapshot.diff.job.report.persistent.time`: Persistence time for snapshot diff job reports (Default: 7d).
    *   `ozone.om.snapshot.diff.cleanup.service.run.interval`: Interval for the snapshot diff cleanup service (Default: 1m).
    *   `ozone.om.snapshot.diff.cleanup.service.timeout`: Timeout for the snapshot diff cleanup service (Default: 5m).
    *   `ozone.om.snapshot.cache.cleanup.service.run.interval`: Interval for the snapshot cache cleanup service (Default: 1m).
    *   `ozone.snapshot.filtering.limit.per.task`: The maximum number of snapshots to be filtered in a single run of the snapshot filtering service (Default: 2).
    *   `ozone.snapshot.deleting.limit.per.task`: The maximum number of snapshots to be deleted in a single run of the snapshot deleting service (Default: 10).
    *   `ozone.snapshot.filtering.service.interval`: Interval for the snapshot filtering service (Default: 60s).
    *   `ozone.snapshot.deleting.service.timeout`: Timeout for the snapshot deleting service (Default: 300s).
    *   `ozone.snapshot.deleting.service.interval`: Interval for the snapshot deleting service (Default: 30s).
    *   `ozone.snapshot.deep.cleaning.enabled`: Enable deep cleaning of snapshots (Default: false).

*   **Performance and Resource Management**
    *   `ozone.om.snapshot.rocksdb.metrics.enabled`: Enable detailed RocksDB metrics for snapshots (Default: false). Use for debugging/monitoring.
    *   `ozone.om.snapshot.cache.max.size`: Maximum size of the snapshot cache soft limit (Default: 10).
    *   `ozone.om.snapshot.db.max.open.files`: Maximum number of open files for the snapshot database (Default: 100).

*   **Snapshot Provider (Internal)**
    *   `ozone.om.snapshot.provider.socket.timeout`: Socket timeout for the snapshot provider (Default: 5s).
    *   `ozone.om.snapshot.provider.connection.timeout`: Connection timeout for the snapshot provider (Default: 5s).
    *   `ozone.om.snapshot.provider.request.timeout`: Request timeout for the snapshot provider (Default: 5m).

## Recon-Specific Settings

These settings, defined in `ozone-default.xml`, apply specifically to Recon.
*   `ozone.recon.om.snapshot.task.initial.delay`: Initial delay for the OM snapshot task in Recon (Default: 1m).
*   `ozone.recon.om.snapshot.task.interval.delay`: Interval for the OM snapshot task in Recon (Default: 5s).
*   `ozone.recon.om.snapshot.task.flush.param`: Flush parameter for the OM snapshot task in Recon (Default: false).
