---
title: "Improve Snapshot Compaction Scale"
weight: 1
menu:
   main:
      parent: Features
summary: Reduce the disk usage occupied by Ozone Snapshot metadata.
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
# Improving Snapshot Scale:

[HDDS-13003](https://issues.apache.org/jira/browse/HDDS-13003)

# Problem Statement

In Apache Ozone, snapshots currently take a checkpoint of the Active Object Store (AOS) RocksDB each time a snapshot is created and track the compaction of SST files over time. This model works efficiently when snapshots are short-lived, as they merely serve as hard links to the AOS RocksDB. However, over time, if an older snapshot persists while significant churn occurs in the AOS RocksDB (due to compactions and writes), the snapshot RocksDB may diverge significantly from both the AOS RocksDB and other snapshot RocksDB instances. This divergence increases storage requirements linearly with the number of snapshots.

# Solution Proposal:

The primary inefficiency in the current snapshotting mechanism stems from constant RocksDB compactions in AOS, which can cause a key, file, or directory entry to appear in multiple SST files. Ideally, each unique key, file, or directory entry should reside in only one SST file, eliminating redundant storage and mitigating the multiplier effect caused by snapshots. If implemented correctly, the total RocksDB size would be proportional to the total number of unique keys in the system rather than the number of snapshots.

## Snapshot Compaction:

Currently, automatic RocksDB compactions are disabled for snapshot RocksDB to preserve snapshot diff performance, preventing any form of compaction. However, snapshots can be compacted if the next snapshot in the chain is a checkpoint of the previous snapshot plus a diff stored in a separate SST file. The proposed approach involves rewriting snapshots iteratively from the beginning of the snapshot chain and restructuring them in a separate directory. P.S This has got nothing to do with compacting snapshot’s rocksdb, we are not going to enable rocksdb auto compaction on snapshot rocksdb.

1. ### Introducing a last compaction time:

   A new boolean flag (`needsCompaction`), timestamp (`lastCompactionTime`), int `version` will be added to snapshot metadata. If absent, `needsCompaction` will default to `true`.   
   A new list of Map\<String, List\<Longs\>\> (`uncompactedSstFiles`) also needs to be added to snapshot meta as part of snapshot create operation; this would be storing the original list of sst files in the uncompacted copy of the snapshot corresponding to keyTable/fileTable/DirectoryTable. This should be done as part of the snapshot create operation.  
   Since this is not going to be consistent across all OMs this would have to be written to a local yaml file inside the snapshot directory and this can be maintained in the SnapshotChainManager in memory on startup. So all updates should not go via ratis.  
   An additional Map\<Integer, Map\<String, List\<Long\>\>\> (`compactedSstFiles`) also needs to be added to snapshotMeta. This will be maintaining a list of sstFiles of different versions of compacted snapshots. The key here would be the version number of snapshots.

2. ### Snapshot Cache Lock for Read Prevention

   A snapshot lock will be introduced in the snapshot cache to prevent reads on a specific snapshot during compaction. This ensures no active reads occur while replacing the underlying RocksDB instance.

3. ### Directory Structure Changes

   Snapshots currently reside in the `db.checkpoints` directory. The proposal introduces a `db.checkpoints.compacted` directory for compacted snapshots. The directory format should be as follows:

| om.db-\<snapshot\_id\>.\<version\> |
| :---- |

4. ### Optimized Snapshot Diff Computation:

To compute a snapshot diff:

* If both snapshots are compacted, their compacted versions will be used. The diff b/w two compacted snapshot should be present in one sst file.  
* If the target snapshot is uncompacted & the source snapshot is compacted(other way is not possible as we always compact snapshots in order) and if the DAG has all the sst files corresponding to the uncompacted snapshot version of the compacted snapshot which would be captured as part of the snapshot metadata, then an efficient diff can be performed with the information present in the DAG. Use `uncompactedSstFiles` from each of the snapshot’s meta  
* Otherwise, a full diff will be computed between the compacted source and the compacted target snapshot. Delta sst files would be computed corresponding to the latest version number of the target snapshot(version number of target snapshot would always be greater)  
* Changes in the full diff logic is required to check inode ids of sst files and remove the common sst files b/w source and target snapshots. 


5. ### Snapshot Compaction Workflow

   A background Snapshot compaction service should be added which would be done by iterating through the snapshot chain in the same order as the global snapshot chain. This is to ensure the snapshot created after is always compacted after all the snapshots previously created are compacted. Snapshot compaction should only occur once the snapshot has undergone SST filtering. The following steps outline the process:  
1. **Create a RocksDB checkpoint** of the path previous snapshot corresponding to the bucket in the chain (if it exists). `version` of previous snapshot should be strictly greater than the current snapshot’s `version` otherwise skip compacting this snapshot in this iteration.   
2. **Acquire the `SNAPSHOT_GC_LOCK`** for the snapshot ID to prevent garbage collection during compaction\[This is to keep contents of deleted Table contents same while compaction consistent\].  
   1. If there is no path previous snapshot then  
      1.  Take a checkpoint of the same rocksdb instance remove keys that don’t correspond to the bucket from tables `keyTable`, `fileTable`, `directoryTable,deletedTable,deletedDirectoryTable` by running rocksdb delete range api. This should be done if the snapshot has never been compacted before i.e. if `lastCompactionTime` is zero or null. Otherwise just update the `needsCompaction` to False.  
      2. We can trigger a forced manual compaction on the rocksdb instance(i & ii can be behind a flag where in we can just work with the checkpoint of the rocksdb if the flag is disabled).  
   2. If path previous snapshot exists:  
      1. **Compute the diff** between tables (`keyTable`, `fileTable`, `directoryTable`) of the checkpoint and the current snapshot using snapshot diff functionality.  
      2. **Flush changed objects** into separate SST files using the SST file writer, categorizing them by table type.  
      3. **Ingest these SST files** into the RocksDB checkpoint using the `ingestFile` API.  
3. Check if the entire current snapshot has been flushed to disk otherwise wait for the flush to happen.  
4. Truncate `deletedTable,deletedDirectoryTable,snapshotRenamedTable etc. (All tables excepting keyTable/fileTable/directoryTable)` in checkpointed rocksdb and ingest the entire table from deletedTable and deletedDirectoryTable from the current snapshot rocksdb.  
5. **Acquire the snapshot cache lock** to prevent snapshot access during directory updates.\[While performing the snapshot rocksdb directory switch there should be no rocksdb handle with read happening on it\].  
6. **Move the checkpoint directory** into `db.checkpoint.compacted` with the format:

| om.db-\<snapshot\_id\>.\<version\> |
| :---- |

7. **Update snapshot metadata**, setting `lastCompactionTime` and marking `needsCompaction = false` and set the next snapshot in the chain is marked for compaction. If there is no path previous snapshot in the chain then increase `version`  by 1 otherwise set `version` which is equal to the previous snapshot in the chain. Based on the sstFiles in the rocksdb compute Map\<String, List\<Long\>\> and add this Map to `compactedSstFiles` corresponding to the `version` of the snapshot.  
8. **Delete old uncompacted/compacted snapshots**, ensuring unreferenced uncompacted/compacted snapshots are purged during OM startup(This is to handle jvm crash after viii).  
9. **Release the snapshot cache lock** on the snapshot id. Now the snapshot is ready to be used to read.

   

6. ### Computing Changed Objects Between Snapshots

   The following steps outline how to compute changed objects:  
1. **Determine delta SST files**:  
   * Retrieve from DAG if the snapshot was uncompacted previously and the previous snapshot has an uncompacted copy.  
   * Otherwise, compute delta SST files by comparing SST files in both compacted RocksDBs.  
2. **Initialize SST file writers** for `keyTable`, `directoryTable`, and `fileTable`.  
3. **Iterate SST files in parallel**, reading and merging keys to maintain sorted order.(Similar to the MinHeapIterator instead of iterating through multiple tables we would be iterating through multiple sst files concurrently).  
4. **Compare keys** between snapshots to determine changes and write updated objects if and only if they have changed into the SST file.  
   * If the object is present in the target snapshot then do an sstFileWriter.put().  
     * If the object is present in source snapshot but not present in target snapshot then we just have to write a tombstone entry by calling sstFileWriter.delete().  
5. **Ingest these SST files** into the checkpointed RocksDB.

7. ### Handling Snapshot Purge

   Upon snapshot deletion, the `needsCompaction` flag for the next snapshot in the chain is set to `true`, ensuring compaction propagates incrementally across the snapshot chain.

# Conclusion

This approach effectively reduces storage overhead while maintaining efficient snapshot retrieval and diff computation. The total storage would be in the order of total number of keys in the snapshots \+ AOS by reducing overall redundancy of the objects while also making the snapshot diff computation for even older snapshots more computationally efficient.

 

