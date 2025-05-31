# Improving Snapshot Scale:

[HDDS-13003](https://issues.apache.org/jira/browse/HDDS-13003)

# Problem Statement

In Apache Ozone, snapshots currently take a checkpoint of the Active Object Store (AOS) RocksDB each time a snapshot is created and track the compaction of SST files over time. This model works efficiently when snapshots are short-lived, as they merely serve as hard links to the AOS RocksDB. However, over time, if an older snapshot persists while significant churn occurs in the AOS RocksDB (due to compactions and writes), the snapshot RocksDB may diverge significantly from both the AOS RocksDB and other snapshot RocksDB instances. This divergence increases storage requirements linearly with the number of snapshots.

# Solution Proposal:

The primary inefficiency in the current snapshotting mechanism stems from constant RocksDB compactions in AOS, which can cause a key, file, or directory entry to appear in multiple SST files. Ideally, each unique key, file, or directory entry should reside in only one SST file, eliminating redundant storage and mitigating the multiplier effect caused by snapshots. If implemented correctly, the total RocksDB size would be proportional to the total number of unique keys in the system rather than the number of snapshots.

## Snapshot Compaction:

Currently, automatic RocksDB compactions are disabled for snapshot RocksDB to preserve snapshot diff performance, preventing any form of compaction. However, snapshots can be compacted if the next snapshot in the chain is a checkpoint of the previous snapshot plus a diff stored in a separate SST file. The proposed approach involves rewriting snapshots iteratively from the beginning of the snapshot chain and restructuring them in a separate directory. P.S This has got nothing to do with compacting snapshot’s rocksdb, we are not going to enable rocksdb auto compaction on snapshot rocksdb.

1. ### Introducing a last compaction time:

   A new boolean flag (`needsCompaction`) and timestamp (`lastCompactionTime`) will be added to snapshot metadata. If absent, `needsCompaction` will default to `true`.   
   A new list of Map\<String, List\<Longs\>\> (`sstFiles`) also needs to be added to snapshot info; this would be storing the original list of sst files in the uncompacted copy of the snapshot corresponding to keyTable/fileTable/DirectoryTable.  
   Since this is not going to be consistent across all OMs this would have to be written to a local yaml file inside the snapshot directory and this can be maintained in the SnapshotChainManager in memory on startup. So all updates should not go via ratis.

2. ### Snapshot Cache Lock for Read Prevention

   A snapshot lock will be introduced in the snapshot cache to prevent reads on a specific snapshot during compaction. This ensures no active reads occur while replacing the underlying RocksDB instance.

3. ### Directory Structure Changes

   Snapshots currently reside in the `db.checkpoints` directory. The proposal introduces a `db.checkpoints.compacted` directory for compacted snapshots. The directory format should be as follows:

| om.db-\<snapshot\_id\>.\<compaction\_timestamp\> |
| :---- |

4. ### Optimized Snapshot Diff Computation:

To compute a snapshot diff:

* If both snapshots are compacted, their compacted versions will be used. The diff b/w two compacted snapshot should be present in one sst file.
* If the target snapshot is uncompacted & the source snapshot is compacted(other way is not possible as we always compact snapshots in order) and if the DAG has all the sst files corresponding to the uncompacted snapshot version of the compacted snapshot which would be captured as part of the snapshot metadata once a snapshot is compacted, then an efficient diff can be performed with the information present in the DAG.
* Otherwise, a full diff will be computed between the compacted source and the uncompacted target snapshot.
* Changes in the full diff logic is required to check inode ids of sst files and remove the common sst files b/w source and target snapshots.


5. ### Snapshot Compaction Workflow

   Snapshot compaction should only occur once the snapshot has undergone SST filtering. The following steps outline the process:
1. **Create a RocksDB checkpoint** of the path previous snapshot corresponding to the bucket in the chain (if it exists).
2. **Acquire the `SNAPSHOT_GC_LOCK`** for the snapshot ID to prevent garbage collection during compaction.
  1. If there is no path previous snapshot then
    1.  take a checkpoint of the same rocksdb instance remove keys that don’t correspond to the bucket from tables `keyTable`, `fileTable`, `directoryTable,deletedTable,deletedDirectoryTable` by running rocksdb delete range api. We can trigger a forced manual compaction on the rocksdb instance(This can be behind a flag wherein the process can just work with the checkpoint of the rocksdb if the flag is disabled and not perform manual compaction). This should be done if the snapshot has never been compacted before i.e. if `lastCompactionTime` is zero or null. Otherwise just update the `needsCompaction` to False.
  2. If path previous snapshot exists:
    1. **Compute the diff** between tables (`keyTable`, `fileTable`, `directoryTable`) of the checkpoint and the current snapshot using snapshot diff functionality.
    2. **Flush changed objects** into separate SST files using the SST file writer, categorizing them by table type.
    3. **Ingest these SST files** into the RocksDB checkpoint using the `ingestFile` API.
3. Truncate `deletedTable,deletedDirectoryTable,snapshotRenamedTable etc. (All tables excepting keyTable/fileTable/directoryTable)` in checkpointed rocksdb and ingest the entire table from deletedTable and deletedDirectoryTable from the current snapshot rocksdb.
4. **Acquire the snapshot cache lock** to prevent snapshot access during directory updates.
5. **Move the checkpoint directory** into `db.checkpoint.compacted` with the format:

| om.db-\<snapshot\_id\>.\<snapshot\_compaction\_time\> |
| :---- |

6. **Update snapshot metadata**, setting `lastCompactionTime` and marking `needsCompaction = false` and set the next snapshot in the chain is marked for compaction. The `sstFiles` is set by creating Map\<String, List\<Long\>\> from the uncompacted version of the snapshot and this is only set once i.e. `lastCompactionTime` should be zero.
7. **Delete old uncompacted/compacted snapshots**, ensuring unreferenced uncompacted/compacted snapshots are purged during OM startup(This is to handle jvm crash after viii).
8. **Release the snapshot cache lock** on the snapshot id. Now the snapshot is ready to be used to read.



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
