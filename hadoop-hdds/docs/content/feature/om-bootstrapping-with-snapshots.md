---
title: OM Bootstrapping with Snapshots
summary: Design for the OM bootstrapping process that is snapshot aware
date: 2025-08-05
jira: HDDS-12090
status: implemented
author: Swaminathan Balachandran
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


# OM Bootstrapping with Snapshots

# Problem Statement:

The current bootstrapping mechanism for OM has inconsistencies when dealing with Snapshotted OM RocksDBs. Bootstrapping occurs without locking mechanisms, and active transactions may still modify snapshots RocksDB during the process. This can lead to a corrupted RocksDB instance on the follower OM post-bootstrapping. To resolve this, the bootstrapping process must operate on a consistent system state.

Jira Ticket : [https://issues.apache.org/jira/browse/HDDS-12090](https://issues.apache.org/jira/browse/HDDS-12090)

                                                

         

# Background on Snapshots

## Snapshot Operations

When a snapshot is taken on an Ozone bucket, the following steps occur:

1. A RocksDB checkpoint of the active `om.db` is created.  
2. Deleted entries are removed from the `deletedKeyTable` and `deletedDirTable` in the Active Object Store (AOS) RocksDB. This is to just prevent the blocks from getting purged without checking for the key’s presence in the correct snapshot in the snapshot chain.  
3. A new entry is added to the `snapshotInfoTable` in the AOS RocksDB.

## Current Bootstrap Model:

The current model involves the follower OM initiating an HTTP request to the leader OM, which provides a consistent view of its state. Before bucket snapshots were introduced, this process relied solely on an AOS RocksDB checkpoint. However, with snapshots, multiple RocksDB instances (AOS RocksDB \+ snapshot RocksDBs) must be handled, complicating the process.

### Workflow:

* Follower Initiation:  
  Sends an exclude list of files already copied in previous batches.  
* Leader Actions:  
  * Creates an AOS RocksDB checkpoint.  
  * Performs a directory walk through:  
    * AOS RocksDB checkpoint directory.  
    * Snapshot RocksDB directories.  
    * Backup SST file directory (compaction backup directory).  
  * Identifies unique files to be copied in the next batch.  
  * Transfers files in batches, recreating hardlinks on the follower side as needed.

### Issues with the Current Model

1. Active transactions during bootstrapping may modify snapshot RocksDBs, leading to inconsistencies.  
2. Partial data copies can occur when double-buffer flushes or other snapshot-related operations are in progress.  
3. Large snapshot data sizes (often in GBs) require multi-batch transfers, increasing the risk of data corruption.

# Proposed Fixes

## Locking the Snapshot Cache

Snapshot Cache is the class which is responsible for maintaining all rocksdb handles corresponding to a snapshot. The rocksdb handles are closed by the snapshot cache are closed from time to time if there are no references of the rocksdb being used by any of the threads in the system. Hence any operation on a snapshot would go through the snapshot cache increasing the reference count of that snapshot. Implementing a lock for this snapshot cache would prevent any newer threads from requesting a snapshot rocksdb handle from the snapshot cache. Thus any operation under this lock will have a consistent view of the entire snapshot. The only downside to this is that it would block the double buffer thread, hence any operation performed under this thread has to be lightweight so that it doesn’t end up running for a long period of time.(P.S. With Sumit’s implementation of optimized Gatekeeping model and getting rid of double buffer from OM would result in only blocking the snapshot operations which should be fine since these operations are only fired by background threads.)  
   
With the above implementation of a lock there is a way to get a consistent snapshot of the entire OM. Now lets dive into various approaches to overall bootstrap flow.

## Approach 1(Batching files over multiple tarballs):

This approach builds on the current model by introducing size thresholds to manage locks and data transfers more efficiently.

### Workflow:

1. Follower Initiation:  
   * Sends an exclude list of previously copied files (identified by `inodeId`).  
2. Leader Directory Walk:  
   * Walks through AOS RocksDB, snapshot RocksDBs, and backup SST directories to identify files to transfer.  
   * Compares against the exclude list to avoid duplicate transfers.  
3. If the total size of files to be copied is more than **ozone.om.ratis.snapshot.lock.max.total.size.threshold** then the files would be directly sent over the stream as a tarball where the name of the files is the inodeId of the file.  
4. If the total size of files to be copied is less than equal to **ozone.om.ratis.snapshot.lock.max.total.size.threshold** then the snapshot cache lock is taken after waiting for the snapshot cache to completely get empty(No snapshot rocksdb should be open). Under the lock following operations would be performed:  
   * Take the AOS rocksdb checkpoint.  
   * A complete directory walk is done on AOS checkpoint rocksdb directory \+ all the snapshot rocksdb directories \+ backup sst file directory(compaction log directory) to figure out all the files to be copied and any file already present in the exclude list would be excluded.  
   * These files are added to the tarball where again the name of the file would be the inodeId of the file.  
5. As the files are being iterated the path of each file and their corresponding inodeIds would be tracked. When it is the last batch this map would also be written as a text file in the final tarball to recreate all the hardlinks on the follower node.

The only drawback with this approach is that we might end up sending more data over the network because some sst files sent over the network could have been replaced because of compaction running concurrently on the active object store. But at the same time since the entire bootstrap operation is supposed to finish in the order of a few minutes, the amount of extra data would be really minimal assuming we could utmost write 30 MBs of data assuming there are 30000 keys written in 2 mins each key would be around 1 KB.

## Approach 1.1:

This approach builds on the approach1 where along with introducing size thresholds under locks manage locks, we only rely on the number of files changed under the snapshot directory as the threshold.

### Workflow:

1. Follower Initiation:  
   * Sends an exclude list of previously copied files (identified by `inodeId`).  
2. Leader Directory Walk:  
   * Walks through AOS RocksDB, snapshot RocksDBs, and backup SST directories to identify files to transfer.  
   * Compares against the exclude list to avoid duplicate transfers.  
3. If either the total size to be copied or the total number of files under the snapshot rocksdb directory to be copied is more than **ozone.om.ratis.snapshot.max.total.sst.size** respectively then the files would be directly sent over the stream as a tarball where the name of the files is the inodeId of the file.  
4. If the total number of file size  to be copied under the snapshot rocksdb directory is less than equal to **ozone.om.ratis.snapshot.max.total.sst.size** then the snapshot cache lock is taken after waiting for the snapshot cache to completely get empty(No snapshot rocksdb should be open). Under the lock following operations would be performed:  
   * Take the AOS rocksdb checkpoint.  
   * A complete directory walk is done on all the snapshot rocksdb directories to figure out all the files to be copied and any file already present in the exclude list would be excluded.  
   * Hard links of these files are added to tmp directory on the leader.  
   * Exit lock  
   * After the lock all files under the tmp directory, AOS Rocksdb checkpoint directory and compaction backup directory have to be written to the tarball. As the files are being iterated the path of each file and their corresponding inodeIds would be tracked. Since this is the last batch this map would also be written as a text file in the final tarball to recreate all the hardlinks on the follower node.

The drawbacks for this approach is the same as approach 1, but here we are optimizing on the amount of time lock is held by performing very lightweight operations under the lock. So this is a more optimal approach since it minimises the lock wait time on other threads. 

## Approach 2(Single tarball creation under lock):

The approach 2 here proposes to create a single tarball file on the disk and stream the chunks of the tarball over multiple http batch request from the follower.  
Following is the flow for creating the tarball:

1. Snapshot cache lock is taken after waiting for the snapshot cache to become completely empty(No snapshot rocksdb should be open). Under the lock following operations would be performed:  
   * Take the AOS rocksdb checkpoint.  
   * A complete directory walk is done on AOS rocksdb directory \+ all the snapshot rocksdb directories \+ backup sst file directory(compaction log directory) to figure out all the files to be copied to create a single tarball.  
2. This tarball should be streamed batch by batch to the follower in a paginated fashion.

The drawback with this approach is that the double buffer would be blocked for a really long time if there is a lot of data to be tarballed. If the total snapshot size of the OM dbs put together is 1 TB, considering the tarball writes go to an NVMe and considering the write throughput for an NVMe drive is around 5 GB/sec then the tarball write might take a total of 1024/5 secs \= 3 mins. Blocking the double buffer thread for 3 mins seems to be a bad idea, but at the same time this would only happen if there is snapshot operation in flight or in the double buffer queue already.

## Approach 3(Creating a checkpoint of the snapshot rocksdb under lock):

The approach 3 here proposes to create a rocksdb checkpoint for each and every snapshot rocksdb in the system along with the AOS rocksdb under the snapshot cache lock. Outside of the lock we could either create a single tarball file as done in approach 2 or stream the files in batches as multiple tarball file similar to approach 1 to the follower.  
Following is the flow for creating the tarball:

1. Snapshot cache lock is taken after waiting for the snapshot cache to become completely empty(No snapshot rocksdb should be open). Under the lock following operations would be performed:  
   * Take the AOS rocksdb checkpoint.  
   * Take rocksdb checkpoint of each and every snapshot in the system by iterating through the snapshotInfo table of AOS checkpoint rocksdb.  
2. Now the files in the checkpoint directories have to be streamed to the follower as done in either approach 1 or approach 2\.

The drawback with this approach is that this would double the number of hardlinks in the file system which could have potential impact on performance during bootstrap, considering the case in systems where the total number of files and hardlinks in the system order up to 5 million files.

## Recommendations

Approach 1 is the most optimized solution as it balances the amount of time under the lock by minimising the amount of I/O ops inside the lock by introducing another threshold config to track this. Moreover taking this approach will also need the most minimal amount of code change as it doesn’t differ from the current approach by much. While approach 2 might look simpler but this would imply revamping the entire bootstrap logic currently in place and moreover this approach might increase the total amount of time inside the lock which would imply blocking the double buffer thread of extended amounts of time if it comes to this situation, which approach 1 tries to avoid.  
 

Final approach implemented is the Approach 1.1

