---
title: Container Reconciliation
summary: Allow Datanodes to reconcile mismatched container contents regardless of their state.
date: 2024-01-29
jira: HDDS-10239
status: draft
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


# Container Reconciliation

This document outlines the proposed recovery protocol for containers where one or more replicas are not cleanly closed or have potential data inconsistencies. It aims to provide an overview of the planned changes and their implications, focusing on the overall flow and key design decisions.

## Nomenclature
1. Container: A container is a logical unit of storage management in Ozone. It is a collection of blocks that are used to store data.
2. Container Replica/Instance: A container replica is a copy of a container that is stored on a Datanode or a shard of an Erasure Coded Container.
3. Block: A block is a collection of chunks that are used to store data. An Ozone object consists of one or more blocks.
4. Chunk: A chunk is a collection of bytes that are used to store data. A chunk is the smallest unit of read and write in Ozone.

## Background

This proposal is motivated by the need to reconcile mismatched container replica states and contents among container replicas.
This covers
1. Containers replicas that are not cleanly closed.
2. Containers replicas that have potential data inconsistencies due to bugs or broad failure handling on the write path.
3. Silent data corruption that may occur in the system.
4. The need to verify the equality and integrity of all closed containers replicas.
5. Deleted blocks within a container that still exists in some container replicas.
6. The need to simplify the replication manager for how to handle cases where only quasi-closed and unhealthy container replicas are available.

Ideally, a healthy Ozone cluster would contain only open and closed container replicas. However, container replicas commonly end up with a mix of states including quasi-closed and unhealthy that the current system is not able to resolve to cleanly closed replicas. The cause of these states is often bugs or broad failure handling on the write path. While we should fix these causes, they raise the problem that Ozone is not able to reconcile these mismatched container replica states on its own, regardless of their cause. This has lead to significant complexity in the replication manager for how to handle cases where only quasi-closed and unhealthy replicas are available, especially in the case of decommissioning.

Even when all container replicas are closed, the system assumes that these closed container replicas are equal with no way to verify this. During writes a client provides a checksum for the chunk that is written. 
The scanner validates periodically that the checksums of the chunks on disk match the checksums provided by the client. It is possible that the checksum of a chunk on disk does not match the client provided checksum recorded at the time of write. Additionally, during container replica copying, the consistency of the data is not validated, opening the possibility of silent data corruption propagating through the system.

This document proposes a container reconciliation protocol to solve these problems. After implementing the proposal:
1. It should be possible for a cluster to progress to a state where all not open containers are closed and meeting the desired replication factor.
2. We can verify the equality and integrity of all closed containers.

Note: This document does not cover the case where the checksums recorded at the time of write match the chunks locally within a Datanode but differ across replicas. We assume that the replication code path is correct and that the checksums are correct. If this is not the case, the system is already in a failed state and the reconciliation protocol will not be able to recover it. Chunks once written are not updated, thus this scenario is not expected to occur.
## Guiding Principles

1. **User Focus**: Users prioritize data durability and availability above all else.
   - From the user perspective, containers labelled quasi-closed and unhealthy represent compromised durability and availability, regardless of the container's actual contents.

2. **Focus on Recovery Paths**: Focusing on the path to a failed state is secondary to focusing on the path out of failed states.
    - For example, we should not focus on whether it is possible for two replicated closed containers to have differing content, only on whether the system could detect and recover from this case if it were to happen.

3. **System Safety**: If a decision made by software will make data more durable a single trigger is sufficient. If a decision can potentially reduce durability of data or execute an unsafe operation (unlink, trim, delete) then the confidence level has to be high, the clarity of the decision precise and clear and preferably the decision is made within services that have a wider view of the cluster (SCM/Recon).

4. **Datanode Simplicity**: Datanodes should only be responsible for safe decisions and eager to make safe choices, avoiding unsafe autonomy.

## Assumptions

1. A closed container will not accept new blocks from clients.
2. Empty containers are excluded in this proposal. Empty containers create unnecessary noise and overhead in the system but are not relevant to the durability of existing data.
3. If checksums of chunks match locally they should match across replicas.
4. The longest block is always the correct block to preserve at the Datanode level based on the limited information a single Datanode has. Whether the data within a block is ever accessed by the client depends on the consistency between Datanode and Ozone Manager which is not transactional and can vary. The safe decision is to preserve the longest block and let an external entity that process cluster wide usage of blocks decide if the block can be trimmed or deleted. The longest block may contain uncommitted chunks, but the datanodes have no way to verify this and must be defensive about preserving their data.

## Solution Proposal

The proposed solution involves defining a container level checksum that can be used to quickly tell if two containers replicas match or not based on their data. This container checksum can be defined as a three level Merkle tree:

1. Level 1 (leaves): The existing chunk level checksums (written by the client and verified by the existing datanode container scanner).
2. Level 2: A block level checksum created by hashing all the chunk checksums within the block.
3. Level 3 (root): A container level checksum created by hashing all the block checksums within the container.
   1. This top level container hash is what is reported to SCM to detect diverged replicas. SCM does not need block or chunk level hashes.

When SCM sees that replicas of a non-open container have diverged container checksums, it can trigger a reconciliation process on all datanode replicas. SCM does not need to know which container hash is correct (if any of them are correct), only that all containers match. Datanodes will use their merkle tree and those of the other replicas to identify issues with their container. Next, datanodes can read the missing data from existing replicas and use it to repair their container replica.

Since the container hash is generated leveraging the checksum recorded at the time of writing, the container hash represents consistency of the data from a client perspective.

### Phase I (outlined in this document)

1. Add container level checksums that datanodes can compute and store.

2. Add a mechanism for datanodes to reconcile their replica of a container with another datanode's replica so that both replicas can be verified to be equal at the end of the process.

3. Add a mechanism for SCM to trigger this reconciliation as part of the existing heartbeat command protocol SCM uses to communicate with datanodes.

4. An `ozone admin container reconcile <container-id>` CLI that can be used to manually resolve diverged container states among non-open container replicas.
    - When SCM gets this command, it would trigger one reconcile command for each replica. The CLI would be asynchronous so progress could be checked using container level checksum info added to `ozone admin container info` output.

5. Delete blocks that a Container Replica has not yet deleted.

### Phase II (out of scope for this document)

- Automate container reconciliation requests as part of SCM's replication manager.

- Simplify SCM replication manager decommission and recovery logic based on mismatch of container checksums, instead of the combination of all possible container states.

### Phase III (out of scope for this document)

- Extend container level checksum verification to erasure coded containers.
    - EC container replicas do not have the same data and are not expected to have matching container checksums.
    - EC containers already use offline recovery as a reconciliation mechanism.


## Solution Implementation

### Container Hash Tree / Merkle Tree

The only extra information we will need to store is the container merkle tree on each datanode container replica. The current proposal is store this separately as a proto file on disk so that it can be copied over the network exactly as stored. The structure would look something like this (not finalized, for illustrative purposes only):

```diff
diff --git a/hadoop-hdds/interface-client/src/main/proto/DatanodeClientProtocol.proto b/hadoop-hdds/interface-client/src/main/proto/DatanodeClientProtocol.proto
index 718e2a108c7..d8d508af356 100644
--- a/hadoop-hdds/interface-client/src/main/proto/DatanodeClientProtocol.proto
+++ b/hadoop-hdds/interface-client/src/main/proto/DatanodeClientProtocol.proto
@@ -382,6 +382,7 @@ message ChunkInfo {
   repeated KeyValue metadata = 4;
   required ChecksumData checksumData =5;
   optional bytes stripeChecksum = 6;
+  optional bool healthy = 7; // If all the chunks on disk match the expected checksums provided by the client during write
 }
 
 message ChunkInfoList {
@@ -525,3 +526,38 @@ service IntraDatanodeProtocolService {
   rpc download (CopyContainerRequestProto) returns (stream CopyContainerResponseProto);
   rpc upload (stream SendContainerRequest) returns (SendContainerResponse);
 }
+
+/*
+BlockMerkle tree stores the checksums of the chunks in a block.
+The Block checksum is derived from the checksums of the chunks in case of replicated blocks and derived from the
+metadata of the chunks in case of erasure coding.
+Two Blocks across container instances on two nodes have the same checksum if they have the same set of chunks.
+A Block upon deletion will be marked as deleted but will preserve the rest of the metadata.
+*/
+message BlockMerkleTree {
+  optional BlockData blockData = 1; // The chunks in this should be sorted by the order of chunks written.
+  optional ChecksumData checksumData = 2; // Checksum of the checksums of the chunks.
+  optional bool deleted = 3; // If the block is deleted.
+  optional int64 length = 4; // Length of the block.
+  optional int64 chunkCount = 5; // Number of chunks in the block.
+}
+
+/*
+ContainerMerkleTree stores the checksums of the blocks in a container.
+The Container checksum is derived from the checksums of the blocks.
+Two containers across container instances on two nodes have the same checksum if they have the same set of blocks.
+If a block is deleted within the container, the checksum of the container will remain unchanged.
+ */
+message ContainerMerkleTree {
+  enum FailureCause {
+    NO_HEALTHY_CHUNK_FOUND_WITH_PEERS = 1; // No healthy chunk found with peers.
+    NO_PEER_FOUND = 2; // No peer found.
+  }
+  optional int64 containerID = 1; // The container ID.
+  repeated BlockMerkleTree blockMerkleTrees = 2; // The blocks in this should be sorted by the order of blocks written.
+  optional ChecksumData checksumData = 3; // Checksum of the checksums of the blocks.
+  optional int64 length = 5; // Length of the container.
+  optional int64 blockCount = 6; // Number of blocks in the container.
+  optional FailureCause failureCause = 7; // The cause of the failure.
+  optional int64 reconciliationCount = 8; // The reconciliation count.
+}


```
This is written as a file to avoid bloating the RocksDB instance which is in the IO path.

## APIs

The following APIs would be added to datanodes to support container reconciliation. The actions performed when calling them is defined in [Events](#events).

- `reconcileContainer(containerID, List<Replica>)`
    - Instructs a datanode to reconcile its copy of the specified container with the provided list of other container replicas.
    - Datanodes will call `getContainerHashes` for each container replica to identify repairs needed, and use existing chunk read/write APIs to do repairs necessary.
    - This would likely be a new command as part of the SCM heartbeat protocol, not actually a new API.
- `getContainerHashes(containerID)`
    - A datanode API that returns the merkle tree for a given container. The proto structure would be similar to that outlined in [Merkle Tree](#Container-Hash-Tree-/-Merkle-Tree).

## Reconciliation Process

SCM: Storage Container Manager

DN: Datanode

### SCM sets up the reconciliation process as follows:

1. SCM triggers a reconciliation on `DN 1` for container 12 with replicas on `DN 2` and `DN 3`.
   1. `SCM -> reconcileContainer(Container #12, DN2, DN3) -> DN 1` 

### Datanodes set up the reconciliation process as follows:

1. `DN 1` schedules the Merkle Tree to be calculated if it is not already present and report it to SCM. 
   1. SCM can then compare the container replica hashes and schedule reconciliation if they are different.
2. If `DN 1` already has the Merkle Tree locally, it will compare it with the Merkle Trees of the other container replicas and schedule reconciliation if they are different. Example: 
   1. `DN 1 -> getContainerHashes(Container #12) -> DN 2` // Datanode 1 gets the merkle tree of container 12 from Datanode 2.
   2. `DN 1 -> getContainerHashes(Container #12) -> DN 3` // Datanode 1 gets the merkle tree of container 12 from Datanode 3.
   3. ... // Continue for all replicas.

### Reconcile loop once the merkle trees are obtained from all/most replicas:

1. `DN 1` checks if any blocks are missing. For each missing Block:
    1. Make a **union** of all chunks and which DNs has the chunk. It is possible that certain chunks are missing in other container replicas. The **union** of chunks represents all the chunks that are present in any of the replicas.
    2. Read the remote chunks and store them locally and create the local block. Example: 
       1. `DN 1 -> readChunk(Container #12, Block #3, Chunk #1)  -> DN 2` // Read the chunk from DN 2
       2. `DN 1 -> readChunk(Container #12, Block #3, Chunk #2)  -> DN 3` // Read the chunk from DN 3
       3. ... // Continue for all chunks that are in the union of chunks.
       4. Datanode will validate the checksum of the chunk read from the peer Datanode before using it to reconcile. This is already performed by the existing client code. Thus, no silent data corruption will be introduced.
2. `DN 1` checks if any blocks are corrupted. For each corrupted Block: 
    1. Make a union of all chunks and which DN has the chunks. It is possible that certain chunks are corrupted in one of the container replicas. The **union** of chunks represents all the chunks that are present in any of the replicas.
    2. Read the remote chunks and store them locally. Example: 
       1. `DN 1 -> readChunk(Container #12, Block #20, Chunk #13)  -> DN 2`
       2. `DN 1 -> readChunk(Container #12, Block #20, Chunk #21)  -> DN 3`
       3. ... // Continue for all chunks that are in the union of chunks.
       4. Datanode will validate the checksum of the chunk read from the peer Datanode before using it to reconcile. This is already performed by the existing client code. Thus, no silent data corruption will be introduced.
3. `DN 1` deletes any blocks that are marked as deleted.
    1. The block continues to be in the tree with the updated checksum to avoid redundant Merkle tree updates.
4. `DN 1` recomputes Merkle tree and sends it to SCM via ICR (Incremental Container Report).

Note: This document does not cover the case where the checksums recorded at the time of write match the chunks locally within a Datanode but differ across replicas. We assume that the replication code path is correct and that the checksums are correct. If this is not the case, the system is already in a failed state and the reconciliation protocol will not be able to recover it. Chunks once written are not updated, thus this scenario is not expected to occur.

#### `getContainerHashes`

When a datanode receives a request to get container hashes. The following steps are performed:

1. If the Merkle Tree is present, return it.
2. If the Merkle Tree is not present, the call `getContainerHashes` will return and error. 
   1. Datanode missing the Merkle Tree will be schedule the scanner to calculate the Merkle Tree and then report to SCM.
3. If container not closed, return error.
4. If container not found, return error.
5. The merkle tree returned by this call will represent the status of the data on disk (post scanner scrub).

## Sample scenarios

1. **Container is missing blocks**
   1. `DN 1` has 10 blocks, `DN 2` has 11 blocks, `DN 3` has 12 blocks.
   2. `DN 1` will read the missing block from `DN 2` and `DN 3` and store it locally.
   3. `DN 1` will recompute the merkle tree and send it to SCM.

2. **Container has corrupted chunks**
   1. `DN 1` has block 20: chunk 13, `DN 2` has block 12: chunk 13
   2. `DN 1` will read the corrupted block from `DN 2` and store it locally.
   3. `DN 1` will recompute the merkle tree and send it to SCM.

3. Closed container has a chunk that is corrupted
   1. Scanner will detect the corruption and mark the container as unhealthy and reported an updated Merkle tree to SCM.
      1. The Merkle tree will be updated to reflect the current contents of the disk and the hash will be updated to reflect the current contents of the disk.
   2. SCM will trigger a reconciliation process on the Datanode that sees the corruption.
   3. The Datanode will read the corrupted chunk from another Datanode and store it locally as part of reconciliation covered in #2 above.
   4. The Datanode will recompute the Merkle tree and send it to SCM.
   5. In the meantime if a client or a peer Datanode reads the corrupted chunk the checksum match computed by the client code will fail and the client will get a checksum mismatch error.

## Events
A Datanode is still accepting writes and reads while the reconciliation process is ongoing. The reconciliation process is a background process that does not affect the normal operation of the Datanode.
This section defines how container reconciliation will function as events occur in the system.

### Reconciliation Events

 These events occur as part of container reconciliation in the happy path case.

#### On Data Write

- No change to existing datanode operations. Existing chunk checksums passed and validated from the client will be persisted to RocksDB.
- Reconciliation is only performed on containers that are not open. 
- Merkle tree is updated on container close.

#### On Container Close

- Container checksum is calculated using the checksums that were recorded in RocksDB at the time of write. This calculation must finish before the close is acked back to SCM.
    - **Invariant**: All closed containers have a checksum even in the case of restarts and failures because SCM will retry the close command if it does not receive an ack.
      - Null handling should still be in place for containers created before this feature, and to guard against bugs.

#### On Container Scan

1. Scanner reads chunks and compares it to the checksums in RocksDB.
2. Scanner updates Merkle tree to match the current contents of the disk. It does not update RocksDB.
   - Update Merkle tree checksums in a rolling fashion per chunk and block as scanning progresses.
   - **Note**: The checksums recorded in RocksDB are immutable and cannot be updated at any point in time. Only new chunks and their checksums can be added as part of reconciliation.
3. If scanner detects local corruption (RocksDB hash does not match hash calculated from disk), marks container unhealthy and sends an incremental container report to SCM with its calculated container hash.
    - The Merkle Tree is still updated to reflect the current contents of the disk in this case.

#### On Container import

In multiple scenarios it is safer to immediately create a container replica and not wait for reconciliation. 
Example: The cluster has only one single closed container replica for the container.

- Schedule on-demand scan after the container import completes.
    - There may be cases where we need to import a container without verifying the contents.
    - Instead, schedule the imported container for on-demand scanning to be sure SCM knows of corruption if it does not already.
    - **Optimization**: Clear last scanned timestamp on container import to make the background scanner get to it faster if datanode restarts before the on-demand scan completes.

### Concurrent Events

These may happen to the container during reconciliation and must remain consistent during reconciliation. Note that we cannot depend on SCM coordination to shelter datanodes from other commands after SCM has instructed them to reconcile because datanode command queues may get backed up at various points and process commands in a different order, or long after they were initially issued.

#### On Client read of container under reconciliation

- No action required.

This read could be an end client or another datanode asking for data to repair its replica. The result will only be inconsistent if the chunk/block is being repaired. In this case the data is already corrupt and the client will get a read checksum failure either way. Even if all datanodes are reconciling the same container at the same time, this should not cause an availability problem because there has to be one correct replica that others are repairing from that the client can also read.

#### Replicate command for a container being reconciled

- Allow replication to proceed with best effort, possibly with a warning log.

There may be cases where a container is critically under-replicated and we need a good copy of the data as soon as possible. Meanwhile, reconciliation could remain blocked for an unrelated reason. We can provide a way to pause/unpause reconciliation between chunk writes to do reasonably consistent container exports.

#### On block delete

- When SCM sends block delete commands to datanodes, update the merkle tree when the datanode block deleting service runs and processes that block.

    - Merkle tree update should be done before RocksDB update to make sure it is persisted in case of a failure during delete.

    - Merkle tree update should not be done when datanodes first receives the block delete command from SCM, because this command only adds the delete block proto to the container. It does not iterate the blocks to be deleted so we should not add that additional step here.

#### On Container delete for container being reconciled

- Provide a way to interrupt reconciliation and allow container delete to proceed. 

## Backwards Compatibility

Since the scanner can generate the container merkle trees in the background, existing containers created before this feature will still be eligible for reconciliation. These old containers may not have all of their block deletes present in the merkle tree, however, which could cause some false positives about missing blocks on upgrade if one node had already deleted blocks from a container before the upgrade, and another datanode has not yet processed the delete of those blocks.

This can be mitigated by:
1. Having datanodes delete blocks from their container replica on reconciliation that another replica has marked as deleted.
2. Having SCM periodically ask datanodes to reconcile otherwise matching containers when there are no other mismatched containers in the cluster.

The sequence of events would look like this. Assume software v1 does not have container reconciliation, but v2 does.
1. Datanode 1 in v1 deletes block 1 in container 1. This does not leave a tombstone entry because v1 does not have container reconciliation.
2. Cluster is upgraded to v2.
3. Reconciliation is triggered between datanodes 1 and 2 for container 1. Datanode 2 has not yet deleted block 1.
4. Datanode 1 will add block 1 to its container when reconciling with datanode 2.
    - If we stop here, then block 1 will remain orphaned on datanode 1.
5. Datanode 2 deletes block 1 and adds a tombstone entry for it since the cluster is running software v2 which has container reconciliation.
6. Since container checksums disregard deleted blocks, container 1 will be seen as matching from SCM's point of view. However, periodic reconciliation requests for closed containers will still eventually ask these two replicas to reconcile.
7. Datanode 1 learns that block 1 was deleted from datanode 2, so it moves the block metadata to the deleted table in RocksDB
    - A delete transaction entry with an ID key would need to be created to do this. Currently these are only received from SCM and not created by the datanode.
8. Datanode block deleting service deletes this block as usual.
