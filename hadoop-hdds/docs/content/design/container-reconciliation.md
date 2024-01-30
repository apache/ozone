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

## Background

Ideally, a healthy Ozone cluster would contain only open and closed containers. However, container replicas commonly end up with a mix of states including quasi-closed and unhealthy that the current system is not able to resolve to cleanly closed replicas. The cause of these states is often bugs or broad failure handling on the write path. While we should fix these causes, they raise the problem that Ozone is not able to reconcile these mismatched container states on its own, regardless of their cause. This has lead to significant complexity in the replication manager for how to handle cases where only quasi-closed and unhealthy replicas are availalbe, especially in the case of decommissioning.

Even when all replicas are closed, the system assumes that these closed container replicas are equal with no way to verify this. Checksumming is done for individual chunks within each container, but if two container replicas somehow end up with chunks that differ in length or content despite being marked closed with local checksums matching, the system has no way to detect or resolve this anomaly.

This document proposes a container reconciliation protocol to solve these problems. After implementing the proposal:
1. It should be possible for a cluster to progress to a state where it has only properly replicated closed and open containers.
2. We can verify the equality and integrity of all closed containers.

## Guiding Principles

1. **User Focus**: Users prioritize data durability and availability above all else.
   - From the user perspective, containers labelled quasi-closed and unhealthy represent compromised durability and availability, regardless of the container's actual contents.

2. **Focus on Recovery Paths**: Focusing on the path to a failed state is secondary to focusing on the path out of failed states.
    - For example, we should not focus on whether it is possible for two replicated closed containers with locally matching chunk checksums to have differing content, only on whether the system could detect and recover from this case if it were to happen.

3. **System Safety**:  If a decision made by software will make data more durable a single trigger is sufficient. If a decision can potentially reduce durability of data or execute an unsafe operation (unlink, trim, delete) then the confidence level has to be high, the clarity of the decision precise and clear and preferably the decision is made within services that have a wider view of the cluster (SCM/Recon).

4. **Datanode Simplicity**: Datanodes should only be responsible for safe decisions and eager to make safe choices, avoiding unsafe autonomy.

## Assumptions

- A closed container will not accept new blocks from clients.

- Empty containers are excluded in this proposal. Empty containers create unnecessary noise and overhead in the system but are not relevant to the durability of existing data.

- The longest block is always the correct block to preserve at the Datanode level based on the limited information a single Datanode has about the higher level business logic that is storing data.

    - The longest block may contain uncommitted chunks, but the datanodes have no way to verify this and must be defensive about preserving their data.

## Solution Proposal

The proposed solution involves defining a container level checksum that can be used to quickly tell if two containers replicas match or not based on their data. This container checksum can be defined as a three level Merkle tree:

- Level 1 (leaves): The existing chunk level checksums (written by the client and verified by the existing datanode container scanner).
- Level 2: A block level checksum created by hashing all the chunk checksums within the block.
- Level 3 (root): A container level checksum created by hashing all the block checksums within the container.
    - This top level container hash is what is reported to SCM to detect diverged replicas. SCM does not need block or chunk level hashes.

When SCM sees that replicas of a non-open container have diverged container checksums, it can trigger a reconciliation process on all datanode replicas. SCM does not need to know which container hash is correct (if any of them are correct), only that all containers match. Datanodes will use their merkle tree and those of the other replicas to identify issues with their container. Next, datanodes can read the missing data from existing replicas and use it to repair their container replica.

### Phase I (outlined in this document)

- Add container level checksums that datanodes can compute and store.

- Add a mechanism for datanodes to reconcile their replica of a container with another datanode's replica so that both replicas can be verified to be equal at the end of the process.

- Add a mechanism for SCM to trigger this reconciliation as part of the existing heartbeat command protocol SCM uses to communicate with datanodes.

- Add an `ozone admin container reconcile <container-id>` CLI that can be used to manually resolve diverged container states among non-open container replicas.

### Phase II (out of scope for this document)

- Automate container reconciliation requests as part of SCM's replication manager.

- Simplify SCM replication manager decommission and recovery logic based on mismatch of container checksums, instead of the combination of all possible container states.

### Phase III (out of scope for this document)

- Extend container level checksum verification to erasure coded containers.
    - EC container replicas do not have the same data and are not expected to have matching container checksums.
    - EC containers already use offline recovery as a reconciliation mechanism.

## Solution Implementation

### Storage

The only extra information we will need to store is the container merkle tree on each datanode container replica. The current proposal is store this separately as a proto file on disk so that it can be copied over the network exactly as stored. The structure would look something like this (not finalized, for illustrative purposes only):

```
ContainerChecksum: {
  algorithm: CRC32
  checksum: 12345
  repeated BlockChecksum
}
BlockChecksum: {
  algorithm: CRC32
  checksum: 12345
  length: 5
  deleted: false
  healthy: true
  repeated ChunkChecksum
}
ChunkChecksum: {
  algorithm: CRC32
  checksum: 12345
  length: 5
  offset: 5
  healthy: true
}
```

- The `deleted` flag in the block proto allows it to serve as a tombstone entry so that block deletion does not affect the container level hash.

- The `algorithm` field is the algorithm used to aggregate hashes from the layer below which results in that object's current hash.
    - We can define an order of chunks and blocks based on their IDs so that the hash does not need to be commutative (order independent).

- The `healthy` field in the chunk checksum being true indicates that the computed chunk checksum used to fill in the merkle tree matches the chunk checksum stored in RocksDB.

- The `healthy` field in the block checksum being true indicates that all chunks within the block are marked as `healthy = true`.

- We may be able to re-use the `ChunkInfo` and `ChecksumData` messages in `DatanodeClientProtocol.proto` for chunk information and checksums. Other structures in the merkle tree storage will require new objects to be defined.

## APIs

The following APIs would be added to datanodes to support container reconciliation. The actions performed when calling them is defined in [Events](#events).

- `reconcileContainer(containerID, List<Replica>)`
    - Instructs a datanode to reconcile its copy of the specified container with the provided list of replica's containers.
    - Datanodes will call `getContainerHashes` for each replica to identify repairs needed, and use existing chunk read/write APIs to do repairs necessary.
    - This would likely be a new command as part of the SCM heartbeat protocol, not actually a new API.
- `getContainerHashes(containerID)`
    - A datanode API that returns the merkle tree for a given container. The proto structure would be similar to that outlined in [Storage](#storage)

## Events

This section defines how container reconciliation will function as events occur in the system.

### Reconiliation Events

 These events occur as part of container reconciliation in the happy path case.

#### On Data Write

- No change to existing datanode operations. Existing chunk checksums passed and validated from the client will be persisted to RocksDB.

#### On Container Close

- Container checksum is calculated synchonously from existing checksums. This calculation must finish before the close is acked back to SCM.
    - **Invariant**: All closed containers have a checksum even in the the case of restarts and failures because SCM will retry the close command if it does not receive an ack.
      - Null handling should still be in place for containers created before this feature, and to guard against bugs.

#### On Container Scan

1. Scanner reads chunks and checks checksums in RocksDB.
2. Scanner updates Merkle tree to match the current contents of the disk. It does not update RocksDB.
    - Update Merkle tree checksums in a rolling fashion per chunk and block as scanning progresses.
    -  Do not let the scanner overwrite the RocksDB value, otherwise we cannot locally detect corruption anymore.
        - This is not a deal breaker since SCM would see it on the next full container report, but it may slow down bitrot detection.
3. If scanner detects local corruption (RocksDB hash does not match hash calculated from disk), marks container unhealthy and sends an incrmeental container report to SCM with its calculated container hash.
    - The Merkle Tree is still updated to reflect the current contents of the disk in this case.

#### On Container import

- Schedule on-demand scan after the container import completes.
    - There may be cases where we need to import a slightly corrupt container since it is our best option. Therefore, do not do a synchronous scan of container and fail the import if corruption is detected.
    - Instead, schedule the imported container for on-demand scanning to be sure SCM knows of corruption if it does not already.
    - **Optimization**: Clear last scanned timestamp on container import to make the background scanner get to it faster if datanode restarts before the on-demand scan completes.

#### On `getContainerHashes`

- Return the current Merkle Tree of the specified container.
    - The tree will be calculated from existing chunk checksums if it is not present. However, this should not happen in the majority of cases since it will be calculated on container close.
- If container not closed, return error.
- If container not found, return error.

- The merkle tree returned by this call will always be consistent.
    - The hash of each node in the tree will always accurately represent a hash of its children.
- The merkle tree returned by this call may not always match the current state of the container.
    - The container can be repaired or corrupted any time between the scan that generated the tree and the call to read the container's tree.
    - If the caller decides it needs to read a chunk based on the merkle tree obtained, it will have to re-check the checksum obtained as part of the chunk read to verify the data received matches what it was expecting.

#### On `reconcileContainer`

- Use existing merkle tree, unless its not there, then compute it.
    - The tree will be calculated from existing chunk checksums if it is not present. However, this should not happen in the majority of cases since it will be calculated on container close.
1. For each replica:
    1. Datanode calls `getContainerHashes`
    2. Datanode diffs this with its current merkle tree to find all chunks that are:
        1. Not part of blocks that are marked as deleted in this or the other replica's tree.
        2. Not matching the chunk's checksum value in this datanode's tree.
    3. For each chunk in the other replica's tree that does not match this replica's tree:
        1. If the chunk or block is not present in our container, save it in memory as needing to be imported.
        2. If the chunk or block is not marked `healthy` in our merkle tree (known to be corrupted by the scanner), save it in memory as needing to be reconciled.
        3. If chunk checksums differ but our chunk is marked `healthy`, ignore this chunk.
            - If the chunk is corrupted in the other replica or got corrupted while we were reading it, we should not import it.
            - If the chunk is corrupted on one of the replicas but the `healthy` flag has not been updated yet to reflect this, it will be resolved when the scanner reaches the container again.
            - If the same chunk is truly `healthy` in both replicas but the checksums do not match, there has been an irrecoverable error on the write path where different data was written to each replica but they both passed checksum validation.
        4. Datanode calls existing `readChunk` API on the other datanode for each chunk that was selected for repair during the tree scan.
        5. Datanode builds/patches/adds to block file.
            - Use existing read/write chunk APIs for this. Read/write checksum validation is built in to these already.
    4. Datanode recomputes container merkle tree from new chunks and existing checksums that were not repaired.
        - By recomputing the tree after reconciling with each replica, it can exit the loop if the trees from all other replicas match.

### Concurrent Events

These may happen to the container during reconciliation and must remain consistent during reconciliation. Note that we cannot depend on SCM coordination to shelter datanodes from other commands after SCM has instructed them to reconcile because datanode command queues may get backed up at various points and process commands in a different order, or long after they were initially issued.

#### On Client read of container under reconciliation

- No action required.

This read could be an end client or another datanode asking for data to repair its replica. The result will only be inconsistent if the chunk/block is being repaired. In this case the data is already corrupt and the client will get a read checksum failure either way. Even if all datanodes are reconciling the same container at the same time, this should not cause an availability problem because there has to be one correct replica that others are repairing from that the client can also read.

#### Replicate command for a container being reconciled

- Allow replication to proceed with best effort, possibly with a warning log.

There may be cases where a container is critically under-replicated and we need a good copy of the data as soon as possible. Meanwhile reconciliation could remained blocked for an unrelated reason. We can provide a way to pause/unpause reconciliation between chunk writes to do reasonably consistent container exports.

#### On block delete

- When SCM sends block delete commands to datanodes, update the merkle tree when the datanode block deleting service runs and processes that block.

    - Merkle tree update should be done before RocksDB update to make sure it is persisted in case of a failure during the delete.

    - Callers of `getContainerHashes` will ignore blocks that either they marked for deletion or the peer has marked for deletion.

    - Merkle tree update should not be done when datanodes first receive the block delete command from SCM, because this command only adds the delete block proto to the container. It does not iterate the blocks to be deleted so we should not add that additional step here.

#### On Container delete for container being reconciled

- Provide a way to interrupt reconciliation and allow container delete to proceed.

## Backwards Compatibility

Since the scanner can generate the container merkle trees in the background, existing containers created before this feature will still be eligible for reconciliation. These old containers may not have all of their block deletes present in the merkle tree, however, which could cause some false positives about missing blocks on upgrade if one node had already deleted blocks from a container before the upgrade, and another datanode has not yet processed the delete of those blocks.

