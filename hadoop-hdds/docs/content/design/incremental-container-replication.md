---
title: Incremental Container Replication
summary: Allow Datanodes to catch up with missing data in containers via incremental replication between the higher sequence ID to lower sequence ID.
date: 2026-03-13
jira: HDDS-14794
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

# Incremental Container Replication

## 1. Introduction

Ozone currently handles container replication by transferring the entire container (RocksDB plus all chunk files) as a single compressed tarball. 
While this approach guarantees that the target Datanode receives an exact copy of the source container, it is highly inefficient for scenarios where the target Datanode already possesses a slightly older version of the container (e.g., due to a temporary network partition, node reboot, or a Ratis follower falling behind).

This document proposes an **Incremental Container Replication** mechanism. If two container replicas have two different sequence IDs, we can setup incremental replication between the higher sequence ID to lower sequence ID instead of doing full container replication. By leveraging the monotonically increasing `BlockCommitSequenceId` of containers, a Datanode with a stale container replica can request only the delta (new blocks, chunks, and tombstones) from a fully up-to-date source Datanode, rather than re-downloading the entire container.

## 2. Background and Motivation

When a container replica falls behind (its `BlockCommitSequenceId` is lower than the sequence ID on other replicas), SCM currently handles this by considering the stale replica as invalid. SCM will typically schedule a full `ReplicateContainerCommand`. 
The target Datanode downloads the full container tarball, replacing its local copy entirely. 

This leads to several issues:
1.  **Network Waste**: A 5GB container might only be missing 5MB of recently appended data. Transferring 5GB is a 1000x overhead.
2.  **Disk I/O and Write Amplification**: Re-writing 5GB of identical data wastes Disk I/O, which is especially detrimental to SSD longevity and cluster performance.
3.  **Recovery Time**: In massive cluster events (e.g., a rack power cycling), the recovery traffic for catching up slightly stale containers can bottleneck the network and delay the time-to-healthy for the cluster.

Since Ozone containers are generally **Append-Only** (chunks are immutable once written), the existing data on a stale replica is overwhelmingly likely to be valid and identical to the source's data up to that lower sequence ID.

## 3. Incremental Replication Proposal

The incremental replication mechanism allows the target Datanode to specify its current `BlockCommitSequenceId` when requesting a container download. The source Datanode will package and send only the blocks committed *after* that sequence ID, along with their associated chunks.

### 3.1. SCM and Command Flow

Currently, `DownloadAndImportReplicator` (on the target Datanode) checks if a container already exists locally. If it does, the replication task is skipped (`Status.SKIPPED`). 

In the new flow:
1.  SCM continues to send `ReplicateContainerCommand` to a target Datanode that has a stale replica.
2.  The target Datanode detects that the container exists locally but has a lower `BlockCommitSequenceId` than the requested replication source.
3.  The target Datanode initiates an *incremental* download request, providing its local `BlockCommitSequenceId` to the source.

### 3.2. Wire Protocol Changes

We introduce a `fromSequenceId` field in the gRPC protocol for container copying.

In `DatanodeClientProtocol.proto`:
```protobuf
message CopyContainerRequestProto {
  required int64 containerID = 1;
  required uint64 readOffset = 2;
  optional uint64 len = 3;
  optional uint32 version = 4;
  optional CopyContainerCompressProto compression = 5;
  // If set, requests an incremental copy starting AFTER this sequence ID
  optional uint64 fromSequenceId = 6; 
}
```

### 3.3. Source Datanode: Delta Export

`OnDemandContainerReplicationSource` will be updated to handle `fromSequenceId`:
1.  Open an iterator over the container's RocksDB block data.
2.  Filter for blocks and deleted blocks (tombstones) where `BlockCommitSequenceId > fromSequenceId`.
3.  Package only the filtered block metadata and their newly referenced `.chunk` files into the tarball.
4.  Include a metadata marker (e.g., in the tarball's `container.yaml` or a new manifest file) indicating this is an incremental tarball and the base sequence ID it was generated against.

### 3.4. Target Datanode: Delta Import

The target Datanode must merge the incremental tarball into its existing replica.
1.  Extract the tarball into a temporary staging location.
2.  Verify that the current local sequence ID still matches the base sequence ID of the incremental update. If not, abort the incremental import.
3.  Move the new `.chunk` files into the local container's `chunks/` directory.
4.  Apply the new block and tombstone metadata into the local container's RocksDB.
5.  Update the local `KeyValueContainerData`'s `BlockCommitSequenceId` to the new highest sequence ID.

## 4. Special Considerations and Edge Cases

### 4.1. Block Deletions

If blocks were deleted on the source Datanode after the `fromSequenceId`, the target Datanode must also apply these deletions. 

To provide some context on how Ozone stores container metadata: Ozone uses RocksDB for each container to store block metadata. When a block is deleted, RocksDB either inserts a "Tombstone" marker (a deletion record) or moves the block entry into a specific "Deleted" column family/table, depending on the exact Ozone version and layout. 

*   Because the block is no longer in the active block table, a simple scan of active blocks greater than `fromSequenceId` will miss these deletions.
*   Therefore, the source Datanode's export process must explicitly scan the deleted block records/tombstones committed after `fromSequenceId` and include these deleted block IDs in the incremental export. 
*   This ensures the target Datanode can locally delete them during the import phase, preventing "space leaks" where the target permanently retains chunks that the rest of the cluster has deleted. 

### 4.2. Fallback to Full Replication

If the source Datanode is running an older version of software that does not support `fromSequenceId`, it builds the full tarball. The target Datanode must be prepared to detect this (e.g., missing incremental marker) and wipe its local container, falling back to the standard full import process.
Conversely, if the incremental import fails for any reason (e.g., RocksDB corruption, missing chunks in the delta), the target should wipe the container and re-request a full replicate.

### 4.3. Interaction with Container Reconciliation (HDDS-10239)

HDDS-10239 proposes a Merkle-Tree-based reconciliation for repairing corrupted containers. 
*   **HDDS-10239 (Merkle Tree)** provides a heavy-weight, fine-grained repair mechanism ideal for data corruption (bit-rot).
*   **Incremental Replication** provides a lightweight, coarse-grained catch-up mechanism ideal for lagging replicas. 

The two mechanisms are strictly complementary. Incremental Replication optimizes the happy path of a lagging Ratis follower, while Merkle Reconciliation handles arbitrary divergence.

## 5. Upgrade and Compatibility

The introduction of `fromSequenceId` is a purely additive protobuf change. 
*   **Old Target -> New Source**: The target won't send `fromSequenceId`. The source behaves normally (full export).
*   **New Target -> Old Source**: The target sends `fromSequenceId`. The old source's gRPC server ignores the unknown field and provides a full export. The new target detects it's a full export and proceeds with full import.

Both scenarios safely fall back to the existing behavior during a rolling upgrade.

## 6. Alternative Proposals

### 6.1. Fetching Missing Chunks via ReadChunk API
Instead of creating a new tarball export path (`OnDemandContainerReplicationSource`), the target Datanode could identify its missing sequence IDs and issue standard `ReadChunk` and `GetBlock` RPCs to the source to fetch the missing data. 
*   **Pros**: Doesn't require modifications to the tarball creation/extraction process. Uses existing read paths.
*   **Cons**: Requires the target to know *exactly* which blocks/chunks belong to the delta (implying the source first needs to send a list of new blocks), which involves more chatty RPC round-trips compared to a single streaming gRPC tarball download. The tarball approach is more aligned with the existing `CopyContainerRequestProto` flow.
