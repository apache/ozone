/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.checksum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;

/**
 * This class represents the difference between our replica of a container and a peer's replica of a container.
 * It summarizes the operations we need to do to reconcile our replica with the peer replica it was compared to.
 */
public class ContainerDiffReport {
  private final List<ContainerProtos.BlockMerkleTree> missingBlocks;
  private final Map<Long, List<ContainerProtos.ChunkMerkleTree>> missingChunks;
  private final Map<Long, List<ContainerProtos.ChunkMerkleTree>> corruptChunks;
  private final List<DeletedBlock> divergedDeletedBlocks;
  private final long containerID;

  public ContainerDiffReport(long containerID) {
    this.missingBlocks = new ArrayList<>();
    this.missingChunks = new HashMap<>();
    this.corruptChunks = new HashMap<>();
    this.divergedDeletedBlocks = new ArrayList<>();
    this.containerID = containerID;
  }

  public long getContainerID() {
    return containerID;
  }

  /**
   * @param missingBlockMerkleTree The block merkle tree of the block to report as missing.
   */
  public void addMissingBlock(ContainerProtos.BlockMerkleTree missingBlockMerkleTree) {
    this.missingBlocks.add(missingBlockMerkleTree);
  }

  /**
   * @param blockId The ID of the block with missing chunks identified.
   * @param missingChunkMerkleTree The chunk within this block to report as missing.
   */
  public void addMissingChunk(long blockId, ContainerProtos.ChunkMerkleTree missingChunkMerkleTree) {
    this.missingChunks.computeIfAbsent(blockId, any -> new ArrayList<>()).add(missingChunkMerkleTree);
  }

  /**
   * @param blockId The ID of the block with missing chunks identified.
   * @param corruptChunk The chunk within this block to report as corrupt.
   */
  public void addCorruptChunk(long blockId, ContainerProtos.ChunkMerkleTree corruptChunk) {
    this.corruptChunks.computeIfAbsent(blockId, any -> new ArrayList<>()).add(corruptChunk);
  }

  public void addDivergedDeletedBlock(ContainerProtos.BlockMerkleTree blockMerkleTree) {
    this.divergedDeletedBlocks.add(new DeletedBlock(blockMerkleTree.getBlockID(), blockMerkleTree.getDataChecksum()));
  }

  /**
   * @return A list of BlockMerkleTree objects that were reported as missing.
   */
  public List<ContainerProtos.BlockMerkleTree> getMissingBlocks() {
    return missingBlocks;
  }

  /**
   * @return A map of block IDs to lists of missing ChunkMerkleTree objects within those blocks.
   */
  public Map<Long, List<ContainerProtos.ChunkMerkleTree>> getMissingChunks() {
    return missingChunks;
  }

  /**
   * @return A map of block IDs to lists of corrupt ChunkMerkleTree objects within those blocks.
   */
  public Map<Long, List<ContainerProtos.ChunkMerkleTree>> getCorruptChunks() {
    return corruptChunks;
  }

  public List<DeletedBlock> getDivergedDeletedBlocks() {
    return divergedDeletedBlocks;
  }

  /**
   * If needRepair is true, It means current replica needs blocks/chunks from the peer to repair
   * its container replica. The peer replica still may have corruption, which it will fix when
   * it reconciles with other peers.
   */
  public boolean needsRepair() {
    return !missingBlocks.isEmpty() || !missingChunks.isEmpty() || !corruptChunks.isEmpty() ||
        !divergedDeletedBlocks.isEmpty();
  }

  public long getNumCorruptChunks() {
    return corruptChunks.values().stream().mapToInt(List::size).sum();
  }

  public long getNumMissingChunks() {
    return missingChunks.values().stream().mapToInt(List::size).sum();
  }

  public long getNumMissingBlocks() {
    return missingBlocks.size();
  }

  public long getNumdivergedDeletedBlocks() {
    return divergedDeletedBlocks.size();
  }

  @Override
  public String toString() {
    return "Diff report for container " + containerID + ":" +
        " Missing Blocks: " + getNumMissingBlocks() +
        " Missing Chunks: " + getNumMissingChunks() + " chunks from " + missingChunks.size() + " blocks" +
        " Corrupt Chunks: " + getNumCorruptChunks() + " chunks from " + corruptChunks.size() + " blocks" +
        " Diverged Deleted Blocks: " + getNumdivergedDeletedBlocks();
  }

  /**
   * Represents a block that has been deleted in a peer whose metadata we need to add to our container replica's
   * merkle tree.
   */
  public static class DeletedBlock {
    private final long blockID;
    private final long dataChecksum;

    public DeletedBlock(long blockID, long dataChecksum) {
      this.blockID = blockID;
      this.dataChecksum = dataChecksum;
    }

    public long getBlockID() {
      return blockID;
    }

    public long getDataChecksum() {
      return dataChecksum;
    }
  }
}
