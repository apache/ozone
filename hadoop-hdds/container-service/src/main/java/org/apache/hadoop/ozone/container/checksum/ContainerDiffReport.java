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

  public ContainerDiffReport() {
    this.missingBlocks = new ArrayList<>();
    this.missingChunks = new HashMap<>();
    this.corruptChunks = new HashMap<>();
  }

  public void addMissingBlock(ContainerProtos.BlockMerkleTree missingBlockMerkleTree) {
    this.missingBlocks.add(missingBlockMerkleTree);
  }

  public void addMissingChunk(long blockId, ContainerProtos.ChunkMerkleTree missingChunkMerkleTree) {
    this.missingChunks.computeIfAbsent(blockId, any -> new ArrayList<>()).add(missingChunkMerkleTree);
  }

  public void addCorruptChunk(long blockId, ContainerProtos.ChunkMerkleTree corruptChunk) {
    this.corruptChunks.computeIfAbsent(blockId, any -> new ArrayList<>()).add(corruptChunk);
  }

  public List<ContainerProtos.BlockMerkleTree> getMissingBlocks() {
    return missingBlocks;
  }

  public Map<Long, List<ContainerProtos.ChunkMerkleTree>> getMissingChunks() {
    return missingChunks;
  }

  public Map<Long, List<ContainerProtos.ChunkMerkleTree>> getCorruptChunks() {
    return corruptChunks;
  }

  /**
   * If needRepair is true, It means current replica needs blocks/chunks from the peer to repair
   * its container replica. The peer replica still may have corruption, which it will fix when
   * it reconciles with other peers.
   */
  public boolean needsRepair() {
    return !missingBlocks.isEmpty() || !missingChunks.isEmpty() || !corruptChunks.isEmpty();
  }

  // TODO: HDDS-11763 - Add metrics for missing blocks, missing chunks, corrupt chunks.
  @Override
  public String toString() {
    return "ContainerDiffReport:" +
        " MissingBlocks= " + missingBlocks.size() + " blocks" +
        ", MissingChunks= " + missingChunks.values().stream().mapToInt(List::size).sum()
        + " chunks from " + missingChunks.size() + " blocks" +
        ", CorruptChunks= " + corruptChunks.values().stream().mapToInt(List::size).sum()
        + " chunks from " + corruptChunks.size() + " blocks";
  }
}
