/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.checksum;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockMerkleTreeProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerMerkleTreeProto;
import org.apache.hadoop.ozone.common.ChecksumByteBuffer;
import org.apache.hadoop.ozone.common.ChecksumByteBufferFactory;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * This class represents a Merkle tree that provides one checksum for all data within a container.
 *
 * The leaves of the tree are the checksums of every chunk. Each chunk checksum in a block is further checksumed
 * together to generate the block level checksum. Finally, The checksums of all blocks are checksumed together to
 * create a container level checksum. Note that checksums are order dependent. Chunk checksums are sorted by their
 * offset within a block, and blocks are sorted by their ID.
 *
 * This class can be used to construct a consistent and completely filled {@link ContainerMerkleTreeProto} object.
 * It allows building a container merkle tree from scratch by incrementally adding chunks.
 * The final checksums at higher levels of the tree are not calculated until
 * {@link ContainerMerkleTree#toProto} is called.
 */
public class ContainerMerkleTree {

  private final SortedMap<Long, BlockMerkleTree> blocks;

  /**
   * Constructs an empty Container merkle tree object.
   */
  public ContainerMerkleTree() {
    blocks = new TreeMap<>();
  }

  /**
   * Adds chunks to a block in the tree. The block entry will be created if it is the first time adding chunks to it.
   * If the block entry already exists, the chunks will be added to the existing chunks for that block.
   *
   * @param blockID The ID of the block that these chunks belong to.
   * @param offset2Chunks A map of chunk offset to chunk info, sorted by chunk offset. This will be merged with the
   *    existing sorted list of chunks stored for this block.
   */
  public void addChunks(long blockID, SortedMap<Long, ChunkInfo> offset2Chunks) {
    blocks.getOrDefault(blockID, new BlockMerkleTree(blockID)).addChunks(offset2Chunks);
  }

  /**
   * Uses chunk hashes to compute all remaining hashes in the tree, and returns it as a protobuf object. No checksum
   * computation happens outside of this method.
   *
   * @return A complete protobuf object representation of this tree.
   */
  public ContainerMerkleTreeProto toProto() {
    // Compute checksums and return the result.
    ContainerMerkleTreeProto.Builder treeProto = ContainerMerkleTreeProto.newBuilder();
    // TODO configurable checksum implementation
    ChecksumByteBuffer checksumImpl = ChecksumByteBufferFactory.crc32Impl();
    ByteBuffer containerChecksumBuffer = ByteBuffer.allocate(Long.BYTES * blocks.size());
    for (Map.Entry<Long, BlockMerkleTree> entry: blocks.entrySet()) {
      BlockMerkleTreeProto blockTreeProto = entry.getValue().toProto();
      treeProto.addBlockMerkleTree(blockTreeProto);
      containerChecksumBuffer.putLong(blockTreeProto.getBlockChecksum());
    }
    treeProto.setDataChecksum(checksumImpl.getValue());
    return treeProto.build();
  }

  /**
   * Represents a merkle tree for a single block within a container.
   */
  private static class BlockMerkleTree {
    // Map of each offset within the block to its chunk info.
    // Chunk order in the checksum is determined by their offset.
    private final SortedMap<Long, ChunkInfo> chunks;
    private final long blockID;

    BlockMerkleTree(long blockID) {
      this.blockID = blockID;
      this.chunks = new TreeMap<>();
    }

    /**
     * Adds the specified chunks to this block. This should run in linear time since the {@link SortedMap} parameter
     * is added to an existing {@link TreeMap} internally.
     *
     * @param offset2Chunks A map of chunk offset to chunk info, sorted by chunk offset. This will be merged with the
     *    existing sorted list of chunks stored for this block.
     */
    public void addChunks(SortedMap<Long, ChunkInfo> offset2Chunks) {
      chunks.putAll(offset2Chunks);
    }

    /**
     * Uses chunk hashes to compute a block hash for this tree, and returns it as a protobuf object. No checksum
     * computation happens outside of this method.
     *
     * @return A complete protobuf object representation of this tree.
     */
    public BlockMerkleTreeProto toProto() {
      BlockMerkleTreeProto.Builder blockTreeBuilder = BlockMerkleTreeProto.newBuilder();
      // TODO configurable checksum implementation
      ChecksumByteBuffer checksumImpl = ChecksumByteBufferFactory.crc32Impl();

      for (ChunkInfo chunk: chunks.values()) {
        // TODO can we depend on this ordering to be consistent?
        List<ByteString> chunkChecksums = chunk.getChecksumData().getChecksums();
        blockTreeBuilder.addChunks(chunk.getProtoBufMessage());
        for (ByteString checksum: chunkChecksums) {
          checksumImpl.update(checksum.asReadOnlyByteBuffer());
        }
      }

      return blockTreeBuilder
          .setBlockID(blockID)
          .setBlockChecksum(checksumImpl.getValue())
          .build();
    }
  }
}
