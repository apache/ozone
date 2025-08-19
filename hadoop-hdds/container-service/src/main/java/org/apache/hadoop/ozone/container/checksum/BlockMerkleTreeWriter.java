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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.common.ChecksumByteBuffer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/**
 * Constructs a merkle tree for a single block within a container.
 */
public class BlockMerkleTreeWriter {
  // Map of each offset within the block to its chunk info.
  // Chunk order in the checksum is determined by their offset.
  private final SortedMap<Long, ChunkMerkleTreeWriter> offset2Chunk;
  private final long blockID;

  public BlockMerkleTreeWriter(long blockID) {
    this.blockID = blockID;
    this.offset2Chunk = new TreeMap<>();
  }

  /**
   * Adds the specified chunks to this block. The offset value of the chunk must be unique within the block,
   * otherwise it will overwrite the previous value at that offset.
   *
   * @param chunks A list of chunks to add to this block.
   */
  public void addChunks(ChunkMerkleTreeWriter... chunks) {
    for (ChunkMerkleTreeWriter chunk: chunks) {
      offset2Chunk.put(chunk.getOffset(), chunk);
    }
  }

  public void addChunks(List<ContainerProtos.ChunkInfo> chunkInfos) {
    for (ContainerProtos.ChunkInfo chunkInfo: chunkInfos) {
      offset2Chunk.put(chunkInfo.getOffset(), new ChunkMerkleTreeWriter(chunkInfo, true));
    }
  }

  /**
   * Uses chunk hashes to compute a block hash for this tree, and returns it as a protobuf object. All block checksum
   * computation for the tree happens within this method.
   *
   * @return A complete protobuf object representation of this block tree.
   */
  public ContainerProtos.BlockMerkleTree toProto() {
    ContainerProtos.BlockMerkleTree.Builder blockTreeBuilder = ContainerProtos.BlockMerkleTree.newBuilder();
    ChecksumByteBuffer checksumImpl = ContainerMerkleTreeWriter.CHECKSUM_BUFFER_SUPPLIER.get();
    // Allocate space for block ID + all chunk checksums
    ByteBuffer blockChecksumBuffer = ByteBuffer.allocate(Long.BYTES * (1 + offset2Chunk.size()));
    // Hash the block ID into the beginning of the block checksum calculation
    blockChecksumBuffer.putLong(blockID);

    for (ChunkMerkleTreeWriter chunkTree: offset2Chunk.values()) {
      // Ordering of checksums within a chunk is assumed to be in the order they are written.
      // This assumption is already built in to the code that reads and writes the values (see
      // ChunkInputStream#validateChunk for an example on the client read path).
      // There is no other value we can use to sort these checksums, so we assume the stored proto has them in the
      // correct order.
      ContainerProtos.ChunkMerkleTree chunkTreeProto = chunkTree.toProto();
      blockTreeBuilder.addChunkMerkleTree(chunkTreeProto);
      blockChecksumBuffer.putLong(chunkTreeProto.getDataChecksum());
    }
    blockChecksumBuffer.flip();
    checksumImpl.update(blockChecksumBuffer);

    return blockTreeBuilder
        .setBlockID(blockID)
        .setDataChecksum(checksumImpl.getValue())
        .build();
  }

  /**
   * Gets the block ID for this block merkle tree.
   * @return the block ID
   */
  public long getBlockID() {
    return blockID;
  }

  /**
   * Constructs a merkle tree for a single chunk within a container.
   * Each chunk has multiple checksums within it at each "bytesPerChecksum" interval.
   * This class computes one checksum for the whole chunk by aggregating these.
   */
  public static class ChunkMerkleTreeWriter {
    private final long length;
    private final long offset;
    private final boolean checksumMatches;
    private final long dataChecksum;

    public ChunkMerkleTreeWriter(ContainerProtos.ChunkInfo chunk, boolean checksumMatches) {
      length = chunk.getLen();
      offset = chunk.getOffset();
      this.checksumMatches = checksumMatches;
      ChecksumByteBuffer checksumImpl = ContainerMerkleTreeWriter.CHECKSUM_BUFFER_SUPPLIER.get();
      for (ByteString checksum: chunk.getChecksumData().getChecksumsList()) {
        checksumImpl.update(checksum.asReadOnlyByteBuffer());
      }
      this.dataChecksum = checksumImpl.getValue();
    }

    public ChunkMerkleTreeWriter(ContainerProtos.ChunkMerkleTree chunkTree) {
      length = chunkTree.getLength();
      offset = chunkTree.getOffset();
      checksumMatches = chunkTree.getChecksumMatches();
      dataChecksum = chunkTree.getDataChecksum();
    }

    public long getOffset() {
      return offset;
    }

    /**
     * Computes a single hash for this ChunkInfo object. All chunk level checksum computation happens within this
     * method.
     *
     * @return A complete protobuf representation of this chunk as a leaf in the container merkle tree.
     */
    public ContainerProtos.ChunkMerkleTree toProto() {
      return ContainerProtos.ChunkMerkleTree.newBuilder()
          .setOffset(offset)
          .setLength(length)
          .setChecksumMatches(checksumMatches)
          .setDataChecksum(dataChecksum)
          .build();
    }
  }
}
