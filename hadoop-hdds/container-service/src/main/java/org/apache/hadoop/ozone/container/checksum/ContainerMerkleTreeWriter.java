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
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.common.ChecksumByteBuffer;
import org.apache.hadoop.ozone.common.ChecksumByteBufferFactory;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/**
 * This class constructs a Merkle tree that provides one checksum for all data within a container.
 *
 * As the leaves of the tree, a checksum for each chunk is computed by taking a checksum of all checksums within that
 * chunk. Each chunk checksum in a block is further checksummed together to generate the block level checksum. Finally,
 * The checksums of all blocks are checksummed together to create a container level checksum.
 * Note that checksums are order dependent. Chunk checksums are sorted by their
 * offset within a block, and block checksums are sorted by their block ID.
 *
 * This class can be used to construct a consistent and completely filled {@link ContainerProtos.ContainerMerkleTree}
 * object. It allows building a container merkle tree from scratch by incrementally adding chunks.
 * The final checksums above the leaf levels of the tree are not calculated until
 * {@link ContainerMerkleTreeWriter#toProto} is called.
 */
public class ContainerMerkleTreeWriter {

  private final SortedMap<Long, BlockMerkleTreeWriter> id2Block;
  // All merkle tree generation will use CRC32C to aggregate checksums at each level, regardless of the
  // checksum algorithm used on the underlying data.
  public static final Supplier<ChecksumByteBuffer> CHECKSUM_BUFFER_SUPPLIER = ChecksumByteBufferFactory::crc32CImpl;

  /**
   * Constructs a writer for an initially empty container merkle tree.
   */
  public ContainerMerkleTreeWriter() {
    id2Block = new TreeMap<>();
  }

  /**
   * Constructs a writer for a container merkle tree which initially contains all the information from the specified
   * proto.
   */
  public ContainerMerkleTreeWriter(ContainerProtos.ContainerMerkleTree fromTree) {
    id2Block = new TreeMap<>();
    for (ContainerProtos.BlockMerkleTree blockTree: fromTree.getBlockMerkleTreeList()) {
      long blockID = blockTree.getBlockID();
      addBlock(blockID);
      for (ContainerProtos.ChunkMerkleTree chunkTree: blockTree.getChunkMerkleTreeList()) {
        addChunks(blockID, chunkTree);
      }
    }
  }

  /**
   * Adds chunks to a block in the tree. The block entry will be created if it is the first time adding chunks to it.
   * If the block entry already exists, the chunks will be added to the existing chunks for that block.
   *
   * @param blockID The ID of the block that these chunks belong to.
   * @param checksumMatches True if there were no checksum errors detected with these chunks. False indicates that all
   *    the chunks being added had checksum errors.
   * @param chunks A list of chunks to add to this block. The chunks will be sorted internally by their offset.
   */
  public void addChunks(long blockID, boolean checksumMatches, Collection<ContainerProtos.ChunkInfo> chunks) {
    for (ContainerProtos.ChunkInfo chunk: chunks) {
      addChunks(blockID, checksumMatches, chunk);
    }
  }

  public void addChunks(long blockID, boolean checksumMatches, ContainerProtos.ChunkInfo... chunks) {
    for (ContainerProtos.ChunkInfo chunk: chunks) {
      addChunks(blockID, new ChunkMerkleTreeWriter(chunk, checksumMatches));
    }
  }

  private void addChunks(long blockID, ContainerProtos.ChunkMerkleTree... chunks) {
    for (ContainerProtos.ChunkMerkleTree chunkTree: chunks) {
      addChunks(blockID, new ChunkMerkleTreeWriter(chunkTree));
    }
  }

  private void addChunks(long blockID, ChunkMerkleTreeWriter chunkWriter) {
    id2Block.computeIfAbsent(blockID, BlockMerkleTreeWriter::new).addChunks(chunkWriter);
  }

  /**
   * Adds an empty block to the tree. This method is not a pre-requisite to {@code addChunks}.
   * If the block entry already exists, it will not be modified.
   *
   * @param blockID The ID of the empty block to add to the tree
   */
  public void addBlock(long blockID) {
    id2Block.computeIfAbsent(blockID, BlockMerkleTreeWriter::new);
  }

  /**
   * Uses chunk hashes to compute all remaining hashes in the tree, and returns it as a protobuf object. No checksum
   * computation for the tree happens outside of this method.
   *
   * @return A complete protobuf object representation of this tree.
   */
  public ContainerProtos.ContainerMerkleTree toProto() {
    // Compute checksums and return the result.
    ContainerProtos.ContainerMerkleTree.Builder containerTreeBuilder = ContainerProtos.ContainerMerkleTree.newBuilder();
    ChecksumByteBuffer checksumImpl = CHECKSUM_BUFFER_SUPPLIER.get();
    ByteBuffer containerChecksumBuffer = ByteBuffer.allocate(Long.BYTES * id2Block.size());

    for (BlockMerkleTreeWriter blockTree: id2Block.values()) {
      ContainerProtos.BlockMerkleTree blockTreeProto = blockTree.toProto();
      containerTreeBuilder.addBlockMerkleTree(blockTreeProto);
      // Add the block's checksum to the buffer that will be used to calculate the container checksum.
      containerChecksumBuffer.putLong(blockTreeProto.getDataChecksum());
    }
    containerChecksumBuffer.flip();
    checksumImpl.update(containerChecksumBuffer);

    return containerTreeBuilder
        .setDataChecksum(checksumImpl.getValue())
        .build();
  }

  /**
   * Constructs a merkle tree for a single block within a container.
   */
  private static class BlockMerkleTreeWriter {
    // Map of each offset within the block to its chunk info.
    // Chunk order in the checksum is determined by their offset.
    private final SortedMap<Long, ChunkMerkleTreeWriter> offset2Chunk;
    private final long blockID;

    BlockMerkleTreeWriter(long blockID) {
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

    /**
     * Uses chunk hashes to compute a block hash for this tree, and returns it as a protobuf object. All block checksum
     * computation for the tree happens within this method.
     *
     * @return A complete protobuf object representation of this block tree.
     */
    public ContainerProtos.BlockMerkleTree toProto() {
      ContainerProtos.BlockMerkleTree.Builder blockTreeBuilder = ContainerProtos.BlockMerkleTree.newBuilder();
      ChecksumByteBuffer checksumImpl = CHECKSUM_BUFFER_SUPPLIER.get();
      ByteBuffer blockChecksumBuffer = ByteBuffer.allocate(Long.BYTES * offset2Chunk.size());

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
  }

  /**
   * Constructs a merkle tree for a single chunk within a container.
   * Each chunk has multiple checksums within it at each "bytesPerChecksum" interval.
   * This class computes one checksum for the whole chunk by aggregating these.
   */
  private static class ChunkMerkleTreeWriter {
    private final long length;
    private final long offset;
    private final boolean checksumMatches;
    private final long dataChecksum;

    ChunkMerkleTreeWriter(ContainerProtos.ChunkInfo chunk, boolean checksumMatches) {
      length = chunk.getLen();
      offset = chunk.getOffset();
      this.checksumMatches = checksumMatches;
      ChecksumByteBuffer checksumImpl = CHECKSUM_BUFFER_SUPPLIER.get();
      for (ByteString checksum: chunk.getChecksumData().getChecksumsList()) {
        checksumImpl.update(checksum.asReadOnlyByteBuffer());
      }
      this.dataChecksum = checksumImpl.getValue();
    }

    ChunkMerkleTreeWriter(ContainerProtos.ChunkMerkleTree chunkTree) {
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
