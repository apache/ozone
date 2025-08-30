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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.common.ChecksumByteBuffer;
import org.apache.hadoop.ozone.common.ChecksumByteBufferFactory;
import org.apache.hadoop.ozone.container.checksum.BlockMerkleTreeWriter.ChunkMerkleTreeWriter;

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
  private final SortedMap<Long, BlockMerkleTreeWriter> id2DeletedBlock;
  // All merkle tree generation will use CRC32C to aggregate checksums at each level, regardless of the
  // checksum algorithm used on the underlying data.
  public static final Supplier<ChecksumByteBuffer> CHECKSUM_BUFFER_SUPPLIER = ChecksumByteBufferFactory::crc32CImpl;

  /**
   * Constructs a writer for an initially empty container merkle tree.
   */
  public ContainerMerkleTreeWriter() {
    id2Block = new TreeMap<>();
    id2DeletedBlock = new TreeMap<>();
  }

  /**
   * Constructs a writer for a container merkle tree which initially contains all the information from the specified
   * proto.
   */
  public ContainerMerkleTreeWriter(ContainerProtos.ContainerMerkleTree fromTree) {
    id2Block = new TreeMap<>();
    id2DeletedBlock = new TreeMap<>();
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
   * Adds deleted blocks to the tree. This allows including deleted blocks during tree construction
   * instead of at serialization time, reducing test changes and simplifying the flow.
   *
   * @param deletedBlocks List of deleted block merkle trees to add to the tree
   */
  public void addDeletedBlocks(List<ContainerProtos.BlockMerkleTree> deletedBlocks) {
    for (ContainerProtos.BlockMerkleTree deletedBlock : deletedBlocks) {
      long blockID = deletedBlock.getBlockID();
      // Add the block if it doesn't exist yet
      BlockMerkleTreeWriter blockWriter = id2DeletedBlock.computeIfAbsent(blockID, BlockMerkleTreeWriter::new);
      
      // Add chunks from the deleted block
      for (ContainerProtos.ChunkMerkleTree chunkTree : deletedBlock.getChunkMerkleTreeList()) {
        blockWriter.addChunks(new ChunkMerkleTreeWriter(chunkTree));
      }
    }
  }

  /**
   * Checks if the tree contains any deleted blocks.
   */
  public boolean hasDeletedBlocks() {
    return !id2DeletedBlock.isEmpty();
  }

  /**
   * Returns a collection of all deleted blocks in the tree.
   */
  public Collection<ContainerProtos.BlockMerkleTree> getDeletedBlocks() {
    List<ContainerProtos.BlockMerkleTree> deletedBlocks = new ArrayList<>();
    for (BlockMerkleTreeWriter blockTree : id2DeletedBlock.values()) {
      deletedBlocks.add(blockTree.toProto());
    }
    return deletedBlocks;
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

    // Create a combined sorted map of all blocks (active + deleted) for consistent checksum calculation
    SortedMap<Long, Long> allBlockChecksums = new TreeMap<>();

    // Add active blocks from the tree
    for (BlockMerkleTreeWriter blockTree: id2Block.values()) {
      ContainerProtos.BlockMerkleTree blockTreeProto = blockTree.toProto();
      containerTreeBuilder.addBlockMerkleTree(blockTreeProto);
      allBlockChecksums.put(blockTreeProto.getBlockID(), blockTreeProto.getDataChecksum());
    }

    // Add deleted blocks to the checksum calculation
    for (BlockMerkleTreeWriter deletedBlockTree : id2DeletedBlock.values()) {
      ContainerProtos.BlockMerkleTree deletedBlockTreeProto = deletedBlockTree.toProto();
      containerTreeBuilder.addBlockMerkleTree(deletedBlockTreeProto);
      allBlockChecksums.put(deletedBlockTreeProto.getBlockID(), deletedBlockTreeProto.getDataChecksum());
    }

    // Calculate container checksum using all blocks (active + deleted) in sorted order
    ByteBuffer containerChecksumBuffer = ByteBuffer.allocate(Long.BYTES * allBlockChecksums.size());
    for (Long blockChecksum : allBlockChecksums.values()) {
      containerChecksumBuffer.putLong(blockChecksum);
    }
    containerChecksumBuffer.flip();
    checksumImpl.update(containerChecksumBuffer);

    return containerTreeBuilder
        .setDataChecksum(checksumImpl.getValue())
        .build();
  }
}
