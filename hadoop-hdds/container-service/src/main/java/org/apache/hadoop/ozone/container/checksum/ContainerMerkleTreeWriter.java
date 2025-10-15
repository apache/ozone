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
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
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
      if (blockTree.getDeleted()) {
        setDeletedBlock(blockID, blockTree.getDataChecksum());
      } else {
        addBlock(blockID);
        for (ContainerProtos.ChunkMerkleTree chunkTree: blockTree.getChunkMerkleTreeList()) {
          addChunks(blockID, new ChunkMerkleTreeWriter(chunkTree));
        }
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
      addChunks(blockID, new ChunkMerkleTreeWriter(chunk, checksumMatches));
    }
  }

  public void addChunks(long blockID, boolean checksumMatches, ContainerProtos.ChunkInfo... chunks) {
    for (ContainerProtos.ChunkInfo chunk: chunks) {
      addChunks(blockID, new ChunkMerkleTreeWriter(chunk, checksumMatches));
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
   * Creates a deleted block entry in the merkle tree and assigns the block this fixed checksum.
   * If the block already exists with child data it is overwritten.
   *
   * This method is used on the reconciliation path to update the data checksum used for a deleted block based on a
   * peer's value.
   */
  public void setDeletedBlock(long blockID, long dataChecksum) {
    BlockMerkleTreeWriter blockWriter = new BlockMerkleTreeWriter(blockID);
    blockWriter.markDeleted(dataChecksum);
    id2Block.put(blockID, blockWriter);
  }

  /**
   * Merges the content from the provided tree with this tree writer.
   * Conflicts where this tree writer and the incoming existingTree parameter have an entry for the same block are
   * resolved in the following manner:
   * - A deleted block supersedes a live block
   *   - Data cannot be un-deleted, so if a delete is ever witnessed, that is the state the block should converge to.
   * - If both blocks are either deleted or live, the value in this writer supersedes the value in the existingTree
   * parameter.
   *   - Our writer has the last witnessed information that is going to be persisted after this merge.
   *
   * For example, consider the case where a peer has deleted a block and we have a corrupt copy that has not yet been
   * deleted. When we reconcile with this peer, we will mark the block as deleted and use the peer's checksum in our
   * merkle tree to make the trees converge. The "fix" for corrupted data that is supposed to be deleted is to delete
   * it. After this, if the scanner runs again before the block is deleted, we don't want to update the tree with the
   * scanner's value because it would again diverge from the peer due to data that is expected to be deleted.
   * This would cause the checksum to oscillate back and forth until the block is deleted, instead of converging.
   */
  public ContainerProtos.ContainerMerkleTree update(ContainerProtos.ContainerMerkleTree existingTree) {
    for (ContainerProtos.BlockMerkleTree existingBlockTree: existingTree.getBlockMerkleTreeList()) {
      long blockID = existingBlockTree.getBlockID();
      BlockMerkleTreeWriter ourBlockTree = id2Block.get(blockID);
      if (ourBlockTree != null) {
        // both trees contain the block. We will only consider the incoming/existing value if it does not match our
        // current state
        if (!ourBlockTree.isDeleted() && existingBlockTree.getDeleted()) {
          setDeletedBlock(blockID, existingBlockTree.getDataChecksum());
        }
        // In all other cases, keep using our writer's value over the existing one because either:
        // - The deleted states match between the two blocks OR
        // - Our block is deleted and the existing one is not, so we have the latest value to use.
      } else if (existingBlockTree.getDeleted()) {
        // Our tree does not have this block. Only take the value if it is deleted.
        // The definitive set of live blocks will come from this tree writer.
        setDeletedBlock(blockID, existingBlockTree.getDataChecksum());
      }
    }
    return toProtoBuilder().build();
  }

  /**
   * Adds deleted blocks to this merkle tree. The blocks' checksums are computed from the checksums in the BlockData.
   * If a block with the same ID already exists in the tree, it is overwritten as deleted with the checksum computed
   * from the chunk checksums in the BlockData. If we reconciled with a peer and already marked this block as deleted
   * during that process, this will overwrite that value. If it changes the block's checksum from what the peer had,
   * one more round of reconciliation may be required to bring them in sync.
   *
   * The top level container data checksum is only computed in the returned tree proto if computeChecksum is true.
   * If it is false, the resulting tree proto will have data checksums for each block, but an empty/unset data checksum
   * for the container at the root of the tree.
   */
  public ContainerProtos.ContainerMerkleTree addDeletedBlocks(Collection<BlockData> blocks, boolean computeChecksum) {
    for (BlockData block: blocks) {
      long blockID = block.getLocalID();
      BlockMerkleTreeWriter blockWriter = new BlockMerkleTreeWriter(blockID);
      for (ContainerProtos.ChunkInfo chunkInfo: block.getChunks()) {
        blockWriter.addChunks(new ChunkMerkleTreeWriter(chunkInfo, true));
      }
      blockWriter.markDeleted();
      id2Block.put(blockID, blockWriter);
    }
    ContainerProtos.ContainerMerkleTree.Builder protoBuilder = toProtoBuilder();
    if (!computeChecksum) {
      protoBuilder.clearDataChecksum();
    }
    return protoBuilder.build();
  }

  /**
   * Uses chunk hashes to compute all remaining hashes in the tree, and returns it as a protobuf object. No checksum
   * computation for the tree happens outside of this method.
   *
   * @return A complete protobuf object representation of this tree.
   */
  private ContainerProtos.ContainerMerkleTree.Builder toProtoBuilder() {
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
        .setDataChecksum(checksumImpl.getValue());
  }

  public ContainerProtos.ContainerMerkleTree toProto() {
    return toProtoBuilder().build();
  }

  /**
   * Constructs a merkle tree for a single block within a container.
   */
  private static class BlockMerkleTreeWriter {
    // Map of each offset within the block to its chunk info.
    // Chunk order in the checksum is determined by their offset.
    private final SortedMap<Long, ChunkMerkleTreeWriter> offset2Chunk;
    private final long blockID;
    private boolean deleted;
    private Long dataChecksum;

    BlockMerkleTreeWriter(long blockID) {
      this.blockID = blockID;
      this.offset2Chunk = new TreeMap<>();
      this.deleted = false;
    }

    public void markDeleted(long deletedDataChecksum) {
      this.deleted = true;
      this.dataChecksum = deletedDataChecksum;
    }

    public void markDeleted() {
      this.deleted = true;
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

    public boolean isDeleted() {
      return deleted;
    }

    /**
     * Uses chunk hashes to compute a block hash for this tree, and returns it as a protobuf object. All block checksum
     * computation for the tree happens within this method.
     *
     * @return A complete protobuf object representation of this block tree.
     */
    public ContainerProtos.BlockMerkleTree toProto() {
      ContainerProtos.BlockMerkleTree.Builder blockTreeBuilder = ContainerProtos.BlockMerkleTree.newBuilder();
      if (dataChecksum != null) {
        blockTreeBuilder.setDataChecksum(dataChecksum);
      } else {
        setDataChecksumFromChunks(blockTreeBuilder);
      }

      if (deleted) {
        blockTreeBuilder.clearChunkMerkleTree();
      }

      return blockTreeBuilder
          .setBlockID(blockID)
          .setDeleted(deleted)
          .build();
    }

    private void setDataChecksumFromChunks(ContainerProtos.BlockMerkleTree.Builder blockTreeBuilder) {
      ChecksumByteBuffer checksumImpl = CHECKSUM_BUFFER_SUPPLIER.get();
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
      blockTreeBuilder.setDataChecksum(checksumImpl.getValue());
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
