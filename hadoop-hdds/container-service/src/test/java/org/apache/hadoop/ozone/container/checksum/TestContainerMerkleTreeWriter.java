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

import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.assertTreesSortedAndMatch;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.buildChunk;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.common.ChecksumByteBuffer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestContainerMerkleTreeWriter {
  private ConfigurationSource config;
  private long chunkSize;

  @BeforeEach
  public void init() {
    config = new OzoneConfiguration();
    chunkSize = (long) config.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY, ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_DEFAULT, StorageUnit.BYTES);
  }

  @Test
  public void testBuildEmptyTree() {
    ContainerMerkleTreeWriter tree = new ContainerMerkleTreeWriter();
    ContainerProtos.ContainerMerkleTree treeProto = tree.toProto();
    assertEquals(0, treeProto.getDataChecksum());
    assertEquals(0, treeProto.getBlockMerkleTreeCount());
  }

  @Test
  public void testBuildOneChunkTree() {
    // Seed the expected and actual trees with the same chunk.
    final long blockID = 1;
    ContainerProtos.ChunkInfo chunk = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1, 2, 3}));

    // Build the expected tree proto using the test code.
    ContainerProtos.ChunkMerkleTree chunkTree = buildExpectedChunkTree(chunk);
    ContainerProtos.BlockMerkleTree blockTree = buildExpectedBlockTree(blockID,
        Collections.singletonList(chunkTree));
    ContainerProtos.ContainerMerkleTree expectedTree = buildExpectedContainerTree(Collections.singletonList(blockTree));

    // Use the ContainerMerkleTreeWriter to build the same tree.
    ContainerMerkleTreeWriter actualTree = new ContainerMerkleTreeWriter();
    actualTree.addChunks(blockID, true, chunk);

    // Ensure the trees match.
    ContainerProtos.ContainerMerkleTree actualTreeProto = actualTree.toProto();
    assertTreesSortedAndMatch(expectedTree, actualTreeProto);

    // Do some manual verification of the generated tree as well.
    assertNotEquals(0, actualTreeProto.getDataChecksum());
    assertEquals(1, actualTreeProto.getBlockMerkleTreeCount());

    ContainerProtos.BlockMerkleTree actualBlockTree = actualTreeProto.getBlockMerkleTree(0);
    assertEquals(1, actualBlockTree.getBlockID());
    assertEquals(1, actualBlockTree.getChunkMerkleTreeCount());
    assertNotEquals(0, actualBlockTree.getDataChecksum());

    ContainerProtos.ChunkMerkleTree actualChunkTree = actualBlockTree.getChunkMerkleTree(0);
    assertEquals(0, actualChunkTree.getOffset());
    assertEquals(chunkSize, actualChunkTree.getLength());
    assertNotEquals(0, actualChunkTree.getDataChecksum());
  }

  @Test
  public void testBuildTreeWithMissingChunks() {
    // These chunks will be used to seed both the expected and actual trees.
    final long blockID = 1;
    ContainerProtos.ChunkInfo chunk1 = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    // Chunk 2 is missing.
    ContainerProtos.ChunkInfo chunk3 = buildChunk(config, 2, ByteBuffer.wrap(new byte[]{4, 5, 6}));

    // Build the expected tree proto using the test code.
    ContainerProtos.BlockMerkleTree blockTree = buildExpectedBlockTree(blockID,
        Arrays.asList(buildExpectedChunkTree(chunk1), buildExpectedChunkTree(chunk3)));
    ContainerProtos.ContainerMerkleTree expectedTree = buildExpectedContainerTree(Collections.singletonList(blockTree));

    // Use the ContainerMerkleTree to build the same tree.
    ContainerMerkleTreeWriter actualTree = new ContainerMerkleTreeWriter();
    actualTree.addChunks(blockID, true, chunk1, chunk3);

    // Ensure the trees match.
    ContainerProtos.ContainerMerkleTree actualTreeProto = actualTree.toProto();
    assertTreesSortedAndMatch(expectedTree, actualTreeProto);
  }

  @Test
  public void testBlockIdIncludedInChecksum() {
    // Create a set of chunks to be used in different blocks with identical content.
    ContainerProtos.ChunkInfo chunk1 = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ContainerProtos.ChunkInfo chunk2 = buildChunk(config, 1, ByteBuffer.wrap(new byte[]{4, 5, 6}));

    // Create two blocks with different IDs but identical chunk data
    final long blockID1 = 1;
    final long blockID2 = 2;

    ContainerMerkleTreeWriter tree1 = new ContainerMerkleTreeWriter();
    tree1.addChunks(blockID1, true, chunk1, chunk2);

    ContainerMerkleTreeWriter tree2 = new ContainerMerkleTreeWriter();
    tree2.addChunks(blockID2, true, chunk1, chunk2);

    ContainerProtos.ContainerMerkleTree tree1Proto = tree1.toProto();
    ContainerProtos.ContainerMerkleTree tree2Proto = tree2.toProto();

    // Even though the chunks are identical, the block checksums should be different
    // because the block IDs are different
    ContainerProtos.BlockMerkleTree block1 = tree1Proto.getBlockMerkleTree(0);
    ContainerProtos.BlockMerkleTree block2 = tree2Proto.getBlockMerkleTree(0);

    assertEquals(blockID1, block1.getBlockID());
    assertEquals(blockID2, block2.getBlockID());
    assertNotEquals(block1.getDataChecksum(), block2.getDataChecksum(),
        "Blocks with identical chunks but different IDs should have different checksums");

    // Consequently, the container checksums should also be different
    assertNotEquals(tree1Proto.getDataChecksum(), tree2Proto.getDataChecksum(),
        "Containers with blocks having identical chunks but different IDs should have different checksums");
  }

  @Test
  public void testIdenticalBlocksHaveSameChecksum() {
    // Create a set of chunks to be used in different blocks with identical content.
    ContainerProtos.ChunkInfo chunk1 = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ContainerProtos.ChunkInfo chunk2 = buildChunk(config, 1, ByteBuffer.wrap(new byte[]{4, 5, 6}));

    // Create two blocks with the same ID and identical chunk data
    final long blockID = 1;

    ContainerMerkleTreeWriter tree1 = new ContainerMerkleTreeWriter();
    tree1.addChunks(blockID, true, chunk1, chunk2);

    ContainerMerkleTreeWriter tree2 = new ContainerMerkleTreeWriter();
    tree2.addChunks(blockID, true, chunk1, chunk2);

    ContainerProtos.ContainerMerkleTree tree1Proto = tree1.toProto();
    ContainerProtos.ContainerMerkleTree tree2Proto = tree2.toProto();

    // Blocks with same ID and identical chunks should have same checksums
    ContainerProtos.BlockMerkleTree block1 = tree1Proto.getBlockMerkleTree(0);
    ContainerProtos.BlockMerkleTree block2 = tree2Proto.getBlockMerkleTree(0);

    assertEquals(blockID, block1.getBlockID());
    assertEquals(blockID, block2.getBlockID());
    assertEquals(block1.getDataChecksum(), block2.getDataChecksum(),
        "Blocks with same ID and identical chunks should have same checksums");

    // Container checksums should also be the same
    assertEquals(tree1Proto.getDataChecksum(), tree2Proto.getDataChecksum(),
        "Containers with identical blocks should have same checksums");
  }

  @Test
  public void testContainerReplicasWithDifferentMissingBlocksHaveDifferentChecksums() {
    // Create identical chunk data that will be used across all blocks
    ContainerProtos.ChunkInfo chunk1 = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ContainerProtos.ChunkInfo chunk2 = buildChunk(config, 1, ByteBuffer.wrap(new byte[]{4, 5, 6}));
    
    // Scenario: Container has 5 identical blocks, but different replicas are missing different blocks
    // Replica 1 is missing block 1 (has blocks 2,3,4,5)
    ContainerMerkleTreeWriter replica1 = new ContainerMerkleTreeWriter();
    replica1.addChunks(2, true, chunk1, chunk2);
    replica1.addChunks(3, true, chunk1, chunk2);
    replica1.addChunks(4, true, chunk1, chunk2);
    replica1.addChunks(5, true, chunk1, chunk2);
    
    // Replica 2 is missing block 5 (has blocks 1,2,3,4)
    ContainerMerkleTreeWriter replica2 = new ContainerMerkleTreeWriter();
    replica2.addChunks(1, true, chunk1, chunk2);
    replica2.addChunks(2, true, chunk1, chunk2);
    replica2.addChunks(3, true, chunk1, chunk2);
    replica2.addChunks(4, true, chunk1, chunk2);
    
    ContainerProtos.ContainerMerkleTree replica1Proto = replica1.toProto();
    ContainerProtos.ContainerMerkleTree replica2Proto = replica2.toProto();
    assertNotEquals(replica1Proto.getDataChecksum(), replica2Proto.getDataChecksum(),
        "Container replicas with identical blocks but different missing blocks should have different checksums");
    
    // Verify both replicas have the same number of blocks
    assertEquals(4, replica1Proto.getBlockMerkleTreeCount());
    assertEquals(4, replica2Proto.getBlockMerkleTreeCount());
  }

  @Test
  public void testBuildTreeWithEmptyBlock() {
    final long blockID = 1;
    ContainerProtos.BlockMerkleTree blockTree = buildExpectedBlockTree(blockID, Collections.emptyList());
    ContainerProtos.ContainerMerkleTree expectedTree = buildExpectedContainerTree(Collections.singletonList(blockTree));

    // Use the ContainerMerkleTree to build the same tree.
    ContainerMerkleTreeWriter actualTree = new ContainerMerkleTreeWriter();
    actualTree.addBlock(blockID);

    // Ensure the trees match.
    ContainerProtos.ContainerMerkleTree actualTreeProto = actualTree.toProto();
    assertTreesSortedAndMatch(expectedTree, actualTreeProto);
  }

  @Test
  public void testAddBlockIdempotent() {
    final long blockID = 1;
    // Build the expected proto.
    ContainerProtos.ChunkInfo chunk1 = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ContainerProtos.BlockMerkleTree blockTree = buildExpectedBlockTree(blockID,
        Collections.singletonList(buildExpectedChunkTree(chunk1)));
    ContainerProtos.ContainerMerkleTree expectedTree = buildExpectedContainerTree(Collections.singletonList(blockTree));

    // Use the ContainerMerkleTree to build the same tree, calling addBlock in between adding chunks.
    ContainerMerkleTreeWriter actualTree = new ContainerMerkleTreeWriter();
    actualTree.addBlock(blockID);
    actualTree.addChunks(blockID, true, chunk1);
    // This should not overwrite the chunk already added to the block.
    actualTree.addBlock(blockID);

    // Ensure the trees match.
    ContainerProtos.ContainerMerkleTree actualTreeProto = actualTree.toProto();
    assertTreesSortedAndMatch(expectedTree, actualTreeProto);
  }

  /**
   * A container is a set of blocks. Make sure the tree implementation is not dependent on continuity of block IDs.
   */
  @Test
  public void testBuildTreeWithNonContiguousBlockIDs() {
    // Seed the expected and actual trees with the same chunks.
    final long blockID1 = 1;
    final long blockID3 = 3;
    ContainerProtos.ChunkInfo b1c1 = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ContainerProtos.ChunkInfo b1c2 = buildChunk(config, 1, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ContainerProtos.ChunkInfo b3c1 = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ContainerProtos.ChunkInfo b3c2 = buildChunk(config, 1, ByteBuffer.wrap(new byte[]{1, 2, 3}));

    // Build the expected tree proto using the test code.
    ContainerProtos.BlockMerkleTree blockTree1 = buildExpectedBlockTree(blockID1,
        Arrays.asList(buildExpectedChunkTree(b1c1), buildExpectedChunkTree(b1c2)));
    ContainerProtos.BlockMerkleTree blockTree3 = buildExpectedBlockTree(blockID3,
        Arrays.asList(buildExpectedChunkTree(b3c1), buildExpectedChunkTree(b3c2)));
    ContainerProtos.ContainerMerkleTree expectedTree = buildExpectedContainerTree(
        Arrays.asList(blockTree1, blockTree3));

    // Use the ContainerMerkleTree to build the same tree.
    // Add blocks and chunks out of order to test sorting.
    ContainerMerkleTreeWriter actualTree = new ContainerMerkleTreeWriter();
    actualTree.addChunks(blockID3, true, b3c2, b3c1);
    actualTree.addChunks(blockID1, true, b1c1, b1c2);

    // Ensure the trees match.
    ContainerProtos.ContainerMerkleTree actualTreeProto = actualTree.toProto();
    assertTreesSortedAndMatch(expectedTree, actualTreeProto);
  }

  @Test
  public void testAppendToBlocksWhileBuilding() throws Exception {
    // Seed the expected and actual trees with the same chunks.
    final long blockID1 = 1;
    final long blockID2 = 2;
    final long blockID3 = 3;
    ContainerProtos.ChunkInfo b1c1 = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ContainerProtos.ChunkInfo b1c2 = buildChunk(config, 1, ByteBuffer.wrap(new byte[]{1, 2}));
    ContainerProtos.ChunkInfo b1c3 = buildChunk(config, 2, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ContainerProtos.ChunkInfo b2c1 = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ContainerProtos.ChunkInfo b2c2 = buildChunk(config, 1, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ContainerProtos.ChunkInfo b3c1 = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1}));
    ContainerProtos.ChunkInfo b3c2 = buildChunk(config, 1, ByteBuffer.wrap(new byte[]{2, 3, 4}));

    // Build the expected tree proto using the test code.
    ContainerProtos.BlockMerkleTree blockTree1 = buildExpectedBlockTree(blockID1,
        Arrays.asList(buildExpectedChunkTree(b1c1), buildExpectedChunkTree(b1c2), buildExpectedChunkTree(b1c3)));
    ContainerProtos.BlockMerkleTree blockTree2 = buildExpectedBlockTree(blockID2,
        Arrays.asList(buildExpectedChunkTree(b2c1), buildExpectedChunkTree(b2c2)));
    ContainerProtos.BlockMerkleTree blockTree3 = buildExpectedBlockTree(blockID3,
        Arrays.asList(buildExpectedChunkTree(b3c1), buildExpectedChunkTree(b3c2)));
    ContainerProtos.ContainerMerkleTree expectedTree = buildExpectedContainerTree(
        Arrays.asList(blockTree1, blockTree2, blockTree3));

    // Use the ContainerMerkleTree to build the same tree.
    // Test building by adding chunks to the blocks individually and out of order.
    ContainerMerkleTreeWriter actualTree = new ContainerMerkleTreeWriter();
    // Add all of block 2 first.
    actualTree.addChunks(blockID2, true, b2c1, b2c2);
    // Then add block 1 in multiple steps wth chunks out of order.
    actualTree.addChunks(blockID1, true, b1c2);
    actualTree.addChunks(blockID1, true, b1c3, b1c1);
    // Add a duplicate chunk to block 3. It should overwrite the existing one.
    actualTree.addChunks(blockID3, true, b3c1, b3c2);
    actualTree.addChunks(blockID3, true, b3c2);

    // Ensure the trees match.
    ContainerProtos.ContainerMerkleTree actualTreeProto = actualTree.toProto();
    assertTreesSortedAndMatch(expectedTree, actualTreeProto);
  }

  /**
   * Test that the setDeletedBlock method correctly marks blocks as deleted.
   */
  @Test
  public void testSetDeletedBlock() {
    final long blockID1 = 1;
    final long blockID2 = 2;
    final long deletedChecksum = 123456789L;

    ContainerMerkleTreeWriter treeWriter = new ContainerMerkleTreeWriter();

    // Add a regular block with chunks first
    ContainerProtos.ChunkInfo chunk = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    treeWriter.addChunks(blockID1, true, chunk);

    // Add a deleted block using setDeletedBlock
    treeWriter.setDeletedBlock(blockID2, deletedChecksum);

    ContainerProtos.ContainerMerkleTree actualTree = treeWriter.toProto();

    // Verify we have 2 blocks
    assertEquals(2, actualTree.getBlockMerkleTreeCount());

    // Find and verify the regular block
    ContainerProtos.BlockMerkleTree regularBlock = actualTree.getBlockMerkleTreeList().stream()
        .filter(b -> b.getBlockID() == blockID1)
        .findFirst()
        .orElseThrow(() -> new AssertionError("Regular block not found"));

    assertEquals(blockID1, regularBlock.getBlockID());
    assertFalse(regularBlock.getDeleted());
    assertEquals(1, regularBlock.getChunkMerkleTreeCount());
    assertNotEquals(0, regularBlock.getDataChecksum());

    // Find and verify the deleted block
    ContainerProtos.BlockMerkleTree deletedBlock = actualTree.getBlockMerkleTreeList().stream()
        .filter(b -> b.getBlockID() == blockID2)
        .findFirst()
        .orElseThrow(() -> new AssertionError("Deleted block not found"));

    assertEquals(blockID2, deletedBlock.getBlockID());
    assertTrue(deletedBlock.getDeleted());
    assertEquals(deletedChecksum, deletedBlock.getDataChecksum());
    assertTrue(deletedBlock.getChunkMerkleTreeList().isEmpty(), "Deleted blocks should not have chunk merkle trees");
  }

  /**
   * setDeletedBlock should overwrite any existing block with the checksum provided.
   */
  @Test
  public void testSetDeletedBlockOverwrite() {
    final long blockID = 1;
    final long deletedChecksum = 123456789L;

    ContainerMerkleTreeWriter treeWriter = new ContainerMerkleTreeWriter();

    // Add a regular block with chunks first
    ContainerProtos.ChunkInfo chunk = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    treeWriter.addChunks(blockID, true, chunk);
    // Overwrite the block with a deleted entry that has a different checksum.
    treeWriter.setDeletedBlock(blockID, deletedChecksum);

    ContainerProtos.ContainerMerkleTree actualTree = treeWriter.toProto();
    assertEquals(1, actualTree.getBlockMerkleTreeCount());

    // Find and verify the regular block
    ContainerProtos.BlockMerkleTree deletedBlock = actualTree.getBlockMerkleTreeList().stream()
        .filter(b -> b.getBlockID() == blockID)
        .findFirst()
        .orElseThrow(() -> new AssertionError("block not found"));

    assertEquals(blockID, deletedBlock.getBlockID());
    assertFalse(deletedBlock.getDeleted());
    assertTrue(deletedBlock.getChunkMerkleTreeList().isEmpty());
    assertEquals(deletedChecksum, deletedBlock.getDataChecksum());
  }

  /**
   * Test that a {@link ContainerMerkleTreeWriter} built from a {@link ContainerProtos.ContainerMerkleTree} will
   * produce an identical proto as the input when it is written again. This test covers both regular blocks with
   * chunks, empty blocks, and deleted blocks to ensure all block types are properly preserved during conversion.
   */
  @Test
  public void testProtoToWriterConversion() {
    final long blockID1 = 1;
    final long blockID2 = 2;
    final long blockID3 = 3;
    final long blockID4 = 4;
    final long blockID5 = 5;
    final long deletedBlockChecksum = 123456L;
    ContainerProtos.ChunkInfo b1c1 = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ContainerProtos.ChunkInfo b1c2 = buildChunk(config, 1, ByteBuffer.wrap(new byte[]{1, 2}));
    ContainerProtos.ChunkInfo b1c3 = buildChunk(config, 2, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ContainerProtos.ChunkInfo b2c1 = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ContainerProtos.ChunkInfo b2c2 = buildChunk(config, 1, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ContainerProtos.BlockMerkleTree blockTree1 = buildExpectedBlockTree(blockID1,
        Arrays.asList(buildExpectedChunkTree(b1c1), buildExpectedChunkTree(b1c2), buildExpectedChunkTree(b1c3)));
    ContainerProtos.BlockMerkleTree blockTree2 = buildExpectedBlockTree(blockID2,
        Arrays.asList(buildExpectedChunkTree(b2c1), buildExpectedChunkTree(b2c2)));
    // Test that an empty block is preserved during tree conversion.
    ContainerProtos.BlockMerkleTree blockTree3 = buildExpectedBlockTree(blockID3, Collections.emptyList());
    // Test that a deleted block is preserved during tree conversion.
    ContainerProtos.BlockMerkleTree blockTree4 = buildExpectedDeletedBlockTree(blockID4, deletedBlockChecksum);
    ContainerProtos.ContainerMerkleTree expectedTree = buildExpectedContainerTree(
        Arrays.asList(blockTree1, blockTree2, blockTree3, blockTree4));

    ContainerMerkleTreeWriter treeWriter = new ContainerMerkleTreeWriter(expectedTree);
    ContainerProtos.ContainerMerkleTree actualTree = treeWriter.toProto();
    assertTreesSortedAndMatch(expectedTree, actualTree);

    // Modifying the tree writer created from the proto should also succeed.
    ContainerProtos.ChunkInfo b3c1 = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1}));
    treeWriter.addChunks(blockID3, false, b3c1);
    treeWriter.addBlock(blockID5);

    blockTree3 = buildExpectedBlockTree(blockID3, Collections.singletonList(buildExpectedChunkTree(b3c1, false)));
    ContainerProtos.BlockMerkleTree blockTree5 = buildExpectedBlockTree(blockID5, Collections.emptyList());
    ContainerProtos.ContainerMerkleTree expectedUpdatedTree = buildExpectedContainerTree(
        Arrays.asList(blockTree1, blockTree2, blockTree3, blockTree4, blockTree5));

    assertTreesSortedAndMatch(expectedUpdatedTree, treeWriter.toProto());
  }

  private ContainerProtos.ContainerMerkleTree buildExpectedContainerTree(List<ContainerProtos.BlockMerkleTree> blocks) {
    return ContainerProtos.ContainerMerkleTree.newBuilder()
        .addAllBlockMerkleTree(blocks)
        .setDataChecksum(computeExpectedChecksum(
            blocks.stream()
                .map(ContainerProtos.BlockMerkleTree::getDataChecksum)
                .collect(Collectors.toList())))
        .build();
  }

  private ContainerProtos.BlockMerkleTree buildExpectedBlockTree(long blockID,
      List<ContainerProtos.ChunkMerkleTree> chunks) {
    List<Long> itemsToChecksum = chunks.stream().map(ContainerProtos.ChunkMerkleTree::getDataChecksum)
        .collect(Collectors.toList());
    itemsToChecksum.add(0, blockID);
    return ContainerProtos.BlockMerkleTree.newBuilder()
        .setBlockID(blockID)
        .setDataChecksum(computeExpectedChecksum(itemsToChecksum))
        .addAllChunkMerkleTree(chunks)
        .setDeleted(false)
        .build();
  }

  private ContainerProtos.BlockMerkleTree buildExpectedDeletedBlockTree(long blockID, long dataChecksum) {
    return ContainerProtos.BlockMerkleTree.newBuilder()
        .setBlockID(blockID)
        .setDataChecksum(dataChecksum)
        .setDeleted(true)
        // Deleted blocks should not have chunk merkle trees
        .build();
  }

  private ContainerProtos.ChunkMerkleTree buildExpectedChunkTree(ContainerProtos.ChunkInfo chunk) {
    return buildExpectedChunkTree(chunk, true);
  }

  private ContainerProtos.ChunkMerkleTree buildExpectedChunkTree(ContainerProtos.ChunkInfo chunk,
      boolean checksumMatches) {
    return ContainerProtos.ChunkMerkleTree.newBuilder()
        .setOffset(chunk.getOffset())
        .setLength(chunk.getLen())
        .setDataChecksum(computeExpectedChunkChecksum(chunk.getChecksumData().getChecksumsList()))
        .setChecksumMatches(checksumMatches)
        .build();
  }

  private long computeExpectedChecksum(List<Long> checksums) {
    // Use the same checksum implementation as the tree writer under test.
    ChecksumByteBuffer checksumImpl = ContainerMerkleTreeWriter.CHECKSUM_BUFFER_SUPPLIER.get();
    ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES * checksums.size());
    checksums.forEach(longBuffer::putLong);
    longBuffer.flip();
    checksumImpl.update(longBuffer);
    return checksumImpl.getValue();
  }

  private long computeExpectedChunkChecksum(List<ByteString> checksums) {
    // Use the same checksum implementation as the tree writer under test.
    ChecksumByteBuffer checksumImpl = ContainerMerkleTreeWriter.CHECKSUM_BUFFER_SUPPLIER.get();
    checksums.forEach(b -> checksumImpl.update(b.asReadOnlyByteBuffer()));
    return checksumImpl.getValue();
  }
}
