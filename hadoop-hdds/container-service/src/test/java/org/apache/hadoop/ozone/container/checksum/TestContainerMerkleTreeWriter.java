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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
    ContainerProtos.BlockMerkleTree blockTree = buildExpectedBlockTree(blockID, chunkTree);
    ContainerProtos.ContainerMerkleTree expectedTree = buildExpectedContainerTree(blockTree);

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
        buildExpectedChunkTree(chunk1), buildExpectedChunkTree(chunk3));
    ContainerProtos.ContainerMerkleTree expectedTree = buildExpectedContainerTree(blockTree);

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
    ContainerProtos.BlockMerkleTree blockTree = buildExpectedBlockTree(blockID);
    ContainerProtos.ContainerMerkleTree expectedTree = buildExpectedContainerTree(blockTree);

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
        buildExpectedChunkTree(chunk1));
    ContainerProtos.ContainerMerkleTree expectedTree = buildExpectedContainerTree(blockTree);

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
        buildExpectedChunkTree(b1c1), buildExpectedChunkTree(b1c2));
    ContainerProtos.BlockMerkleTree blockTree3 = buildExpectedBlockTree(blockID3,
        buildExpectedChunkTree(b3c1), buildExpectedChunkTree(b3c2));
    ContainerProtos.ContainerMerkleTree expectedTree = buildExpectedContainerTree(blockTree1, blockTree3);

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
        buildExpectedChunkTree(b1c1), buildExpectedChunkTree(b1c2), buildExpectedChunkTree(b1c3));
    ContainerProtos.BlockMerkleTree blockTree2 = buildExpectedBlockTree(blockID2,
        buildExpectedChunkTree(b2c1), buildExpectedChunkTree(b2c2));
    ContainerProtos.BlockMerkleTree blockTree3 = buildExpectedBlockTree(blockID3,
        buildExpectedChunkTree(b3c1), buildExpectedChunkTree(b3c2));
    ContainerProtos.ContainerMerkleTree expectedTree = buildExpectedContainerTree(blockTree1, blockTree2, blockTree3);

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

    // Find and verify the overwritten deleted block
    ContainerProtos.BlockMerkleTree deletedBlock = actualTree.getBlockMerkleTreeList().stream()
        .filter(b -> b.getBlockID() == blockID)
        .findFirst()
        .orElseThrow(() -> new AssertionError("block not found"));

    assertEquals(blockID, deletedBlock.getBlockID());
    assertTrue(deletedBlock.getDeleted());
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
        buildExpectedChunkTree(b1c1), buildExpectedChunkTree(b1c2), buildExpectedChunkTree(b1c3));
    ContainerProtos.BlockMerkleTree blockTree2 = buildExpectedBlockTree(blockID2,
        buildExpectedChunkTree(b2c1), buildExpectedChunkTree(b2c2));
    // Test that an empty block is preserved during tree conversion.
    ContainerProtos.BlockMerkleTree blockTree3 = buildExpectedBlockTree(blockID3);
    // Test that a deleted block is preserved during tree conversion.
    ContainerProtos.BlockMerkleTree blockTree4 = buildExpectedDeletedBlockTree(blockID4, deletedBlockChecksum);
    ContainerProtos.ContainerMerkleTree expectedTree = buildExpectedContainerTree(blockTree1,
        blockTree2, blockTree3, blockTree4);

    ContainerMerkleTreeWriter treeWriter = new ContainerMerkleTreeWriter(expectedTree);
    ContainerProtos.ContainerMerkleTree actualTree = treeWriter.toProto();
    assertTreesSortedAndMatch(expectedTree, actualTree);

    // Modifying the tree writer created from the proto should also succeed.
    ContainerProtos.ChunkInfo b3c1 = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1}));
    treeWriter.addChunks(blockID3, false, b3c1);
    treeWriter.addBlock(blockID5);

    blockTree3 = buildExpectedBlockTree(blockID3, buildExpectedChunkTree(b3c1, false));
    ContainerProtos.BlockMerkleTree blockTree5 = buildExpectedBlockTree(blockID5);
    ContainerProtos.ContainerMerkleTree expectedUpdatedTree = buildExpectedContainerTree(blockTree1,
        blockTree2, blockTree3, blockTree4, blockTree5);

    assertTreesSortedAndMatch(expectedUpdatedTree, treeWriter.toProto());
  }

  /**
   * Tests adding deleted blocks to an empty tree for cases where the final tree checksum should and should not be
   * computed.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAddDeletedBlocksToEmptyTree(boolean computeChecksum) {
    final long containerId = 1L;
    final long blockID1 = 1L;
    final long blockID2 = 2L;

    ContainerMerkleTreeWriter treeWriter = new ContainerMerkleTreeWriter();

    // Create deleted blocks with chunks - always use 2 blocks
    List<BlockData> deletedBlocks = Arrays.asList(
        ContainerMerkleTreeTestUtils.buildBlockData(config, containerId, blockID1),
        ContainerMerkleTreeTestUtils.buildBlockData(config, containerId, blockID2)
    );

    ContainerProtos.ContainerMerkleTree result = treeWriter.addDeletedBlocks(deletedBlocks, computeChecksum);

    // Verify container has 2 blocks
    assertEquals(2, result.getBlockMerkleTreeCount());

    // Verify both blocks are marked as deleted with no chunks
    ContainerProtos.BlockMerkleTree block1 = result.getBlockMerkleTreeList().stream()
        .filter(b -> b.getBlockID() == blockID1)
        .findFirst()
        .orElseThrow(() -> new AssertionError("Block 1 not found"));
    assertTrue(block1.getDeleted());
    assertTrue(block1.getChunkMerkleTreeList().isEmpty());

    ContainerProtos.BlockMerkleTree block2 = result.getBlockMerkleTreeList().stream()
        .filter(b -> b.getBlockID() == blockID2)
        .findFirst()
        .orElseThrow(() -> new AssertionError("Block 2 not found"));
    assertTrue(block2.getDeleted());
    assertTrue(block2.getChunkMerkleTreeList().isEmpty());

    if (computeChecksum) {
      assertTrue(result.hasDataChecksum());
      assertNotEquals(0, result.getDataChecksum());
      assertTrue(block1.hasDataChecksum());
      assertNotEquals(0, block1.getDataChecksum());
      assertTrue(block2.hasDataChecksum());
      assertNotEquals(0, block2.getDataChecksum());
    } else {
      // Top level tree checksum should not be populated, but individual blocks will have checksums.
      assertFalse(result.hasDataChecksum());
      assertTrue(block1.hasDataChecksum());
      assertTrue(block2.hasDataChecksum());
    }
  }

  /**
   * Test adding deleted blocks to a tree that already has data, including overwriting existing blocks.
   */
  @Test
  public void testAddDeletedBlocksWithExistingData() {
    final long containerId = 1L;
    final long blockID1 = 1L;
    final long blockID2 = 2L;
    final long blockID3 = 3L;

    ContainerMerkleTreeWriter treeWriter = new ContainerMerkleTreeWriter();

    // Add some existing live blocks
    ContainerProtos.ChunkInfo chunk1 = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ContainerProtos.ChunkInfo chunk2 = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{4, 5, 6}));
    treeWriter.addChunks(blockID1, true, chunk1); // This will be overwritten
    treeWriter.addChunks(blockID2, true, chunk2); // This will remain

    // Create deleted blocks - one overlapping, one new
    List<BlockData> deletedBlocks = Arrays.asList(
        ContainerMerkleTreeTestUtils.buildBlockData(config, containerId, blockID1), // Overwrite existing block
        ContainerMerkleTreeTestUtils.buildBlockData(config, containerId, blockID3)  // New deleted block
    );

    ContainerProtos.ContainerMerkleTree result = treeWriter.addDeletedBlocks(deletedBlocks, true);

    // Verify we have 3 blocks total
    assertEquals(3, result.getBlockMerkleTreeCount());

    // Verify block1 was overwritten and is now deleted
    ContainerProtos.BlockMerkleTree block1 = result.getBlockMerkleTreeList().stream()
        .filter(b -> b.getBlockID() == blockID1)
        .findFirst()
        .orElseThrow(() -> new AssertionError("Block 1 not found"));

    assertTrue(block1.getDeleted());
    assertTrue(block1.getChunkMerkleTreeList().isEmpty());

    // Verify block2 remains live with its chunks
    ContainerProtos.BlockMerkleTree block2 = result.getBlockMerkleTreeList().stream()
        .filter(b -> b.getBlockID() == blockID2)
        .findFirst()
        .orElseThrow(() -> new AssertionError("Block 2 not found"));

    assertFalse(block2.getDeleted());
    assertEquals(1, block2.getChunkMerkleTreeCount());

    // Verify block3 is the new deleted block
    ContainerProtos.BlockMerkleTree block3 = result.getBlockMerkleTreeList().stream()
        .filter(b -> b.getBlockID() == blockID3)
        .findFirst()
        .orElseThrow(() -> new AssertionError("Block 3 not found"));

    assertTrue(block3.getDeleted());
    assertTrue(block3.getChunkMerkleTreeList().isEmpty());
  }

  /**
   * Test that deleted blocks take precedence when the same block exists in both live and deleted states.
   */
  @Test
  public void testDeletedBlocksTakePrecedence() {
    final long containerId = 1L;
    final long blockID = 1L;

    ContainerMerkleTreeWriter treeWriter = new ContainerMerkleTreeWriter();

    // First add a live block
    ContainerProtos.ChunkInfo chunk = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    treeWriter.addChunks(blockID, true, chunk);

    // Get the checksum of the live block
    ContainerProtos.ContainerMerkleTree initialTree = treeWriter.toProto();
    long liveBlockChecksum = initialTree.getBlockMerkleTree(0).getDataChecksum();

    // Now add the same block as deleted - it should overwrite
    List<BlockData> deletedBlocks = Collections.singletonList(
        ContainerMerkleTreeTestUtils.buildBlockData(config, containerId, blockID)
    );

    ContainerProtos.ContainerMerkleTree result = treeWriter.addDeletedBlocks(deletedBlocks, true);

    assertEquals(1, result.getBlockMerkleTreeCount());

    ContainerProtos.BlockMerkleTree finalBlock = result.getBlockMerkleTree(0);
    assertTrue(finalBlock.getDeleted());
    assertTrue(finalBlock.getChunkMerkleTreeList().isEmpty());

    // The checksum should be different since it's computed from the deleted block's data
    assertNotEquals(liveBlockChecksum, finalBlock.getDataChecksum());
  }

  /**
   * If both trees contain a block and ours is live while existing is deleted,
   * the deleted one supersedes and its checksum should be used.
   */
  @Test
  public void testUpdateConflictExistingDeleted() {
    final long blockID = 1L;

    // Our writer has a live block
    ContainerMerkleTreeWriter writer = new ContainerMerkleTreeWriter();
    ContainerProtos.ChunkInfo chunk = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    writer.addChunks(blockID, true, chunk);

    // Existing tree marks the same block as deleted with a specific checksum
    final long deletedChecksum = 987654321L;
    ContainerProtos.BlockMerkleTree existingDeleted = buildExpectedDeletedBlockTree(blockID, deletedChecksum);
    ContainerProtos.ContainerMerkleTree existingTree = ContainerProtos.ContainerMerkleTree.newBuilder()
        .addBlockMerkleTree(existingDeleted)
        .build();

    ContainerProtos.ContainerMerkleTree result = writer.update(existingTree);

    // Expect the deleted state from existing to override the live state in writer
    ContainerProtos.ContainerMerkleTree expected = buildExpectedContainerTree(
        buildExpectedDeletedBlockTree(blockID, deletedChecksum));
    assertTreesSortedAndMatch(expected, result);
  }

  /**
   * If both trees contain the same live block, our writer's value wins.
   */
  @Test
  public void testUpdateConflictBothLive() {
    final long blockID = 1L;

    // Our writer live block with one set of chunks
    ContainerMerkleTreeWriter writer = new ContainerMerkleTreeWriter();
    ContainerProtos.ChunkInfo ourChunk = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{10, 20, 30}));
    writer.addChunks(blockID, true, ourChunk);

    // Existing tree has same blockID but different content
    ContainerProtos.ChunkInfo existingChunk = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{7, 8, 9}));
    ContainerProtos.BlockMerkleTree existingLive = buildExpectedBlockTree(blockID,
        buildExpectedChunkTree(existingChunk));
    ContainerProtos.ContainerMerkleTree existingTree = ContainerProtos.ContainerMerkleTree.newBuilder()
        .addBlockMerkleTree(existingLive)
        .build();

    ContainerProtos.ContainerMerkleTree result = writer.update(existingTree);

    // Expect our writer's live block to be preserved
    ContainerProtos.ContainerMerkleTree expected = buildExpectedContainerTree(
        buildExpectedBlockTree(blockID, buildExpectedChunkTree(ourChunk)));
    assertTreesSortedAndMatch(expected, result);
  }

  /**
   * If our writer has a deleted block and the existing tree has it as live,
   * our deleted value wins since we have the latest information.
   */
  @Test
  public void testUpdateConflictExistingLive() {
    final long blockID = 3L;

    // Our writer marks the block as deleted
    final long ourDeletedChecksum = 12345L;
    ContainerMerkleTreeWriter writer = new ContainerMerkleTreeWriter();
    writer.setDeletedBlock(blockID, ourDeletedChecksum);

    // Existing tree has a live version of the block
    ContainerProtos.ChunkInfo existingChunk = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{4, 5, 6}));
    ContainerProtos.BlockMerkleTree existingLive = buildExpectedBlockTree(blockID,
        buildExpectedChunkTree(existingChunk));
    ContainerProtos.ContainerMerkleTree existingTree = ContainerProtos.ContainerMerkleTree.newBuilder()
        .addBlockMerkleTree(existingLive)
        .build();

    ContainerProtos.ContainerMerkleTree result = writer.update(existingTree);

    // Expect our deleted entry to be preserved
    ContainerProtos.ContainerMerkleTree expected = buildExpectedContainerTree(
        buildExpectedDeletedBlockTree(blockID, ourDeletedChecksum));
    assertTreesSortedAndMatch(expected, result);
  }

  /**
   * If both the writer's tree and existing tree have deleted versions of a block, our writer's checksum wins.
   */
  @Test
  public void testUpdateConflictBothDeleted() {
    final long blockID = 4L;
    final long ourDeletedChecksum = 111L;
    final long existingDeletedChecksum = 222L;

    ContainerMerkleTreeWriter writer = new ContainerMerkleTreeWriter();
    writer.setDeletedBlock(blockID, ourDeletedChecksum);

    ContainerProtos.BlockMerkleTree existingDeleted = buildExpectedDeletedBlockTree(blockID, existingDeletedChecksum);
    ContainerProtos.ContainerMerkleTree existingTree = ContainerProtos.ContainerMerkleTree.newBuilder()
        .addBlockMerkleTree(existingDeleted)
        .build();

    ContainerProtos.ContainerMerkleTree result = writer.update(existingTree);

    ContainerProtos.ContainerMerkleTree expected = buildExpectedContainerTree(
        buildExpectedDeletedBlockTree(blockID, ourDeletedChecksum));
    assertTreesSortedAndMatch(expected, result);
  }

  /**
   * Merge the existing tree with the tree writer by:
   * - including deleted blocks from the existing tree into our tree writer.
   * - ignoring live blocks from the existing tree and overwriting them with our tree writer.
   */
  @Test
  public void testUpdateMergesTrees() {
    final long existingLiveBlockID = 5L;
    final long existingDeletedBlockID = 6L;
    final long existingDeletedChecksum = 555L;
    final long ourLiveBlockID = 7L;
    final long ourDeletedBlockID = 8L;
    final long ourDeletedChecksum = 444L;

    // Our writer contains a live block not present in the existing tree
    ContainerMerkleTreeWriter writer = new ContainerMerkleTreeWriter();
    ContainerProtos.ChunkInfo ourLiveChunk = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{9, 9, 9}));
    writer.addChunks(ourLiveBlockID, true, ourLiveChunk);
    // Our writer also includes a deleted block not present in the existing tree
    writer.setDeletedBlock(ourDeletedBlockID, ourDeletedChecksum);

    // Existing tree contains a deleted block (should be included) and a live block (should be ignored)
    ContainerProtos.BlockMerkleTree existingDeleted = buildExpectedDeletedBlockTree(existingDeletedBlockID,
        existingDeletedChecksum);
    ContainerProtos.ChunkInfo existingLiveChunk = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{7, 7, 7}));
    ContainerProtos.BlockMerkleTree existingLiveBlock = buildExpectedBlockTree(existingLiveBlockID,
        buildExpectedChunkTree(existingLiveChunk));
    ContainerProtos.ContainerMerkleTree existingTree = ContainerProtos.ContainerMerkleTree.newBuilder()
        .addBlockMerkleTree(existingDeleted)
        .addBlockMerkleTree(existingLiveBlock)
        .build();

    ContainerProtos.ContainerMerkleTree result = writer.update(existingTree);

    // Expect union: our live block + existing deleted block, but not the existing live block
    ContainerProtos.ContainerMerkleTree expected = buildExpectedContainerTree(
        buildExpectedDeletedBlockTree(existingDeletedBlockID, existingDeletedChecksum),
        buildExpectedBlockTree(ourLiveBlockID, buildExpectedChunkTree(ourLiveChunk)),
        buildExpectedDeletedBlockTree(ourDeletedBlockID, ourDeletedChecksum));
    assertTreesSortedAndMatch(expected, result);
  }

  private ContainerProtos.ContainerMerkleTree buildExpectedContainerTree(
      ContainerProtos.BlockMerkleTree... blocks) {
    List<ContainerProtos.BlockMerkleTree> blockList = Arrays.asList(blocks);
    return ContainerProtos.ContainerMerkleTree.newBuilder()
        .addAllBlockMerkleTree(blockList)
        .setDataChecksum(computeExpectedChecksum(
            blockList.stream()
                .map(ContainerProtos.BlockMerkleTree::getDataChecksum)
                .collect(Collectors.toList())))
        .build();
  }

  private ContainerProtos.BlockMerkleTree buildExpectedBlockTree(long blockID,
      ContainerProtos.ChunkMerkleTree... chunks) {
    List<ContainerProtos.ChunkMerkleTree> chunkList = Arrays.asList(chunks);
    List<Long> itemsToChecksum = chunkList.stream().map(ContainerProtos.ChunkMerkleTree::getDataChecksum)
        .collect(Collectors.toList());
    itemsToChecksum.add(0, blockID);
    return ContainerProtos.BlockMerkleTree.newBuilder()
        .setBlockID(blockID)
        .setDataChecksum(computeExpectedChecksum(itemsToChecksum))
        .addAllChunkMerkleTree(chunkList)
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
