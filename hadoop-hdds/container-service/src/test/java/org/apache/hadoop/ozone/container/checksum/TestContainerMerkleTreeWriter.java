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
   * Test that a {@link ContainerMerkleTreeWriter} built from a {@link ContainerProtos.ContainerMerkleTree} will
   * write produce an identical proto as the input when it is written again.
   */
  @Test
  public void testProtoToWriterConversion() {
    final long blockID1 = 1;
    final long blockID2 = 2;
    final long blockID3 = 3;
    final long blockID4 = 4;
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
    ContainerProtos.ContainerMerkleTree expectedTree = buildExpectedContainerTree(
        Arrays.asList(blockTree1, blockTree2, blockTree3));

    ContainerMerkleTreeWriter treeWriter = new ContainerMerkleTreeWriter(expectedTree);
    assertTreesSortedAndMatch(expectedTree, treeWriter.toProto());

    // Modifying the tree writer created from the proto should also succeed.
    ContainerProtos.ChunkInfo b3c1 = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1}));
    treeWriter.addChunks(blockID3, false, b3c1);
    treeWriter.addBlock(blockID4);

    blockTree3 = buildExpectedBlockTree(blockID3, Collections.singletonList(buildExpectedChunkTree(b3c1, false)));
    ContainerProtos.BlockMerkleTree blockTree4 = buildExpectedBlockTree(blockID4, Collections.emptyList());
    ContainerProtos.ContainerMerkleTree expectedUpdatedTree = buildExpectedContainerTree(
        Arrays.asList(blockTree1, blockTree2, blockTree3, blockTree4));

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
    return ContainerProtos.BlockMerkleTree.newBuilder()
        .setBlockID(blockID)
        .setDataChecksum(computeExpectedChecksum(
            chunks.stream()
                .map(ContainerProtos.ChunkMerkleTree::getDataChecksum)
                .collect(Collectors.toList())))
        .addAllChunkMerkleTree(chunks)
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
