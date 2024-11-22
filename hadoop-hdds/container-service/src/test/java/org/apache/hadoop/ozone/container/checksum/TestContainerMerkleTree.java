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

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.assertTreesSortedAndMatch;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.buildChunk;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TestContainerMerkleTree {
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
    ContainerMerkleTree tree = new ContainerMerkleTree();
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

    // Use the ContainerMerkleTree to build the same tree.
    ContainerMerkleTree actualTree = new ContainerMerkleTree();
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
    assertNotEquals(0, actualBlockTree.getBlockChecksum());

    ContainerProtos.ChunkMerkleTree actualChunkTree = actualBlockTree.getChunkMerkleTree(0);
    assertEquals(0, actualChunkTree.getOffset());
    assertEquals(chunkSize, actualChunkTree.getLength());
    assertNotEquals(0, actualChunkTree.getChunkChecksum());
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
    ContainerMerkleTree actualTree = new ContainerMerkleTree();
    actualTree.addChunks(blockID, true, chunk1, chunk3);

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
    ContainerMerkleTree actualTree = new ContainerMerkleTree();
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
    ContainerMerkleTree actualTree = new ContainerMerkleTree();
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

  private ContainerProtos.ContainerMerkleTree buildExpectedContainerTree(List<ContainerProtos.BlockMerkleTree> blocks) {
    return ContainerProtos.ContainerMerkleTree.newBuilder()
        .addAllBlockMerkleTree(blocks)
        .setDataChecksum(computeExpectedChecksum(
            blocks.stream()
                .map(ContainerProtos.BlockMerkleTree::getBlockChecksum)
                .collect(Collectors.toList())))
        .build();
  }

  private ContainerProtos.BlockMerkleTree buildExpectedBlockTree(long blockID,
      List<ContainerProtos.ChunkMerkleTree> chunks) {
    return ContainerProtos.BlockMerkleTree.newBuilder()
        .setBlockID(blockID)
        .setBlockChecksum(computeExpectedChecksum(
            chunks.stream()
                .map(ContainerProtos.ChunkMerkleTree::getChunkChecksum)
                .collect(Collectors.toList())))
        .addAllChunkMerkleTree(chunks)
        .build();
  }

  private ContainerProtos.ChunkMerkleTree buildExpectedChunkTree(ContainerProtos.ChunkInfo chunk) {
    return ContainerProtos.ChunkMerkleTree.newBuilder()
        .setOffset(chunk.getOffset())
        .setLength(chunk.getLen())
        .setChunkChecksum(computeExpectedChunkChecksum(chunk.getChecksumData().getChecksumsList()))
        .build();
  }

  private long computeExpectedChecksum(List<Long> checksums) {
    CRC32 crc32 = new CRC32();
    ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES * checksums.size());
    checksums.forEach(longBuffer::putLong);
    longBuffer.flip();
    crc32.update(longBuffer);
    return crc32.getValue();
  }

  private long computeExpectedChunkChecksum(List<ByteString> checksums) {
    CRC32 crc32 = new CRC32();
    checksums.forEach(b -> crc32.update(b.asReadOnlyByteBuffer()));
    return crc32.getValue();
  }
}
