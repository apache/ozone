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

import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.assertContainerDiffMatch;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.buildTestTree;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.buildTestTreeWithMismatches;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.getDeletedBlockData;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.updateTreeProto;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests For computing the diff of two container merkle trees.
 */
public class TestContainerDiff {
  private static final long CONTAINER_ID = 1L;
  @TempDir
  private File testDir;
  private KeyValueContainerData container;
  private ContainerChecksumTreeManager checksumManager;
  private ConfigurationSource config;

  @BeforeEach
  public void init() {
    container = mock(KeyValueContainerData.class);
    when(container.getContainerID()).thenReturn(CONTAINER_ID);
    when(container.getMetadataPath()).thenReturn(testDir.getAbsolutePath());
    checksumManager = new ContainerChecksumTreeManager(new OzoneConfiguration());
    config = new OzoneConfiguration();
  }

  @AfterEach
  public void cleanup() throws IOException {
    // Unregister metrics for the next test run.
    if (checksumManager != null) {
      checksumManager.stop();
    }
  }

  /**
   * The number of mismatched to be introduced in the container diff. The arguments are
   * number of missing blocks, number of missing chunks, number of corrupt chunks.
   */
  public static Stream<Arguments> getContainerDiffMismatches() {
    return Stream.of(
        Arguments.of(0, 0, 1),
        Arguments.of(0, 1, 0),
        Arguments.of(1, 0, 0),
        Arguments.of(1, 2, 3),
        Arguments.of(2, 3, 1),
        Arguments.of(3, 1, 2),
        Arguments.of(2, 2, 3),
        Arguments.of(3, 2, 2),
        Arguments.of(2, 1, 4),
        Arguments.of(2, 3, 4),
        Arguments.of(1, 2, 4),
        Arguments.of(3, 3, 3),
        Arguments.of(3, 3, 0),
        Arguments.of(3, 0, 3),
        Arguments.of(0, 3, 3));
  }

  /**
   * Test if our merkle tree has missing blocks and chunks. If our tree has mismatches with respect to the
   * peer then we need to include that mismatch in the container diff.
   */
  @ParameterizedTest(name = "Missing blocks: {0}, Missing chunks: {1}, Corrupt chunks: {2}")
  @MethodSource("getContainerDiffMismatches")
  public void testContainerDiffWithMismatches(int numMissingBlock, int numMissingChunk,
                                              int numCorruptChunk) throws Exception {
    ContainerMerkleTreeWriter peerMerkleTree = buildTestTree(config);
    Pair<ContainerProtos.ContainerMerkleTree, ContainerDiffReport> buildResult =
        buildTestTreeWithMismatches(peerMerkleTree, numMissingBlock, numMissingChunk, numCorruptChunk);
    ContainerDiffReport expectedDiff = buildResult.getRight();
    ContainerProtos.ContainerMerkleTree ourMerkleTree = buildResult.getLeft();
    updateTreeProto(container, ourMerkleTree);
    ContainerProtos.ContainerChecksumInfo peerChecksumInfo = ContainerProtos.ContainerChecksumInfo.newBuilder()
        .setContainerID(container.getContainerID())
        .setContainerMerkleTree(peerMerkleTree.toProto()).build();
    ContainerProtos.ContainerChecksumInfo checksumInfo = checksumManager.read(container);
    ContainerDiffReport diff = checksumManager.diff(checksumInfo, peerChecksumInfo);
    assertTrue(checksumManager.getMetrics().getMerkleTreeDiffLatencyNS().lastStat().total() > 0);
    assertContainerDiffMatch(expectedDiff, diff);
    assertEquals(1, checksumManager.getMetrics().getRepairContainerDiffs());
    assertEquals(numMissingBlock, checksumManager.getMetrics().getMissingBlocksIdentified());
    assertEquals(numMissingChunk, checksumManager.getMetrics().getMissingChunksIdentified());
    assertEquals(numCorruptChunk, checksumManager.getMetrics().getCorruptChunksIdentified());
  }

  /**
   * Test if a peer which has missing blocks and chunks affects our container diff. If the peer tree has mismatches
   * with respect to our merkle tree then we should not include that mismatch in the container diff.
   * The ContainerDiff generated by the peer when it reconciles with our merkle tree will capture that mismatch.
   */
  @ParameterizedTest(name = "Missing blocks: {0}, Missing chunks: {1}, Corrupt chunks: {2}")
  @MethodSource("getContainerDiffMismatches")
  public void testPeerWithMismatchesHasNoDiff(int numMissingBlock, int numMissingChunk,
                                              int numCorruptChunk) throws Exception {
    ContainerMerkleTreeWriter ourMerkleTree = buildTestTree(config);
    Pair<ContainerProtos.ContainerMerkleTree, ContainerDiffReport> buildResult =
        buildTestTreeWithMismatches(ourMerkleTree, numMissingBlock, numMissingChunk, numCorruptChunk);
    ContainerProtos.ContainerMerkleTree peerMerkleTree =  buildResult.getLeft();
    checksumManager.updateTree(container, ourMerkleTree);
    ContainerProtos.ContainerChecksumInfo peerChecksumInfo = ContainerProtos.ContainerChecksumInfo.newBuilder()
        .setContainerID(container.getContainerID())
        .setContainerMerkleTree(peerMerkleTree).build();
    ContainerProtos.ContainerChecksumInfo checksumInfo = checksumManager.read(container);
    ContainerDiffReport diff = checksumManager.diff(checksumInfo, peerChecksumInfo);
    assertFalse(diff.needsRepair());
    assertEquals(checksumManager.getMetrics().getNoRepairContainerDiffs(), 1);
    assertEquals(0, checksumManager.getMetrics().getMissingBlocksIdentified());
    assertEquals(0, checksumManager.getMetrics().getMissingChunksIdentified());
    assertEquals(0, checksumManager.getMetrics().getCorruptChunksIdentified());
  }

  @Test
  public void testContainerWithNoDiff() throws Exception {
    ContainerMerkleTreeWriter ourMerkleTree = buildTestTree(config);
    ContainerMerkleTreeWriter peerMerkleTree = buildTestTree(config);
    checksumManager.updateTree(container, ourMerkleTree);
    ContainerProtos.ContainerChecksumInfo peerChecksumInfo = ContainerProtos.ContainerChecksumInfo.newBuilder()
        .setContainerID(container.getContainerID())
        .setContainerMerkleTree(peerMerkleTree.toProto()).build();
    ContainerProtos.ContainerChecksumInfo checksumInfo = checksumManager.read(container);
    ContainerDiffReport diff = checksumManager.diff(checksumInfo, peerChecksumInfo);
    assertTrue(checksumManager.getMetrics().getMerkleTreeDiffLatencyNS().lastStat().total() > 0);
    assertFalse(diff.needsRepair());
    assertEquals(checksumManager.getMetrics().getNoRepairContainerDiffs(), 1);
  }

  @Test
  void testContainerDiffWithBlockDeletionInPeer() throws Exception {
    // Setup deleted blocks only in the peer container tree.
    List<Long> deletedBlockIDs = Arrays.asList(6L, 7L, 8L, 9L, 10L);
    ContainerProtos.ContainerMerkleTree peerMerkleTree = buildTestTree(config, 10, 6, 7, 8, 9, 10);
    // Create only 5 blocks in our tree. The peer has 5 more blocks that it has deleted.
    ContainerMerkleTreeWriter dummy = buildTestTree(config, 5);
    // Introduce block corruption in our merkle tree.
    ContainerProtos.ContainerMerkleTree ourMerkleTree = buildTestTreeWithMismatches(dummy, 3, 3, 3).getLeft();

    ContainerProtos.ContainerChecksumInfo.Builder peerChecksumInfoBuilder = ContainerProtos.ContainerChecksumInfo
        .newBuilder()
        .setContainerMerkleTree(peerMerkleTree).setContainerID(CONTAINER_ID);

    updateTreeProto(container, ourMerkleTree);

    ContainerProtos.ContainerChecksumInfo peerChecksumInfo = peerChecksumInfoBuilder.build();
    ContainerProtos.ContainerChecksumInfo checksumInfo = checksumManager.read(container);
    ContainerDiffReport containerDiff = checksumManager.diff(checksumInfo, peerChecksumInfo);
    // The diff should not have any missing block/missing chunk/corrupt chunks as the blocks are deleted
    // in peer merkle tree.
    assertFalse(containerDiff.getMissingBlocks().isEmpty());
    // Missing block does not contain the deleted blocks 6L to 10L
    assertFalse(containerDiff.getMissingBlocks().stream().anyMatch(blockTree ->
        deletedBlockIDs.contains(blockTree.getBlockID())));
    assertFalse(containerDiff.getMissingBlocks().isEmpty());
    assertFalse(containerDiff.getMissingChunks().isEmpty());

    // Recreate peer checksum info without deleted blocks.
    ContainerProtos.ContainerChecksumInfo peerInfoNoDeletes = ContainerProtos.ContainerChecksumInfo
        .newBuilder()
        .setContainerMerkleTree(buildTestTree(config, 10).toProto())
        .setContainerID(CONTAINER_ID)
        .build();
    checksumInfo = checksumManager.read(container);
    containerDiff = checksumManager.diff(checksumInfo, peerInfoNoDeletes);

    assertFalse(containerDiff.getMissingBlocks().isEmpty());
    // Missing block does not contain the deleted blocks 6L to 10L
    assertTrue(containerDiff.getMissingBlocks().stream().anyMatch(blockTree ->
        deletedBlockIDs.contains(blockTree.getBlockID())));
  }

  /**
   * Test to check if the container diff consists of blocks that are missing in our merkle tree but
   * they are deleted in the peer's merkle tree.
   */
  @Test
  void testDeletedBlocksInPeerAndBoth() throws Exception {
    ContainerProtos.ContainerMerkleTree peerMerkleTree = buildTestTree(config, 5, 1, 2, 3, 4, 5);
    // Introduce missing blocks in our merkle tree
    ContainerProtos.ContainerMerkleTree ourMerkleTree =
        buildTestTreeWithMismatches(new ContainerMerkleTreeWriter(peerMerkleTree), 3, 0, 0).getLeft();

//    List<ContainerProtos.BlockMerkleTree> deletedBlockList = new ArrayList<>();
//    List<Long> blockIDs = Arrays.asList(1L, 2L, 3L, 4L, 5L);
//    for (Long blockID : blockIDs) {
//      deletedBlockList.add(ContainerProtos.BlockMerkleTree.newBuilder().setBlockID(blockID).build());
//    }

    // Mark all the blocks as deleted in peer merkle tree
    ContainerProtos.ContainerChecksumInfo peerChecksumInfo = ContainerProtos.ContainerChecksumInfo
        .newBuilder()
        .setContainerMerkleTree(peerMerkleTree).setContainerID(CONTAINER_ID)
        .build();

    updateTreeProto(container, ourMerkleTree);
    ContainerProtos.ContainerChecksumInfo checksumInfo = checksumManager.read(container);
    ContainerDiffReport containerDiff = checksumManager.diff(checksumInfo, peerChecksumInfo);

    // The diff should not have any missing block/missing chunk/corrupt chunks as the blocks are deleted
    // in peer merkle tree.
    assertTrue(containerDiff.getMissingBlocks().isEmpty());
    assertTrue(containerDiff.getMissingChunks().isEmpty());
    assertTrue(containerDiff.getMissingChunks().isEmpty());

    // Delete blocks in our merkle tree as well.
    checksumManager.addDeletedBlocks(container, getDeletedBlockData(config, 1, 2, 3, 4, 5));
    checksumInfo = checksumManager.read(container);
    containerDiff = checksumManager.diff(checksumInfo, peerChecksumInfo);

    // The diff should not have any missing block/missing chunk/corrupt chunks as the blocks are deleted
    // in both merkle tree.
    assertTrue(containerDiff.getMissingBlocks().isEmpty());
    assertTrue(containerDiff.getMissingChunks().isEmpty());
    assertTrue(containerDiff.getMissingChunks().isEmpty());
  }

  /**
   * Test to check if the container diff consists of blocks that are corrupted in our merkle tree but also deleted in
   * our merkle tree.
   */
  @Test
  void testDeletedBlocksInOurContainerOnly() throws Exception {
    // Setup deleted blocks only in the peer container checksum
    ContainerMerkleTreeWriter peerMerkleTree = buildTestTree(config);
    // Introduce block corruption in our merkle tree.
    ContainerProtos.ContainerMerkleTree ourMerkleTree = buildTestTreeWithMismatches(peerMerkleTree, 0, 3, 3).getLeft();
    ContainerProtos.ContainerChecksumInfo peerChecksumInfo = ContainerProtos.ContainerChecksumInfo
        .newBuilder().setContainerMerkleTree(peerMerkleTree.toProto()).setContainerID(CONTAINER_ID).build();

    updateTreeProto(container, ourMerkleTree);
    checksumManager.addDeletedBlocks(container, getDeletedBlockData(config, 1L, 2L, 3L, 4L, 5L));

    ContainerProtos.ContainerChecksumInfo checksumInfo = checksumManager.read(container);
    ContainerDiffReport containerDiff = checksumManager.diff(checksumInfo, peerChecksumInfo);

    // The diff should not have any missing block/missing chunk/corrupt chunks as the blocks are deleted
    // in our merkle tree.
    assertTrue(containerDiff.getMissingBlocks().isEmpty());
    assertTrue(containerDiff.getMissingChunks().isEmpty());
    assertTrue(containerDiff.getMissingChunks().isEmpty());
  }

  /**
   * Test to check if the container diff consists of blocks that are corrupted in our merkle tree but also deleted in
   * our peer tree.
   */
  @Test
  void testCorruptionInOurMerkleTreeAndDeletedBlocksInPeer() throws Exception {
    // Setup deleted blocks only in the peer container tree
    ContainerProtos.ContainerMerkleTree peerMerkleTree = buildTestTree(config, 5, 1, 2, 3, 4, 5);
    // Create our tree the same as the peer, but introduce corruption instead of deleting blocks.
    ContainerProtos.ContainerMerkleTree ourMerkleTree =
        buildTestTreeWithMismatches(buildTestTree(config, 5), 0, 3, 3).getLeft();

    ContainerProtos.ContainerChecksumInfo peerChecksumInfo = ContainerProtos.ContainerChecksumInfo
        .newBuilder()
        .setContainerMerkleTree(peerMerkleTree).setContainerID(CONTAINER_ID)
        .build();

    updateTreeProto(container, ourMerkleTree);

    ContainerProtos.ContainerChecksumInfo checksumInfo = checksumManager.read(container);
    ContainerDiffReport containerDiff = checksumManager.diff(checksumInfo, peerChecksumInfo);

    // The diff should not have any missing block/missing chunk/corrupt chunks as the blocks are deleted
    // in peer merkle tree.
    assertTrue(containerDiff.getMissingBlocks().isEmpty());
    assertTrue(containerDiff.getMissingChunks().isEmpty());
    assertTrue(containerDiff.getMissingChunks().isEmpty());
  }
}
