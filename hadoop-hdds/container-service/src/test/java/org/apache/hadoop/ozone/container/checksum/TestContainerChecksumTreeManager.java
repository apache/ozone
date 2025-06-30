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
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.assertTreesSortedAndMatch;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.buildTestTree;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.buildTestTreeWithMismatches;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.readChecksumFile;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.writeContainerDataTreeProto;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestContainerChecksumTreeManager {

  private static final Logger LOG = LoggerFactory.getLogger(TestContainerChecksumTreeManager.class);

  private static final long CONTAINER_ID = 1L;
  @TempDir
  private File testDir;
  private KeyValueContainerData container;
  private File checksumFile;
  private ContainerChecksumTreeManager checksumManager;
  private ConfigurationSource config;

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

  @BeforeEach
  public void init() {
    container = mock(KeyValueContainerData.class);
    when(container.getContainerID()).thenReturn(CONTAINER_ID);
    when(container.getMetadataPath()).thenReturn(testDir.getAbsolutePath());
    checksumFile = new File(testDir, CONTAINER_ID + ".tree");
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

  @Test
  public void testWriteEmptyTreeToFile() throws Exception {
    assertEquals(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(checksumManager.getMetrics().getCreateMerkleTreeLatencyNS().lastStat().total(), 0);
    checksumManager.writeContainerDataTree(container, new ContainerMerkleTreeWriter());
    assertTrue(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    assertTrue(checksumManager.getMetrics().getCreateMerkleTreeLatencyNS().lastStat().total() > 0);

    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);

    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    assertTrue(checksumInfo.getDeletedBlocksList().isEmpty());
    ContainerProtos.ContainerMerkleTree treeProto = checksumInfo.getContainerMerkleTree();
    assertEquals(0, treeProto.getDataChecksum());
    assertTrue(treeProto.getBlockMerkleTreeList().isEmpty());
  }

  @Test
  public void testWriteEmptyBlockListToFile() throws Exception {
    assertEquals(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    checksumManager.markBlocksAsDeleted(container, Collections.emptySet());
    assertTrue(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total() > 0);

    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);

    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    assertTrue(checksumInfo.getDeletedBlocksList().isEmpty());
    ContainerProtos.ContainerMerkleTree treeProto = checksumInfo.getContainerMerkleTree();
    assertEquals(0, treeProto.getDataChecksum());
    assertTrue(treeProto.getBlockMerkleTreeList().isEmpty());
  }

  @Test
  public void testWriteOnlyTreeToFile() throws Exception {
    assertEquals(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(checksumManager.getMetrics().getCreateMerkleTreeLatencyNS().lastStat().total(), 0);
    ContainerMerkleTreeWriter tree = buildTestTree(config);
    checksumManager.writeContainerDataTree(container, tree);
    assertTrue(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);

    assertTrue(checksumManager.getMetrics().getCreateMerkleTreeLatencyNS().lastStat().total() > 0);
    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    assertTrue(checksumInfo.getDeletedBlocksList().isEmpty());
    // TestContainerMerkleTree verifies that going from ContainerMerkleTree to its proto is consistent.
    // Therefore, we can use the proto version of our expected tree to check what was written to the file.
    assertTreesSortedAndMatch(tree.toProto(), checksumInfo.getContainerMerkleTree());
  }

  @Test
  public void testWriteOnlyDeletedBlocksToFile() throws Exception {
    assertEquals(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    List<Long> expectedBlocksToDelete = Arrays.asList(1L, 2L, 3L);
    checksumManager.markBlocksAsDeleted(container, new ArrayList<>(expectedBlocksToDelete));
    assertTrue(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().changed());

    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);

    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    assertEquals(expectedBlocksToDelete, getDeletedBlockIDs(checksumInfo));
    ContainerProtos.ContainerMerkleTree treeProto = checksumInfo.getContainerMerkleTree();
    assertEquals(0, treeProto.getDataChecksum());
    assertTrue(treeProto.getBlockMerkleTreeList().isEmpty());
  }

  @Test
  public void testWriteDuplicateDeletedBlocks() throws Exception {
    // Blocks are expected to appear in the file deduplicated in this order.
    List<Long> expectedBlocksToDelete = Arrays.asList(1L, 2L, 3L);
    // Pass a duplicate block, it should be filtered out.
    checksumManager.markBlocksAsDeleted(container, Arrays.asList(1L, 2L, 2L, 3L));
    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);
    assertEquals(expectedBlocksToDelete, getDeletedBlockIDs(checksumInfo));

    // Blocks are expected to appear in the file deduplicated in this order.
    expectedBlocksToDelete = Arrays.asList(1L, 2L, 3L, 4L);
    // Pass another set of blocks. This and the previous list passed should be joined, deduplicated, and sorted.
    checksumManager.markBlocksAsDeleted(container, Arrays.asList(2L, 2L, 3L, 4L));
    checksumInfo = readChecksumFile(container);
    assertEquals(expectedBlocksToDelete, getDeletedBlockIDs(checksumInfo));
  }

  @Test
  public void testWriteBlocksOutOfOrder() throws Exception {
    // Blocks are expected to be written to the file in this order.
    List<Long> expectedBlocksToDelete = Arrays.asList(1L, 2L, 3L);
    checksumManager.markBlocksAsDeleted(container, Arrays.asList(3L, 1L, 2L));
    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);
    assertEquals(expectedBlocksToDelete, getDeletedBlockIDs(checksumInfo));
  }

  @Test
  public void testDeletedBlocksPreservedOnTreeWrite() throws Exception {
    assertEquals(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(checksumManager.getMetrics().getCreateMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(checksumManager.getMetrics().getReadContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    List<Long> expectedBlocksToDelete = Arrays.asList(1L, 2L, 3L);
    checksumManager.markBlocksAsDeleted(container, new ArrayList<>(expectedBlocksToDelete));
    ContainerMerkleTreeWriter tree = buildTestTree(config);
    checksumManager.writeContainerDataTree(container, tree);
    assertTrue(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    assertTrue(checksumManager.getMetrics().getReadContainerMerkleTreeLatencyNS().lastStat().total() > 0);

    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);

    assertTrue(checksumManager.getMetrics().getCreateMerkleTreeLatencyNS().lastStat().total() > 0);
    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    assertEquals(expectedBlocksToDelete, getDeletedBlockIDs(checksumInfo));
    assertTreesSortedAndMatch(tree.toProto(), checksumInfo.getContainerMerkleTree());
  }

  @Test
  public void testTreePreservedOnDeletedBlocksWrite() throws Exception {
    assertEquals(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(checksumManager.getMetrics().getCreateMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(checksumManager.getMetrics().getReadContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    ContainerMerkleTreeWriter tree = buildTestTree(config);
    checksumManager.writeContainerDataTree(container, tree);
    List<Long> expectedBlocksToDelete = Arrays.asList(1L, 2L, 3L);
    checksumManager.markBlocksAsDeleted(container, new ArrayList<>(expectedBlocksToDelete));
    assertTrue(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    assertTrue(checksumManager.getMetrics().getReadContainerMerkleTreeLatencyNS().lastStat().total() > 0);

    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);

    assertTrue(checksumManager.getMetrics().getCreateMerkleTreeLatencyNS().lastStat().total() > 0);
    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    assertEquals(expectedBlocksToDelete, getDeletedBlockIDs(checksumInfo));
    assertTreesSortedAndMatch(tree.toProto(), checksumInfo.getContainerMerkleTree());
  }

  @Test
  public void testReadContainerMerkleTreeMetric() throws Exception {
    assertEquals(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(checksumManager.getMetrics().getReadContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    ContainerMerkleTreeWriter tree = buildTestTree(config);
    checksumManager.writeContainerDataTree(container, tree);
    assertTrue(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    assertTrue(checksumManager.getMetrics().getReadContainerMerkleTreeLatencyNS().lastStat().total() > 0);
  }

  /**
   * Updates to the container checksum file are written to a tmp file and then swapped in to place. Test that when
   * the write to the tmp file fails, the main file that is read from is left intact.
   */
  @Test
  public void testTmpFileWriteFailure() throws Exception {
    File tmpFile = ContainerChecksumTreeManager.getTmpContainerChecksumFile(container);
    File finalFile = ContainerChecksumTreeManager.getContainerChecksumFile(container);

    assertFalse(tmpFile.exists());
    assertFalse(finalFile.exists());
    ContainerMerkleTreeWriter tree = buildTestTree(config);
    checksumManager.writeContainerDataTree(container, tree);
    assertFalse(tmpFile.exists());
    assertTrue(finalFile.exists());

    // Make the write to the tmp file fail by removing permissions on its parent.
    assertTrue(tmpFile.getParentFile().setWritable(false));
    try {
      checksumManager.writeContainerDataTree(container, tree);
      fail("Write to the tmp file should have failed.");
    } catch (IOException ex) {
      LOG.info("Write to the tmp file failed as expected with the following exception: ", ex);
    }
    assertFalse(tmpFile.exists());
    // The original file should still remain valid.
    assertTrue(finalFile.exists());
    assertTreesSortedAndMatch(tree.toProto(), readChecksumFile(container).getContainerMerkleTree());
  }

  @Test
  public void testCorruptedFile() throws Exception {
    // Write file
    File finalFile = ContainerChecksumTreeManager.getContainerChecksumFile(container);
    assertFalse(finalFile.exists());
    ContainerMerkleTreeWriter tree = buildTestTree(config);
    checksumManager.writeContainerDataTree(container, tree);
    assertTrue(finalFile.exists());

    // Corrupt the file so it is not a valid protobuf.
    Files.write(finalFile.toPath(), new byte[]{1, 2, 3},
        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);

    // Direct read should throw to verify the proto is not valid.
    assertThrows(IOException.class, () -> readChecksumFile(container));

    // The manager's read/modify/write cycle should account for the corruption and overwrite the entry.
    // No exception should be thrown.
    checksumManager.writeContainerDataTree(container, tree);
    assertTreesSortedAndMatch(tree.toProto(), readChecksumFile(container).getContainerMerkleTree());
  }

  /**
   * An empty file will be interpreted by protobuf to be an object with default values.
   * The checksum manager should overwrite this if it is encountered.
   */
  @Test
  public void testEmptyFile() throws Exception {
    // Write file
    File finalFile = ContainerChecksumTreeManager.getContainerChecksumFile(container);
    assertFalse(finalFile.exists());
    ContainerMerkleTreeWriter tree = buildTestTree(config);
    checksumManager.writeContainerDataTree(container, tree);
    assertTrue(finalFile.exists());

    // Truncate the file to zero length.
    Files.write(finalFile.toPath(), new byte[0],
        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);
    assertEquals(0, finalFile.length());

    // The truncated file will be interpreted as an empty protobuf object.
    // Use a test helper method to read it directly and confirm this.
    ContainerProtos.ContainerChecksumInfo emptyInfo = readChecksumFile(container);
    assertFalse(emptyInfo.hasContainerID());
    assertFalse(emptyInfo.hasContainerMerkleTree());

    // The manager's read/modify/write cycle should account for the empty file and overwrite it with a valid entry.
    // No exception should be thrown.
    checksumManager.writeContainerDataTree(container, tree);
    ContainerProtos.ContainerChecksumInfo info = readChecksumFile(container);
    assertTreesSortedAndMatch(tree.toProto(), info.getContainerMerkleTree());
    assertEquals(CONTAINER_ID, info.getContainerID());
  }

  @Test
  public void testContainerWithNoDiff() throws Exception {
    ContainerMerkleTreeWriter ourMerkleTree = buildTestTree(config);
    ContainerMerkleTreeWriter peerMerkleTree = buildTestTree(config);
    checksumManager.writeContainerDataTree(container, ourMerkleTree);
    ContainerProtos.ContainerChecksumInfo peerChecksumInfo = ContainerProtos.ContainerChecksumInfo.newBuilder()
            .setContainerID(container.getContainerID())
            .setContainerMerkleTree(peerMerkleTree.toProto()).build();
    Optional<ContainerProtos.ContainerChecksumInfo> checksumInfo = checksumManager.read(container);
    ContainerDiffReport diff = checksumManager.diff(checksumInfo.get(), peerChecksumInfo);
    assertTrue(checksumManager.getMetrics().getMerkleTreeDiffLatencyNS().lastStat().total() > 0);
    assertFalse(diff.needsRepair());
    assertEquals(checksumManager.getMetrics().getNoRepairContainerDiffs(), 1);
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
    writeContainerDataTreeProto(container, ourMerkleTree);
    ContainerProtos.ContainerChecksumInfo peerChecksumInfo = ContainerProtos.ContainerChecksumInfo.newBuilder()
        .setContainerID(container.getContainerID())
        .setContainerMerkleTree(peerMerkleTree.toProto()).build();
    Optional<ContainerProtos.ContainerChecksumInfo> checksumInfo = checksumManager.read(container);
    ContainerDiffReport diff = checksumManager.diff(checksumInfo.get(), peerChecksumInfo);
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
    checksumManager.writeContainerDataTree(container, ourMerkleTree);
    ContainerProtos.ContainerChecksumInfo peerChecksumInfo = ContainerProtos.ContainerChecksumInfo.newBuilder()
        .setContainerID(container.getContainerID())
        .setContainerMerkleTree(peerMerkleTree).build();
    Optional<ContainerProtos.ContainerChecksumInfo> checksumInfo = checksumManager.read(container);
    ContainerDiffReport diff = checksumManager.diff(checksumInfo.get(), peerChecksumInfo);
    assertFalse(diff.needsRepair());
    assertEquals(checksumManager.getMetrics().getNoRepairContainerDiffs(), 1);
    assertEquals(0, checksumManager.getMetrics().getMissingBlocksIdentified());
    assertEquals(0, checksumManager.getMetrics().getMissingChunksIdentified());
    assertEquals(0, checksumManager.getMetrics().getCorruptChunksIdentified());
  }

  @Test
  public void testFailureContainerMerkleTreeMetric() throws IOException {
    ContainerProtos.ContainerChecksumInfo peerChecksum = ContainerProtos.ContainerChecksumInfo.newBuilder().build();
    ContainerMerkleTreeWriter ourMerkleTree = buildTestTree(config);
    checksumManager.writeContainerDataTree(container, ourMerkleTree);
    Optional<ContainerProtos.ContainerChecksumInfo> checksumInfo = checksumManager.read(container);
    assertThrows(StorageContainerException.class, () -> checksumManager.diff(checksumInfo.get(), peerChecksum));
    assertEquals(checksumManager.getMetrics().getMerkleTreeDiffFailure(), 1);
  }

  /**
   * Test to check if the container diff consists of blocks that are missing in our merkle tree but
   * they are deleted in the peer's merkle tree.
   */
  @Test
  void testDeletedBlocksInPeerAndBoth() throws Exception {
    ContainerMerkleTreeWriter peerMerkleTree = buildTestTree(config);
    // Introduce missing blocks in our merkle tree
    ContainerProtos.ContainerMerkleTree ourMerkleTree = buildTestTreeWithMismatches(peerMerkleTree, 3, 0, 0).getLeft();

    List<ContainerProtos.BlockMerkleTree> deletedBlockList = new ArrayList<>();
    List<Long> blockIDs = Arrays.asList(1L, 2L, 3L, 4L, 5L);
    for (Long blockID : blockIDs) {
      deletedBlockList.add(ContainerProtos.BlockMerkleTree.newBuilder().setBlockID(blockID).build());
    }

    // Mark all the blocks as deleted in peer merkle tree
    ContainerProtos.ContainerChecksumInfo peerChecksumInfo = ContainerProtos.ContainerChecksumInfo
        .newBuilder().setContainerMerkleTree(peerMerkleTree.toProto()).setContainerID(CONTAINER_ID)
        .addAllDeletedBlocks(deletedBlockList).build();

    writeContainerDataTreeProto(container, ourMerkleTree);
    Optional<ContainerProtos.ContainerChecksumInfo> checksumInfo = checksumManager.read(container);
    ContainerDiffReport containerDiff = checksumManager.diff(checksumInfo.get(), peerChecksumInfo);

    // The diff should not have any missing block/missing chunk/corrupt chunks as the blocks are deleted
    // in peer merkle tree.
    assertTrue(containerDiff.getMissingBlocks().isEmpty());
    assertTrue(containerDiff.getMissingChunks().isEmpty());
    assertTrue(containerDiff.getMissingChunks().isEmpty());

    // Delete blocks in our merkle tree as well.
    checksumManager.markBlocksAsDeleted(container, blockIDs);
    checksumInfo = checksumManager.read(container);
    containerDiff = checksumManager.diff(checksumInfo.get(), peerChecksumInfo);

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
    List<Long> deletedBlockList = Arrays.asList(1L, 2L, 3L, 4L, 5L);
    ContainerProtos.ContainerChecksumInfo peerChecksumInfo = ContainerProtos.ContainerChecksumInfo
        .newBuilder().setContainerMerkleTree(peerMerkleTree.toProto()).setContainerID(CONTAINER_ID).build();

    writeContainerDataTreeProto(container, ourMerkleTree);
    checksumManager.markBlocksAsDeleted(container, deletedBlockList);

    Optional<ContainerProtos.ContainerChecksumInfo> checksumInfo = checksumManager.read(container);
    ContainerDiffReport containerDiff = checksumManager.diff(checksumInfo.get(), peerChecksumInfo);

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
    // Setup deleted blocks only in the peer container checksum
    ContainerMerkleTreeWriter peerMerkleTree = buildTestTree(config);
    // Introduce block corruption in our merkle tree.
    ContainerProtos.ContainerMerkleTree ourMerkleTree = buildTestTreeWithMismatches(peerMerkleTree, 0, 3, 3).getLeft();

    List<ContainerProtos.BlockMerkleTree> deletedBlockList = new ArrayList<>();
    List<Long> blockIDs = Arrays.asList(1L, 2L, 3L, 4L, 5L);
    for (Long blockID : blockIDs) {
      deletedBlockList.add(ContainerProtos.BlockMerkleTree.newBuilder().setBlockID(blockID).build());
    }

    ContainerProtos.ContainerChecksumInfo peerChecksumInfo = ContainerProtos.ContainerChecksumInfo
        .newBuilder().setContainerMerkleTree(peerMerkleTree.toProto()).setContainerID(CONTAINER_ID)
        .addAllDeletedBlocks(deletedBlockList).build();

    writeContainerDataTreeProto(container, ourMerkleTree);

    Optional<ContainerProtos.ContainerChecksumInfo> checksumInfo = checksumManager.read(container);
    ContainerDiffReport containerDiff = checksumManager.diff(checksumInfo.get(), peerChecksumInfo);

    // The diff should not have any missing block/missing chunk/corrupt chunks as the blocks are deleted
    // in peer merkle tree.
    assertTrue(containerDiff.getMissingBlocks().isEmpty());
    assertTrue(containerDiff.getMissingChunks().isEmpty());
    assertTrue(containerDiff.getMissingChunks().isEmpty());
  }

  @Test
  void testContainerDiffWithBlockDeletionInPeer() throws Exception {
    // Setup deleted blocks only in the peer container checksum
    ContainerMerkleTreeWriter peerMerkleTree = buildTestTree(config, 10);
    // Create only 5 blocks
    ContainerMerkleTreeWriter dummy = buildTestTree(config, 5);
    // Introduce block corruption in our merkle tree.
    ContainerProtos.ContainerMerkleTree ourMerkleTree = buildTestTreeWithMismatches(dummy, 3, 3, 3).getLeft();

    List<ContainerProtos.BlockMerkleTree> deletedBlockList = new ArrayList<>();
    List<Long> blockIDs = Arrays.asList(6L, 7L, 8L, 9L, 10L);
    for (Long blockID : blockIDs) {
      deletedBlockList.add(ContainerProtos.BlockMerkleTree.newBuilder().setBlockID(blockID).build());
    }

    ContainerProtos.ContainerChecksumInfo.Builder peerChecksumInfoBuilder = ContainerProtos.ContainerChecksumInfo
        .newBuilder().setContainerMerkleTree(peerMerkleTree.toProto()).setContainerID(CONTAINER_ID)
        .addAllDeletedBlocks(deletedBlockList);

    writeContainerDataTreeProto(container, ourMerkleTree);

    ContainerProtos.ContainerChecksumInfo peerChecksumInfo = peerChecksumInfoBuilder.build();
    Optional<ContainerProtos.ContainerChecksumInfo> checksumInfo = checksumManager.read(container);
    ContainerDiffReport containerDiff = checksumManager.diff(checksumInfo.get(), peerChecksumInfo);
    // The diff should not have any missing block/missing chunk/corrupt chunks as the blocks are deleted
    // in peer merkle tree.
    assertFalse(containerDiff.getMissingBlocks().isEmpty());
    // Missing block does not contain the deleted blocks 6L to 10L
    assertFalse(containerDiff.getMissingBlocks().stream().anyMatch(any ->
        blockIDs.contains(any.getBlockID())));
    assertFalse(containerDiff.getMissingBlocks().isEmpty());
    assertFalse(containerDiff.getMissingChunks().isEmpty());

    // Clear deleted blocks to add them in missing blocks.
    peerChecksumInfo = peerChecksumInfoBuilder.clearDeletedBlocks().build();
    checksumInfo = checksumManager.read(container);
    containerDiff = checksumManager.diff(checksumInfo.get(), peerChecksumInfo);

    assertFalse(containerDiff.getMissingBlocks().isEmpty());
    // Missing block does not contain the deleted blocks 6L to 10L
    assertTrue(containerDiff.getMissingBlocks().stream().anyMatch(any ->
        blockIDs.contains(any.getBlockID())));
  }

  @Test
  public void testChecksumTreeFilePath() {
    assertEquals(checksumFile.getAbsolutePath(),
        ContainerChecksumTreeManager.getContainerChecksumFile(container).getAbsolutePath());
  }

  private List<Long> getDeletedBlockIDs(ContainerProtos.ContainerChecksumInfo checksumInfo) {
    return checksumInfo.getDeletedBlocksList().stream()
        .map(ContainerProtos.BlockMerkleTree::getBlockID)
        .collect(Collectors.toList());
  }
}
