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
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.buildBlockData;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.buildTestTree;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.readChecksumFile;
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
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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

  @BeforeEach
  public void init() {
    container = mock(KeyValueContainerData.class);
    when(container.getContainerID()).thenReturn(CONTAINER_ID);
    when(container.getMetadataPath()).thenReturn(testDir.getAbsolutePath());
    // File name is hardcoded here to check if the file name has been changed, since this would
    // need additional compatibility handling.
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
    checksumManager.updateTree(container, new ContainerMerkleTreeWriter());
    assertTrue(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    assertTrue(checksumManager.getMetrics().getCreateMerkleTreeLatencyNS().lastStat().total() > 0);

    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);

    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    ContainerProtos.ContainerMerkleTree treeProto = checksumInfo.getContainerMerkleTree();
    assertEquals(0, treeProto.getDataChecksum());
    assertTrue(treeProto.getBlockMerkleTreeList().isEmpty());
  }

  @Test
  public void testWriteEmptyBlockListToFile() throws Exception {
    assertEquals(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    checksumManager.addDeletedBlocks(container, Collections.emptySet());
    assertTrue(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total() > 0);

    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);

    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    ContainerProtos.ContainerMerkleTree treeProto = checksumInfo.getContainerMerkleTree();
    assertEquals(0, treeProto.getDataChecksum());
    assertTrue(treeProto.getBlockMerkleTreeList().isEmpty());
  }

  @Test
  public void testWriteOnlyTreeToFile() throws Exception {
    assertEquals(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(checksumManager.getMetrics().getCreateMerkleTreeLatencyNS().lastStat().total(), 0);
    ContainerMerkleTreeWriter tree = buildTestTree(config);
    checksumManager.updateTree(container, tree);
    assertTrue(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);

    assertTrue(checksumManager.getMetrics().getCreateMerkleTreeLatencyNS().lastStat().total() > 0);
    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    // TestContainerMerkleTree verifies that going from ContainerMerkleTree to its proto is consistent.
    // Therefore, we can use the proto version of our expected tree to check what was written to the file.
    assertTreesSortedAndMatch(tree.toProto(), checksumInfo.getContainerMerkleTree());
  }

  @Test
  public void testAddDeletedBlocksOnly() throws Exception {
    assertEquals(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    BlockData block1 = buildBlockData(config, CONTAINER_ID, 1);
    BlockData block2 = buildBlockData(config, CONTAINER_ID, 3);
    BlockData block3 = buildBlockData(config, CONTAINER_ID, 7);
    checksumManager.addDeletedBlocks(container, Arrays.asList(block1, block2, block3));
    assertTrue(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().changed());

    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);

    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    assertEquals(Arrays.asList(1L, 3L, 7L), getDeletedBlockIDs(checksumInfo));
    ContainerProtos.ContainerMerkleTree treeProto = checksumInfo.getContainerMerkleTree();
    assertEquals(3, treeProto.getBlockMerkleTreeList().size());
    // When only deleted blocks are added to the tree, a data checksum should not be generated.
    assertFalse(ContainerChecksumTreeManager.hasDataChecksum(checksumInfo));
  }

  @Test
  public void testAddDuplicateDeletedBlocks() throws Exception {
    BlockData block1 = buildBlockData(config, CONTAINER_ID, 1);
    BlockData block2 = buildBlockData(config, CONTAINER_ID, 2);
    BlockData block3 = buildBlockData(config, CONTAINER_ID, 3);
    // Block list should be deduplicated after being written.
    checksumManager.addDeletedBlocks(container, Arrays.asList(block1, block2, block2, block3));
    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);
    assertEquals(Arrays.asList(1L, 2L, 3L), getDeletedBlockIDs(checksumInfo));

    // Pass another set of blocks. This and the previous list passed should be joined, deduplicated, and sorted.
    BlockData block4 = buildBlockData(config, CONTAINER_ID, 4);
    checksumManager.addDeletedBlocks(container, Arrays.asList(block1, block1, block4));
    checksumInfo = readChecksumFile(container);
    assertEquals(Arrays.asList(1L, 2L, 3L, 4L), getDeletedBlockIDs(checksumInfo));
  }

  @Test
  public void testWriteBlocksOutOfOrder() throws Exception {
    BlockData block1 = buildBlockData(config, CONTAINER_ID, 1);
    BlockData block2 = buildBlockData(config, CONTAINER_ID, 2);
    BlockData block3 = buildBlockData(config, CONTAINER_ID, 3);
    checksumManager.addDeletedBlocks(container, Arrays.asList(block2, block1, block3));
    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);
    assertEquals(Arrays.asList(1L, 2L, 3L), getDeletedBlockIDs(checksumInfo));
  }

  @Test
  public void testDeletedBlocksPreservedOnTreeWrite() throws Exception {
    assertEquals(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(checksumManager.getMetrics().getCreateMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(checksumManager.getMetrics().getReadContainerMerkleTreeLatencyNS().lastStat().total(), 0);

    ArrayList<BlockData> expectedBlocksToDelete = new ArrayList<>();
    expectedBlocksToDelete.add(buildBlockData(config, CONTAINER_ID, 1));
    expectedBlocksToDelete.add(buildBlockData(config, CONTAINER_ID, 2));
    checksumManager.addDeletedBlocks(container, expectedBlocksToDelete);

    ContainerMerkleTreeWriter tree = buildTestTree(config);
    checksumManager.updateTree(container, tree);
    assertTrue(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    assertTrue(checksumManager.getMetrics().getReadContainerMerkleTreeLatencyNS().lastStat().total() > 0);

    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);

    assertTrue(checksumManager.getMetrics().getCreateMerkleTreeLatencyNS().lastStat().total() > 0);
    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    assertEquals(Arrays.asList(1L, 2L), getDeletedBlockIDs(checksumInfo));
    assertTreesSortedAndMatch(tree.toProto(), checksumInfo.getContainerMerkleTree());
  }

  @Test
  public void testTreePreservedOnDeletedBlocksWrite() throws Exception {
    assertEquals(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(checksumManager.getMetrics().getCreateMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(checksumManager.getMetrics().getReadContainerMerkleTreeLatencyNS().lastStat().total(), 0);

    ArrayList<BlockData> expectedBlocksToDelete = new ArrayList<>();
    expectedBlocksToDelete.add(buildBlockData(config, CONTAINER_ID, 1));
    expectedBlocksToDelete.add(buildBlockData(config, CONTAINER_ID, 2));

    // Create the initial version of the tree to keep in memory.
    ContainerMerkleTreeWriter tree = buildTestTree(config);
    // Write this version of the tree to the disk.
    checksumManager.updateTree(container, tree);

    // Write deleted blocks to the disk.
    checksumManager.addDeletedBlocks(container, expectedBlocksToDelete);
    // independently update our in-memory tree with the expected block deletions for reference.
    tree.addDeletedBlocks(expectedBlocksToDelete, true);

    assertTrue(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    assertTrue(checksumManager.getMetrics().getReadContainerMerkleTreeLatencyNS().lastStat().total() > 0);

    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);

    // The in-memory and on-disk trees should still match.
    assertTrue(checksumManager.getMetrics().getCreateMerkleTreeLatencyNS().lastStat().total() > 0);
    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    assertEquals(Arrays.asList(1L, 2L), getDeletedBlockIDs(checksumInfo));
    assertTreesSortedAndMatch(tree.toProto(), checksumInfo.getContainerMerkleTree());
  }

  @Test
  public void testReadContainerMerkleTreeMetric() throws Exception {
    assertEquals(checksumManager.getMetrics().getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(checksumManager.getMetrics().getReadContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    ContainerMerkleTreeWriter tree = buildTestTree(config);
    checksumManager.updateTree(container, tree);
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
    checksumManager.updateTree(container, tree);
    assertFalse(tmpFile.exists());
    assertTrue(finalFile.exists());

    // Make the write to the tmp file fail by removing permissions on its parent.
    assertTrue(tmpFile.getParentFile().setWritable(false));
    try {
      checksumManager.updateTree(container, tree);
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
    checksumManager.updateTree(container, tree);
    assertTrue(finalFile.exists());

    // Corrupt the file so it is not a valid protobuf.
    Files.write(finalFile.toPath(), new byte[]{1, 2, 3},
        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);

    // Direct read should throw to verify the proto is not valid.
    assertThrows(IOException.class, () -> readChecksumFile(container));

    // The manager's read/modify/write cycle should account for the corruption and overwrite the entry.
    // No exception should be thrown.
    checksumManager.updateTree(container, tree);
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
    checksumManager.updateTree(container, tree);
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
    checksumManager.updateTree(container, tree);
    ContainerProtos.ContainerChecksumInfo info = readChecksumFile(container);
    assertTreesSortedAndMatch(tree.toProto(), info.getContainerMerkleTree());
    assertEquals(CONTAINER_ID, info.getContainerID());
  }

  @Test
  public void testFailureContainerMerkleTreeMetric() throws IOException {
    ContainerProtos.ContainerChecksumInfo peerChecksum = ContainerProtos.ContainerChecksumInfo.newBuilder().build();
    ContainerMerkleTreeWriter ourMerkleTree = buildTestTree(config);
    checksumManager.updateTree(container, ourMerkleTree);
    ContainerProtos.ContainerChecksumInfo checksumInfo = checksumManager.read(container);
    assertThrows(StorageContainerException.class, () -> checksumManager.diff(checksumInfo, peerChecksum));
    assertEquals(checksumManager.getMetrics().getMerkleTreeDiffFailure(), 1);
  }

  @Test
  public void testChecksumTreeFilePath() {
    assertEquals(checksumFile.getAbsolutePath(),
        ContainerChecksumTreeManager.getContainerChecksumFile(container).getAbsolutePath());
  }

  @Test
  public void testHasDataChecksum() {
    assertFalse(ContainerChecksumTreeManager.hasDataChecksum(null));

    ContainerProtos.ContainerChecksumInfo empty = ContainerProtos.ContainerChecksumInfo.newBuilder().build();
    assertFalse(ContainerChecksumTreeManager.hasDataChecksum(empty));

    ContainerProtos.ContainerChecksumInfo treeNoChecksum = ContainerProtos.ContainerChecksumInfo.newBuilder()
        .setContainerMerkleTree(buildTestTree(config).addDeletedBlocks(Collections.emptyList(), false))
        .build();
    assertFalse(ContainerChecksumTreeManager.hasDataChecksum(treeNoChecksum));

    ContainerProtos.ContainerMerkleTree zeroChecksumTree = ContainerProtos.ContainerMerkleTree.newBuilder()
        .setDataChecksum(0)
        .build();
    ContainerProtos.ContainerChecksumInfo treeWithZeroChecksum = ContainerProtos.ContainerChecksumInfo.newBuilder()
        .setContainerMerkleTree(zeroChecksumTree)
        .build();
    assertTrue(ContainerChecksumTreeManager.hasDataChecksum(treeWithZeroChecksum));

    ContainerProtos.ContainerChecksumInfo treeWithDataChecksum = ContainerProtos.ContainerChecksumInfo.newBuilder()
        .setContainerMerkleTree(buildTestTree(config).toProto())
        .build();
    assertTrue(ContainerChecksumTreeManager.hasDataChecksum(treeWithDataChecksum));
  }

  private List<Long> getDeletedBlockIDs(ContainerProtos.ContainerChecksumInfo checksumInfo) {
    return checksumInfo.getContainerMerkleTree().getBlockMerkleTreeList().stream()
        .filter(ContainerProtos.BlockMerkleTree::getDeleted)
        .map(ContainerProtos.BlockMerkleTree::getBlockID)
        .collect(Collectors.toList());
  }
}
