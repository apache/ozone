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
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.assertTreesSortedAndMatch;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.buildChunk;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.readChecksumFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestContainerChecksumTreeManager {

  private static final Logger LOG = LoggerFactory.getLogger(TestContainerChecksumTreeManager.class);

  private static final long CONTAINER_ID = 1L;
  @TempDir
  private File testDir;
  private KeyValueContainerData container;
  private File checksumFile;
  private ContainerChecksumTreeManager checksumManager;
  private ContainerMerkleTreeMetrics metrics;
  private ConfigurationSource config;

  @BeforeEach
  public void init() {
    container = mock(KeyValueContainerData.class);
    when(container.getContainerID()).thenReturn(CONTAINER_ID);
    when(container.getMetadataPath()).thenReturn(testDir.getAbsolutePath());
    checksumFile = new File(testDir, CONTAINER_ID + ".tree");
    checksumManager = new ContainerChecksumTreeManager(new OzoneConfiguration());
    metrics = checksumManager.getMetrics();
    config = new OzoneConfiguration();
  }

  @Test
  public void testWriteEmptyTreeToFile() throws Exception {
    assertEquals(metrics.getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(metrics.getCreateMerkleTreeLatencyNS().lastStat().total(), 0);
    checksumManager.writeContainerDataTree(container, new ContainerMerkleTree());
    assertTrue(metrics.getWriteContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    assertTrue(metrics.getCreateMerkleTreeLatencyNS().lastStat().total() > 0);

    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);

    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    assertTrue(checksumInfo.getDeletedBlocksList().isEmpty());
    ContainerProtos.ContainerMerkleTree treeProto = checksumInfo.getContainerMerkleTree();
    assertEquals(0, treeProto.getDataChecksum());
    assertTrue(treeProto.getBlockMerkleTreeList().isEmpty());
  }

  @Test
  public void testWriteEmptyBlockListToFile() throws Exception {
    assertEquals(metrics.getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    checksumManager.markBlocksAsDeleted(container, Collections.emptySet());
    assertTrue(metrics.getWriteContainerMerkleTreeLatencyNS().lastStat().total() > 0);

    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);

    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    assertTrue(checksumInfo.getDeletedBlocksList().isEmpty());
    ContainerProtos.ContainerMerkleTree treeProto = checksumInfo.getContainerMerkleTree();
    assertEquals(0, treeProto.getDataChecksum());
    assertTrue(treeProto.getBlockMerkleTreeList().isEmpty());
  }

  @Test
  public void testWriteOnlyTreeToFile() throws Exception {
    assertEquals(metrics.getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(metrics.getCreateMerkleTreeLatencyNS().lastStat().total(), 0);
    ContainerMerkleTree tree = buildTestTree();
    checksumManager.writeContainerDataTree(container, tree);
    assertTrue(metrics.getWriteContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);

    assertTrue(metrics.getCreateMerkleTreeLatencyNS().lastStat().total() > 0);
    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    assertTrue(checksumInfo.getDeletedBlocksList().isEmpty());
    // TestContainerMerkleTree verifies that going from ContainerMerkleTree to its proto is consistent.
    // Therefore, we can use the proto version of our expected tree to check what was written to the file.
    assertTreesSortedAndMatch(tree.toProto(), checksumInfo.getContainerMerkleTree());
  }

  @Test
  public void testWriteOnlyDeletedBlocksToFile() throws Exception {
    assertEquals(metrics.getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    List<Long> expectedBlocksToDelete = Arrays.asList(1L, 2L, 3L);
    checksumManager.markBlocksAsDeleted(container, new ArrayList<>(expectedBlocksToDelete));
    assertTrue(metrics.getWriteContainerMerkleTreeLatencyNS().changed());

    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);

    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    assertEquals(expectedBlocksToDelete, checksumInfo.getDeletedBlocksList());
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
    assertEquals(expectedBlocksToDelete, checksumInfo.getDeletedBlocksList());

    // Blocks are expected to appear in the file deduplicated in this order.
    expectedBlocksToDelete = Arrays.asList(1L, 2L, 3L, 4L);
    // Pass another set of blocks. This and the previous list passed should be joined, deduplicated, and sorted.
    checksumManager.markBlocksAsDeleted(container, Arrays.asList(2L, 2L, 3L, 4L));
    checksumInfo = readChecksumFile(container);
    assertEquals(expectedBlocksToDelete, checksumInfo.getDeletedBlocksList());
  }

  @Test
  public void testWriteBlocksOutOfOrder() throws Exception {
    // Blocks are expected to be written to the file in this order.
    List<Long> expectedBlocksToDelete = Arrays.asList(1L, 2L, 3L);
    checksumManager.markBlocksAsDeleted(container, Arrays.asList(3L, 1L, 2L));
    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);
    assertEquals(expectedBlocksToDelete, checksumInfo.getDeletedBlocksList());
  }

  @Test
  public void testDeletedBlocksPreservedOnTreeWrite() throws Exception {
    assertEquals(metrics.getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(metrics.getCreateMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(metrics.getReadContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    List<Long> expectedBlocksToDelete = Arrays.asList(1L, 2L, 3L);
    checksumManager.markBlocksAsDeleted(container, new ArrayList<>(expectedBlocksToDelete));
    assertEquals(metrics.getReadContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    ContainerMerkleTree tree = buildTestTree();
    checksumManager.writeContainerDataTree(container, tree);
    assertTrue(metrics.getWriteContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    assertTrue(metrics.getReadContainerMerkleTreeLatencyNS().lastStat().total() > 0);

    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);

    assertTrue(metrics.getCreateMerkleTreeLatencyNS().lastStat().total() > 0);
    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    assertEquals(expectedBlocksToDelete, checksumInfo.getDeletedBlocksList());
    assertTreesSortedAndMatch(tree.toProto(), checksumInfo.getContainerMerkleTree());
  }

  @Test
  public void testTreePreservedOnDeletedBlocksWrite() throws Exception {
    assertEquals(metrics.getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(metrics.getCreateMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(metrics.getReadContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    ContainerMerkleTree tree = buildTestTree();
    checksumManager.writeContainerDataTree(container, tree);
    assertEquals(metrics.getReadContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    List<Long> expectedBlocksToDelete = Arrays.asList(1L, 2L, 3L);
    checksumManager.markBlocksAsDeleted(container, new ArrayList<>(expectedBlocksToDelete));
    assertTrue(metrics.getWriteContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    assertTrue(metrics.getReadContainerMerkleTreeLatencyNS().lastStat().total() > 0);

    ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumFile(container);

    assertTrue(metrics.getCreateMerkleTreeLatencyNS().lastStat().total() > 0);
    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    assertEquals(expectedBlocksToDelete, checksumInfo.getDeletedBlocksList());
    assertTreesSortedAndMatch(tree.toProto(), checksumInfo.getContainerMerkleTree());
  }

  @Test
  public void testReadContainerMerkleTreeMetric() throws Exception {
    assertEquals(metrics.getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(metrics.getReadContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    ContainerMerkleTree tree = buildTestTree();
    checksumManager.writeContainerDataTree(container, tree);
    assertEquals(metrics.getReadContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    checksumManager.writeContainerDataTree(container, tree);
    assertTrue(metrics.getWriteContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    assertTrue(metrics.getReadContainerMerkleTreeLatencyNS().lastStat().total() > 0);
  }

  @Test
  public void testTmpFileWriteFailure() throws Exception {
    File tmpFile = ContainerChecksumTreeManager.getTmpContainerChecksumFile(container);
    File finalFile = ContainerChecksumTreeManager.getContainerChecksumFile(container);

    assertFalse(tmpFile.exists());
    assertFalse(finalFile.exists());
    ContainerMerkleTree tree = buildTestTree();
    checksumManager.writeContainerDataTree(container, tree);
    assertFalse(tmpFile.exists());
    assertTrue(finalFile.exists());

    // Make the write to the tmp file fail by removing permissions on its parent.
    assertTrue(finalFile.getParentFile().setWritable(false));
    try {
      checksumManager.writeContainerDataTree(container, tree);
      fail();
    } catch (IOException ex) {
      LOG.info("Write to the tmp file failed as expected with the following exception: ", ex);
    }
    assertFalse(tmpFile.exists());
    // The original file should still remain valid.
    assertTrue(finalFile.exists());
    assertTreesSortedAndMatch(tree.toProto(), readChecksumFile(container).getContainerMerkleTree());
  }

  @Test
  public void testFileRenameFailure() throws Exception {
    File tmpFile = ContainerChecksumTreeManager.getTmpContainerChecksumFile(container);
    File finalFile = ContainerChecksumTreeManager.getContainerChecksumFile(container);

    assertFalse(tmpFile.exists());
    assertFalse(finalFile.exists());
    ContainerMerkleTree tree = buildTestTree();
    checksumManager.writeContainerDataTree(container, tree);
    assertFalse(tmpFile.exists());
    assertTrue(finalFile.exists());

    // Remove write permissions on the final file destination. Write to the tmp file should succeed, but the rename
    // should fail.
    assertTrue(finalFile.setReadOnly());
    // TODO the Files#move call is still overwriting this.
    assertFalse(tmpFile.exists());
    try {
      checksumManager.writeContainerDataTree(container, tree);
      fail("No exception thrown");
    } catch (IOException ex) {
      LOG.info("Write to the tmp file failed as expected with the following exception: ", ex);
    }
    assertTrue(tmpFile.exists());
    // The original file should still remain valid.
    assertTrue(finalFile.exists());
    assertTreesSortedAndMatch(tree.toProto(), readChecksumFile(container).getContainerMerkleTree());

    // Writing again after permission is restored should clear the file.
    assertTrue(finalFile.setWritable(true));
    checksumManager.writeContainerDataTree(container, tree);
    assertFalse(tmpFile.exists());
    assertTrue(finalFile.exists());
    assertTreesSortedAndMatch(tree.toProto(), readChecksumFile(container).getContainerMerkleTree());
  }

  @Test
  public void testCorruptedFile() throws Exception {
    // Write file
    File finalFile = ContainerChecksumTreeManager.getContainerChecksumFile(container);
    assertFalse(finalFile.exists());
    ContainerMerkleTree tree = buildTestTree();
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

  @Test
  public void testChecksumTreeFilePath() {
    assertEquals(checksumFile.getAbsolutePath(),
        ContainerChecksumTreeManager.getContainerChecksumFile(container).getAbsolutePath());
  }

  private ContainerMerkleTree buildTestTree() throws Exception {
    final long blockID1 = 1;
    final long blockID2 = 2;
    final long blockID3 = 3;
    ChunkInfo b1c1 = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ChunkInfo b1c2 = buildChunk(config, 1, ByteBuffer.wrap(new byte[]{4, 5, 6}));
    ChunkInfo b2c1 = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{7, 8, 9}));
    ChunkInfo b2c2 = buildChunk(config, 1, ByteBuffer.wrap(new byte[]{12, 11, 10}));
    ChunkInfo b3c1 = buildChunk(config, 0, ByteBuffer.wrap(new byte[]{13, 14, 15}));
    ChunkInfo b3c2 = buildChunk(config, 1, ByteBuffer.wrap(new byte[]{16, 17, 18}));

    ContainerMerkleTree tree = new ContainerMerkleTree();
    tree.addChunks(blockID1, Arrays.asList(b1c1, b1c2));
    tree.addChunks(blockID2, Arrays.asList(b2c1, b2c2));
    tree.addChunks(blockID3, Arrays.asList(b3c1, b3c2));

    return tree;
  }
}
