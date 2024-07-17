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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import static org.apache.hadoop.ozone.container.checksum.TestContainerMerkleTree.assertTreesSortedAndMatch;
import static org.apache.hadoop.ozone.container.checksum.TestContainerMerkleTree.buildChunk;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestContainerChecksumTreeManager {

  private static final long CONTAINER_ID = 1L;
  @TempDir
  private File testDir;
  private KeyValueContainerData container;
  private File checksumFile;
  private ContainerChecksumTreeManager checksumManager;
  private ContainerMerkleTreeMetrics metrics;

  @BeforeEach
  public void init() {
    container = mock(KeyValueContainerData.class);
    when(container.getContainerID()).thenReturn(CONTAINER_ID);
    when(container.getMetadataPath()).thenReturn(testDir.getAbsolutePath());
    checksumFile = new File(testDir, CONTAINER_ID + ".tree");
    checksumManager = new ContainerChecksumTreeManager(new DatanodeConfiguration());
    metrics = checksumManager.getMetrics();
  }

  @Test
  public void testWriteEmptyTreeToFile() throws Exception {
    assertEquals(metrics.getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(metrics.getCreateMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(metrics.getReadContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    checksumManager.writeContainerDataTree(container, new ContainerMerkleTree());
    assertTrue(metrics.getWriteContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    assertTrue(metrics.getCreateMerkleTreeLatencyNS().lastStat().total() > 0);

    ContainerProtos.ContainerChecksumInfo checksumInfo = readFile();

    assertTrue(metrics.getReadContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    assertTrue(checksumInfo.getDeletedBlocksList().isEmpty());
    ContainerProtos.ContainerMerkleTree treeProto = checksumInfo.getContainerMerkleTree();
    assertEquals(0, treeProto.getDataChecksum());
    assertTrue(treeProto.getBlockMerkleTreeList().isEmpty());
  }

  @Test
  public void testWriteEmptyBlockListToFile() throws Exception {
    assertEquals(metrics.getUpdateContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(metrics.getReadContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    checksumManager.markBlocksAsDeleted(container, new TreeSet<>());
    assertTrue(metrics.getUpdateContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    ContainerProtos.ContainerChecksumInfo checksumInfo = readFile();

    assertTrue(metrics.getReadContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    assertTrue(checksumInfo.getDeletedBlocksList().isEmpty());
    ContainerProtos.ContainerMerkleTree treeProto = checksumInfo.getContainerMerkleTree();
    assertEquals(0, treeProto.getDataChecksum());
    assertTrue(treeProto.getBlockMerkleTreeList().isEmpty());
  }

  @Test
  public void testWriteOnlyTreeToFile() throws Exception {
    assertEquals(metrics.getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(metrics.getReadContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(metrics.getCreateMerkleTreeLatencyNS().lastStat().total(), 0);
    ContainerMerkleTree tree = buildTestTree();
    checksumManager.writeContainerDataTree(container, tree);
    assertTrue(metrics.getWriteContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    ContainerProtos.ContainerChecksumInfo checksumInfo = readFile();

    assertTrue(metrics.getCreateMerkleTreeLatencyNS().lastStat().total() > 0);
    assertTrue(metrics.getReadContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    assertTrue(checksumInfo.getDeletedBlocksList().isEmpty());
    // TestContainerMerkleTree verifies that going from ContainerMerkleTree to its proto is consistent.
    // Therefore, we can use the proto version of our expected tree to check what was written to the file.
    assertTreesSortedAndMatch(tree.toProto(), checksumInfo.getContainerMerkleTree());
  }

  @Test
  public void testWriteOnlyDeletedBlocksToFile() throws Exception {
    assertEquals(metrics.getUpdateContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(metrics.getReadContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    List<Long> expectedBlocksToDelete = Arrays.asList(1L, 2L, 3L);
    checksumManager.markBlocksAsDeleted(container, new TreeSet<>(expectedBlocksToDelete));
    assertTrue(metrics.getUpdateContainerMerkleTreeLatencyNS().lastStat().total() > 0);

    ContainerProtos.ContainerChecksumInfo checksumInfo = readFile();

    assertTrue(metrics.getReadContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    assertEquals(expectedBlocksToDelete, checksumInfo.getDeletedBlocksList());
    ContainerProtos.ContainerMerkleTree treeProto = checksumInfo.getContainerMerkleTree();
    assertEquals(0, treeProto.getDataChecksum());
    assertTrue(treeProto.getBlockMerkleTreeList().isEmpty());
  }

  @Test
  public void testDeletedBlocksPreservedOnTreeWrite() throws Exception {
    assertEquals(metrics.getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(metrics.getUpdateContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(metrics.getReadContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(metrics.getCreateMerkleTreeLatencyNS().lastStat().total(), 0);
    List<Long> expectedBlocksToDelete = Arrays.asList(1L, 2L, 3L);
    checksumManager.markBlocksAsDeleted(container, new TreeSet<>(expectedBlocksToDelete));
    ContainerMerkleTree tree = buildTestTree();
    checksumManager.writeContainerDataTree(container, tree);
    assertTrue(metrics.getWriteContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    assertTrue(metrics.getUpdateContainerMerkleTreeLatencyNS().lastStat().total() > 0);

    ContainerProtos.ContainerChecksumInfo checksumInfo = readFile();

    assertTrue(metrics.getCreateMerkleTreeLatencyNS().lastStat().total() > 0);
    assertTrue(metrics.getReadContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    assertEquals(expectedBlocksToDelete, checksumInfo.getDeletedBlocksList());
    assertTreesSortedAndMatch(tree.toProto(), checksumInfo.getContainerMerkleTree());
  }

  @Test
  public void testTreePreservedOnDeletedBlocksWrite() throws Exception {
    assertEquals(metrics.getWriteContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(metrics.getUpdateContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(metrics.getReadContainerMerkleTreeLatencyNS().lastStat().total(), 0);
    assertEquals(metrics.getCreateMerkleTreeLatencyNS().lastStat().total(), 0);
    ContainerMerkleTree tree = buildTestTree();
    checksumManager.writeContainerDataTree(container, tree);
    List<Long> expectedBlocksToDelete = Arrays.asList(1L, 2L, 3L);
    checksumManager.markBlocksAsDeleted(container, new TreeSet<>(expectedBlocksToDelete));
    assertTrue(metrics.getWriteContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    assertTrue(metrics.getUpdateContainerMerkleTreeLatencyNS().lastStat().total() > 0);

    ContainerProtos.ContainerChecksumInfo checksumInfo = readFile();

    assertTrue(metrics.getCreateMerkleTreeLatencyNS().lastStat().total() > 0);
    assertTrue(metrics.getReadContainerMerkleTreeLatencyNS().lastStat().total() > 0);
    assertEquals(CONTAINER_ID, checksumInfo.getContainerID());
    assertEquals(expectedBlocksToDelete, checksumInfo.getDeletedBlocksList());
    assertTreesSortedAndMatch(tree.toProto(), checksumInfo.getContainerMerkleTree());
  }

  @Test
  public void testChecksumTreeFilePath() {
    assertEquals(checksumFile.getAbsolutePath(),
        checksumManager.getContainerChecksumFile(container).getAbsolutePath());
  }

  private ContainerMerkleTree buildTestTree() throws Exception {
    final long blockID1 = 1;
    final long blockID2 = 2;
    final long blockID3 = 3;
    ChunkInfo b1c1 = buildChunk(0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ChunkInfo b1c2 = buildChunk(1, ByteBuffer.wrap(new byte[]{4, 5, 6}));
    ChunkInfo b2c1 = buildChunk(0, ByteBuffer.wrap(new byte[]{7, 8, 9}));
    ChunkInfo b2c2 = buildChunk(1, ByteBuffer.wrap(new byte[]{12, 11, 10}));
    ChunkInfo b3c1 = buildChunk(0, ByteBuffer.wrap(new byte[]{13, 14, 15}));
    ChunkInfo b3c2 = buildChunk(1, ByteBuffer.wrap(new byte[]{16, 17, 18}));

    ContainerMerkleTree tree = new ContainerMerkleTree();
    tree.addChunks(blockID1, Arrays.asList(b1c1, b1c2));
    tree.addChunks(blockID2, Arrays.asList(b2c1, b2c2));
    tree.addChunks(blockID3, Arrays.asList(b3c1, b3c2));

    return tree;
  }

  private ContainerProtos.ContainerChecksumInfo readFile() throws IOException {
    try (FileInputStream inStream = new FileInputStream(checksumFile)) {
      return ContainerProtos.ContainerChecksumInfo.parseFrom(inStream);
    }
  }
}
