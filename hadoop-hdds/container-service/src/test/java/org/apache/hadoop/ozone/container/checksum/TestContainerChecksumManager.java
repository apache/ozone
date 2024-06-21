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

class TestContainerChecksumManager {

  private KeyValueContainerData container;
  private final long containerID = 1L;
  @TempDir
  private File testDir;
  private File checksumFile;
  private ContainerChecksumManager checksumManager;

  @BeforeEach
  public void init() {
    container = mock(KeyValueContainerData.class);
    when(container.getContainerID()).thenReturn(containerID);
    when(container.getMetadataPath()).thenReturn(testDir.getAbsolutePath());
    checksumFile = new File(testDir, containerID + ".checksum");
    checksumManager = new ContainerChecksumManager(new DatanodeConfiguration());
  }

  @Test
  public void testWriteEmptyTreeToFile() throws Exception {
    checksumManager.writeContainerMerkleTree(container, new ContainerMerkleTree());
    ContainerProtos.ContainerChecksumInfo checksumInfo = readFile();

    assertEquals(containerID, checksumInfo.getContainerID());
    assertTrue(checksumInfo.getDeletedBlocksList().isEmpty());
    ContainerProtos.ContainerMerkleTree treeProto = checksumInfo.getDataMerkleTree();
    assertEquals(0, treeProto.getDataChecksum());
    assertTrue(treeProto.getBlockMerkleTreeList().isEmpty());
  }

  @Test
  public void testWriteEmptyBlockListToFile() throws Exception {
    checksumManager.markBlocksAsDeleted(container, new TreeSet<>());
    ContainerProtos.ContainerChecksumInfo checksumInfo = readFile();

    assertEquals(containerID, checksumInfo.getContainerID());
    assertTrue(checksumInfo.getDeletedBlocksList().isEmpty());
    ContainerProtos.ContainerMerkleTree treeProto = checksumInfo.getDataMerkleTree();
    assertEquals(0, treeProto.getDataChecksum());
    assertTrue(treeProto.getBlockMerkleTreeList().isEmpty());
  }

  @Test
  public void testWriteToFileTreeOnly() throws Exception {
    ContainerMerkleTree tree = buildTestTree();
    checksumManager.writeContainerMerkleTree(container, tree);

    ContainerProtos.ContainerChecksumInfo checksumInfo = readFile();

    assertEquals(containerID, checksumInfo.getContainerID());
    assertTrue(checksumInfo.getDeletedBlocksList().isEmpty());
    // TestContainerMerkleTree verifies that going from ContainerMerkleTree to its proto is consistent.
    // Therefore, we can use the proto version of our expected tree to check what was written to the file.
    assertTreesSortedAndMatch(tree.toProto(), checksumInfo.getDataMerkleTree());
  }

  @Test
  public void testWriteToFileDeletedBlocksOnly() throws Exception {
    List<Long> expectedBlocksToDelete = Arrays.asList(1L, 2L, 3L);
    checksumManager.markBlocksAsDeleted(container, new TreeSet<>(expectedBlocksToDelete));

    ContainerProtos.ContainerChecksumInfo checksumInfo = readFile();

    assertEquals(containerID, checksumInfo.getContainerID());
    assertEquals(expectedBlocksToDelete, checksumInfo.getDeletedBlocksList());
    ContainerProtos.ContainerMerkleTree treeProto = checksumInfo.getDataMerkleTree();
    assertEquals(0, treeProto.getDataChecksum());
    assertTrue(treeProto.getBlockMerkleTreeList().isEmpty());
  }

  @Test
  public void testDeletedBlocksPreservedOnTreeWrite() throws Exception {
    List<Long> expectedBlocksToDelete = Arrays.asList(1L, 2L, 3L);
    checksumManager.markBlocksAsDeleted(container, new TreeSet<>(expectedBlocksToDelete));
    ContainerMerkleTree tree = buildTestTree();
    checksumManager.writeContainerMerkleTree(container, tree);

    ContainerProtos.ContainerChecksumInfo checksumInfo = readFile();

    assertEquals(containerID, checksumInfo.getContainerID());
    assertEquals(expectedBlocksToDelete, checksumInfo.getDeletedBlocksList());
    assertTreesSortedAndMatch(tree.toProto(), checksumInfo.getDataMerkleTree());
  }

  @Test
  public void testTreePreservedWithDeletedBlocks() throws Exception {
    ContainerMerkleTree tree = buildTestTree();
    checksumManager.writeContainerMerkleTree(container, tree);
    List<Long> expectedBlocksToDelete = Arrays.asList(1L, 2L, 3L);
    checksumManager.markBlocksAsDeleted(container, new TreeSet<>(expectedBlocksToDelete));

    ContainerProtos.ContainerChecksumInfo checksumInfo = readFile();

    assertEquals(containerID, checksumInfo.getContainerID());
    assertEquals(expectedBlocksToDelete, checksumInfo.getDeletedBlocksList());
    assertTreesSortedAndMatch(tree.toProto(), checksumInfo.getDataMerkleTree());
  }

  private ContainerMerkleTree buildTestTree() throws Exception {
    // Seed the expected and actual trees with the same chunks.
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
