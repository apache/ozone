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

import static org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager.getContainerChecksumFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/**
 * Helper methods for testing container checksum tree files and container reconciliation.
 */
public final class ContainerMerkleTreeTestUtils {
  private ContainerMerkleTreeTestUtils() {
  }

  public static void assertTreesSortedAndMatch(ContainerProtos.ContainerMerkleTree expectedTree,
                                               ContainerProtos.ContainerMerkleTree actualTree) {
    assertEquals(expectedTree.getDataChecksum(), actualTree.getDataChecksum());
    assertEquals(expectedTree.getBlockMerkleTreeCount(), actualTree.getBlockMerkleTreeCount());

    long prevBlockID = -1;
    for (int blockIndex = 0; blockIndex < expectedTree.getBlockMerkleTreeCount(); blockIndex++) {
      ContainerProtos.BlockMerkleTree expectedBlockTree = expectedTree.getBlockMerkleTree(blockIndex);
      ContainerProtos.BlockMerkleTree actualBlockTree = actualTree.getBlockMerkleTree(blockIndex);

      // Blocks should be sorted by block ID.
      long currentBlockID = actualBlockTree.getBlockID();
      assertTrue(prevBlockID < currentBlockID);
      prevBlockID = currentBlockID;

      assertEquals(expectedBlockTree.getBlockID(), actualBlockTree.getBlockID());
      assertEquals(expectedBlockTree.getDataChecksum(), actualBlockTree.getDataChecksum());
      assertEquals(expectedBlockTree.getDeleted(), actualBlockTree.getDeleted());

      long prevChunkOffset = -1;
      for (int chunkIndex = 0; chunkIndex < expectedBlockTree.getChunkMerkleTreeCount(); chunkIndex++) {
        ContainerProtos.ChunkMerkleTree expectedChunkTree = expectedBlockTree.getChunkMerkleTree(chunkIndex);
        ContainerProtos.ChunkMerkleTree actualChunkTree = actualBlockTree.getChunkMerkleTree(chunkIndex);

        // Chunks should be sorted by offset.
        long currentChunkOffset = actualChunkTree.getOffset();
        assertTrue(prevChunkOffset < currentChunkOffset);
        prevChunkOffset = currentChunkOffset;

        assertEquals(expectedChunkTree.getOffset(), actualChunkTree.getOffset());
        assertEquals(expectedChunkTree.getLength(), actualChunkTree.getLength());
        assertEquals(expectedChunkTree.getDataChecksum(), actualChunkTree.getDataChecksum());
        assertEquals(expectedChunkTree.getChecksumMatches(), actualChunkTree.getChecksumMatches());
      }
    }
  }

  /**
   * Builds a ChunkInfo object using the provided information. No new checksums are calculated, so this can be used
   * as either the leaves of pre-computed merkle trees that serve as expected values, or as building blocks to pass
   * to ContainerMerkleTree to have it build the whole tree from this information.
   *
   * @param indexInBlock   Which chunk number within a block this is. The chunk's offset is automatically calculated
   *                       from this based on a fixed length.
   * @param chunkChecksums The checksums within the chunk. Each is assumed to apply to a fixed value
   *                       "bytesPerChecksum" amount of data and are assumed to be contiguous.
   * @return The ChunkInfo proto object built from this information.
   */
  public static ContainerProtos.ChunkInfo buildChunk(ConfigurationSource config, int indexInBlock,
                                                     ByteBuffer... chunkChecksums) {
    final long chunkSize = (long) config.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY, ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_DEFAULT, StorageUnit.BYTES);
    final int bytesPerChecksum = config.getObject(OzoneClientConfig.class).getBytesPerChecksum();

    // Each chunk checksum is added under the same ChecksumData object.
    ContainerProtos.ChecksumData checksumData = ContainerProtos.ChecksumData.newBuilder()
        .setType(ContainerProtos.ChecksumType.CRC32)
        .setBytesPerChecksum(bytesPerChecksum)
        .addAllChecksums(Arrays.stream(chunkChecksums)
            .map(ByteString::copyFrom)
            .collect(Collectors.toList()))
        .build();

    return ContainerProtos.ChunkInfo.newBuilder()
        .setChecksumData(checksumData)
        .setChunkName("chunk")
        .setOffset(indexInBlock)
        .setLen(chunkSize)
        .build();
  }

  /**
   * This reads the checksum file for a container from the disk without synchronization/coordination between readers
   * and writers within a datanode.
   */
  public static ContainerProtos.ContainerChecksumInfo readChecksumFile(ContainerData data) throws IOException {
    try (InputStream inStream = Files.newInputStream(getContainerChecksumFile(data).toPath())) {
      return ContainerProtos.ContainerChecksumInfo.parseFrom(inStream);
    }
  }

  /**
   * Builds a {@link ContainerMerkleTreeWriter} representing arbitrary data. This can be used to test that the same
   * structure is preserved throughout serialization, deserialization, and API calls.
   */
  public static ContainerMerkleTreeWriter buildTestTree(ConfigurationSource conf) {
    return buildTestTree(conf, 5);
  }

  public static ContainerMerkleTreeWriter buildTestTree(ConfigurationSource conf, int numBlocks) {
    ContainerMerkleTreeWriter tree = new ContainerMerkleTreeWriter();

    byte byteValue = 1;
    for (int i = 0; i < numBlocks; i++) {
      long blockID = i + 1;
      for (int chunkIndex = 0; chunkIndex < 4; chunkIndex++) {
        tree.addChunks(blockID, true,
            buildChunk(conf, chunkIndex, ByteBuffer.wrap(new byte[]{byteValue++, byteValue++, byteValue++})));
      }
    }
    return tree;
  }

  /**
   * Builds a tree with continuous block IDs from 1 to numLiveBlocks, then writes marks the specified IDs within that
   * set of blocks as deleted.
   */
  public static ContainerProtos.ContainerMerkleTree buildTestTree(ConfigurationSource conf, int numLiveBlocks,
                                                        long... deletedBlockIDs) {

    ContainerMerkleTreeWriter treeWriter = buildTestTree(conf, numLiveBlocks);
    return treeWriter.addDeletedBlocks(getDeletedBlockData(conf, deletedBlockIDs), true);
  }

  public static List<BlockData> getDeletedBlockData(ConfigurationSource conf, long... blockIDs) {
    List<BlockData> deletedBlockData = new ArrayList<>();
    // Container ID within the block is not used in these tests.
    Arrays.stream(blockIDs).forEach(id -> deletedBlockData.add(buildBlockData(conf, 1, id)));
    return deletedBlockData;
  }

  /**
   * Returns a Pair of merkle tree and the expected container diff for that merkle tree.
   */
  public static Pair<ContainerProtos.ContainerMerkleTree, ContainerDiffReport>
      buildTestTreeWithMismatches(ContainerMerkleTreeWriter originalTree, int numMissingBlocks, int numMissingChunks,
                                  int numCorruptChunks) {

    ContainerProtos.ContainerMerkleTree.Builder treeBuilder = originalTree.toProto().toBuilder();
    ContainerDiffReport diff = new ContainerDiffReport(1);

    introduceMissingBlocks(treeBuilder, numMissingBlocks, diff);
    introduceMissingChunks(treeBuilder, numMissingChunks, diff);
    introduceCorruptChunks(treeBuilder, numCorruptChunks, diff);
    ContainerProtos.ContainerMerkleTree build = treeBuilder.build();
    return Pair.of(build, diff);
  }

  /**
   * Writes a ContainerMerkleTree proto directly into a container without using a ContainerMerkleTreeWriter.
   */
  public static void updateTreeProto(ContainerData data, ContainerProtos.ContainerMerkleTree tree)
      throws IOException {
    ContainerProtos.ContainerChecksumInfo checksumInfo = ContainerProtos.ContainerChecksumInfo.newBuilder()
        .setContainerID(data.getContainerID())
        .setContainerMerkleTree(tree).build();
    File checksumFile = getContainerChecksumFile(data);

    try (OutputStream outputStream = Files.newOutputStream(checksumFile.toPath())) {
      checksumInfo.writeTo(outputStream);
    } catch (IOException ex) {
      throw new IOException("Error occurred when writing container merkle tree for containerID "
          + data.getContainerID(), ex);
    }
    data.setDataChecksum(checksumInfo.getContainerMerkleTree().getDataChecksum());
  }

  /**
   * Introduces missing blocks by removing blocks sequentially from the tree.
   */
  private static void introduceMissingBlocks(ContainerProtos.ContainerMerkleTree.Builder treeBuilder,
      int numMissingBlocks, ContainerDiffReport diff) {
    Random random = new Random();
    for (int blockIndex = 0; blockIndex < numMissingBlocks; blockIndex++) {
      ContainerProtos.BlockMerkleTree blockMerkleTree = treeBuilder.getBlockMerkleTree(blockIndex);
      diff.addMissingBlock(blockMerkleTree);
      treeBuilder.removeBlockMerkleTree(blockIndex);
      treeBuilder.setDataChecksum(random.nextLong());
    }
  }

  /**
   * Introduces missing chunks by removing the first chunk from each block. If more chunks must be removed,
   * it will resume removing the next chunk from each block until numMissingChunks have been removed.
   */
  private static void introduceMissingChunks(ContainerProtos.ContainerMerkleTree.Builder treeBuilder,
       int numMissingChunks, ContainerDiffReport diff) {
    Random random = new Random();

    int numChunksRemoved = 0;
    boolean hasChunks = true;
    while (numChunksRemoved < numMissingChunks && hasChunks) {
      hasChunks = false;
      for (int blockIndex = 0; blockIndex < treeBuilder.getBlockMerkleTreeCount() &&
          numChunksRemoved < numMissingChunks; blockIndex++) {
        ContainerProtos.BlockMerkleTree.Builder blockBuilder = treeBuilder.getBlockMerkleTreeBuilder(blockIndex);
        if (blockBuilder.getChunkMerkleTreeCount() > 0) {
          ContainerProtos.ChunkMerkleTree chunkMerkleTree = blockBuilder.getChunkMerkleTree(0);
          diff.addMissingChunk(blockBuilder.getBlockID(), chunkMerkleTree);
          blockBuilder.removeChunkMerkleTree(0);
          blockBuilder.setDataChecksum(random.nextLong());
          treeBuilder.setDataChecksum(random.nextLong());
          hasChunks = true;
          numChunksRemoved++;
        }
      }
    }
    // Make sure we removed the expected number of chunks.
    assertTrue(hasChunks);
  }

  /**
   * Introduces corrupt chunks by corrupting the first chunk from each block. If more chunks must be corrupted,
   * it will resume corrupting the next chunk from each block until numCorruptChunks have been corrupted.
   */
  private static void introduceCorruptChunks(ContainerProtos.ContainerMerkleTree.Builder treeBuilder,
      int numCorruptChunks, ContainerDiffReport diff) {
    Random random = new Random();
    boolean hasChunks = true;
    int numChunksCorrupted = 0;
    int chunkIndex = 0;

    while (numChunksCorrupted < numCorruptChunks && hasChunks) {
      hasChunks = false;
      for (int blockIndex = 0; blockIndex < treeBuilder.getBlockMerkleTreeCount() &&
          numChunksCorrupted < numCorruptChunks; blockIndex++) {
        ContainerProtos.BlockMerkleTree.Builder blockBuilder = treeBuilder.getBlockMerkleTreeBuilder(blockIndex);
        if (chunkIndex < blockBuilder.getChunkMerkleTreeCount()) {
          ContainerProtos.ChunkMerkleTree.Builder chunkBuilder = blockBuilder.getChunkMerkleTreeBuilder(chunkIndex);
          diff.addCorruptChunk(blockBuilder.getBlockID(), chunkBuilder.build());
          chunkBuilder.setDataChecksum(random.nextLong());
          chunkBuilder.setChecksumMatches(false);
          blockBuilder.setDataChecksum(random.nextLong());
          treeBuilder.setDataChecksum(random.nextLong());
          hasChunks = true;
          numChunksCorrupted++;
        }
      }
      chunkIndex++;
    }
    // Make sure we corrupted the expected number of chunks.
    assertTrue(hasChunks);
  }

  public static void assertContainerDiffMatch(ContainerDiffReport expectedDiff,
                                              ContainerDiffReport actualDiff) {
    assertNotNull(expectedDiff, "Expected diff is null");
    assertNotNull(actualDiff, "Actual diff is null");
    assertEquals(expectedDiff.getMissingBlocks().size(), actualDiff.getMissingBlocks().size(),
        "Mismatch in number of missing blocks");
    assertEquals(expectedDiff.getMissingChunks().size(), actualDiff.getMissingChunks().size(),
        "Mismatch in number of missing chunks");
    assertEquals(expectedDiff.getCorruptChunks().size(), actualDiff.getCorruptChunks().size(),
        "Mismatch in number of corrupt chunks");

    List<ContainerProtos.BlockMerkleTree> expectedMissingBlocks = expectedDiff.getMissingBlocks().stream().sorted(
        Comparator.comparing(ContainerProtos.BlockMerkleTree::getBlockID)).collect(Collectors.toList());
    List<ContainerProtos.BlockMerkleTree> actualMissingBlocks = actualDiff.getMissingBlocks().stream().sorted(
        Comparator.comparing(ContainerProtos.BlockMerkleTree::getBlockID)).collect(Collectors.toList());
    for (int i = 0; i < expectedMissingBlocks.size(); i++) {
      ContainerProtos.BlockMerkleTree expectedBlockMerkleTree = expectedMissingBlocks.get(i);
      ContainerProtos.BlockMerkleTree actualBlockMerkleTree = actualMissingBlocks.get(i);
      assertEquals(expectedBlockMerkleTree.getBlockID(), actualBlockMerkleTree.getBlockID());
      assertEquals(expectedBlockMerkleTree.getChunkMerkleTreeCount(),
          actualBlockMerkleTree.getChunkMerkleTreeCount());
      assertEquals(expectedBlockMerkleTree.getDataChecksum(), actualBlockMerkleTree.getDataChecksum());
      assertEqualsChunkMerkleTree(expectedBlockMerkleTree.getChunkMerkleTreeList(),
          actualBlockMerkleTree.getChunkMerkleTreeList(), expectedBlockMerkleTree.getBlockID());
    }

    // Check missing chunks
    Map<Long, List<ContainerProtos.ChunkMerkleTree>> expectedMissingChunks = expectedDiff.getMissingChunks();
    Map<Long, List<ContainerProtos.ChunkMerkleTree>> actualMissingChunks = actualDiff.getMissingChunks();

    for (Map.Entry<Long, List<ContainerProtos.ChunkMerkleTree>> entry : expectedMissingChunks.entrySet()) {
      Long blockId = entry.getKey();
      List<ContainerProtos.ChunkMerkleTree> expectedChunks = entry.getValue().stream().sorted(
          Comparator.comparing(ContainerProtos.ChunkMerkleTree::getOffset)).collect(Collectors.toList());
      List<ContainerProtos.ChunkMerkleTree> actualChunks = actualMissingChunks.get(blockId).stream().sorted(
          Comparator.comparing(ContainerProtos.ChunkMerkleTree::getOffset)).collect(Collectors.toList());

      assertNotNull(actualChunks, "Missing chunks for block " + blockId + " not found in actual diff");
      assertEquals(expectedChunks.size(), actualChunks.size(),
          "Mismatch in number of missing chunks for block " + blockId);
      assertEqualsChunkMerkleTree(expectedChunks, actualChunks, blockId);
    }

    // Check corrupt chunks
    Map<Long, List<ContainerProtos.ChunkMerkleTree>> expectedCorruptChunks = expectedDiff.getCorruptChunks();
    Map<Long, List<ContainerProtos.ChunkMerkleTree>> actualCorruptChunks = actualDiff.getCorruptChunks();

    for (Map.Entry<Long, List<ContainerProtos.ChunkMerkleTree>> entry : expectedCorruptChunks.entrySet()) {
      Long blockId = entry.getKey();
      List<ContainerProtos.ChunkMerkleTree> expectedChunks = entry.getValue().stream().sorted(
          Comparator.comparing(ContainerProtos.ChunkMerkleTree::getOffset)).collect(Collectors.toList());
      List<ContainerProtos.ChunkMerkleTree> actualChunks = actualCorruptChunks.get(blockId).stream().sorted(
          Comparator.comparing(ContainerProtos.ChunkMerkleTree::getOffset)).collect(Collectors.toList());

      assertNotNull(actualChunks, "Corrupt chunks for block " + blockId + " not found in actual diff");
      assertEquals(expectedChunks.size(), actualChunks.size(),
          "Mismatch in number of corrupt chunks for block " + blockId);
      assertEqualsChunkMerkleTree(expectedChunks, actualChunks, blockId);
    }
  }

  private static void assertEqualsChunkMerkleTree(List<ContainerProtos.ChunkMerkleTree> expectedChunkMerkleTreeList,
                                                  List<ContainerProtos.ChunkMerkleTree> actualChunkMerkleTreeList,
                                                  Long blockId) {
    assertEquals(expectedChunkMerkleTreeList.size(), actualChunkMerkleTreeList.size());
    for (int j = 0; j < expectedChunkMerkleTreeList.size(); j++) {
      ContainerProtos.ChunkMerkleTree expectedChunk = expectedChunkMerkleTreeList.get(j);
      ContainerProtos.ChunkMerkleTree actualChunk = actualChunkMerkleTreeList.get(j);
      assertEquals(expectedChunk.getOffset(), actualChunk.getOffset(), "Mismatch in chunk offset for block "
          + blockId);
      assertEquals(expectedChunk.getDataChecksum(), actualChunk.getDataChecksum(),
          "Mismatch in chunk checksum for block " + blockId);
    }
  }

  /**
   * This function checks whether the container checksum file exists for a container in a given datanode.
   */
  public static boolean containerChecksumFileExists(HddsDatanodeService hddsDatanode, long containerID) {
    OzoneContainer ozoneContainer = hddsDatanode.getDatanodeStateMachine().getContainer();
    Container<?> container = ozoneContainer.getController().getContainer(containerID);
    return getContainerChecksumFile(container.getContainerData()).exists();
  }

  /**
   * This function verifies that the in-memory data checksum matches the one stored in the container data and
   * the RocksDB.
   *
   * @param containerData The container data to verify.
   * @param conf          The Ozone configuration.
   * @throws IOException If an error occurs while reading the checksum info or RocksDB.
   */
  public static void verifyAllDataChecksumsMatch(KeyValueContainerData containerData, OzoneConfiguration conf)
      throws IOException {
    assertNotNull(containerData, "Container data should not be null");
    ContainerProtos.ContainerChecksumInfo containerChecksumInfo = ContainerChecksumTreeManager
        .readChecksumInfo(containerData);
    assertNotNull(containerChecksumInfo);
    long dataChecksum = containerChecksumInfo.getContainerMerkleTree().getDataChecksum();
    Long dbDataChecksum;
    try (DBHandle dbHandle = BlockUtils.getDB(containerData, conf)) {
      dbDataChecksum = dbHandle.getStore().getMetadataTable().get(containerData.getContainerDataChecksumKey());
    }

    if (containerData.getDataChecksum() == 0) {
      assertEquals(containerData.getDataChecksum(), dataChecksum);
      // RocksDB checksum can be null if the file doesn't exist or when the file is created by
      // the block deleting service. 0 checksum will be stored when the container is loaded without
      // merkle tree.
      assertThat(dbDataChecksum).isIn(0L, null);
    } else {
      // In-Memory, Container Merkle Tree file, RocksDB checksum should be equal
      assertEquals(containerData.getDataChecksum(), dataChecksum, "In-memory data checksum should match " +
          "the one in the checksum file.");
      assertEquals(dbDataChecksum, dataChecksum);
    }
  }

  public static BlockData buildBlockData(ConfigurationSource config, long containerID, long blockID) {
    BlockData blockData = new BlockData(new BlockID(containerID, blockID));
    byte byteValue = 0;
    blockData.addChunk(buildChunk(config, 0, ByteBuffer.wrap(new byte[]{byteValue++, byteValue++, byteValue++})));
    blockData.addChunk(buildChunk(config, 1, ByteBuffer.wrap(new byte[]{byteValue++, byteValue++, byteValue++})));
    blockData.addChunk(buildChunk(config, 2, ByteBuffer.wrap(new byte[]{byteValue++, byteValue++, byteValue++})));
    return blockData;
  }
}
