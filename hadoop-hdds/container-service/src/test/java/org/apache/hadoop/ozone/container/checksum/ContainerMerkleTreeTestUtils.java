/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.checksum;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Helper methods for testing container checksum tree files and container reconciliation.
 */
public final class ContainerMerkleTreeTestUtils {
  private ContainerMerkleTreeTestUtils() { }

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
      assertEquals(expectedBlockTree.getBlockChecksum(), actualBlockTree.getBlockChecksum());

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
        assertEquals(expectedChunkTree.getChunkChecksum(), actualChunkTree.getChunkChecksum());
      }
    }
  }

  /**
   * Builds a ChunkInfo object using the provided information. No new checksums are calculated, so this can be used
   * as either the leaves of pre-computed merkle trees that serve as expected values, or as building blocks to pass
   * to ContainerMerkleTree to have it build the whole tree from this information.
   *
   * @param indexInBlock Which chunk number within a block this is. The chunk's offset is automatically calculated
   *     from this based on a fixed length.
   * @param chunkChecksums The checksums within the chunk. Each is assumed to apply to a fixed value
   *     "bytesPerChecksum" amount of data and are assumed to be contiguous.
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
    try (FileInputStream inStream = new FileInputStream(ContainerChecksumTreeManager.getContainerChecksumFile(data))) {
      return ContainerProtos.ContainerChecksumInfo.parseFrom(inStream);
    }
  }

  /**
   * Builds a {@link ContainerMerkleTree} representing arbitrary data. This can be used to test that the same
   * structure is preserved throughout serialization, deserialization, and API calls.
   */
  public static ContainerMerkleTree buildTestTree(ConfigurationSource conf) {
    ContainerMerkleTree tree = new ContainerMerkleTree();
    byte byteValue = 1;
    for (int blockIndex = 1; blockIndex <= 5; blockIndex++) {
      List<ContainerProtos.ChunkInfo> chunks = new ArrayList<>();
      for (int chunkIndex = 0; chunkIndex < 4; chunkIndex++) {
        chunks.add(buildChunk(conf, chunkIndex, ByteBuffer.wrap(new byte[]{byteValue++, byteValue++, byteValue++})));
      }
      tree.addChunks(blockIndex, chunks);
    }
    return tree;
  }

  /**
   * Returns a Pair of merkle tree and the expected container diff for that merkle tree.
   */
  public static Pair<ContainerMerkleTree, ContainerChecksumTreeManager.ContainerDiff> buildTestTreeWithMismatches(
      ConfigurationSource conf, int numMissingBlocks, int numMissingChunks, int numCorruptChunks) {

    ContainerMerkleTree tree = buildTestTree(conf);
    ContainerChecksumTreeManager.ContainerDiff diff = new ContainerChecksumTreeManager.ContainerDiff();
    Random random = new Random();

    List<Long> blockIds = new ArrayList<>(Arrays.asList(1L, 2L, 3L, 4L, 5L));
    introduceMissingBlocks(tree, diff, blockIds, numMissingBlocks, random);
    introduceMissingChunks(tree, diff, blockIds, numMissingChunks, random);
    introduceCorruptChunks(tree, diff, blockIds, numCorruptChunks, random, conf);

    return Pair.of(tree, diff);
  }

  private static void introduceMissingBlocks(ContainerMerkleTree tree,
                                             ContainerChecksumTreeManager.ContainerDiff diff,
                                             List<Long> blockIds,
                                             int numMissingBlocks,
                                             Random random) {
    for (int i = 0; i < numMissingBlocks && !blockIds.isEmpty(); i++) {
      int index = random.nextInt(blockIds.size());
      long blockId = blockIds.remove(index);
      ContainerMerkleTree.BlockMerkleTree blockTree = tree.remove(blockId);
      diff.addMissingBlock(blockTree.toProto());
    }
  }

  private static void introduceMissingChunks(ContainerMerkleTree tree,
                                             ContainerChecksumTreeManager.ContainerDiff diff,
                                             List<Long> blockIds,
                                             int numMissingChunks,
                                             Random random) {
    for (int i = 0; i < numMissingChunks && !blockIds.isEmpty(); i++) {
      long blockId = blockIds.get(random.nextInt(blockIds.size()));
      ContainerMerkleTree.BlockMerkleTree blockTree = tree.get(blockId);
      List<Long> chunkOffsets = getChunkOffsets(blockTree);

      if (!chunkOffsets.isEmpty()) {
        long offset = chunkOffsets.remove(random.nextInt(chunkOffsets.size()));
        ContainerMerkleTree.ChunkMerkleTree chunkTree = blockTree.removeChunk(offset);
        diff.addMissingChunk(blockId, chunkTree.toProto());
      }
    }
  }

  private static void introduceCorruptChunks(ContainerMerkleTree tree,
                                             ContainerChecksumTreeManager.ContainerDiff diff,
                                             List<Long> blockIds,
                                             int numCorruptChunks,
                                             Random random,
                                             ConfigurationSource conf) {
    // Create a map to keep track of corrupted chunks per block
    Map<Long, Set<Long>> corruptedChunksPerBlock = new HashMap<>();

    int corruptionsIntroduced = 0;
    while (corruptionsIntroduced < numCorruptChunks && !blockIds.isEmpty()) {
      // Randomly select a block
      int blockIndex = random.nextInt(blockIds.size());
      long blockId = blockIds.get(blockIndex);
      ContainerMerkleTree.BlockMerkleTree blockTree = tree.get(blockId);

      // Get available chunk offsets for this block
      List<Long> availableChunkOffsets = getChunkOffsets(blockTree);

      // Remove already corrupted chunks for this block
      availableChunkOffsets.removeAll(corruptedChunksPerBlock.getOrDefault(blockId, new HashSet<>()));

      if (!availableChunkOffsets.isEmpty()) {
        // Randomly select an available chunk offset
        int chunkIndex = random.nextInt(availableChunkOffsets.size());
        long offset = availableChunkOffsets.get(chunkIndex);

        // Remove the original chunk
        ContainerMerkleTree.ChunkMerkleTree chunkMerkleTree = blockTree.removeChunk(offset);

        // Create and add corrupt chunk
        ContainerProtos.ChunkInfo corruptChunk = buildChunk(conf, (int) offset, ByteBuffer.wrap(new byte[]{5, 10, 15}));
        tree.addChunk(blockId, corruptChunk);
        blockTree.setHealthy(offset, false);
        diff.addCorruptChunk(blockId, chunkMerkleTree.toProto());

        // Mark this chunk as corrupted for this block
        corruptedChunksPerBlock.computeIfAbsent(blockId, k -> new HashSet<>()).add(offset);

        corruptionsIntroduced++;
      } else {
        // If no available chunks in this block, remove it from consideration
        blockIds.remove(blockIndex);
      }
    }
  }

  private static List<Long> getChunkOffsets(ContainerMerkleTree.BlockMerkleTree blockTree) {
    return blockTree.toProto().getChunkMerkleTreeList().stream()
        .map(ContainerProtos.ChunkMerkleTree::getOffset)
        .collect(Collectors.toList());
  }

  public static void assertContainerDiffMatch(ContainerChecksumTreeManager.ContainerDiff expectedDiff,
      ContainerChecksumTreeManager.ContainerDiff actualDiff) {
    assertNotNull(expectedDiff, "Expected diff is null");
    assertNotNull(actualDiff, "Actual diff is null");
    assertEquals(expectedDiff.getMissingBlocks().size(), actualDiff.getMissingBlocks().size(),
        "Mismatch in number of missing blocks");
    assertEquals(expectedDiff.getMissingChunks().size(), actualDiff.getMissingChunks().size(),
        "Mismatch in number of missing chunks");
    assertEquals(expectedDiff.getCorruptChunks().size(), actualDiff.getCorruptChunks().size(),
        "Mismatch in number of corrupt blocks");

    List<ContainerProtos.BlockMerkleTree> expectedMissingBlocks = expectedDiff.getMissingBlocks().stream().sorted(
                Comparator.comparing(ContainerProtos.BlockMerkleTree::getBlockID)).collect(Collectors.toList());
    List<ContainerProtos.BlockMerkleTree> actualMissingBlocks = expectedDiff.getMissingBlocks().stream().sorted(
        Comparator.comparing(ContainerProtos.BlockMerkleTree::getBlockID)).collect(Collectors.toList());
    for (int i = 0; i < expectedMissingBlocks.size(); i++) {
      ContainerProtos.BlockMerkleTree expectedBlockMerkleTree = expectedMissingBlocks.get(i);
      ContainerProtos.BlockMerkleTree actualBlockMerkleTree = actualMissingBlocks.get(i);
      assertEquals(expectedBlockMerkleTree.getBlockID(), actualBlockMerkleTree.getBlockID());
      assertEquals(expectedBlockMerkleTree.getChunkMerkleTreeCount(),
          actualBlockMerkleTree.getChunkMerkleTreeCount());
      assertEquals(expectedBlockMerkleTree.getBlockChecksum(), actualBlockMerkleTree.getBlockChecksum());
      assertEqualsChunkMerkleTree(expectedBlockMerkleTree.getChunkMerkleTreeList(),
          actualBlockMerkleTree.getChunkMerkleTreeList());
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
      assertEqualsChunkMerkleTree(expectedChunks, actualChunks);
      for (int i = 0; i < expectedChunks.size(); i++) {
        ContainerProtos.ChunkMerkleTree expectedChunk = expectedChunks.get(i);
        ContainerProtos.ChunkMerkleTree actualChunk = actualChunks.get(i);
        assertEquals(expectedChunk.getOffset(), actualChunk.getOffset(),
            "Mismatch in chunk offset for block " + blockId);
        assertEquals(expectedChunk.getChunkChecksum(), actualChunk.getChunkChecksum(),
            "Mismatch in chunk checksum for block " + blockId);
      }
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
      assertEqualsChunkMerkleTree(expectedChunks, actualChunks);
    }
  }

  private static void assertEqualsChunkMerkleTree(List<ContainerProtos.ChunkMerkleTree> expectedChunkMerkleTreeList,
                                                  List<ContainerProtos.ChunkMerkleTree> actualChunkMerkleTreeList) {
    assertEquals(expectedChunkMerkleTreeList.size(), actualChunkMerkleTreeList.size());
    for (int j = 0; j < expectedChunkMerkleTreeList.size(); j++) {
      ContainerProtos.ChunkMerkleTree expectedChunk = expectedChunkMerkleTreeList.get(j);
      ContainerProtos.ChunkMerkleTree actualChunk = actualChunkMerkleTreeList.get(j);
      assertEquals(expectedChunk.getOffset(), actualChunk.getOffset(), "Mismatch in chunk offset");
      assertEquals(expectedChunk.getChunkChecksum(), actualChunk.getChunkChecksum(), "Mismatch in chunk checksum");
    }
  }

  /**
   * This function checks whether the container checksum file exists.
   */
  public static boolean containerChecksumFileExists(HddsDatanodeService hddsDatanode,
                                                    ContainerInfo containerInfo) {
    OzoneContainer ozoneContainer = hddsDatanode.getDatanodeStateMachine().getContainer();
    Container container = ozoneContainer.getController().getContainer(containerInfo.getContainerID());
    return ContainerChecksumTreeManager.checksumFileExist(container);
  }
}
