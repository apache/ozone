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
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
            .setOffset(indexInBlock * chunkSize)
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
    final long blockID1 = 1;
    final long blockID2 = 2;
    final long blockID3 = 3;
    ContainerProtos.ChunkInfo b1c1 = buildChunk(conf, 0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ContainerProtos.ChunkInfo b1c2 = buildChunk(conf, 1, ByteBuffer.wrap(new byte[]{4, 5, 6}));
    ContainerProtos.ChunkInfo b2c1 = buildChunk(conf, 0, ByteBuffer.wrap(new byte[]{7, 8, 9}));
    ContainerProtos.ChunkInfo b2c2 = buildChunk(conf, 1, ByteBuffer.wrap(new byte[]{12, 11, 10}));
    ContainerProtos.ChunkInfo b3c1 = buildChunk(conf, 0, ByteBuffer.wrap(new byte[]{13, 14, 15}));
    ContainerProtos.ChunkInfo b3c2 = buildChunk(conf, 1, ByteBuffer.wrap(new byte[]{16, 17, 18}));

    ContainerMerkleTree tree = new ContainerMerkleTree();
    tree.addChunks(blockID1, Arrays.asList(b1c1, b1c2));
    tree.addChunks(blockID2, Arrays.asList(b2c1, b2c2));
    tree.addChunks(blockID3, Arrays.asList(b3c1, b3c2));

    return tree;
  }

  public static ContainerMerkleTree buildTestTreeWithMismatches(ConfigurationSource conf, boolean missingBlocks,
                                                                boolean missingChunks, boolean corruptChunks) {
    final long blockID1 = 1;
    final long blockID2 = 2;
    final long blockID3 = 3;
    ContainerProtos.ChunkInfo b1c1 = buildChunk(conf, 0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ContainerProtos.ChunkInfo b1c2 = buildChunk(conf, 1, ByteBuffer.wrap(new byte[]{4, 5, 6}));
    ContainerProtos.ChunkInfo b2c1 = buildChunk(conf, 0, ByteBuffer.wrap(new byte[]{7, 8, 9}));
    ContainerProtos.ChunkInfo b2c2 = buildChunk(conf, 1, ByteBuffer.wrap(new byte[]{12, 11, 10}));
    ContainerProtos.ChunkInfo b3c1 = buildChunk(conf, 0, ByteBuffer.wrap(new byte[]{13, 14, 15}));
    ContainerProtos.ChunkInfo b3c2 = buildChunk(conf, 1, ByteBuffer.wrap(new byte[]{16, 17, 18}));

    ContainerMerkleTree tree = new ContainerMerkleTree();
    tree.addChunks(blockID1, Arrays.asList(b1c1, b1c2));

    if (corruptChunks) {
      ContainerProtos.ChunkInfo corruptB1c2 = buildChunk(conf, 1, ByteBuffer.wrap(new byte[]{2, 4, 6}));
      tree.addChunks(blockID1, Arrays.asList(b1c1, corruptB1c2));
    } else {
      tree.addChunks(blockID1, Arrays.asList(b1c1, b1c2));
    }

    if (missingChunks) {
      tree.addChunks(blockID2, Arrays.asList(b2c1));
    } else {
      tree.addChunks(blockID2, Arrays.asList(b2c1, b2c2));
    }

    if (!missingBlocks) {
      tree.addChunks(blockID3, Arrays.asList(b3c1, b3c2));
    }
    return tree;
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
