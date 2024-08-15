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
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
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
   * This function checks whether the container checksum file exists.
   */
  public static boolean containerChecksumFileExists(HddsDatanodeService hddsDatanode,
                                                    ContainerInfo containerInfo)
      throws IOException {
    OzoneContainer ozoneContainer = hddsDatanode.getDatanodeStateMachine().getContainer();
    Container container = ozoneContainer.getController().getContainer(containerInfo.getContainerID());
    File containerChecksumFile = ContainerChecksumTreeManager.getContainerChecksumFile(container.getContainerData());
    return containerChecksumFile.exists();
  }

  public static ContainerProtos.ContainerMerkleTree buildContainerMerkleTree(KeyValueContainerData containerData,
                                                                             ConfigurationSource conf)
      throws IOException {
    ContainerMerkleTree containerMerkleTree = new ContainerMerkleTree();
    try (DBHandle dbHandle = BlockUtils.getDB(containerData, conf);
         BlockIterator<BlockData> blockIterator = dbHandle.getStore().
             getBlockIterator(containerData.getContainerID())) {
      while (blockIterator.hasNext()) {
        BlockData blockData = blockIterator.nextBlock();
        List<ContainerProtos.ChunkInfo> chunkInfos = blockData.getChunks();
        containerMerkleTree.addChunks(blockData.getLocalID(), chunkInfos);
      }
    }
    return containerMerkleTree.toProto();
  }
}
