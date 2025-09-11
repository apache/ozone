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

package org.apache.hadoop.ozone.container.keyvalue.impl;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.apache.hadoop.ozone.OzoneConsts.INCREMENTAL_CHUNK_LIST;
import static org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil.isSameSchemaVersion;
import static org.apache.hadoop.ozone.container.keyvalue.impl.BlockManagerImpl.FULL_CHUNK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.ContainerTestVersionInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.io.TempDir;

/**
 * This class is used to test key related operations on the container.
 */
public class TestBlockManagerImpl {

  @TempDir
  private Path folder;
  private OzoneConfiguration config;
  private String scmId = UUID.randomUUID().toString();
  private KeyValueContainer keyValueContainer;
  private BlockData blockData;
  private BlockData blockData1;
  private BlockManagerImpl blockManager;

  private ContainerLayoutVersion layout;
  private String schemaVersion;

  private void initTest(ContainerTestVersionInfo versionInfo)
      throws Exception {
    this.layout = versionInfo.getLayout();
    this.schemaVersion = versionInfo.getSchemaVersion();
    this.config = new OzoneConfiguration();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, config);
    initialize();
  }

  private void initialize() throws Exception {
    UUID datanodeId = UUID.randomUUID();
    HddsVolume hddsVolume = new HddsVolume.Builder(folder.toString())
        .conf(config)
        .datanodeUuid(datanodeId.toString())
        .build();
    StorageVolumeUtil.checkVolume(hddsVolume, scmId, scmId, config,
        null, null);

    VolumeSet volumeSet = mock(MutableVolumeSet.class);

    RoundRobinVolumeChoosingPolicy volumeChoosingPolicy = mock(RoundRobinVolumeChoosingPolicy.class);
    when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenReturn(hddsVolume);

    KeyValueContainerData keyValueContainerData = new KeyValueContainerData(1L,
        layout,
        (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
        datanodeId.toString());

    keyValueContainer = new KeyValueContainer(
        keyValueContainerData, config);

    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);

    // Creating BlockData
    BlockID blockID = new BlockID(1L, 1L);
    blockData = new BlockData(blockID);
    blockData.addMetadata(OzoneConsts.VOLUME, OzoneConsts.OZONE);
    blockData.addMetadata(OzoneConsts.OWNER,
        OzoneConsts.OZONE_SIMPLE_HDFS_USER);
    List<ContainerProtos.ChunkInfo> chunkList = new ArrayList<>();
    ChunkInfo info = new ChunkInfo(String.format("%d.data.%d", blockID
        .getLocalID(), 0), 0, 1024);
    chunkList.add(info.getProtoBufMessage());
    blockData.setChunks(chunkList);

    // Creating BlockData
    blockID = new BlockID(1L, 2L);
    blockData1 = new BlockData(blockID);
    blockData1.addMetadata(OzoneConsts.VOLUME, OzoneConsts.OZONE);
    blockData1.addMetadata(OzoneConsts.OWNER,
        OzoneConsts.OZONE_SIMPLE_HDFS_USER);
    List<ContainerProtos.ChunkInfo> chunkList1 = new ArrayList<>();
    ChunkInfo info1 = new ChunkInfo(String.format("%d.data.%d", blockID
        .getLocalID(), 0), 0, 1024);
    chunkList1.add(info1.getProtoBufMessage());
    blockData1.setChunks(chunkList1);
    blockData1.setBlockCommitSequenceId(1);

    // Create KeyValueContainerManager
    blockManager = new BlockManagerImpl(config);

  }

  @AfterEach
  public void cleanup() {
    BlockUtils.shutdownCache(config);
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testPutBlock(ContainerTestVersionInfo versionInfo)
      throws Exception {
    initTest(versionInfo);
    assertEquals(0, keyValueContainer.getContainerData().getBlockCount());
    //Put Block with bcsId != 0
    blockManager.putBlock(keyValueContainer, blockData1);

    BlockData fromGetBlockData;
    //Check Container's bcsId
    fromGetBlockData = blockManager.getBlock(keyValueContainer,
        blockData1.getBlockID());
    assertEquals(1, keyValueContainer.getContainerData().getBlockCount());
    assertEquals(1,
        keyValueContainer.getContainerData().getBlockCommitSequenceId());
    assertEquals(1, fromGetBlockData.getBlockCommitSequenceId());

    //Put Block with bcsId == 0
    blockManager.putBlock(keyValueContainer, blockData);

    //Check Container's bcsId
    fromGetBlockData = blockManager.getBlock(keyValueContainer,
        blockData.getBlockID());
    assertEquals(2, keyValueContainer.getContainerData().getBlockCount());
    assertEquals(0, fromGetBlockData.getBlockCommitSequenceId());
    assertEquals(1,
        keyValueContainer.getContainerData().getBlockCommitSequenceId());

  }

  @ContainerTestVersionInfo.ContainerTest
  public void testPutAndGetBlock(ContainerTestVersionInfo versionInfo)
      throws Exception {
    initTest(versionInfo);
    assertEquals(0, keyValueContainer.getContainerData().getBlockCount());
    //Put Block
    blockManager.putBlock(keyValueContainer, blockData);

    assertEquals(1, keyValueContainer.getContainerData().getBlockCount());
    //Get Block
    BlockData fromGetBlockData = blockManager.getBlock(keyValueContainer,
        blockData.getBlockID());

    assertEquals(blockData.getContainerID(), fromGetBlockData.getContainerID());
    assertEquals(blockData.getLocalID(), fromGetBlockData.getLocalID());
    assertEquals(blockData.getChunks().size(),
        fromGetBlockData.getChunks().size());
    assertEquals(blockData.getMetadata().size(), fromGetBlockData.getMetadata()
        .size());

  }

  @ContainerTestVersionInfo.ContainerTest
  public void testPutBlockForClosed(ContainerTestVersionInfo versionInfo)
      throws Exception {
    initTest(versionInfo);
    KeyValueContainerData containerData = keyValueContainer.getContainerData();
    assertEquals(0, containerData.getBlockCount());
    keyValueContainer.close();
    assertEquals(CLOSED, keyValueContainer.getContainerState());

    try (DBHandle db = BlockUtils.getDB(containerData, config)) {
      // 1. Put Block with bcsId = 1, Overwrite BCS ID = true
      blockData1 = createBlockData(1L, 2L, 1, 0, 2048, 1);
      blockManager.putBlockForClosedContainer(keyValueContainer, blockData1, true);

      BlockData fromGetBlockData;
      //Check Container's bcsId
      fromGetBlockData = blockManager.getBlock(keyValueContainer, blockData.getBlockID());
      assertEquals(1, containerData.getBlockCount());
      assertEquals(1, containerData.getBlockCommitSequenceId());
      assertEquals(1, fromGetBlockData.getBlockCommitSequenceId());
      assertEquals(1, db.getStore().getMetadataTable().get(containerData.getBcsIdKey()));
      assertEquals(1, db.getStore().getMetadataTable().get(containerData.getBlockCountKey()));

      // 2. Put Block with bcsId = 2, Overwrite BCS ID = false
      BlockData blockData2 = createBlockData(1L, 3L, 1, 0, 2048, 2);
      blockManager.putBlockForClosedContainer(keyValueContainer, blockData2, false);

      // The block should be written, but we won't be able to read it, As expected BcsId < container's BcsId
      // fails during block read.
      assertThrows(StorageContainerException.class, () -> blockManager
          .getBlock(keyValueContainer, blockData2.getBlockID()));
      fromGetBlockData = db.getStore().getBlockDataTable().get(containerData.getBlockKey(blockData2.getLocalID()));
      assertEquals(2, containerData.getBlockCount());
      // BcsId should still be 1, as the BcsId is not overwritten
      assertEquals(1, containerData.getBlockCommitSequenceId());
      assertEquals(2, fromGetBlockData.getBlockCommitSequenceId());
      assertEquals(2, db.getStore().getMetadataTable().get(containerData.getBlockCountKey()));
      assertEquals(1, db.getStore().getMetadataTable().get(containerData.getBcsIdKey()));

      // 3. Put Block with bcsId = 2, Overwrite BCS ID = true
      // This should succeed as we are overwriting the BcsId, The container BcsId should be updated to 2
      // The block count should not change.
      blockManager.putBlockForClosedContainer(keyValueContainer, blockData2, true);
      fromGetBlockData = blockManager.getBlock(keyValueContainer, blockData2.getBlockID());
      assertEquals(2, containerData.getBlockCount());
      assertEquals(2, containerData.getBlockCommitSequenceId());
      assertEquals(2, fromGetBlockData.getBlockCommitSequenceId());
      assertEquals(2, db.getStore().getMetadataTable().get(containerData.getBlockCountKey()));
      assertEquals(2, db.getStore().getMetadataTable().get(containerData.getBcsIdKey()));

      // 4. Put Block with bcsId = 1 < container bcsId, Overwrite BCS ID = true
      // We are overwriting an existing block with lower bcsId than container bcsId. Container bcsId should not change
      BlockData blockData3 = createBlockData(1L, 1L, 1, 0, 2048, 1);
      blockManager.putBlockForClosedContainer(keyValueContainer, blockData3, true);
      fromGetBlockData = blockManager.getBlock(keyValueContainer, blockData3.getBlockID());
      assertEquals(3, containerData.getBlockCount());
      assertEquals(2, containerData.getBlockCommitSequenceId());
      assertEquals(1, fromGetBlockData.getBlockCommitSequenceId());
      assertEquals(3, db.getStore().getMetadataTable().get(containerData.getBlockCountKey()));
      assertEquals(2, db.getStore().getMetadataTable().get(containerData.getBcsIdKey()));

      // writeChunk updates the in-memory state of the used bytes, putBlock persists the in-memory state to the DB.
      // We are only doing putBlock without writeChunk, the used bytes should be 0.
      assertEquals(0, db.getStore().getMetadataTable().get(containerData.getBytesUsedKey()));
    }
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testListBlock(ContainerTestVersionInfo versionInfo)
      throws Exception {
    initTest(versionInfo);
    blockManager.putBlock(keyValueContainer, blockData);
    List<BlockData> listBlockData = blockManager.listBlock(
        keyValueContainer, 1, 10);
    assertNotNull(listBlockData);
    assertEquals(1, listBlockData.size());

    for (long i = 2; i <= 10; i++) {
      BlockID blockID = new BlockID(1L, i);
      blockData = new BlockData(blockID);
      blockData.addMetadata(OzoneConsts.VOLUME, OzoneConsts.OZONE);
      blockData.addMetadata(OzoneConsts.OWNER,
          OzoneConsts.OZONE_SIMPLE_HDFS_USER);
      List<ContainerProtos.ChunkInfo> chunkList = new ArrayList<>();
      ChunkInfo info = new ChunkInfo(String.format("%d.data.%d", blockID
          .getLocalID(), 0), 0, 1024);
      chunkList.add(info.getProtoBufMessage());
      blockData.setChunks(chunkList);
      blockManager.putBlock(keyValueContainer, blockData);
    }

    listBlockData = blockManager.listBlock(
        keyValueContainer, 1, 10);
    assertNotNull(listBlockData);
    assertEquals(10, listBlockData.size());
  }

  private BlockData createBlockData(long containerID, long blockNo,
      int chunkID, long offset, long len, long bcsID)
      throws IOException {
    BlockID blockID = new BlockID(containerID, blockNo);
    blockData = new BlockData(blockID);
    List<ContainerProtos.ChunkInfo> chunkList1 = new ArrayList<>();
    ChunkInfo info1 = new ChunkInfo(String.format("%d_chunk_%d", blockID
        .getLocalID(), chunkID), offset, len);
    chunkList1.add(info1.getProtoBufMessage());
    blockData.setChunks(chunkList1);
    blockData.setBlockCommitSequenceId(bcsID);
    blockData.addMetadata(INCREMENTAL_CHUNK_LIST, "");

    return blockData;
  }

  private BlockData createBlockDataWithOneFullChunk(long containerID,
      long blockNo, int chunkID, long offset, long len, long bcsID)
      throws IOException {
    BlockID blockID = new BlockID(containerID, blockNo);
    blockData = new BlockData(blockID);
    List<ContainerProtos.ChunkInfo> chunkList1 = new ArrayList<>();
    ChunkInfo info1 = new ChunkInfo(String.format("%d_chunk_%d", blockID
        .getLocalID(), 1), 0, 4 * 1024 * 1024);
    info1.addMetadata(FULL_CHUNK, "");

    ChunkInfo info2 = new ChunkInfo(String.format("%d_chunk_%d", blockID
        .getLocalID(), chunkID), offset, len);
    chunkList1.add(info1.getProtoBufMessage());
    chunkList1.add(info2.getProtoBufMessage());
    blockData.setChunks(chunkList1);
    blockData.setBlockCommitSequenceId(bcsID);
    blockData.addMetadata(INCREMENTAL_CHUNK_LIST, "");

    return blockData;
  }

  private BlockData createBlockDataWithThreeFullChunks(long containerID,
      long blockNo, long bcsID) throws IOException {
    BlockID blockID = new BlockID(containerID, blockNo);
    blockData = new BlockData(blockID);
    List<ContainerProtos.ChunkInfo> chunkList1 = new ArrayList<>();
    long chunkLimit = 4 * 1024 * 1024;
    for (int i = 1; i < 4; i++) {
      ChunkInfo info1 = new ChunkInfo(
          String.format("%d_chunk_%d", blockID.getLocalID(), i),
          chunkLimit * i, chunkLimit);
      info1.addMetadata(FULL_CHUNK, "");
      chunkList1.add(info1.getProtoBufMessage());
    }
    blockData.setChunks(chunkList1);
    blockData.setBlockCommitSequenceId(bcsID);
    blockData.addMetadata(INCREMENTAL_CHUNK_LIST, "");

    return blockData;
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testFlush1(ContainerTestVersionInfo versionInfo)
      throws Exception {
    initTest(versionInfo);
    Assumptions.assumeFalse(
        isSameSchemaVersion(schemaVersion, OzoneConsts.SCHEMA_V1));
    // simulates writing 1024 bytes, hsync,
    // write another 1024 bytes, hsync
    // write another 1024 bytes, hsync
    long containerID = 1;
    long blockNo = 2;
    // put 1st chunk
    blockData1 = createBlockData(containerID, blockNo, 1, 0, 1024,
        1);
    blockManager.putBlock(keyValueContainer, blockData1, false);
    // put 2nd chunk
    BlockData blockData2 = createBlockData(containerID, blockNo, 1, 0, 2048,
        2);
    blockManager.putBlock(keyValueContainer, blockData2, false);
    assertEquals(1, keyValueContainer.getContainerData().getBlockCount());

    BlockData getBlockData = blockManager.getBlock(keyValueContainer,
        new BlockID(containerID, blockNo));
    assertEquals(2048, getBlockData.getSize());
    assertEquals(2, getBlockData.getBlockCommitSequenceId());
    List<ContainerProtos.ChunkInfo> chunkInfos = getBlockData.getChunks();
    assertEquals(1, chunkInfos.size());
    assertEquals(2048, chunkInfos.get(0).getLen());
    assertEquals(0, chunkInfos.get(0).getOffset());

    // put 3rd chunk, end-of-block
    BlockData blockData3 = createBlockData(containerID, blockNo, 1, 0, 3072,
        3);
    blockManager.putBlock(keyValueContainer, blockData3, true);
    assertEquals(1, keyValueContainer.getContainerData().getBlockCount());

    getBlockData = blockManager.getBlock(keyValueContainer,
        new BlockID(containerID, blockNo));
    assertEquals(3072, getBlockData.getSize());
    assertEquals(3, getBlockData.getBlockCommitSequenceId());
    chunkInfos = getBlockData.getChunks();
    assertEquals(1, chunkInfos.size());
    assertEquals(3072, chunkInfos.get(0).getLen());
    assertEquals(0, chunkInfos.get(0).getOffset());
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testFlush2(ContainerTestVersionInfo versionInfo)
      throws Exception {
    initTest(versionInfo);
    Assumptions.assumeFalse(
        isSameSchemaVersion(schemaVersion, OzoneConsts.SCHEMA_V1));
    // simulates writing a full chunk + 1024 bytes, hsync,
    // write another 1024 bytes, hsync
    // write another 1024 bytes, hsync
    long containerID = 1;
    long blockNo = 2;
    long chunkLimit = 4 * 1024 * 1024;
    // first hsync (a full chunk + 1024 bytes)
    blockData1 = createBlockDataWithOneFullChunk(containerID,
        blockNo, 2, chunkLimit, 1024, 1);
    blockManager.putBlock(keyValueContainer, blockData1, false);
    // second hsync (1024 bytes)
    BlockData blockData2 = createBlockData(containerID, blockNo, 2,
        chunkLimit, 2048, 2);
    blockManager.putBlock(keyValueContainer, blockData2, false);
    assertEquals(1, keyValueContainer.getContainerData().getBlockCount());
    // third hsync (1024 bytes)
    BlockData blockData3 = createBlockData(containerID, blockNo, 2,
        chunkLimit, 3072, 3);
    blockManager.putBlock(keyValueContainer, blockData3, false);
    assertEquals(1, keyValueContainer.getContainerData().getBlockCount());

    // verify that first chunk is full, second chunk is 3072 bytes
    BlockData getBlockData = blockManager.getBlock(keyValueContainer,
        new BlockID(containerID, blockNo));
    assertEquals(3072 + chunkLimit, getBlockData.getSize());
    assertEquals(3, getBlockData.getBlockCommitSequenceId());
    List<ContainerProtos.ChunkInfo> chunkInfos = getBlockData.getChunks();
    assertEquals(2, chunkInfos.size());
    assertEquals(chunkLimit, chunkInfos.get(0).getLen());
    assertEquals(0, chunkInfos.get(0).getOffset());
    assertEquals(3072, chunkInfos.get(1).getLen());
    assertEquals(chunkLimit, chunkInfos.get(1).getOffset());
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testFlush3(ContainerTestVersionInfo versionInfo)
      throws Exception {
    initTest(versionInfo);
    Assumptions.assumeFalse(
        isSameSchemaVersion(schemaVersion, OzoneConsts.SCHEMA_V1));
    // simulates writing 1024 bytes, hsync,
    // and then write till 4 chunks are full
    long containerID = 1;
    long blockNo = 2;
    long chunkLimit = 4 * 1024 * 1024;
    // first hsync (1024 bytes)
    blockData1 = createBlockDataWithOneFullChunk(containerID, blockNo, 2,
        chunkLimit, 1024, 1);
    blockManager.putBlock(keyValueContainer, blockData1, false);
    // full flush (4 chunks)
    BlockData blockData2 = createBlockDataWithThreeFullChunks(
        containerID, blockNo, 2);
    blockManager.putBlock(keyValueContainer, blockData2, false);
    assertEquals(1, keyValueContainer.getContainerData().getBlockCount());

    // verify that the four chunks are full
    BlockData getBlockData = blockManager.getBlock(keyValueContainer,
        new BlockID(containerID, blockNo));
    assertEquals(chunkLimit * 4, getBlockData.getSize());
    assertEquals(2, getBlockData.getBlockCommitSequenceId());
    List<ContainerProtos.ChunkInfo> chunkInfos = getBlockData.getChunks();
    assertEquals(4, chunkInfos.size());
    for (int i = 0; i < 4; i++) {
      assertEquals(chunkLimit, chunkInfos.get(i).getLen());
      assertEquals(chunkLimit * i, chunkInfos.get(i).getOffset());
    }
  }
}
