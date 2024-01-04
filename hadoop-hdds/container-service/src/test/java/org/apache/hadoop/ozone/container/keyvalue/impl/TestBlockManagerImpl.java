/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.keyvalue.impl;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.ContainerTestVersionInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class is used to test key related operations on the container.
 */
public class TestBlockManagerImpl {

  @TempDir
  private Path folder;
  private OzoneConfiguration config;
  private String scmId = UUID.randomUUID().toString();
  private VolumeSet volumeSet;
  private RoundRobinVolumeChoosingPolicy volumeChoosingPolicy;
  private KeyValueContainerData keyValueContainerData;
  private KeyValueContainer keyValueContainer;
  private BlockData blockData;
  private BlockData blockData1;
  private BlockManagerImpl blockManager;
  private BlockID blockID;
  private BlockID blockID1;

  private ContainerLayoutVersion layout;
  private String schemaVersion;

  private void initTest(ContainerTestVersionInfo versionInfo)
      throws Exception {
    this.layout = versionInfo.getLayout();
    this.schemaVersion = versionInfo.getSchemaVersion();
    this.config = new OzoneConfiguration();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, config);
    initilaze();
  }

  private void initilaze() throws Exception {
    UUID datanodeId = UUID.randomUUID();
    HddsVolume hddsVolume = new HddsVolume.Builder(folder.toString())
        .conf(config)
        .datanodeUuid(datanodeId.toString())
        .build();
    StorageVolumeUtil.checkVolume(hddsVolume, scmId, scmId, config,
        null, null);

    volumeSet = mock(MutableVolumeSet.class);

    volumeChoosingPolicy = mock(RoundRobinVolumeChoosingPolicy.class);
    when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenReturn(hddsVolume);

    keyValueContainerData = new KeyValueContainerData(1L,
        layout,
        (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
        datanodeId.toString());

    keyValueContainer = new KeyValueContainer(
        keyValueContainerData, config);

    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);

    // Creating BlockData
    blockID = new BlockID(1L, 1L);
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
    blockID1 = new BlockID(1L, 2L);
    blockData1 = new BlockData(blockID1);
    blockData1.addMetadata(OzoneConsts.VOLUME, OzoneConsts.OZONE);
    blockData1.addMetadata(OzoneConsts.OWNER,
        OzoneConsts.OZONE_SIMPLE_HDFS_USER);
    List<ContainerProtos.ChunkInfo> chunkList1 = new ArrayList<>();
    ChunkInfo info1 = new ChunkInfo(String.format("%d.data.%d", blockID1
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
  public void testListBlock(ContainerTestVersionInfo versionInfo)
      throws Exception {
    initTest(versionInfo);
    blockManager.putBlock(keyValueContainer, blockData);
    List<BlockData> listBlockData = blockManager.listBlock(
        keyValueContainer, 1, 10);
    assertNotNull(listBlockData);
    assertEquals(1, listBlockData.size());

    for (long i = 2; i <= 10; i++) {
      blockID = new BlockID(1L, i);
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
}
