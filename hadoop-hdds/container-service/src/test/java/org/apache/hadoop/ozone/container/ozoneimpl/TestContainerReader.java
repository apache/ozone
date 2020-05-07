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

package org.apache.hadoop.ozone.container.ozoneimpl;

import com.google.common.primitives.Longs;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConsts.DB_BLOCK_COUNT_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.DB_CONTAINER_BYTES_USED_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.DB_PENDING_DELETE_BLOCK_COUNT_KEY;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;

/**
 * Test ContainerReader class which loads containers from disks.
 */
public class TestContainerReader {

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  private MutableVolumeSet volumeSet;
  private HddsVolume hddsVolume;
  private ContainerSet containerSet;
  private ConfigurationSource conf;


  private RoundRobinVolumeChoosingPolicy volumeChoosingPolicy;
  private UUID datanodeId;
  private String scmId = UUID.randomUUID().toString();
  private int blockCount = 10;
  private long blockLen = 1024;

  @Before
  public void setup() throws Exception {

    File volumeDir = tempDir.newFolder();
    volumeSet = Mockito.mock(MutableVolumeSet.class);
    containerSet = new ContainerSet();
    conf = new OzoneConfiguration();

    datanodeId = UUID.randomUUID();
    hddsVolume = new HddsVolume.Builder(volumeDir
        .getAbsolutePath()).conf(conf).datanodeUuid(datanodeId
        .toString()).build();

    volumeSet = mock(MutableVolumeSet.class);
    volumeChoosingPolicy = mock(RoundRobinVolumeChoosingPolicy.class);
    Mockito.when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenReturn(hddsVolume);

    for (int i=0; i<2; i++) {
      KeyValueContainerData keyValueContainerData = new KeyValueContainerData(i,
          ChunkLayOutVersion.FILE_PER_BLOCK,
          (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
          datanodeId.toString());

      KeyValueContainer keyValueContainer =
          new KeyValueContainer(keyValueContainerData,
              conf);
      keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);


      List<Long> blkNames;
      if (i % 2 == 0) {
        blkNames = addBlocks(keyValueContainer,  true);
        markBlocksForDelete(keyValueContainer, true, blkNames, i);
      } else {
        blkNames = addBlocks(keyValueContainer, false);
        markBlocksForDelete(keyValueContainer, false, blkNames, i);
      }

    }
  }


  private void markBlocksForDelete(KeyValueContainer keyValueContainer,
      boolean setMetaData, List<Long> blockNames, int count) throws Exception {
    try(ReferenceCountedDB metadataStore = BlockUtils.getDB(keyValueContainer
        .getContainerData(), conf)) {

      for (int i = 0; i < count; i++) {
        byte[] blkBytes = Longs.toByteArray(blockNames.get(i));
        byte[] blkInfo = metadataStore.getStore().get(blkBytes);

        byte[] deletingKeyBytes =
            DFSUtil.string2Bytes(OzoneConsts.DELETING_KEY_PREFIX +
                blockNames.get(i));

        metadataStore.getStore().delete(blkBytes);
        metadataStore.getStore().put(deletingKeyBytes, blkInfo);
      }

      if (setMetaData) {
        metadataStore.getStore().put(DB_PENDING_DELETE_BLOCK_COUNT_KEY,
            Longs.toByteArray(count));
        long blkCount = Longs.fromByteArray(
            metadataStore.getStore().get(DB_BLOCK_COUNT_KEY));
        metadataStore.getStore().put(DB_BLOCK_COUNT_KEY,
            Longs.toByteArray(blkCount - count));
        long bytesUsed = Longs.fromByteArray(
            metadataStore.getStore().get(DB_CONTAINER_BYTES_USED_KEY));
        metadataStore.getStore().put(DB_CONTAINER_BYTES_USED_KEY,
            Longs.toByteArray(bytesUsed - (count * blockLen)));

      }
    }

  }

  private List<Long> addBlocks(KeyValueContainer keyValueContainer,
      boolean setMetaData) throws Exception {
    long containerId = keyValueContainer.getContainerData().getContainerID();

    List<Long> blkNames = new ArrayList<>();
    try(ReferenceCountedDB metadataStore = BlockUtils.getDB(keyValueContainer
        .getContainerData(), conf)) {

      for (int i = 0; i < blockCount; i++) {
        // Creating BlockData
        BlockID blockID = new BlockID(containerId, i);
        BlockData blockData = new BlockData(blockID);
        blockData.addMetadata(OzoneConsts.VOLUME, OzoneConsts.OZONE);
        blockData.addMetadata(OzoneConsts.OWNER,
            OzoneConsts.OZONE_SIMPLE_HDFS_USER);
        List<ContainerProtos.ChunkInfo> chunkList = new ArrayList<>();
        ChunkInfo info = new ChunkInfo(String.format("%d.data.%d", blockID
            .getLocalID(), 0), 0, blockLen);
        chunkList.add(info.getProtoBufMessage());
        blockData.setChunks(chunkList);
        blkNames.add(blockID.getLocalID());
        metadataStore.getStore().put(Longs.toByteArray(blockID.getLocalID()),
            blockData
                .getProtoBufMessage().toByteArray());
      }

      if (setMetaData) {
        metadataStore.getStore().put(DB_BLOCK_COUNT_KEY,
            Longs.toByteArray(blockCount));
        metadataStore.getStore().put(OzoneConsts.DB_CONTAINER_BYTES_USED_KEY,
            Longs.toByteArray(blockCount * blockLen));
      }
    }

    return blkNames;
  }

  @Test
  public void testContainerReader() throws Exception {
    ContainerReader containerReader = new ContainerReader(volumeSet,
        hddsVolume, containerSet, conf);

    Thread thread = new Thread(containerReader);
    thread.start();
    thread.join();

    Assert.assertEquals(2, containerSet.containerCount());

    for (int i=0; i < 2; i++) {
      Container keyValueContainer = containerSet.getContainer(i);

      KeyValueContainerData keyValueContainerData = (KeyValueContainerData)
          keyValueContainer.getContainerData();

      // Verify block related metadata.
      Assert.assertEquals(blockCount - i,
          keyValueContainerData.getKeyCount());

      Assert.assertEquals((blockCount - i) * blockLen,
          keyValueContainerData.getBytesUsed());

      Assert.assertEquals(i,
          keyValueContainerData.getNumPendingDeletionBlocks());
    }
  }
}
