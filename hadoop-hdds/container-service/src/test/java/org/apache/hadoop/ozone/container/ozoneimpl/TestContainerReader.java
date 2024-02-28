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

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.ContainerTestVersionInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.DELETED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.RECOVERING;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.UNHEALTHY;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.createDbInstancesForTestIfNeeded;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;

/**
 * Test ContainerReader class which loads containers from disks.
 */
@RunWith(Parameterized.class)
public class TestContainerReader {

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  private MutableVolumeSet volumeSet;
  private HddsVolume hddsVolume;
  private ContainerSet containerSet;
  private OzoneConfiguration conf;


  private RoundRobinVolumeChoosingPolicy volumeChoosingPolicy;
  private UUID datanodeId;
  private String clusterId = UUID.randomUUID().toString();
  private int blockCount = 10;
  private long blockLen = 1024;

  private final ContainerLayoutVersion layout;
  private String schemaVersion;

  public TestContainerReader(ContainerTestVersionInfo versionInfo) {
    this.layout = versionInfo.getLayout();
    this.schemaVersion = versionInfo.getSchemaVersion();
    this.conf = new OzoneConfiguration();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);
  }


  @Parameterized.Parameters
  public static Iterable<Object[]> parameters() {
    return ContainerTestVersionInfo.versionParameters();
  }

  @Before
  public void setup() throws Exception {

    File volumeDir = tempDir.newFolder();
    volumeSet = Mockito.mock(MutableVolumeSet.class);
    containerSet = new ContainerSet(1000);

    datanodeId = UUID.randomUUID();
    hddsVolume = new HddsVolume.Builder(volumeDir
        .getAbsolutePath()).conf(conf).datanodeUuid(datanodeId
        .toString()).clusterID(clusterId).build();
    StorageVolumeUtil.checkVolume(hddsVolume, clusterId, clusterId, conf,
        null, null);

    volumeSet = mock(MutableVolumeSet.class);
    volumeChoosingPolicy = mock(RoundRobinVolumeChoosingPolicy.class);
    Mockito.when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenReturn(hddsVolume);

    for (int i = 0; i < 2; i++) {
      KeyValueContainerData keyValueContainerData = new KeyValueContainerData(i,
          layout,
          (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
          datanodeId.toString());

      KeyValueContainer keyValueContainer =
          new KeyValueContainer(keyValueContainerData,
              conf);
      keyValueContainer.create(volumeSet, volumeChoosingPolicy, clusterId);


      List<Long> blkNames;
      if (i % 2 == 0) {
        blkNames = addBlocks(keyValueContainer,  true);
        markBlocksForDelete(keyValueContainer, true, blkNames, i);
      } else {
        blkNames = addBlocks(keyValueContainer, false);
        markBlocksForDelete(keyValueContainer, false, blkNames, i);
      }
    }
    // Close the RocksDB instance for this container and remove from the cache
    // so it does not affect the ContainerReader, which avoids using the cache
    // at startup for performance reasons.
    ContainerCache.getInstance(conf).shutdownCache();
  }

  @After
  public void cleanup() {
    BlockUtils.shutdownCache(conf);
  }

  private void markBlocksForDelete(KeyValueContainer keyValueContainer,
      boolean setMetaData, List<Long> blockNames, int count) throws Exception {
    KeyValueContainerData cData = keyValueContainer.getContainerData();
    try (DBHandle metadataStore = BlockUtils.getDB(cData, conf)) {

      for (int i = 0; i < count; i++) {
        Table<String, BlockData> blockDataTable =
                metadataStore.getStore().getBlockDataTable();

        Long localID = blockNames.get(i);
        String blk = cData.getBlockKey(localID);
        BlockData blkInfo = blockDataTable.get(blk);

        blockDataTable.delete(blk);
        blockDataTable.put(cData.getDeletingBlockKey(localID), blkInfo);
      }

      if (setMetaData) {
        // Pending delete blocks are still counted towards the block count
        // and bytes used metadata values, so those do not change.
        Table<String, Long> metadataTable =
                metadataStore.getStore().getMetadataTable();
        metadataTable.put(cData.getPendingDeleteBlockCountKey(),
            (long)count);
      }
    }

  }

  private List<Long> addBlocks(KeyValueContainer keyValueContainer,
      boolean setMetaData) throws Exception {
    long containerId = keyValueContainer.getContainerData().getContainerID();
    KeyValueContainerData cData = keyValueContainer.getContainerData();
    List<Long> blkNames = new ArrayList<>();
    try (DBHandle metadataStore = BlockUtils.getDB(cData, conf)) {

      for (int i = 0; i < blockCount; i++) {
        // Creating BlockData
        BlockID blockID = new BlockID(containerId, i);
        BlockData blockData = new BlockData(blockID);
        blockData.addMetadata(OzoneConsts.VOLUME, OzoneConsts.OZONE);
        blockData.addMetadata(OzoneConsts.OWNER,
            OzoneConsts.OZONE_SIMPLE_HDFS_USER);
        List<ContainerProtos.ChunkInfo> chunkList = new ArrayList<>();
        long localBlockID = blockID.getLocalID();
        ChunkInfo info = new ChunkInfo(String.format(
                "%d.data.%d", localBlockID, 0), 0, blockLen);
        chunkList.add(info.getProtoBufMessage());
        blockData.setChunks(chunkList);
        blkNames.add(localBlockID);
        metadataStore.getStore().getBlockDataTable()
                .put(cData.getBlockKey(localBlockID), blockData);
      }

      if (setMetaData) {
        metadataStore.getStore().getMetadataTable()
                .put(cData.getBlockCountKey(), (long)blockCount);
        metadataStore.getStore().getMetadataTable()
                .put(cData.getBytesUsedKey(), blockCount * blockLen);
      }
    }

    return blkNames;
  }

  @Test
  public void testContainerReader() throws Exception {
    KeyValueContainerData recoveringContainerData = new KeyValueContainerData(
        10, layout, (long) StorageUnit.GB.toBytes(5),
        UUID.randomUUID().toString(), datanodeId.toString());
    //create a container with recovering state
    recoveringContainerData.setState(RECOVERING);

    KeyValueContainer recoveringKeyValueContainer =
        new KeyValueContainer(recoveringContainerData,
            conf);
    recoveringKeyValueContainer.create(
        volumeSet, volumeChoosingPolicy, clusterId);

    ContainerReader containerReader = new ContainerReader(volumeSet,
        hddsVolume, containerSet, conf, true);

    Thread thread = new Thread(containerReader);
    thread.start();
    thread.join();

    //recovering container should be marked unhealthy, so the count should be 3
    Assert.assertEquals(UNHEALTHY, containerSet.getContainer(
        recoveringContainerData.getContainerID()).getContainerState());
    Assert.assertEquals(3, containerSet.containerCount());

    for (int i = 0; i < 2; i++) {
      Container keyValueContainer = containerSet.getContainer(i);

      KeyValueContainerData keyValueContainerData = (KeyValueContainerData)
          keyValueContainer.getContainerData();

      // Verify block related metadata.
      Assert.assertEquals(blockCount,
          keyValueContainerData.getBlockCount());

      Assert.assertEquals(blockCount * blockLen,
          keyValueContainerData.getBytesUsed());

      Assert.assertEquals(i,
          keyValueContainerData.getNumPendingDeletionBlocks());
    }
  }

  @Test
  public void testContainerReaderWithLoadException() throws Exception {
    MutableVolumeSet volumeSet1;
    HddsVolume hddsVolume1;
    ContainerSet containerSet1 = new ContainerSet(1000);
    File volumeDir1 = tempDir.newFolder();
    RoundRobinVolumeChoosingPolicy volumeChoosingPolicy1;

    volumeSet1 = Mockito.mock(MutableVolumeSet.class);
    UUID datanode = UUID.randomUUID();
    hddsVolume1 = new HddsVolume.Builder(volumeDir1
        .getAbsolutePath()).conf(conf).datanodeUuid(datanode
        .toString()).clusterID(clusterId).build();
    StorageVolumeUtil.checkVolume(hddsVolume1, clusterId, clusterId, conf,
        null, null);
    volumeChoosingPolicy1 = mock(RoundRobinVolumeChoosingPolicy.class);
    Mockito.when(volumeChoosingPolicy1.chooseVolume(anyList(), anyLong()))
        .thenReturn(hddsVolume1);

    int containerCount = 3;
    for (int i = 0; i < containerCount; i++) {
      KeyValueContainerData keyValueContainerData = new KeyValueContainerData(i,
          layout,
          (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
          datanodeId.toString());
      KeyValueContainer keyValueContainer =
          new KeyValueContainer(keyValueContainerData, conf);
      keyValueContainer.create(volumeSet1, volumeChoosingPolicy1, clusterId);

      if (i == 0) {
        // rename first container directory name
        String containerPathStr =
            keyValueContainer.getContainerData().getContainerPath();
        File containerPath = new File(containerPathStr);
        String renamePath = containerPathStr + "-aa";
        Assert.assertTrue(containerPath.renameTo(new File(renamePath)));
      }
    }
    ContainerCache.getInstance(conf).shutdownCache();

    ContainerReader containerReader = new ContainerReader(volumeSet1,
        hddsVolume1, containerSet1, conf, true);
    containerReader.readVolume(hddsVolume1.getHddsRootDir());
    Assert.assertEquals(containerCount - 1, containerSet1.containerCount());
  }

  @Test
  public void testMultipleContainerReader() throws Exception {
    final int volumeNum = 10;
    StringBuffer datanodeDirs = new StringBuffer();
    File[] volumeDirs = new File[volumeNum];
    for (int i = 0; i < volumeNum; i++) {
      volumeDirs[i] = tempDir.newFolder();
      datanodeDirs = datanodeDirs.append(volumeDirs[i]).append(",");
    }

    BlockUtils.shutdownCache(conf);
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY,
        datanodeDirs.toString());
    conf.set(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR,
        datanodeDirs.toString());
    MutableVolumeSet volumeSets =
        new MutableVolumeSet(datanodeId.toString(), clusterId, conf, null,
            StorageVolume.VolumeType.DATA_VOLUME, null);
    createDbInstancesForTestIfNeeded(volumeSets, clusterId, clusterId, conf);
    ContainerCache cache = ContainerCache.getInstance(conf);
    cache.shutdownCache();

    RoundRobinVolumeChoosingPolicy policy =
        new RoundRobinVolumeChoosingPolicy();

    final int containerCount = 100;
    blockCount = containerCount;
    for (int i = 0; i < containerCount; i++) {
      KeyValueContainerData keyValueContainerData =
          new KeyValueContainerData(i, layout,
              (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
              datanodeId.toString());

      KeyValueContainer keyValueContainer =
          new KeyValueContainer(keyValueContainerData,
              conf);
      keyValueContainer.create(volumeSets, policy, clusterId);

      List<Long> blkNames;
      if (i % 2 == 0) {
        blkNames = addBlocks(keyValueContainer, true);
        markBlocksForDelete(keyValueContainer, true, blkNames, i);
      } else {
        blkNames = addBlocks(keyValueContainer, false);
        markBlocksForDelete(keyValueContainer, false, blkNames, i);
      }
    }
    // Close the RocksDB instance for this container and remove from the cache
    // so it does not affect the ContainerReader, which avoids using the cache
    // at startup for performance reasons.
    cache.shutdownCache();

    List<StorageVolume> volumes = volumeSets.getVolumesList();
    ContainerReader[] containerReaders = new ContainerReader[volumeNum];
    Thread[] threads = new Thread[volumeNum];
    for (int i = 0; i < volumeNum; i++) {
      containerReaders[i] = new ContainerReader(volumeSets,
          (HddsVolume) volumes.get(i), containerSet, conf, true);
      threads[i] = new Thread(containerReaders[i]);
    }
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < volumeNum; i++) {
      threads[i].start();
    }
    for (int i = 0; i < volumeNum; i++) {
      threads[i].join();
    }
    System.out.println("Open " + volumeNum + " Volume with " + containerCount +
        " costs " + (System.currentTimeMillis() - startTime) / 1000 + "s");
    Assert.assertEquals(containerCount,
        containerSet.getContainerMap().entrySet().size());
    // There should be no open containers cached by the ContainerReader as it
    // opens and closed them avoiding the cache.
    Assert.assertEquals(0, cache.size());
  }

  @Test
  public void testMarkedDeletedContainerCleared() throws Exception {
    KeyValueContainerData containerData = new KeyValueContainerData(
        101, layout, (long) StorageUnit.GB.toBytes(5),
        UUID.randomUUID().toString(), datanodeId.toString());
    //create a container with deleted state
    containerData.setState(DELETED);

    KeyValueContainer kvContainer =
        new KeyValueContainer(containerData, conf);
    kvContainer.create(
        volumeSet, volumeChoosingPolicy, clusterId);
    long baseCount = 0;
    if (containerData.hasSchema(OzoneConsts.SCHEMA_V3)) {
      // add db entry for the container ID 101 for V3
      baseCount = addDbEntry(containerData);
    }
    ContainerReader containerReader = new ContainerReader(volumeSet,
        hddsVolume, containerSet, conf, false);

    containerReader.run();

    // assert that tmp dir is empty
    File[] leftoverContainers =
        hddsVolume.getDeletedContainerDir().listFiles();
    Assertions.assertNotNull(leftoverContainers);
    Assertions.assertEquals(0, leftoverContainers.length);

    Assertions.assertNull(containerSet.getContainer(101));

    if (containerData.hasSchema(OzoneConsts.SCHEMA_V3)) {
      // verify if newly added container is not present as added
      try (DBHandle dbHandle = BlockUtils.getDB(
          kvContainer.getContainerData(), conf)) {
        DatanodeStoreSchemaThreeImpl store = (DatanodeStoreSchemaThreeImpl)
            dbHandle.getStore();
        Assertions.assertEquals(baseCount, store.getMetadataTable()
            .getEstimatedKeyCount());
      }
    }
  }

  private long addDbEntry(KeyValueContainerData containerData)
      throws Exception {
    try (DBHandle dbHandle = BlockUtils.getDB(containerData, conf)) {
      DatanodeStoreSchemaThreeImpl store = (DatanodeStoreSchemaThreeImpl)
          dbHandle.getStore();
      Table<String, Long> metadataTable = store.getMetadataTable();
      long baseSize = metadataTable.getEstimatedKeyCount();
      metadataTable.put(containerData.getBytesUsedKey(), 0L);
      metadataTable.put(containerData.getBlockCountKey(), 0L);
      metadataTable.put(containerData.getPendingDeleteBlockCountKey(), 0L);

      // The new keys should have been added in the MetadataTable
      Assertions.assertEquals(baseSize + 3,
          metadataTable.getEstimatedKeyCount());
      return baseSize;
    }
  }
}
