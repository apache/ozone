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

package org.apache.hadoop.ozone.container.ozoneimpl;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.DELETED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.RECOVERING;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.UNHEALTHY;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.verifyAllDataChecksumsMatch;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.createDbInstancesForTestIfNeeded;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.getKeyValueHandler;
import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.utils.db.InMemoryTestTable;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeWriter;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.ContainerTestVersionInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.metadata.ContainerCreateInfo;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaOneImpl;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaTwoImpl;
import org.apache.hadoop.ozone.container.metadata.WitnessedContainerMetadataStore;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ratis.util.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test ContainerReader class which loads containers from disks.
 */
public class TestContainerReader {

  private MutableVolumeSet volumeSet;
  private HddsVolume hddsVolume;
  private ContainerSet containerSet;
  private WitnessedContainerMetadataStore mockMetadataStore;
  private OzoneConfiguration conf;

  private RoundRobinVolumeChoosingPolicy volumeChoosingPolicy;
  private UUID datanodeId;
  private String clusterId = UUID.randomUUID().toString();
  private int blockCount = 10;
  private long blockLen = 1024;

  private ContainerLayoutVersion layout;
  private KeyValueHandler keyValueHandler;

  @TempDir
  private Path tempDir;

  private void setup(ContainerTestVersionInfo versionInfo) throws Exception {
    setLayoutAndSchemaVersion(versionInfo);
    File volumeDir =
        Files.createDirectory(tempDir.resolve("volumeDir")).toFile();
    this.conf = new OzoneConfiguration();
    volumeSet = mock(MutableVolumeSet.class);
    mockMetadataStore = mock(WitnessedContainerMetadataStore.class);
    when(mockMetadataStore.getContainerCreateInfoTable()).thenReturn(new InMemoryTestTable<>());
    containerSet = newContainerSet(1000, mockMetadataStore);

    datanodeId = UUID.randomUUID();
    hddsVolume = new HddsVolume.Builder(volumeDir
        .getAbsolutePath()).conf(conf).datanodeUuid(datanodeId
        .toString()).clusterID(clusterId).build();
    StorageVolumeUtil.checkVolume(hddsVolume, clusterId, clusterId, conf,
        null, null);

    volumeSet = mock(MutableVolumeSet.class);
    volumeChoosingPolicy = mock(RoundRobinVolumeChoosingPolicy.class);
    when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenReturn(hddsVolume);

    for (int i = 0; i < 2; i++) {
      KeyValueContainerData keyValueContainerData =
          new KeyValueContainerData(i, layout, (long) StorageUnit.GB.toBytes(5),
              UUID.randomUUID().toString(), datanodeId.toString());

      KeyValueContainer keyValueContainer =
          new KeyValueContainer(keyValueContainerData,
              conf);
      keyValueContainer.create(volumeSet, volumeChoosingPolicy, clusterId);


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
    ContainerCache.getInstance(conf).shutdownCache();
    keyValueHandler = getKeyValueHandler(conf, UUID.randomUUID().toString(), containerSet, volumeSet);
  }

  @AfterEach
  public void cleanup() {
    BlockUtils.shutdownCache(conf);
  }

  private void markBlocksForDelete(KeyValueContainer keyValueContainer,
      boolean setMetaData, List<Long> blockNames, int count) throws Exception {
    KeyValueContainerData cData = keyValueContainer.getContainerData();
    try (DBHandle metadataStore = BlockUtils.getDB(cData, conf)) {

      if (metadataStore.getStore() instanceof DatanodeStoreSchemaThreeImpl) {
        DatanodeStoreSchemaThreeImpl schemaThree = (DatanodeStoreSchemaThreeImpl) metadataStore.getStore();
        Table<String, StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction> delTxTable =
            schemaThree.getDeleteTransactionTable();

        // Fix: Use the correct container prefix format for the delete transaction key
        String containerPrefix = cData.containerPrefix();
        long txId = System.currentTimeMillis();
        String txKey = containerPrefix + txId; // This ensures the key matches the container prefix

        StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction.Builder deleteTxBuilder =
            StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction.newBuilder()
                .setTxID(txId)
                .setContainerID(cData.getContainerID())
                .setCount(count);

        for (int i = 0; i < count; i++) {
          Long localID = blockNames.get(i);
          deleteTxBuilder.addLocalID(localID);
        }

        StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction deleteTx = deleteTxBuilder.build();
        delTxTable.put(txKey, deleteTx); // Use the properly formatted key

      } else if (metadataStore.getStore() instanceof DatanodeStoreSchemaTwoImpl) {
        DatanodeStoreSchemaTwoImpl schemaTwoStore = (DatanodeStoreSchemaTwoImpl) metadataStore.getStore();
        Table<Long, StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction> delTxTable =
            schemaTwoStore.getDeleteTransactionTable();

        long txId = System.currentTimeMillis();
        StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction.Builder deleteTxBuilder =
            StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction.newBuilder()
                .setTxID(txId)
                .setContainerID(cData.getContainerID())
                .setCount(count);

        for (int i = 0; i < count; i++) {
          Long localID = blockNames.get(i);
          deleteTxBuilder.addLocalID(localID);
        }

        StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction deleteTx = deleteTxBuilder.build();
        delTxTable.put(txId, deleteTx);

      } else if (metadataStore.getStore() instanceof DatanodeStoreSchemaOneImpl) {
        // Schema 1: Move blocks to deleting prefix (this part looks correct)
        Table<String, BlockData> blockDataTable = metadataStore.getStore().getBlockDataTable();
        for (int i = 0; i < count; i++) {
          Long localID = blockNames.get(i);
          String blk = cData.getBlockKey(localID);
          BlockData blkInfo = blockDataTable.get(blk);
          blockDataTable.delete(blk);
          blockDataTable.put(cData.getDeletingBlockKey(localID), blkInfo);
        }
      }

      if (setMetaData) {
        Table<String, Long> metadataTable = metadataStore.getStore().getMetadataTable();
        metadataTable.put(cData.getPendingDeleteBlockCountKey(), (long)count);
        // Also set the pending deletion size
        long deletionSize = count * blockLen;
        metadataTable.put(cData.getPendingDeleteBlockBytesKey(), deletionSize);
      }

      metadataStore.getStore().flushDB();
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
            .put(cData.getBlockCountKey(), (long) blockCount);
        metadataStore.getStore().getMetadataTable()
            .put(cData.getBytesUsedKey(), blockCount * blockLen);
      }
    }

    return blkNames;
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testContainerReader(ContainerTestVersionInfo versionInfo)
      throws Exception {
    setLayoutAndSchemaVersion(versionInfo);
    setup(versionInfo);

    ContainerReader containerReader = new ContainerReader(volumeSet,
        hddsVolume, containerSet, conf, true);
    Thread thread = new Thread(containerReader);
    thread.start();
    thread.join();
    long originalCommittedBytes = hddsVolume.getCommittedBytes();
    ContainerCache.getInstance(conf).shutdownCache();

    long recoveringContainerId = 10;
    KeyValueContainerData recoveringContainerData = new KeyValueContainerData(
        recoveringContainerId, layout, (long) StorageUnit.GB.toBytes(5),
        UUID.randomUUID().toString(), datanodeId.toString());
    //create a container with recovering state
    recoveringContainerData.setState(RECOVERING);

    KeyValueContainer recoveringKeyValueContainer =
        new KeyValueContainer(recoveringContainerData,
            conf);
    recoveringKeyValueContainer.create(
        volumeSet, volumeChoosingPolicy, clusterId);

    thread = new Thread(containerReader);
    thread.start();
    thread.join();

    // no change, only open containers have committed space
    assertEquals(originalCommittedBytes, hddsVolume.getCommittedBytes());

    // Ratis replicated recovering containers are deleted upon datanode startup
    if (recoveringKeyValueContainer.getContainerData().getReplicaIndex() == 0) {
      assertNull(containerSet.getContainer(recoveringContainerData.getContainerID()));
      assertEquals(2, containerSet.containerCount());
    } else {
      //recovering container should be marked unhealthy, so the count should be 3
      assertEquals(UNHEALTHY, containerSet.getContainer(
          recoveringContainerData.getContainerID()).getContainerState());
      assertEquals(3, containerSet.containerCount());
    }

    for (int i = 0; i < 2; i++) {
      Container keyValueContainer = containerSet.getContainer(i);

      KeyValueContainerData keyValueContainerData = (KeyValueContainerData)
          keyValueContainer.getContainerData();

      // Verify block related metadata.
      assertEquals(blockCount,
          keyValueContainerData.getBlockCount());

      assertEquals(blockCount * blockLen,
          keyValueContainerData.getBytesUsed());

      assertEquals(i,
          keyValueContainerData.getNumPendingDeletionBlocks());

      assertTrue(keyValueContainerData.isCommittedSpace());
    }
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testContainerReaderWithLoadException(
      ContainerTestVersionInfo versionInfo) throws Exception {
    setLayoutAndSchemaVersion(versionInfo);
    setup(versionInfo);
    MutableVolumeSet volumeSet1;
    HddsVolume hddsVolume1;
    ContainerSet containerSet1 = newContainerSet();
    File volumeDir1 =
        Files.createDirectory(tempDir.resolve("volumeDir" + 1)).toFile();
    RoundRobinVolumeChoosingPolicy volumeChoosingPolicy1;

    volumeSet1 = mock(MutableVolumeSet.class);
    UUID datanode = UUID.randomUUID();
    hddsVolume1 = new HddsVolume.Builder(volumeDir1
        .getAbsolutePath()).conf(conf).datanodeUuid(datanode
        .toString()).clusterID(clusterId).build();
    StorageVolumeUtil.checkVolume(hddsVolume1, clusterId, clusterId, conf,
        null, null);
    volumeChoosingPolicy1 = mock(RoundRobinVolumeChoosingPolicy.class);
    when(volumeChoosingPolicy1.chooseVolume(anyList(), anyLong()))
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
        assertTrue(containerPath.renameTo(new File(renamePath)));
      }
    }
    ContainerCache.getInstance(conf).shutdownCache();

    ContainerReader containerReader = new ContainerReader(volumeSet1,
        hddsVolume1, containerSet1, conf, true);
    containerReader.readVolume(hddsVolume1.getHddsRootDir());
    assertEquals(containerCount - 1, containerSet1.containerCount());
    for (Container c : containerSet1.getContainerMap().values()) {
      if (c.getContainerData().getContainerID() == 0) {
        assertFalse(c.getContainerData().isCommittedSpace());
      } else {
        assertTrue(c.getContainerData().isCommittedSpace());
      }
    }
    assertEquals(hddsVolume1.getCommittedBytes(), (containerCount - 1) * StorageUnit.GB.toBytes(5));
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testContainerReaderWithInvalidDbPath(
      ContainerTestVersionInfo versionInfo) throws Exception {
    setLayoutAndSchemaVersion(versionInfo);
    setup(versionInfo);
    MutableVolumeSet volumeSet1;
    HddsVolume hddsVolume1;
    ContainerSet containerSet1 = newContainerSet();
    File volumeDir1 =
        Files.createDirectory(tempDir.resolve("volumeDirDbDelete")).toFile();
    RoundRobinVolumeChoosingPolicy volumeChoosingPolicy1;

    volumeSet1 = mock(MutableVolumeSet.class);
    UUID datanode = UUID.randomUUID();
    hddsVolume1 = new HddsVolume.Builder(volumeDir1
        .getAbsolutePath()).conf(conf).datanodeUuid(datanode
        .toString()).clusterID(clusterId).build();
    StorageVolumeUtil.checkVolume(hddsVolume1, clusterId, clusterId, conf,
        null, null);
    volumeChoosingPolicy1 = mock(RoundRobinVolumeChoosingPolicy.class);
    when(volumeChoosingPolicy1.chooseVolume(anyList(), anyLong()))
        .thenReturn(hddsVolume1);

    List<File> dbPathList = new ArrayList<>();
    int containerCount = 3;
    for (int i = 0; i < containerCount; i++) {
      KeyValueContainerData keyValueContainerData = new KeyValueContainerData(i,
          layout,
          (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
          datanodeId.toString());
      KeyValueContainer keyValueContainer =
          new KeyValueContainer(keyValueContainerData, conf);
      keyValueContainer.create(volumeSet1, volumeChoosingPolicy1, clusterId);
      dbPathList.add(keyValueContainerData.getDbFile());
    }
    ContainerCache.getInstance(conf).shutdownCache();
    for (File dbPath : dbPathList) {
      FileUtils.deleteFully(dbPath.toPath());
    }

    LogCapturer dnLogs = LogCapturer.captureLogs(ContainerReader.class);
    dnLogs.clearOutput();
    ContainerReader containerReader = new ContainerReader(volumeSet1,
        hddsVolume1, containerSet1, conf, true);
    containerReader.readVolume(hddsVolume1.getHddsRootDir());
    assertEquals(0, containerSet1.containerCount());
    assertEquals(0, hddsVolume1.getCommittedBytes());
    assertThat(dnLogs.getOutput()).contains("Container DB file is missing");
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @ContainerTestVersionInfo.ContainerTest
  public void testMultipleContainerReader(ContainerTestVersionInfo versionInfo)
      throws Exception {
    setLayoutAndSchemaVersion(versionInfo);
    setup(versionInfo);
    final int volumeNum = 10;
    StringBuffer datanodeDirs = new StringBuffer();
    File[] volumeDirs = new File[volumeNum];
    for (int i = 0; i < volumeNum; i++) {
      volumeDirs[i] =
          Files.createDirectory(tempDir.resolve("volumeDir" + i)).toFile();
      datanodeDirs = datanodeDirs.append(volumeDirs[i]).append(',');
    }

    BlockUtils.shutdownCache(conf);
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY,
        datanodeDirs.toString());
    conf.set(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR,
        datanodeDirs.toString());
    MutableVolumeSet volumeSets =
        new MutableVolumeSet(datanodeId.toString(), clusterId, conf, null,
            StorageVolume.VolumeType.DATA_VOLUME, null);
    for (StorageVolume v : volumeSets.getVolumesList()) {
      StorageVolumeUtil.checkVolume(v, clusterId, clusterId, conf,
          null, null);
    }
    createDbInstancesForTestIfNeeded(volumeSets, clusterId, clusterId, conf);
    ContainerCache cache = ContainerCache.getInstance(conf);
    cache.shutdownCache();

    RoundRobinVolumeChoosingPolicy policy =
        new RoundRobinVolumeChoosingPolicy();

    final int containerCount = 100;
    blockCount = containerCount;

    KeyValueContainer conflict01 = null;
    KeyValueContainer conflict02 = null;
    KeyValueContainer conflict11 = null;
    KeyValueContainer conflict12 = null;
    KeyValueContainer conflict21 = null;
    KeyValueContainer conflict22 = null;
    KeyValueContainer ec1 = null;
    KeyValueContainer ec2 = null;
    KeyValueContainer ec3 = null;
    KeyValueContainer ec4 = null;
    KeyValueContainer ec5 = null;
    KeyValueContainer ec6 = null;
    KeyValueContainer ec7 = null;
    long baseBCSID = 10L;

    for (int i = 0; i < containerCount; i++) {
      if (i == 0) {
        // Create a duplicate container with ID 0. Both have the same BSCID
        conflict01 =
            createContainerWithId(0, volumeSets, policy, baseBCSID, 0);
        conflict02 =
            createContainerWithId(0, volumeSets, policy, baseBCSID, 0);
      } else if (i == 1) {
        // Create a duplicate container with ID 1 so that the one has a
        // larger BCSID
        conflict11 =
            createContainerWithId(1, volumeSets, policy, baseBCSID, 0);
        conflict12 = createContainerWithId(
            1, volumeSets, policy, baseBCSID - 1, 0);
      } else if (i == 2) {
        conflict21 =
            createContainerWithId(i, volumeSets, policy, baseBCSID, 0);
        conflict22 =
            createContainerWithId(i, volumeSets, policy, baseBCSID, 0);
        conflict22.close();
      } else if (i == 3) {
        ec1 = createContainerWithId(i, volumeSets, policy, baseBCSID, 1);
        ec2 = createContainerWithId(i, volumeSets, policy, baseBCSID, 1);
      } else if (i == 4) {
        ec3 = createContainerWithId(i, volumeSets, policy, baseBCSID, 1);
        ec4 = createContainerWithId(i, volumeSets, policy, baseBCSID, 2);
        ec3.close();
        ec4.close();
        mockMetadataStore.getContainerCreateInfoTable().put(ContainerID.valueOf(i), ContainerCreateInfo.valueOf(
            ContainerProtos.ContainerDataProto.State.CLOSED, 1));
      } else if (i == 5) {
        ec5 = createContainerWithId(i, volumeSets, policy, baseBCSID, 1);
        ec6 = createContainerWithId(i, volumeSets, policy, baseBCSID, 2);
        ec6.close();
        ec5.close();
        mockMetadataStore.getContainerCreateInfoTable().put(ContainerID.valueOf(i), ContainerCreateInfo.valueOf(
            ContainerProtos.ContainerDataProto.State.CLOSED, 2));
      } else if (i == 6) {
        ec7 = createContainerWithId(i, volumeSets, policy, baseBCSID, 3);
        ec7.close();
        mockMetadataStore.getContainerCreateInfoTable().put(ContainerID.valueOf(i), ContainerCreateInfo.valueOf(
            ContainerProtos.ContainerDataProto.State.CLOSED, -1));
      } else {
        createContainerWithId(i, volumeSets, policy, baseBCSID, 0);
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
    long startTime = Time.monotonicNow();
    for (int i = 0; i < volumeNum; i++) {
      threads[i].start();
    }
    for (int i = 0; i < volumeNum; i++) {
      threads[i].join();
    }
    System.out.println("Open " + volumeNum + " Volume with " + containerCount +
        " costs " + (Time.monotonicNow() - startTime) / 1000 + "s");
    assertEquals(containerCount,
        containerSet.getContainerMap().entrySet().size());
    assertEquals(volumeSet.getFailedVolumesList().size(), 0);

    // One of the conflict01 or conflict02 should have had its container path
    // removed.
    List<Path> paths = new ArrayList<>();
    paths.add(Paths.get(conflict01.getContainerData().getContainerPath()));
    paths.add(Paths.get(conflict02.getContainerData().getContainerPath()));
    int exist = 0;
    for (Path p : paths) {
      if (Files.exists(p)) {
        exist++;
      }
    }
    assertEquals(1, exist);
    assertThat(paths).contains(Paths.get(
        containerSet.getContainer(0).getContainerData().getContainerPath()));

    // For conflict1, the one with the larger BCSID should win, which is
    // conflict11.
    assertFalse(Files.exists(Paths.get(
        conflict12.getContainerData().getContainerPath())));
    assertEquals(conflict11.getContainerData().getContainerPath(),
        containerSet.getContainer(1).getContainerData().getContainerPath());
    assertEquals(baseBCSID, containerSet.getContainer(1)
        .getContainerData().getBlockCommitSequenceId());

    // For conflict2, the closed on (conflict22) should win.
    assertFalse(Files.exists(Paths.get(
        conflict21.getContainerData().getContainerPath())));
    assertEquals(conflict22.getContainerData().getContainerPath(),
        containerSet.getContainer(2).getContainerData().getContainerPath());
    assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED,
        containerSet.getContainer(2).getContainerData().getState());

    // For the EC conflict, both containers should be left on disk
    assertTrue(Files.exists(Paths.get(ec1.getContainerData().getContainerPath())));
    assertTrue(Files.exists(Paths.get(ec2.getContainerData().getContainerPath())));
    assertNotNull(containerSet.getContainer(3));

    // For EC conflict with different replica index, all container present but containerSet loaded with same
    // replica index as the one in DB.
    assertTrue(Files.exists(Paths.get(ec3.getContainerData().getContainerPath())));
    assertTrue(Files.exists(Paths.get(ec4.getContainerData().getContainerPath())));
    assertEquals(containerSet.getContainer(ec3.getContainerData().getContainerID()).getContainerData()
        .getReplicaIndex(), ec3.getContainerData().getReplicaIndex());

    assertTrue(Files.exists(Paths.get(ec5.getContainerData().getContainerPath())));
    assertTrue(Files.exists(Paths.get(ec6.getContainerData().getContainerPath())));
    assertEquals(containerSet.getContainer(ec6.getContainerData().getContainerID()).getContainerData()
        .getReplicaIndex(), ec6.getContainerData().getReplicaIndex());

    // for EC container whose entry in DB with replica index -1, is allowed to be loaded
    assertTrue(Files.exists(Paths.get(ec7.getContainerData().getContainerPath())));
    assertEquals(3, mockMetadataStore.getContainerCreateInfoTable().get(
        ContainerID.valueOf(ec7.getContainerData().getContainerID())).getReplicaIndex());

    // There should be no open containers cached by the ContainerReader as it
    // opens and closed them avoiding the cache.
    assertEquals(0, cache.size());
  }

  private KeyValueContainer createContainerWithId(int id, VolumeSet volSet,
      VolumeChoosingPolicy policy, long bcsid, int replicaIndex)
      throws Exception {
    KeyValueContainerData keyValueContainerData =
        new KeyValueContainerData(id, layout,
            (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
            datanodeId.toString());
    keyValueContainerData.setReplicaIndex(replicaIndex);

    KeyValueContainer keyValueContainer =
        new KeyValueContainer(keyValueContainerData,
            conf);
    keyValueContainer.create(volSet, policy, clusterId);

    List<Long> blkNames;
    if (id % 2 == 0) {
      blkNames = addBlocks(keyValueContainer, true);
      markBlocksForDelete(keyValueContainer, true, blkNames, id);
    } else {
      blkNames = addBlocks(keyValueContainer, false);
      markBlocksForDelete(keyValueContainer, false, blkNames, id);
    }
    setBlockCommitSequence(keyValueContainerData, bcsid);
    return keyValueContainer;
  }

  private void setBlockCommitSequence(KeyValueContainerData cData, long val)
      throws IOException {
    try (DBHandle metadataStore = BlockUtils.getDB(cData, conf)) {
      metadataStore.getStore().getMetadataTable()
          .put(cData.getBcsIdKey(), val);
      metadataStore.getStore().flushDB();
    }
    cData.updateBlockCommitSequenceId(val);
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testMarkedDeletedContainerCleared(
      ContainerTestVersionInfo versionInfo) throws Exception {
    setLayoutAndSchemaVersion(versionInfo);
    setup(versionInfo);
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

    // verify container data and perform cleanup
    ContainerReader containerReader = new ContainerReader(volumeSet,
        hddsVolume, containerSet, conf, true);

    containerReader.run();

    // assert that tmp dir is empty
    File[] leftoverContainers =
        hddsVolume.getDeletedContainerDir().listFiles();
    assertNotNull(leftoverContainers);
    assertEquals(0, leftoverContainers.length);

    assertNull(containerSet.getContainer(101));

    if (containerData.hasSchema(OzoneConsts.SCHEMA_V3)) {
      // verify if newly added container is not present as added
      try (DBHandle dbHandle = BlockUtils.getDB(
          kvContainer.getContainerData(), conf)) {
        DatanodeStoreSchemaThreeImpl store = (DatanodeStoreSchemaThreeImpl)
            dbHandle.getStore();
        assertEquals(baseCount, store.getMetadataTable()
            .getEstimatedKeyCount());
      }
    }
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testContainerLoadingWithMerkleTreePresent(ContainerTestVersionInfo versionInfo)
      throws Exception {
    setLayoutAndSchemaVersion(versionInfo);
    setup(versionInfo);

    // Create a container with blocks and write MerkleTree
    KeyValueContainer container = createContainer(10L);
    KeyValueContainerData containerData = container.getContainerData();
    ContainerMerkleTreeWriter treeWriter = ContainerMerkleTreeTestUtils.buildTestTree(conf);
    ContainerChecksumTreeManager checksumManager = keyValueHandler.getChecksumManager();
    keyValueHandler.updateContainerChecksum(container, treeWriter);
    long expectedDataChecksum = checksumManager.read(containerData).getContainerMerkleTree().getDataChecksum();

    // Test container loading
    ContainerCache.getInstance(conf).shutdownCache();
    ContainerReader containerReader = new ContainerReader(volumeSet, hddsVolume, containerSet, conf, true);
    containerReader.run();

    // Verify container was loaded successfully and data checksum is set
    Container<?> loadedContainer = containerSet.getContainer(10L);
    assertNotNull(loadedContainer);
    KeyValueContainerData loadedData = (KeyValueContainerData) loadedContainer.getContainerData();
    assertNotSame(containerData, loadedData);
    assertEquals(expectedDataChecksum, loadedData.getDataChecksum());
    verifyAllDataChecksumsMatch(loadedData, conf);
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testContainerLoadingWithMerkleTreeFallbackToRocksDB(ContainerTestVersionInfo versionInfo)
      throws Exception {
    setLayoutAndSchemaVersion(versionInfo);
    setup(versionInfo);

    KeyValueContainer container = createContainer(11L);
    KeyValueContainerData containerData = container.getContainerData();
    ContainerMerkleTreeWriter treeWriter = ContainerMerkleTreeTestUtils.buildTestTree(conf);
    ContainerChecksumTreeManager checksumManager = new ContainerChecksumTreeManager(conf);
    ContainerProtos.ContainerChecksumInfo checksumInfo = checksumManager.updateTree(containerData, treeWriter);
    long dataChecksum = checksumInfo.getContainerMerkleTree().getDataChecksum();

    // Verify no checksum in RocksDB initially
    try (DBHandle dbHandle = BlockUtils.getDB(containerData, conf)) {
      Long dbDataChecksum = dbHandle.getStore().getMetadataTable().get(containerData.getContainerDataChecksumKey());
      assertNull(dbDataChecksum);
    }
    ContainerCache.getInstance(conf).shutdownCache();

    // Test container loading - should read from MerkleTree and store in RocksDB
    ContainerReader containerReader = new ContainerReader(volumeSet, hddsVolume, containerSet, conf, true);
    containerReader.run();

    // Verify container uses checksum from MerkleTree
    Container<?> loadedContainer = containerSet.getContainer(11L);
    assertNotNull(loadedContainer);
    KeyValueContainerData loadedData = (KeyValueContainerData) loadedContainer.getContainerData();
    assertNotSame(containerData, loadedData);
    assertEquals(dataChecksum, loadedData.getDataChecksum());

    // Verify checksum was stored in RocksDB as fallback
    verifyAllDataChecksumsMatch(loadedData, conf);
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testContainerLoadingWithNoChecksumAnywhere(ContainerTestVersionInfo versionInfo)
      throws Exception {
    setLayoutAndSchemaVersion(versionInfo);
    setup(versionInfo);

    KeyValueContainer container = createContainer(12L);
    KeyValueContainerData containerData = container.getContainerData();
    // Verify no checksum in RocksDB
    try (DBHandle dbHandle = BlockUtils.getDB(containerData, conf)) {
      Long dbDataChecksum = dbHandle.getStore().getMetadataTable().get(containerData.getContainerDataChecksumKey());
      assertNull(dbDataChecksum);
    }

    File checksumFile = ContainerChecksumTreeManager.getContainerChecksumFile(containerData);
    assertFalse(checksumFile.exists());

    // Test container loading - should default to 0
    ContainerCache.getInstance(conf).shutdownCache();
    ContainerReader containerReader = new ContainerReader(volumeSet, hddsVolume, containerSet, conf, true);
    containerReader.run();

    // Verify container loads with default checksum of 0
    Container<?> loadedContainer = containerSet.getContainer(12L);
    assertNotNull(loadedContainer);
    KeyValueContainerData loadedData = (KeyValueContainerData) loadedContainer.getContainerData();
    assertNotSame(containerData, loadedData);
    assertEquals(0L, loadedData.getDataChecksum());

    // The checksum is not stored in rocksDB as the checksum file doesn't exist.
    verifyAllDataChecksumsMatch(loadedData, conf);
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testContainerLoadingWithoutMerkleTree(ContainerTestVersionInfo versionInfo)
      throws Exception {
    setLayoutAndSchemaVersion(versionInfo);
    setup(versionInfo);

    KeyValueContainer container = createContainer(13L);
    KeyValueContainerData containerData = container.getContainerData();
    ContainerMerkleTreeWriter treeWriter = new ContainerMerkleTreeWriter();
    keyValueHandler.updateContainerChecksum(container, treeWriter);
    // Create an empty checksum file that exists but has no valid merkle tree
    assertTrue(ContainerChecksumTreeManager.getContainerChecksumFile(containerData).exists());

    // Verify no checksum in RocksDB initially
    try (DBHandle dbHandle = BlockUtils.getDB(containerData, conf)) {
      Long dbDataChecksum = dbHandle.getStore().getMetadataTable().get(containerData.getContainerDataChecksumKey());
      assertNull(dbDataChecksum);
    }

    ContainerCache.getInstance(conf).shutdownCache();

    // Test container loading - should handle when checksum file is present without the container merkle tree and
    // default to 0.
    ContainerReader containerReader = new ContainerReader(volumeSet, hddsVolume, containerSet, conf, true);
    containerReader.run();

    // Verify container loads with default checksum of 0 when checksum file doesn't have merkle tree
    Container<?> loadedContainer = containerSet.getContainer(13L);
    assertNotNull(loadedContainer);
    KeyValueContainerData loadedData = (KeyValueContainerData) loadedContainer.getContainerData();
    assertNotSame(containerData, loadedData);
    assertEquals(0L, loadedData.getDataChecksum());
    verifyAllDataChecksumsMatch(loadedData, conf);
  }

  private KeyValueContainer createContainer(long containerId) throws Exception {
    KeyValueContainerData containerData = new KeyValueContainerData(containerId, layout,
        (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(), datanodeId.toString());
    containerData.setState(ContainerProtos.ContainerDataProto.State.CLOSED);
    KeyValueContainer container = new KeyValueContainer(containerData, conf);
    container.create(volumeSet, volumeChoosingPolicy, clusterId);
    return container;
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
      assertEquals(baseSize + 3,
          metadataTable.getEstimatedKeyCount());
      return baseSize;
    }
  }

  private void setLayoutAndSchemaVersion(
      ContainerTestVersionInfo versionInfo) {
    layout = versionInfo.getLayout();
    String schemaVersion = versionInfo.getSchemaVersion();
    conf = new OzoneConfiguration();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);
  }
}
