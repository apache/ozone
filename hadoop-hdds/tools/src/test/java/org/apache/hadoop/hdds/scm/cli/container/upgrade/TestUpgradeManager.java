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
package org.apache.hadoop.hdds.scm.cli.container.upgrade;

import com.google.common.collect.Lists;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.CodecTestUtil;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.impl.BlockManagerImpl;
import org.apache.hadoop.ozone.container.keyvalue.impl.FilePerBlockStrategy;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaThreeDBDefinition;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.COMMIT_STAGE;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.WRITE_STAGE;
import static org.apache.hadoop.ozone.container.common.states.endpoint.VersionEndpointTask.LOG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for UpgradeManager class.
 */
public class TestUpgradeManager {
  private static final String SCM_ID = UUID.randomUUID().toString();
  private static final OzoneConfiguration CONF = new OzoneConfiguration();

  @TempDir
  private File testRoot;
  private MutableVolumeSet volumeSet;
  private UUID datanodeId;
  private RoundRobinVolumeChoosingPolicy volumeChoosingPolicy;

  private BlockManager blockManager;
  private FilePerBlockStrategy chunkManager;
  private ContainerSet containerSet;

  @BeforeEach
  public void setup() throws Exception {
    DatanodeConfiguration dc = CONF.getObject(DatanodeConfiguration.class);
    dc.setContainerSchemaV3Enabled(true);
    CONF.setFromObject(dc);

    final File volume1Path = new File(testRoot, "volume1");
    final File volume2Path = new File(testRoot, "volume2");

    assertTrue(volume1Path.mkdirs());
    assertTrue(volume2Path.mkdirs());

    final File metadataPath = new File(testRoot, "metadata");
    assertTrue(metadataPath.mkdirs());

    CONF.set(HDDS_DATANODE_DIR_KEY,
        volume1Path.getAbsolutePath() + "," + volume2Path.getAbsolutePath());
    CONF.set(OZONE_METADATA_DIRS, metadataPath.getAbsolutePath());
    datanodeId = UUID.randomUUID();
    volumeSet = new MutableVolumeSet(datanodeId.toString(), SCM_ID, CONF,
        null, StorageVolume.VolumeType.DATA_VOLUME, null);

    // create rocksdb instance in volume dir
    final List<HddsVolume> volumes = new ArrayList<>();
    for (StorageVolume storageVolume : volumeSet.getVolumesList()) {
      HddsVolume hddsVolume = (HddsVolume) storageVolume;
      StorageVolumeUtil.checkVolume(hddsVolume, SCM_ID, SCM_ID, CONF, null,
          null);
      volumes.add(hddsVolume);
    }

    DatanodeDetails datanodeDetails = mock(DatanodeDetails.class);
    when(datanodeDetails.getUuidString()).thenReturn(datanodeId.toString());
    when(datanodeDetails.getUuid()).thenReturn(datanodeId);

    volumeChoosingPolicy = mock(RoundRobinVolumeChoosingPolicy.class);
    final AtomicInteger loopCount = new AtomicInteger(0);
    when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenAnswer(invocation -> {
          final int ii = loopCount.getAndIncrement() % volumes.size();
          return volumes.get(ii);
        });

    containerSet = new ContainerSet(1000);

    blockManager = new BlockManagerImpl(CONF);
    chunkManager = new FilePerBlockStrategy(true, blockManager, null);
  }

  @BeforeAll
  public static void beforeClass() {
    CodecBuffer.enableLeakDetection();
  }

  @AfterEach
  public void after() throws Exception {
    CodecTestUtil.gc();
  }

  @Test
  public void testUpgrade() throws IOException {
    int num = 2;

    final Map<KeyValueContainerData, Map<String, BlockData>>
        keyValueContainerBlockDataMap = genSchemaV2Containers(num);
    assertEquals(num, keyValueContainerBlockDataMap.size());

    shutdownAllVolume();

    final UpgradeManager upgradeManager = new UpgradeManager();
    final List<UpgradeManager.Result> results =
        upgradeManager.run(CONF,
            StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList()));

    checkV3MetaData(keyValueContainerBlockDataMap, results, upgradeManager);
  }

  private Map<String, BlockData> putAnyBlockData(KeyValueContainerData data,
                                                 KeyValueContainer container,
                                                 int numBlocks) {
    // v3 key ==> block data
    final Map<String, BlockData> containerBlockDataMap = new HashMap<>();

    int txnID = 0;
    for (int i = 0; i < numBlocks; i++) {
      txnID = txnID + 1;
      BlockID blockID =
          ContainerTestHelper.getTestBlockID(data.getContainerID());
      BlockData kd = new BlockData(blockID);
      List<ContainerProtos.ChunkInfo> chunks = Lists.newArrayList();
      putChunksInBlock(1, i, chunks, container, blockID);
      kd.setChunks(chunks);

      try {
        final String localIDKey = Long.toString(blockID.getLocalID());
        final String blockKey = DatanodeSchemaThreeDBDefinition
            .getContainerKeyPrefix(data.getContainerID()) + localIDKey;
        blockManager.putBlock(container, kd);
        containerBlockDataMap.put(blockKey, kd);
      } catch (IOException exception) {
        LOG.warn("Failed to put block: " + blockID.getLocalID()
            + " in BlockDataTable.", exception);
      }
    }

    return containerBlockDataMap;
  }

  private void putChunksInBlock(int numOfChunksPerBlock, int i,
                                List<ContainerProtos.ChunkInfo> chunks,
                                KeyValueContainer container, BlockID blockID) {
    final long chunkLength = 100;
    try {
      for (int k = 0; k < numOfChunksPerBlock; k++) {
        final String chunkName = String.format("%d_chunk_%d_block_%d",
            blockID.getContainerBlockID().getLocalID(), k, i);
        final long offset = k * chunkLength;
        ContainerProtos.ChunkInfo info =
            ContainerProtos.ChunkInfo.newBuilder().setChunkName(chunkName)
                .setLen(chunkLength).setOffset(offset)
                .setChecksumData(Checksum.getNoChecksumDataProto()).build();
        chunks.add(info);
        ChunkInfo chunkInfo = new ChunkInfo(chunkName, offset, chunkLength);
        try (ChunkBuffer chunkData = ChunkBuffer.allocate((int) chunkLength)) {
          chunkManager.writeChunk(container, blockID, chunkInfo, chunkData, WRITE_STAGE);
          chunkManager.writeChunk(container, blockID, chunkInfo, chunkData, COMMIT_STAGE);
        }
      }
    } catch (IOException ex) {
      LOG.warn("Putting chunks in blocks was not successful for BlockID: "
          + blockID);
    }
  }

  private Map<KeyValueContainerData, Map<String, BlockData>>
      genSchemaV2Containers(int numContainers) throws IOException {
    CONF.setBoolean(DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED, false);

    // container id ==> blocks
    final Map<KeyValueContainerData, Map<String, BlockData>> checkBlockDataMap =
        new HashMap<>();

    // create container
    for (int i = 0; i < numContainers; i++) {
      long containerId = ContainerTestHelper.getTestContainerID();

      KeyValueContainerData data = new KeyValueContainerData(containerId,
          ContainerLayoutVersion.FILE_PER_BLOCK,
          ContainerTestHelper.CONTAINER_MAX_SIZE, UUID.randomUUID().toString(),
          datanodeId.toString());
      data.setSchemaVersion(OzoneConsts.SCHEMA_V2);

      KeyValueContainer container = new KeyValueContainer(data, CONF);
      container.create(volumeSet, volumeChoosingPolicy, SCM_ID);

      containerSet.addContainer(container);
      data = (KeyValueContainerData) containerSet.getContainer(containerId)
          .getContainerData();
      data.setSchemaVersion(OzoneConsts.SCHEMA_V2);

      final Map<String, BlockData> blockDataMap =
          putAnyBlockData(data, container, 10);

      data.closeContainer();
      container.close();

      checkBlockDataMap.put(data, blockDataMap);
    }
    return checkBlockDataMap;
  }

  public void shutdownAllVolume() {
    for (StorageVolume storageVolume : volumeSet.getVolumesList()) {
      storageVolume.shutdown();
    }
  }

  private void checkV3MetaData(Map<KeyValueContainerData,
      Map<String, BlockData>> blockDataMap, List<UpgradeManager.Result> results,
      UpgradeManager upgradeManager) throws IOException {
    Map<Long, UpgradeTask.UpgradeContainerResult> resultMap = new HashMap<>();

    for (UpgradeManager.Result result : results) {
      resultMap.putAll(result.getResultMap());
    }

    for (Map.Entry<KeyValueContainerData, Map<String, BlockData>> entry :
        blockDataMap.entrySet()) {
      final KeyValueContainerData containerData = entry.getKey();
      final Map<String, BlockData> blockKeyValue = entry.getValue();

      final UpgradeTask.UpgradeContainerResult result =
          resultMap.get(containerData.getContainerID());
      final KeyValueContainerData v3ContainerData =
          (KeyValueContainerData) result.getNewContainerData();

      final DatanodeStoreSchemaThreeImpl datanodeStoreSchemaThree =
          upgradeManager.getDBStore(v3ContainerData.getVolume());
      final Table<String, BlockData> blockDataTable =
          datanodeStoreSchemaThree.getBlockDataTable();

      for (Map.Entry<String, BlockData> blockDataEntry : blockKeyValue
          .entrySet()) {
        final String v3key = blockDataEntry.getKey();
        final BlockData blockData = blockDataTable.get(v3key);
        final BlockData originBlockData = blockDataEntry.getValue();

        assertEquals(originBlockData.getSize(), blockData.getSize());
        assertEquals(originBlockData.getLocalID(), blockData.getLocalID());
      }
    }
  }
}
