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

package org.apache.hadoop.ozone.repair.datanode.schemaupgrade;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V2;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V3;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.COMMIT_STAGE;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.WRITE_STAGE;
import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.apache.hadoop.ozone.container.metadata.DatanodeSchemaThreeDBDefinition.getContainerKeyPrefix;
import static org.apache.ozone.test.IntLambda.withTextFromSystemIn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.CodecTestUtil;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.DatanodeLayoutStorage;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
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
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.apache.hadoop.ozone.repair.OzoneRepair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

/**
 * Tests for {@link UpgradeContainerSchema} class.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestUpgradeContainerSchema {
  private static final String SCM_ID = UUID.randomUUID().toString();
  private OzoneConfiguration conf;

  private MutableVolumeSet volumeSet;
  private DatanodeDetails datanodeDetails;
  private RoundRobinVolumeChoosingPolicy volumeChoosingPolicy;

  private BlockManager blockManager;
  private FilePerBlockStrategy chunkManager;
  private ContainerSet containerSet;
  private List<HddsVolume> volumes;

  @BeforeAll
  void init() {
    CodecBuffer.enableLeakDetection();
  }

  @BeforeEach
  void setup(@TempDir Path testRoot) throws Exception {
    conf = new OzoneConfiguration();

    DatanodeConfiguration dc = conf.getObject(DatanodeConfiguration.class);
    dc.setContainerSchemaV3Enabled(true);
    conf.setFromObject(dc);

    final Path volume1Path = Files.createDirectories(testRoot.resolve("volume1").toAbsolutePath());
    final Path volume2Path = Files.createDirectories(testRoot.resolve("volume2").toAbsolutePath());
    final Path metadataPath = Files.createDirectories(testRoot.resolve("metadata").toAbsolutePath());

    conf.set(HDDS_DATANODE_DIR_KEY, volume1Path + "," + volume2Path);
    conf.set(OZONE_METADATA_DIRS, metadataPath.toString());

    datanodeDetails = MockDatanodeDetails.randomDatanodeDetails();
  }

  private void initDatanode(HDDSLayoutFeature layoutFeature) throws IOException {
    DatanodeLayoutStorage layoutStorage = new DatanodeLayoutStorage(conf,
        datanodeDetails.getUuidString(),
        layoutFeature.layoutVersion());
    layoutStorage.initialize();

    String idFilePath = Objects.requireNonNull(HddsServerUtil.getDatanodeIdFilePath(conf), "datanode.id path");
    ContainerUtils.writeDatanodeDetailsTo(datanodeDetails, new File(idFilePath), conf);

    volumeSet = new MutableVolumeSet(datanodeDetails.getUuidString(), SCM_ID, conf,
        null, StorageVolume.VolumeType.DATA_VOLUME, null);

    // create rocksdb instance in volume dir
    volumes = new ArrayList<>();
    for (StorageVolume storageVolume : volumeSet.getVolumesList()) {
      HddsVolume hddsVolume = (HddsVolume) storageVolume;
      StorageVolumeUtil.checkVolume(hddsVolume, SCM_ID, SCM_ID, conf, null,
          null);
      volumes.add(hddsVolume);
    }

    volumeChoosingPolicy = mock(RoundRobinVolumeChoosingPolicy.class);
    final AtomicInteger loopCount = new AtomicInteger(0);
    when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenAnswer(invocation -> {
          final int ii = loopCount.getAndIncrement() % volumes.size();
          return volumes.get(ii);
        });

    containerSet = newContainerSet();

    blockManager = new BlockManagerImpl(conf);
    chunkManager = new FilePerBlockStrategy(true, blockManager);
  }

  @AfterEach
  void after() throws Exception {
    CodecTestUtil.gc();
  }

  @Test
  void failsBeforeOzoneUpgrade() throws IOException {
    initDatanode(HDDSLayoutFeature.DATANODE_SCHEMA_V2);
    genSchemaV2Containers(1);
    shutdownAllVolume();
    List<VolumeUpgradeResult> results = runCommand(false, GenericCli.EXECUTION_ERROR_EXIT_CODE);
    assertNull(results);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testUpgrade(boolean dryRun) throws IOException {
    initDatanode(HDDSLayoutFeature.DATANODE_SCHEMA_V3);

    final Map<KeyValueContainerData, Map<String, BlockData>>
        keyValueContainerBlockDataMap = genSchemaV2Containers(2);

    shutdownAllVolume();

    List<VolumeUpgradeResult> results = runCommand(dryRun, 0);
    assertNotNull(results);
    assertEquals(2, results.size());
    for (VolumeUpgradeResult result : results) {
      assertTrue(result.isSuccess());
      for (ContainerUpgradeResult cr : result.getResultMap().values()) {
        assertSame(ContainerUpgradeResult.Status.SUCCESS, cr.getStatus());
        KeyValueContainerData pre = assertInstanceOf(KeyValueContainerData.class, cr.getOriginContainerData());
        KeyValueContainerData post = assertInstanceOf(KeyValueContainerData.class, cr.getNewContainerData());
        assertEquals(SCHEMA_V2, pre.getSchemaVersion());
        assertEquals(SCHEMA_V3, post.getSchemaVersion());
        assertEquals(pre.getState(), post.getState());
        String schemaVersionKey = "schemaVersion\\s*:\\W*";
        assertThat(new File(cr.getBackupContainerFilePath()))
            .exists()
            .content(UTF_8)
            .containsPattern(schemaVersionKey + SCHEMA_V2);
        assertThat(new File(cr.getNewContainerFilePath()))
            .exists()
            .content(UTF_8)
            .containsPattern(schemaVersionKey + (dryRun ? SCHEMA_V2 : SCHEMA_V3));
      }
    }

    if (!dryRun) {
      checkV3MetaData(keyValueContainerBlockDataMap, results);
    }
  }

  private List<VolumeUpgradeResult> runCommand(boolean dryRun, int expectedExitCode) {
    CommandLine cmd = new OzoneRepair().getCmd();

    List<String> argList = Stream.of(HDDS_DATANODE_DIR_KEY, OZONE_METADATA_DIRS)
        .flatMap(key -> Stream.of("-D", key + "=" + conf.get(key)))
        .collect(Collectors.toList());
    argList.addAll(Arrays.asList("datanode", "upgrade-container-schema"));
    if (dryRun) {
      argList.add("--dry-run");
    }

    int exitCode = withTextFromSystemIn("y")
        .execute(() -> cmd.execute(argList.toArray(new String[0])));
    assertEquals(expectedExitCode, exitCode);

    UpgradeContainerSchema subject = cmd
        .getSubcommands().get("datanode")
        .getSubcommands().get("upgrade-container-schema")
        .getCommand();

    return subject.getLastResults();
  }

  private Map<String, BlockData> putAnyBlockData(
      KeyValueContainerData data,
      KeyValueContainer container,
      int numBlocks
  ) throws IOException {
    // v3 key ==> block data
    final Map<String, BlockData> containerBlockDataMap = new HashMap<>();

    int txnID = 0;
    for (int i = 0; i < numBlocks; i++) {
      txnID = txnID + 1;
      BlockID blockID =
          ContainerTestHelper.getTestBlockID(data.getContainerID());
      BlockData kd = new BlockData(blockID);
      List<ContainerProtos.ChunkInfo> chunks = new ArrayList<>();
      putChunksInBlock(1, i, chunks, container, blockID);
      kd.setChunks(chunks);

      final String localIDKey = Long.toString(blockID.getLocalID());
      final String blockKey = getContainerKeyPrefix(data.getContainerID()) + localIDKey;
      blockManager.putBlock(container, kd);
      containerBlockDataMap.put(blockKey, kd);
    }

    return containerBlockDataMap;
  }

  private void putChunksInBlock(
      int numOfChunksPerBlock,
      int i,
      List<ContainerProtos.ChunkInfo> chunks,
      KeyValueContainer container,
      BlockID blockID
  ) throws IOException {
    final long chunkLength = 100;
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
  }

  private Map<KeyValueContainerData, Map<String, BlockData>>
      genSchemaV2Containers(int numContainers) throws IOException {
    conf.setBoolean(DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED, false);

    // container id ==> blocks
    final Map<KeyValueContainerData, Map<String, BlockData>> checkBlockDataMap =
        new HashMap<>();

    // create container
    for (int i = 0; i < numContainers; i++) {
      long containerId = ContainerTestHelper.getTestContainerID();

      KeyValueContainerData data = new KeyValueContainerData(containerId,
          ContainerLayoutVersion.FILE_PER_BLOCK,
          ContainerTestHelper.CONTAINER_MAX_SIZE, UUID.randomUUID().toString(),
          datanodeDetails.getUuidString());
      data.setSchemaVersion(SCHEMA_V2);

      KeyValueContainer container = new KeyValueContainer(data, conf);
      container.create(volumeSet, volumeChoosingPolicy, SCM_ID);

      containerSet.addContainer(container);
      data = (KeyValueContainerData) containerSet.getContainer(containerId)
          .getContainerData();
      data.setSchemaVersion(SCHEMA_V2);

      final Map<String, BlockData> blockDataMap =
          putAnyBlockData(data, container, 10);

      data.closeContainer();
      container.close();

      checkBlockDataMap.put(data, blockDataMap);
    }

    assertEquals(numContainers, checkBlockDataMap.size());

    return checkBlockDataMap;
  }

  public void shutdownAllVolume() {
    for (StorageVolume storageVolume : volumeSet.getVolumesList()) {
      storageVolume.shutdown();
    }
  }

  private void checkV3MetaData(Map<KeyValueContainerData,
      Map<String, BlockData>> blockDataMap, List<VolumeUpgradeResult> results) throws IOException {
    Map<Long, VolumeUpgradeResult> volumeResults = new HashMap<>();

    for (VolumeUpgradeResult result : results) {
      result.getResultMap().forEach((k, v) -> volumeResults.put(k, result));
    }

    for (Map.Entry<KeyValueContainerData, Map<String, BlockData>> entry :
        blockDataMap.entrySet()) {
      final KeyValueContainerData containerData = entry.getKey();
      final Map<String, BlockData> blockKeyValue = entry.getValue();
      Long containerID = containerData.getContainerID();

      final DatanodeStoreSchemaThreeImpl datanodeStoreSchemaThree =
          volumeResults.get(containerID).getStore();
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
