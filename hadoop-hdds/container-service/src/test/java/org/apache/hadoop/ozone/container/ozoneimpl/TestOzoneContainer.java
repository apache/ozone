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

package org.apache.hadoop.ozone.container.ozoneimpl;


import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.ChunkLayoutTestInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.UUID;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.DISK_OUT_OF_SPACE;
import static org.junit.Assert.assertEquals;

/**
 * This class is used to test OzoneContainer.
 */
@RunWith(Parameterized.class)
public class TestOzoneContainer {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneContainer.class);

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OzoneConfiguration conf;
  private String clusterId = UUID.randomUUID().toString();
  private MutableVolumeSet volumeSet;
  private RoundRobinVolumeChoosingPolicy volumeChoosingPolicy;
  private KeyValueContainerData keyValueContainerData;
  private KeyValueContainer keyValueContainer;
  private final DatanodeDetails datanodeDetails = createDatanodeDetails();
  private HashMap<String, Long> commitSpaceMap; //RootDir -> committed space
  private final int numTestContainers = 10;

  private final ChunkLayOutVersion layout;

  public TestOzoneContainer(ChunkLayOutVersion layout) {
    this.layout = layout;
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> parameters() {
    return ChunkLayoutTestInfo.chunkLayoutParameters();
  }

  @Before
  public void setUp() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, folder.getRoot()
        .getAbsolutePath());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        folder.newFolder().getAbsolutePath());
    commitSpaceMap = new HashMap<String, Long>();
    volumeSet = new MutableVolumeSet(datanodeDetails.getUuidString(), conf,
        null);
    volumeChoosingPolicy = new RoundRobinVolumeChoosingPolicy();
  }

  @After
  public void cleanUp() throws Exception {
    if (volumeSet != null) {
      volumeSet.shutdown();
      volumeSet = null;
    }
  }

  @Test
  public void testBuildContainerMap() throws Exception {

    // Format the volumes
    for (HddsVolume volume : volumeSet.getVolumesList()) {
      volume.format(clusterId);
      commitSpaceMap.put(getVolumeKey(volume), Long.valueOf(0));
    }

    // Add containers to disk
    for (int i = 0; i < numTestContainers; i++) {
      long freeBytes = 0;
      long volCommitBytes;
      long maxCap = (long) StorageUnit.GB.toBytes(1);

      HddsVolume myVolume;

      keyValueContainerData = new KeyValueContainerData(i,
          layout,
          maxCap, UUID.randomUUID().toString(),
          datanodeDetails.getUuidString());
      keyValueContainer = new KeyValueContainer(
          keyValueContainerData, conf);
      keyValueContainer.create(volumeSet, volumeChoosingPolicy, clusterId);
      myVolume = keyValueContainer.getContainerData().getVolume();

      freeBytes = addBlocks(keyValueContainer, 2, 3);

      // update our expectation of volume committed space in the map
      volCommitBytes = commitSpaceMap.get(getVolumeKey(myVolume)).longValue();
      Preconditions.checkState(freeBytes >= 0);
      commitSpaceMap.put(getVolumeKey(myVolume),
          Long.valueOf(volCommitBytes + freeBytes));
      BlockUtils.removeDB(keyValueContainerData, conf);
    }

    DatanodeStateMachine stateMachine = Mockito.mock(
        DatanodeStateMachine.class);
    StateContext context = Mockito.mock(StateContext.class);
    Mockito.when(stateMachine.getDatanodeDetails()).thenReturn(datanodeDetails);
    Mockito.when(context.getParent()).thenReturn(stateMachine);
    // When OzoneContainer is started, the containers from disk should be
    // loaded into the containerSet.
    // Also expected to initialize committed space for each volume.
    OzoneContainer ozoneContainer = new
        OzoneContainer(datanodeDetails, conf, context, null);

    ContainerSet containerset = ozoneContainer.getContainerSet();
    assertEquals(numTestContainers, containerset.containerCount());

    verifyCommittedSpace(ozoneContainer);
  }

  @Test
  public void testBuildNodeReport() throws Exception {
    String path = folder.getRoot()
            .getAbsolutePath();
    conf.set(OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR,
            String.join(",",
            path + "/ratis1", path + "/ratis2", path + "ratis3"));
    DatanodeStateMachine stateMachine = Mockito.mock(
            DatanodeStateMachine.class);
    StateContext context = Mockito.mock(StateContext.class);
    Mockito.when(stateMachine.getDatanodeDetails()).thenReturn(datanodeDetails);
    Mockito.when(context.getParent()).thenReturn(stateMachine);
    // When OzoneContainer is started, the containers from disk should be
    // loaded into the containerSet.
    // Also expected to initialize committed space for each volume.
    OzoneContainer ozoneContainer = new
            OzoneContainer(datanodeDetails, conf, context, null);
    Assert.assertEquals(volumeSet.getVolumesList().size(),
            ozoneContainer.getNodeReport().getStorageReportList().size());
    Assert.assertEquals(3,
            ozoneContainer.getNodeReport().getMetadataStorageReportList()
                    .size());

  }

  @Test
  public void testBuildNodeReportWithDefaultRatisLogDir() throws Exception {
    DatanodeStateMachine stateMachine = Mockito.mock(
            DatanodeStateMachine.class);
    StateContext context = Mockito.mock(StateContext.class);
    Mockito.when(stateMachine.getDatanodeDetails()).thenReturn(datanodeDetails);
    Mockito.when(context.getParent()).thenReturn(stateMachine);
    // When OzoneContainer is started, the containers from disk should be
    // loaded into the containerSet.
    // Also expected to initialize committed space for each volume.
    OzoneContainer ozoneContainer = new
            OzoneContainer(datanodeDetails, conf, context, null);
    Assert.assertEquals(volumeSet.getVolumesList().size(),
            ozoneContainer.getNodeReport().getStorageReportList().size());
    Assert.assertEquals(1,
            ozoneContainer.getNodeReport().getMetadataStorageReportList()
                    .size());
  }


  @Test
  public void testContainerCreateDiskFull() throws Exception {
    long containerSize = (long) StorageUnit.MB.toBytes(100);

    // Format the volumes
    for (HddsVolume volume : volumeSet.getVolumesList()) {
      volume.format(UUID.randomUUID().toString());

      // eat up all available space except size of 1 container
      volume.incCommittedBytes(volume.getAvailable() - containerSize);
      // eat up 10 bytes more, now available space is less than 1 container
      volume.incCommittedBytes(10);
    }
    keyValueContainerData = new KeyValueContainerData(99,
        layout, containerSize,
        UUID.randomUUID().toString(), datanodeDetails.getUuidString());
    keyValueContainer = new KeyValueContainer(keyValueContainerData, conf);

    // we expect an out of space Exception
    StorageContainerException e = LambdaTestUtils.intercept(
        StorageContainerException.class,
        () -> keyValueContainer.
            create(volumeSet, volumeChoosingPolicy, clusterId)
    );
    if (!DISK_OUT_OF_SPACE.equals(e.getResult())) {
      LOG.info("Unexpected error during container creation", e);
    }
    assertEquals(DISK_OUT_OF_SPACE, e.getResult());
  }

  //verify committed space on each volume
  private void verifyCommittedSpace(OzoneContainer oc) {
    for (HddsVolume dnVol : oc.getVolumeSet().getVolumesList()) {
      String key = getVolumeKey(dnVol);
      long expectedCommit = commitSpaceMap.get(key).longValue();
      long volumeCommitted = dnVol.getCommittedBytes();
      assertEquals("Volume committed space not initialized correctly",
          expectedCommit, volumeCommitted);
    }
  }

  private long addBlocks(KeyValueContainer container,
      int blocks, int chunksPerBlock) throws Exception {
    String strBlock = "block";
    String strChunk = "-chunkFile";
    int datalen = 65536;
    long usedBytes = 0;

    long freeBytes = container.getContainerData().getMaxSize();
    long containerId = container.getContainerData().getContainerID();
    ReferenceCountedDB db = BlockUtils.getDB(container
        .getContainerData(), conf);

    Table<String, Long> metadataTable = db.getStore().getMetadataTable();
    Table<String, BlockData> blockDataTable = db.getStore().getBlockDataTable();

    for (int bi = 0; bi < blocks; bi++) {
      // Creating BlockData
      BlockID blockID = new BlockID(containerId, bi);
      BlockData blockData = new BlockData(blockID);
      List<ContainerProtos.ChunkInfo> chunkList = new ArrayList<>();

      chunkList.clear();
      for (int ci = 0; ci < chunksPerBlock; ci++) {
        String chunkName = strBlock + bi + strChunk + ci;
        long offset = ci * (long) datalen;
        ChunkInfo info = new ChunkInfo(chunkName, offset, datalen);
        usedBytes += datalen;
        chunkList.add(info.getProtoBufMessage());
      }
      blockData.setChunks(chunkList);
      blockDataTable.put(Long.toString(blockID.getLocalID()), blockData);
    }

    // Set Block count and used bytes.
    metadataTable.put(OzoneConsts.BLOCK_COUNT, (long)blocks);
    metadataTable.put(OzoneConsts.CONTAINER_BYTES_USED, usedBytes);

    // remaining available capacity of the container
    db.close();
    return (freeBytes - usedBytes);
  }

  private String getVolumeKey(HddsVolume volume) {
    return volume.getHddsRootDir().getPath();
  }

  private DatanodeDetails createDatanodeDetails() {
    Random random = new Random();
    String ipAddress =
        random.nextInt(256) + "." + random.nextInt(256) + "." + random
            .nextInt(256) + "." + random.nextInt(256);

    DatanodeDetails.Port containerPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.STANDALONE, 0);
    DatanodeDetails.Port ratisPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.RATIS, 0);
    DatanodeDetails.Port restPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.REST, 0);
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(UUID.randomUUID())
        .setHostName("localhost")
        .setIpAddress(ipAddress)
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort);
    return builder.build();
  }
}
