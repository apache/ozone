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
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.ContainerTestVersionInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.UUID;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.DISK_OUT_OF_SPACE;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.createDbInstancesForTestIfNeeded;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * This class is used to test OzoneContainer.
 */
public class TestOzoneContainer {

  @TempDir
  private Path folder;

  private OzoneConfiguration conf;
  private String clusterId = UUID.randomUUID().toString();
  private MutableVolumeSet volumeSet;
  private RoundRobinVolumeChoosingPolicy volumeChoosingPolicy;
  private KeyValueContainerData keyValueContainerData;
  private KeyValueContainer keyValueContainer;
  private final DatanodeDetails datanodeDetails = createDatanodeDetails();
  private HashMap<String, Long> commitSpaceMap; //RootDir -> committed space

  private ContainerLayoutVersion layout;
  private String schemaVersion;

  private void initTest(ContainerTestVersionInfo versionInfo) throws Exception {
    this.layout = versionInfo.getLayout();
    this.schemaVersion = versionInfo.getSchemaVersion();
    this.conf = new OzoneConfiguration();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);
    setup();
  }

  private void setup() throws Exception {
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, folder.toString());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        Files.createDirectory(folder.resolve("MetadataDir")).toString());
    commitSpaceMap = new HashMap<>();
    volumeSet = new MutableVolumeSet(datanodeDetails.getUuidString(),
        clusterId, conf, null, StorageVolume.VolumeType.DATA_VOLUME, null);
    createDbInstancesForTestIfNeeded(volumeSet, clusterId, clusterId, conf);
    volumeChoosingPolicy = new RoundRobinVolumeChoosingPolicy();
  }

  @AfterEach
  public void cleanUp() {
    BlockUtils.shutdownCache(conf);

    if (volumeSet != null) {
      volumeSet.shutdown();
      volumeSet = null;
    }
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testBuildContainerMap(ContainerTestVersionInfo versionInfo)
      throws Exception {
    initTest(versionInfo);
    // Format the volumes
    List<HddsVolume> volumes =
        StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList());
    for (HddsVolume volume : volumes) {
      volume.format(clusterId);
      commitSpaceMap.put(getVolumeKey(volume), Long.valueOf(0));
    }

    // Add containers to disk
    int numTestContainers = 10;
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
    }
    BlockUtils.shutdownCache(conf);
    OzoneContainer ozoneContainer = ContainerTestUtils
        .getOzoneContainer(datanodeDetails, conf);

    ContainerSet containerset = ozoneContainer.getContainerSet();
    assertEquals(numTestContainers, containerset.containerCount());

    verifyCommittedSpace(ozoneContainer);
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testBuildNodeReport(ContainerTestVersionInfo versionInfo)
      throws Exception {
    initTest(versionInfo);
    String path = folder.toString();
    conf.set(OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR,
        String.join(",",
            path + "/ratis1", path + "/ratis2", path + "ratis3"));

    File[] dbPaths = new File[3];
    StringBuilder dbDirString = new StringBuilder();
    for (int i = 0; i < 3; i++) {
      dbPaths[i] =
          Files.createDirectory(folder.resolve(Integer.toString(i))).toFile();
      dbDirString.append(dbPaths[i]).append(",");
    }
    conf.set(OzoneConfigKeys.HDDS_DATANODE_CONTAINER_DB_DIR,
        dbDirString.toString());
    ContainerTestUtils.enableSchemaV3(conf);
    OzoneContainer ozoneContainer = ContainerTestUtils
        .getOzoneContainer(datanodeDetails, conf);
    Assertions.assertEquals(volumeSet.getVolumesList().size(),
        ozoneContainer.getNodeReport().getStorageReportList().size());
    Assertions.assertEquals(3,
        ozoneContainer.getNodeReport().getMetadataStorageReportList()
            .size());
    Assertions.assertEquals(3,
        ozoneContainer.getNodeReport().getDbStorageReportList().size());
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testBuildNodeReportWithDefaultRatisLogDir(
      ContainerTestVersionInfo versionInfo) throws Exception {
    initTest(versionInfo);
    OzoneContainer ozoneContainer = ContainerTestUtils
        .getOzoneContainer(datanodeDetails, conf);
    Assertions.assertEquals(volumeSet.getVolumesList().size(),
        ozoneContainer.getNodeReport().getStorageReportList().size());
    Assertions.assertEquals(1,
        ozoneContainer.getNodeReport().getMetadataStorageReportList()
            .size());
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testContainerCreateDiskFull(ContainerTestVersionInfo versionInfo)
      throws Exception {
    initTest(versionInfo);
    long containerSize = (long) StorageUnit.MB.toBytes(100);

    List<HddsVolume> volumes =
        StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList());
    // Format the volumes
    for (HddsVolume volume : volumes) {
      volume.format(clusterId);

      // eat up all available space except size of 1 container
      volume.incCommittedBytes(volume.getAvailable() - containerSize);
      // eat up 10 bytes more, now available space is less than 1 container
      volume.incCommittedBytes(10);
    }
    keyValueContainerData = new KeyValueContainerData(99,
        layout, containerSize,
        UUID.randomUUID().toString(), datanodeDetails.getUuidString());
    keyValueContainer = new KeyValueContainer(keyValueContainerData, conf);

    StorageContainerException e = assertThrows(
        StorageContainerException.class,
        () -> keyValueContainer.
            create(volumeSet, volumeChoosingPolicy, clusterId)
    );
    assertEquals(DISK_OUT_OF_SPACE, e.getResult());
  }

  //verify committed space on each volume
  private void verifyCommittedSpace(OzoneContainer oc) {
    List<HddsVolume> volumes = StorageVolumeUtil.getHddsVolumesList(
        oc.getVolumeSet().getVolumesList());
    for (HddsVolume dnVol : volumes) {
      String key = getVolumeKey(dnVol);
      long expectedCommit = commitSpaceMap.get(key).longValue();
      long volumeCommitted = dnVol.getCommittedBytes();
      assertEquals(expectedCommit, volumeCommitted,
          "Volume committed space not initialized correctly");
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
    KeyValueContainerData cData = container.getContainerData();
    try (DBHandle db = BlockUtils.getDB(cData, conf)) {

      Table<String, Long> metadataTable =
          db.getStore().getMetadataTable();
      Table<String, BlockData> blockDataTable =
          db.getStore().getBlockDataTable();

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
        blockDataTable.put(cData.getBlockKey(blockID.getLocalID()),
            blockData);
      }

      // Set Block count and used bytes.
      metadataTable.put(cData.getBlockCountKey(), (long) blocks);
      metadataTable.put(cData.getBytesUsedKey(), usedBytes);
    }
    // remaining available capacity of the container
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
