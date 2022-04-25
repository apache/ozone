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
package org.apache.hadoop.ozone.container.keyvalue;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.createDbInstancesForTestIfNeeded;
import static org.junit.Assert.assertNotNull;

/**
 * Base class for tests identifying issues with key value container contents.
 */
public class TestKeyValueContainerIntegrityChecks {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestKeyValueContainerIntegrityChecks.class);

  private final ContainerLayoutTestInfo containerLayoutTestInfo;
  private MutableVolumeSet volumeSet;
  private OzoneConfiguration conf;
  private File testRoot;
  private ChunkManager chunkManager;
  private String datanodeID = UUID.randomUUID().toString();
  private String clusterID = UUID.randomUUID().toString();

  protected static final int UNIT_LEN = 1024;
  protected static final int CHUNK_LEN = 3 * UNIT_LEN;
  protected static final int CHUNKS_PER_BLOCK = 4;

  public TestKeyValueContainerIntegrityChecks(
      ContainerTestVersionInfo versionInfo) {
    this.conf = new OzoneConfiguration();
    ContainerTestVersionInfo.setTestSchemaVersion(
        versionInfo.getSchemaVersion(), conf);
    if (versionInfo.getLayout()
        .equals(ContainerLayoutVersion.FILE_PER_BLOCK)) {
      containerLayoutTestInfo = ContainerLayoutTestInfo.FILE_PER_BLOCK;
    } else {
      containerLayoutTestInfo = ContainerLayoutTestInfo.FILE_PER_CHUNK;
    }
  }

  @Parameterized.Parameters public static Iterable<Object[]> data() {
    return ContainerTestVersionInfo.versionParameters();
  }

  @Before public void setUp() throws Exception {
    LOG.info("Testing  layout:{}", containerLayoutTestInfo.getLayout());
    this.testRoot = GenericTestUtils.getRandomizedTestDir();
    conf.set(HDDS_DATANODE_DIR_KEY, testRoot.getAbsolutePath());
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, testRoot.getAbsolutePath());
    containerLayoutTestInfo.updateConfig(conf);
    volumeSet = new MutableVolumeSet(UUID.randomUUID().toString(), clusterID,
        conf, null, StorageVolume.VolumeType.DATA_VOLUME, null);
    createDbInstancesForTestIfNeeded(volumeSet, clusterID, clusterID, conf);
    chunkManager = containerLayoutTestInfo.createChunkManager(true, null);
  }

  @After public void teardown() {
    BlockUtils.shutdownCache(conf);
    volumeSet.shutdown();
    FileUtil.fullyDelete(testRoot);
  }

  protected ContainerLayoutVersion getChunkLayout() {
    return containerLayoutTestInfo.getLayout();
  }

  protected OzoneConfiguration getConf() {
    return conf;
  }


  /**
   * Creates a container with normal and deleted blocks.
   * First it will insert normal blocks, and then it will insert
   * deleted blocks.
   */
  protected KeyValueContainer createContainerWithBlocks(long containerId,
      int normalBlocks, int deletedBlocks) throws Exception {
    String strBlock = "block";
    String strChunk = "-chunkFile";
    long totalBlocks = normalBlocks + deletedBlocks;
    int bytesPerChecksum = 2 * UNIT_LEN;
    Checksum checksum = new Checksum(ContainerProtos.ChecksumType.SHA256,
        bytesPerChecksum);
    byte[] chunkData = RandomStringUtils.randomAscii(CHUNK_LEN).getBytes(UTF_8);
    ChecksumData checksumData = checksum.computeChecksum(chunkData);
    DispatcherContext writeStage = new DispatcherContext.Builder()
        .setStage(DispatcherContext.WriteChunkStage.WRITE_DATA)
        .build();
    DispatcherContext commitStage = new DispatcherContext.Builder()
        .setStage(DispatcherContext.WriteChunkStage.COMMIT_DATA)
        .build();

    KeyValueContainerData containerData = new KeyValueContainerData(containerId,
        containerLayoutTestInfo.getLayout(),
        (long) CHUNKS_PER_BLOCK * CHUNK_LEN * totalBlocks,
        UUID.randomUUID().toString(), UUID.randomUUID().toString());
    KeyValueContainer container = new KeyValueContainer(containerData, conf);
    container.create(volumeSet, new RoundRobinVolumeChoosingPolicy(),
        clusterID);
    try (DBHandle metadataStore = BlockUtils.getDB(containerData,
        conf)) {
      assertNotNull(containerData.getChunksPath());
      File chunksPath = new File(containerData.getChunksPath());
      containerLayoutTestInfo.validateFileCount(chunksPath, 0, 0);

      List<ContainerProtos.ChunkInfo> chunkList = new ArrayList<>();
      for (int i = 0; i < totalBlocks; i++) {
        BlockID blockID = new BlockID(containerId, i);
        BlockData blockData = new BlockData(blockID);

        chunkList.clear();
        for (long chunkCount = 0; chunkCount < CHUNKS_PER_BLOCK; chunkCount++) {
          String chunkName = strBlock + i + strChunk + chunkCount;
          long offset = chunkCount * CHUNK_LEN;
          ChunkInfo info = new ChunkInfo(chunkName, offset, CHUNK_LEN);
          info.setChecksumData(checksumData);
          chunkList.add(info.getProtoBufMessage());
          chunkManager.writeChunk(container, blockID, info,
              ByteBuffer.wrap(chunkData), writeStage);
          chunkManager.writeChunk(container, blockID, info,
              ByteBuffer.wrap(chunkData), commitStage);
        }
        blockData.setChunks(chunkList);

        // normal key
        String key = containerData.blockKey(blockID.getLocalID());
        if (i >= normalBlocks) {
          // deleted key
          key = containerData.deletingBlockKey(blockID.getLocalID());
        }
        metadataStore.getStore().getBlockDataTable().put(key, blockData);
      }

      containerLayoutTestInfo.validateFileCount(chunksPath, totalBlocks,
          totalBlocks * CHUNKS_PER_BLOCK);
    }

    return container;
  }

}
