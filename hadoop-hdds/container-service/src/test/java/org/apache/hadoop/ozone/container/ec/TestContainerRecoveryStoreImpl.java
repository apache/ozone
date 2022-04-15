/**
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
package org.apache.hadoop.ozone.container.ec;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume.VolumeType;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion.FILE_PER_BLOCK;
import static org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion.FILE_PER_CHUNK;
import static org.apache.hadoop.ozone.container.ec.ContainerRecoveryStoreImpl.CHUNK_DIR;
import static org.apache.hadoop.ozone.container.ec.ContainerRecoveryStoreImpl.RECOVER_DIR;
import static org.apache.hadoop.ozone.container.ec.ContainerRecoveryStoreImpl.getChunkName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * Test for {@link ContainerRecoveryStoreImpl}.
 */
@RunWith(Parameterized.class)
public class TestContainerRecoveryStoreImpl {
  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  private static final int NUM_VOLUMES = 3;
  private static final int NUM_BLOCKS_PER_CONTAINER = 5;
  private static final int NUM_CHUNKS_PER_BLOCK = 5;
  private static final int CHUNK_SIZE = 1024;
  private static final int CONTAINER_MAX_SIZE = 102400;
  private static final String CHUNK_FAIL_MSG = "Write chunk failed.";
  private static final String BLOCK_MGR_FAIL_MSG = "Put block failed.";

  private static final byte[] CHUNK_DATA =
      RandomStringUtils.randomAscii(CHUNK_SIZE).getBytes(UTF_8);

  private final String datanodeId = UUID.randomUUID().toString();
  private final String pipelineId = UUID.randomUUID().toString();
  private final String clusterId = UUID.randomUUID().toString();

  private OzoneConfiguration conf;

  private MutableVolumeSet hddsVolumeSet;

  private long containerID;
  private int replicaIndex;
  private ContainerLayoutVersion layout;
  private KeyValueContainer container;

  private ContainerRecoveryStoreImpl store;

  public TestContainerRecoveryStoreImpl(ContainerLayoutVersion layout) {
    this.layout = layout;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
        {FILE_PER_CHUNK},
        {FILE_PER_BLOCK}
    });
  }

  @Before
  public void setup() throws IOException {
    this.conf = new OzoneConfiguration();

    StringBuilder datanodeDirs = new StringBuilder();
    File[] volumeDirs = new File[NUM_VOLUMES];
    for (int i = 0; i < NUM_VOLUMES; i++) {
      volumeDirs[i] = tempDir.newFolder();
      datanodeDirs.append(volumeDirs[i]).append(",");
    }
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, datanodeDirs.toString());
    hddsVolumeSet = new MutableVolumeSet(datanodeId, clusterId, conf, null,
        VolumeType.DATA_VOLUME, null);

    store = new ContainerRecoveryStoreImpl(hddsVolumeSet, conf);

    containerID = 1L;
    replicaIndex = 1;
    KeyValueContainerData containerData = new KeyValueContainerData(
        containerID, layout, CONTAINER_MAX_SIZE, pipelineId, datanodeId);
    container = new KeyValueContainer(containerData, conf);
    // necessary fields from the caller
    containerData.setState(ContainerProtos.ContainerDataProto.State.CLOSED);
    containerData.setReplicaIndex(replicaIndex);
    containerData.setSchemaVersion(OzoneConsts.SCHEMA_V2);
  }

  @Test
  public void testWriteChunkNormal() throws IOException {
    populateContainer();

    // check directory structure for the container under recovery
    int containerCount = 0;
    for (HddsVolume hddsVolume : StorageVolumeUtil.getHddsVolumesList(
        hddsVolumeSet.getVolumesList())) {
      File clusterIDDir = new File(hddsVolume.getStorageDir(),
          hddsVolume.getClusterID());
      File recovDir = new File(clusterIDDir, RECOVER_DIR);

      // This is the chosen volume for container
      if (recovDir.exists()) {
        File containerDir = new File(recovDir, Long.toString(containerID));
        assertTrue(containerDir.exists());

        File chunksDir = new File(containerDir, CHUNK_DIR);
        assertTrue(chunksDir.exists());

        for (int b = 0; b < NUM_BLOCKS_PER_CONTAINER; b++) {
          BlockID blockID = new BlockID(containerID, b);
          long offset = 0L;
          for (int c = 0; c < NUM_CHUNKS_PER_BLOCK; c++) {
            ChunkInfo chunkInfo = new ChunkInfo(
                getChunkName(blockID, c), offset, CHUNK_SIZE);
            File chunkFile = layout.getChunkFile(chunksDir, blockID, chunkInfo);

            // check local file exist
            assertTrue(chunkFile.exists());
            // check local file size
            long expectedSize = layout == FILE_PER_CHUNK ? CHUNK_SIZE
                : NUM_CHUNKS_PER_BLOCK * CHUNK_SIZE;
            assertEquals(expectedSize, Files.size(chunkFile.toPath()));

            offset += CHUNK_SIZE;
          }
        }
        containerCount++;
      }
    }
    assertEquals(1, containerCount);
  }

  @Test
  public void testWriteChunkFail() throws IOException {
    mockBadChunkManager();

    try {
      populateContainer();
      fail("Layout is bad, writeChunk should fail.");
    } catch (IOException e) {
      assertEquals(CHUNK_FAIL_MSG, e.getMessage());
    }

    for (HddsVolume hddsVolume : StorageVolumeUtil.getHddsVolumesList(
        hddsVolumeSet.getVolumesList())) {
      File clusterIDDir = new File(hddsVolume.getStorageDir(),
          hddsVolume.getClusterID());
      File recovDir = new File(clusterIDDir, RECOVER_DIR);

      // This is the chosen volume for container
      if (recovDir.exists()) {
        File containerDir = new File(recovDir, Long.toString(containerID));
        // check the container dir is cleaned up
        assertFalse(containerDir.exists());
      }
    }
  }

  @Test
  public void testConsolidateContainerFromCache() throws IOException {
    populateContainer();

    store.consolidateContainer(container);
    assertNotNull(container);

    // check created on-disk container
    DataTransferThrottler throttler = mock(DataTransferThrottler.class);
    doNothing().when(throttler).throttle(anyLong(), any());

    assertTrue(container.scanMetaData());
    assertTrue(container.scanData(throttler, null));

    // check recover directory is cleaned up
    for (HddsVolume hddsVolume : StorageVolumeUtil.getHddsVolumesList(
        hddsVolumeSet.getVolumesList())) {
      File clusterIDDir = new File(hddsVolume.getStorageDir(),
          hddsVolume.getClusterID());
      File recovDir = new File(clusterIDDir, RECOVER_DIR);

      if (recovDir.exists()) {
        File containerDir = new File(recovDir, Long.toString(containerID));
        assertFalse(containerDir.exists());
      }
    }
  }

  @Test
  public void testConsolidateContainerFail() throws IOException {
    populateContainer();

    mockBadBlockManager();

    try {
      store.consolidateContainer(container);
      fail("BlockManager is bad, consolidateContainer should fail.");
    } catch (IOException e) {
      assertEquals(BLOCK_MGR_FAIL_MSG, e.getMessage());
    }

    // check container is cleaned up
    assertFalse(new File(container.getContainerData().getMetadataPath())
        .exists());

    // check recover directory is cleaned up
    for (HddsVolume hddsVolume : StorageVolumeUtil.getHddsVolumesList(
        hddsVolumeSet.getVolumesList())) {
      File clusterIDDir = new File(hddsVolume.getStorageDir(),
          hddsVolume.getClusterID());
      File recovDir = new File(clusterIDDir, RECOVER_DIR);

      if (recovDir.exists()) {
        File containerDir = new File(recovDir, Long.toString(containerID));
        assertFalse(containerDir.exists());
      }
    }
  }

  private void populateContainer() throws IOException {
    for (int b = 0; b < NUM_BLOCKS_PER_CONTAINER; b++) {
      BlockID blockID = new BlockID(containerID, b);
      long offset = 0L;

      for (int c = 0; c < NUM_CHUNKS_PER_BLOCK; c++) {
        ChunkInfo chunkInfo = new ChunkInfo(
            getChunkName(blockID, c), offset, CHUNK_SIZE);
        ChunkBuffer data = ChunkBuffer.wrap(ByteBuffer.wrap(CHUNK_DATA));

        store.writeChunk(container, blockID, chunkInfo, data,
            null, c == NUM_CHUNKS_PER_BLOCK - 1);

        offset += CHUNK_SIZE;
      }
    }
  }

  private void mockBadChunkManager() throws IOException {
    ChunkManager chunkManager = mock(ChunkManager.class);
    doThrow(new StorageContainerException(CHUNK_FAIL_MSG, null))
        .when(chunkManager)
        .writeChunk(any(), any(), any(), any(ChunkBuffer.class), any());
    store.setChunkManager(chunkManager);
  }

  private void mockBadBlockManager() throws IOException {
    BlockManager blockManager = mock(BlockManager.class);
    doThrow(new IOException(BLOCK_MGR_FAIL_MSG))
        .when(blockManager).putBlock(any(), any());
    store.setBlockManager(blockManager);
  }
}
