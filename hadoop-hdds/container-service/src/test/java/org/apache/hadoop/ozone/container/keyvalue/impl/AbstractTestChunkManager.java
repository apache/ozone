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

package org.apache.hadoop.ozone.container.keyvalue.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeIOStats;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.ContainerLayoutTestInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helpers for ChunkManager implementation tests.
 */
public abstract class AbstractTestChunkManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractTestChunkManager.class);

  private HddsVolume hddsVolume;
  private KeyValueContainerData keyValueContainerData;
  private KeyValueContainer keyValueContainer;
  private BlockID blockID;
  private ChunkInfo chunkInfo;
  private ByteBuffer data;
  private byte[] header;
  private BlockManager blockManager;

  protected abstract ContainerLayoutTestInfo getStrategy();

  protected ChunkManager createTestSubject() {
    blockManager = new BlockManagerImpl(new OzoneConfiguration());
    return getStrategy().createChunkManager(true, blockManager);
  }

  @BeforeEach
  public final void setUp(@TempDir File confDir) throws Exception {
    OzoneConfiguration config = new OzoneConfiguration();
    getStrategy().updateConfig(config);
    UUID datanodeId = UUID.randomUUID();
    UUID clusterId = UUID.randomUUID();
    hddsVolume = new HddsVolume.Builder(confDir
        .getAbsolutePath()).conf(config).datanodeUuid(datanodeId
        .toString()).clusterID(clusterId.toString()).build();
    hddsVolume.format(clusterId.toString());
    hddsVolume.createWorkingDir(clusterId.toString(), null);

    VolumeSet volumeSet = mock(MutableVolumeSet.class);

    RoundRobinVolumeChoosingPolicy volumeChoosingPolicy =
        mock(RoundRobinVolumeChoosingPolicy.class);
    when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenReturn(hddsVolume);

    keyValueContainerData = new KeyValueContainerData(1L,
        ContainerLayoutVersion.getConfiguredVersion(config),
        (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
        datanodeId.toString());

    keyValueContainer = new KeyValueContainer(keyValueContainerData, config);

    keyValueContainer.create(volumeSet, volumeChoosingPolicy,
        UUID.randomUUID().toString());

    header = "my header".getBytes(UTF_8);
    byte[] bytes = "testing write chunks".getBytes(UTF_8);
    data = ByteBuffer.allocate(header.length + bytes.length)
        .put(header).put(bytes);
    rewindBufferToDataStart();

    // Creating BlockData
    blockID = new BlockID(1L, 1L);
    chunkInfo = new ChunkInfo(String.format("%d.data.%d", blockID
        .getLocalID(), 0), 0, bytes.length);
  }

  protected Buffer rewindBufferToDataStart() {
    return data.position(header.length);
  }

  protected void checkChunkFileCount(int expected) {
    //As in Setup, we try to create container, these paths should exist.
    String path = keyValueContainerData.getChunksPath();
    assertNotNull(path);

    File dir = new File(path);
    assertTrue(dir.exists());

    File[] files = dir.listFiles();
    assertNotNull(files);
    assertEquals(expected, files.length);
  }

  /**
   * Helper method to check if a file is in use.
   */
  public static boolean isFileNotInUse(String filePath) {
    try {
      Process process = new ProcessBuilder("lsof", "-f", "--", filePath).start();
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), UTF_8))) {
        String output = reader.readLine();  // If lsof returns no output, the file is not in use
        if (output == null) {
          return true;
        }
        LOG.debug("File is in use: {}", filePath);
        return false;
      } catch (IOException e) {
        LOG.warn("Failed to check if file is in use: {}", filePath, e);
        return false;  // On failure, assume the file is in use
      } finally {
        process.destroy();
      }
    } catch (IOException e) {
      // if process cannot be started, skip the test
      LOG.warn("Failed to check if file is in use: {}", filePath, e);
      abort(e.getMessage());
      return false; // unreachable, abort() throws exception
    }
  }

  protected boolean checkChunkFilesClosed() {
    return checkChunkFilesClosed(keyValueContainerData.getChunksPath());
  }

  /**
   * check that all files under chunk path are closed.
  */
  public static boolean checkChunkFilesClosed(String path) {
    //As in Setup, we try to create container, these paths should exist.
    assertNotNull(path);

    File dir = new File(path);
    assertTrue(dir.exists());

    File[] files = dir.listFiles();
    assertNotNull(files);
    for (File file : files) {
      assertTrue(file.exists());
      assertTrue(file.isFile());
      // check that the file is closed.
      if (!isFileNotInUse(file.getAbsolutePath())) {
        return false;
      }
    }
    return true;
  }

  protected void checkWriteIOStats(long length, long opCount) {
    VolumeIOStats volumeIOStats = hddsVolume.getVolumeIOStats();
    assertEquals(length, volumeIOStats.getWriteBytes());
    assertEquals(opCount, volumeIOStats.getWriteOpCount());
  }

  protected void checkReadIOStats(long length, long opCount) {
    VolumeIOStats volumeIOStats = hddsVolume.getVolumeIOStats();
    assertEquals(length, volumeIOStats.getReadBytes());
    assertEquals(opCount, volumeIOStats.getReadOpCount());
  }

  protected HddsVolume getHddsVolume() {
    return hddsVolume;
  }

  protected KeyValueContainerData getKeyValueContainerData() {
    return keyValueContainerData;
  }

  protected KeyValueContainer getKeyValueContainer() {
    return keyValueContainer;
  }

  protected BlockID getBlockID() {
    return blockID;
  }

  protected ChunkInfo getChunkInfo() {
    return chunkInfo;
  }

  protected ByteBuffer getData() {
    return data;
  }

  protected BlockManager getBlockManager() {
    return blockManager;
  }
}
