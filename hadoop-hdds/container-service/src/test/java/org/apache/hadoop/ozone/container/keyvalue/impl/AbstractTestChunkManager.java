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
package org.apache.hadoop.ozone.container.keyvalue.impl;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeIOStats;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.ChunkLayoutTestInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;

/**
 * Helpers for ChunkManager implementation tests.
 */
public abstract class AbstractTestChunkManager {

  private HddsVolume hddsVolume;
  private KeyValueContainerData keyValueContainerData;
  private KeyValueContainer keyValueContainer;
  private BlockID blockID;
  private ChunkInfo chunkInfo;
  private ByteBuffer data;
  private byte[] header;
  private BlockManager blockManager;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  protected abstract ChunkLayoutTestInfo getStrategy();

  protected ChunkManager createTestSubject() {
    blockManager = new BlockManagerImpl(new OzoneConfiguration());
    return getStrategy().createChunkManager(true, blockManager);
  }

  @Before
  public final void setUp() throws Exception {
    OzoneConfiguration config = new OzoneConfiguration();
    getStrategy().updateConfig(config);
    UUID datanodeId = UUID.randomUUID();
    hddsVolume = new HddsVolume.Builder(folder.getRoot()
        .getAbsolutePath()).conf(config).datanodeUuid(datanodeId
        .toString()).build();

    VolumeSet volumeSet = mock(MutableVolumeSet.class);

    RoundRobinVolumeChoosingPolicy volumeChoosingPolicy =
        mock(RoundRobinVolumeChoosingPolicy.class);
    Mockito.when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenReturn(hddsVolume);

    keyValueContainerData = new KeyValueContainerData(1L,
        ChunkLayOutVersion.getConfiguredVersion(config),
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

  protected DispatcherContext getDispatcherContext() {
    return new DispatcherContext.Builder().build();
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
