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

import static org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils.limitReadSize;

import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeIOStats;
import org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;

/**
 * Implementation of ChunkManager built for running performance tests.
 * Chunks are not written to disk, Reads are returned with zero-filled buffers
 */
public class ChunkManagerDummyImpl implements ChunkManager {

  @Override
  public void writeChunk(Container container, BlockID blockID, ChunkInfo info,
      ChunkBuffer data, DispatcherContext dispatcherContext)
      throws StorageContainerException {

    Objects.requireNonNull(dispatcherContext, "dispatcherContext == null");
    DispatcherContext.WriteChunkStage stage = dispatcherContext.getStage();

    ContainerData containerData = container.getContainerData();

    if (stage.isWrite()) {
      ChunkUtils.validateBufferSize(info.getLen(), data.remaining());

      HddsVolume volume = containerData.getVolume();
      VolumeIOStats volumeIOStats = volume.getVolumeIOStats();
      volumeIOStats.incWriteOpCount();
      volumeIOStats.incWriteBytes(info.getLen());
    }

    if (stage.isCommit()) {
      containerData.updateWriteStats(info.getLen(), false);
    }
  }

  /**
   * return a zero-filled buffer.
   */
  @Override
  public ChunkBuffer readChunk(Container container, BlockID blockID,
      ChunkInfo info, DispatcherContext dispatcherContext)
      throws StorageContainerException {

    limitReadSize(info.getLen());
    // stats are handled in ChunkManagerImpl
    return ChunkBuffer.wrap(ByteBuffer.allocate((int) info.getLen()));
  }

  @Override
  public void deleteChunk(Container container, BlockID blockID,
      ChunkInfo info) {
    // no-op (stats are handled in ChunkManagerImpl)
  }

  @Override
  public void deleteChunks(Container container, BlockData blockData)
      throws StorageContainerException {
    // no-op (stats are handled in ChunkManagerImpl)
  }
}
