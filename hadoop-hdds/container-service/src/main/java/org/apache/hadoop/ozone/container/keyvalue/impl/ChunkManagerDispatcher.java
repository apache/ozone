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

package org.apache.hadoop.ozone.container.keyvalue.impl;

import com.google.common.base.Preconditions;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.common.interfaces.Container;

import org.apache.ratis.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNSUPPORTED_REQUEST;
import static org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion.FILE_PER_BLOCK;
import static org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion.FILE_PER_CHUNK;

/**
 * Selects ChunkManager implementation to use for each chunk operation.
 */
public class ChunkManagerDispatcher implements ChunkManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ChunkManagerDispatcher.class);

  private final Map<ContainerLayoutVersion, ChunkManager> handlers
      = new EnumMap<>(ContainerLayoutVersion.class);

  ChunkManagerDispatcher(boolean sync, BlockManager manager,
                         VolumeSet volSet) {
    handlers.put(FILE_PER_CHUNK,
        new FilePerChunkStrategy(sync, manager, volSet));
    handlers.put(FILE_PER_BLOCK,
        new FilePerBlockStrategy(sync, manager, volSet));
  }

  @Override
  public void writeChunk(Container container, BlockID blockID, ChunkInfo info,
      ChunkBuffer data, DispatcherContext dispatcherContext)
      throws StorageContainerException {

    selectHandler(container)
        .writeChunk(container, blockID, info, data, dispatcherContext);
  }

  public String streamInit(Container container, BlockID blockID)
      throws StorageContainerException {
    return selectHandler(container)
        .streamInit(container, blockID);
  }

  @Override
  public StateMachine.DataChannel getStreamDataChannel(
          Container container, BlockID blockID, ContainerMetrics metrics)
          throws StorageContainerException {
    return selectHandler(container)
            .getStreamDataChannel(container, blockID, metrics);
  }

  @Override
  public void finishWriteChunks(KeyValueContainer kvContainer,
      BlockData blockData) throws IOException {

    selectHandler(kvContainer)
        .finishWriteChunks(kvContainer, blockData);
  }

  @Override
  public ChunkBuffer readChunk(Container container, BlockID blockID,
      ChunkInfo info, DispatcherContext dispatcherContext)
      throws StorageContainerException {

    ChunkBuffer data = selectHandler(container)
        .readChunk(container, blockID, info, dispatcherContext);

    Preconditions.checkState(data != null);
    container.getContainerData().updateReadStats(data.remaining());

    return data;
  }

  @Override
  public void deleteChunk(Container container, BlockID blockID, ChunkInfo info)
      throws StorageContainerException {

    Preconditions.checkNotNull(blockID, "Block ID cannot be null.");

    // Delete the chunk from disk.
    // Do not decrement the ContainerData counters (usedBytes) here as it
    // will be updated while deleting the block from the DB

    selectHandler(container).deleteChunk(container, blockID, info);

  }

  @Override
  public void deleteChunks(Container container, BlockData blockData)
      throws StorageContainerException {

    Preconditions.checkNotNull(blockData, "Block data cannot be null.");

    // Delete the chunks belonging to blockData.
    // Do not decrement the ContainerData counters (usedBytes) here as it
    // will be updated while deleting the block from the DB

    selectHandler(container).deleteChunks(container, blockData);
  }

  @Override
  public void shutdown() {
    handlers.values().forEach(ChunkManager::shutdown);
  }

  private @Nonnull ChunkManager selectHandler(Container container)
      throws StorageContainerException {

    ContainerLayoutVersion layout =
        container.getContainerData().getLayoutVersion();
    return selectVersionHandler(layout);
  }

  private @Nonnull ChunkManager selectVersionHandler(
      ContainerLayoutVersion version)
      throws StorageContainerException {
    ChunkManager versionHandler = handlers.get(version);
    if (versionHandler == null) {
      return throwUnknownLayoutVersion(version);
    }
    return versionHandler;
  }

  private static ChunkManager throwUnknownLayoutVersion(
      ContainerLayoutVersion version) throws StorageContainerException {

    String message = "Unsupported storage container layout: " + version;
    LOG.warn(message);
    // TODO pick best result code
    throw new StorageContainerException(message, UNSUPPORTED_REQUEST);
  }

}
