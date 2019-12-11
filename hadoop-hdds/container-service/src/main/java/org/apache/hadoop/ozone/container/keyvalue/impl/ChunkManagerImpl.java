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
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.common.interfaces.Container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.EnumMap;
import java.util.Map;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNSUPPORTED_REQUEST;

/**
 * This class is for performing chunk related operations.
 */
public class ChunkManagerImpl implements ChunkManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ChunkManagerImpl.class);

  private final boolean persist;
  private final Map<ChunkLayOutVersion, ChunkManager> handlers
      = new EnumMap<>(ChunkLayOutVersion.class);

  public ChunkManagerImpl(boolean sync) {
    this(sync, true);
  }

  public ChunkManagerImpl(boolean sync, boolean persist) {
    this.persist = persist;

    handlers.put(ChunkLayOutVersion.DUMMY, new ChunkManagerDummyImpl());
    handlers.put(ChunkLayOutVersion.V1, new ChunkManagerV1(sync));
    handlers.put(ChunkLayOutVersion.V2, new ChunkManagerV2(sync));
  }

  @Override
  public void writeChunk(Container container, BlockID blockID, ChunkInfo info,
      ChunkBuffer data, DispatcherContext dispatcherContext)
      throws StorageContainerException {

    selectHandler(container)
        .writeChunk(container, blockID, info, data, dispatcherContext);
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

    selectHandler(container)
        .deleteChunk(container, blockID, info);
    container.getContainerData().decrBytesUsed(info.getLen());
  }

  @Override
  public void shutdown() {
    handlers.values().forEach(ChunkManager::shutdown);
  }

  private @Nonnull ChunkManager selectHandler(Container container)
      throws StorageContainerException {

    if (!persist) {
      return handlers.get(ChunkLayOutVersion.DUMMY);
    }

    ChunkLayOutVersion layout = container.getContainerData().getLayOutVersion();
    return selectVersionHandler(layout);
  }

  private @Nonnull ChunkManager selectVersionHandler(ChunkLayOutVersion version)
      throws StorageContainerException {
    ChunkManager handler = handlers.get(version);
    if (handler == null) {
      return throwUnknownLayoutVersion(version);
    }
    return handler;
  }

  private static ChunkManager throwUnknownLayoutVersion(
      ChunkLayOutVersion version) throws StorageContainerException {

    String message = "Unsupported storage container layout: " + version;
    LOG.warn(message);
    // TODO pick best result code
    throw new StorageContainerException(message, UNSUPPORTED_REQUEST);
  }

}
