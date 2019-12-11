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

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeIOStats;
import org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.common.interfaces.Container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.CONTAINER_INTERNAL_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.NO_SUCH_ALGORITHM;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNSUPPORTED_REQUEST;
import static org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion.V2;
import static org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext.WriteChunkStage.COMBINED;
import static org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext.WriteChunkStage.COMMIT_DATA;
import static org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext.WriteChunkStage.WRITE_DATA;
import static org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils.writeData;

/**
 * This class is for performing chunk related operations.
 */
public class ChunkManagerV2 implements ChunkManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ChunkManagerV2.class);

  private final boolean doSyncWrite;

  ChunkManagerV2(boolean sync) {
    doSyncWrite = sync;
  }

  private static void checkLayoutVersion(Container container) {
    Preconditions.checkArgument(
        container.getContainerData().getLayOutVersion() == V2);
  }

  @Override
  public void writeChunk(Container container, BlockID blockID, ChunkInfo info,
      ChunkBuffer data, DispatcherContext dispatcherContext)
      throws StorageContainerException {

    checkLayoutVersion(container);

    Preconditions.checkNotNull(dispatcherContext);
    DispatcherContext.WriteChunkStage stage = dispatcherContext.getStage();

    if (info.getLen() <= 0) {
      LOG.debug("Skip writing empty chunk {} in stage {}", info, stage);
      return;
    }

    try {
      KeyValueContainerData containerData = (KeyValueContainerData) container
          .getContainerData();

      long len = info.getLen();
      if (stage == WRITE_DATA || stage == COMBINED) {
        long offset = info.getOffset();
        File chunkFile = getChunkFile(info, containerData);
        validateChunkFileSize(info, chunkFile);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Writing chunk {} in stage {} to file {}",
              info, stage, chunkFile);
        }

        HddsVolume volume = containerData.getVolume();
        VolumeIOStats volumeIOStats = volume.getVolumeIOStats();

        writeData(chunkFile, data, offset, len, volumeIOStats, doSyncWrite);
      }
      if (stage == COMMIT_DATA || stage == COMBINED) {
        containerData.updateWriteStats(len, false);
      }
    } catch (NoSuchAlgorithmException ex) {
      wrapInStorageContainerException(ex, NO_SUCH_ALGORITHM);
    } catch (ExecutionException ex) {
      wrapInStorageContainerException(ex, CONTAINER_INTERNAL_ERROR);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      wrapInStorageContainerException(e, CONTAINER_INTERNAL_ERROR);
    }
  }

  private static void validateChunkFileSize(ChunkInfo info, File chunkFile)
      throws StorageContainerException {

    long size = chunkFile.length();
    long offset = info.getOffset();
    if (offset != size) {
      String message = String.format(
          "Unexpected offset for %s, got: %d, expected %d",
          info.getChunkName(), offset, size);
      LOG.warn(message);
      throw new StorageContainerException(message, UNSUPPORTED_REQUEST);
    }
  }

  @Override
  public ChunkBuffer readChunk(Container container, BlockID blockID,
      ChunkInfo info, DispatcherContext dispatcherContext)
      throws StorageContainerException {

    checkLayoutVersion(container);

    if (info.getLen() <= 0) {
      LOG.debug("Skip reading empty chunk {}", info);
      return ChunkBuffer.wrap(ByteBuffer.wrap(new byte[0]));
    }

    KeyValueContainerData containerData = (KeyValueContainerData) container
        .getContainerData();

    HddsVolume volume = containerData.getVolume();
    VolumeIOStats volumeIOStats = volume.getVolumeIOStats();

    File chunkFile = getChunkFile(info, containerData);

    long len = info.getLen();
    long offset = info.getOffset();
    ByteBuffer data = ByteBuffer.allocate((int) len);
    ChunkUtils.readData(chunkFile, data, offset, len, volumeIOStats);

    return ChunkBuffer.wrap(data);
  }

  @Override
  public void deleteChunk(Container container, BlockID blockID, ChunkInfo info)
      throws StorageContainerException {

    checkLayoutVersion(container);

    Preconditions.checkNotNull(blockID, "Block ID cannot be null.");
    KeyValueContainerData containerData = (KeyValueContainerData) container
        .getContainerData();

    File chunkFile = getChunkFile(info, containerData);

    // if the chunk file does not exist, it might have already been deleted.
    // The call might be because of reapply of transactions on datanode
    // restart.
    if (!chunkFile.exists()) {
      LOG.warn("Chunk file doe not exist. {}", info);
      return;
    }

    long fileLength = chunkFile.length();
    if ((info.getOffset() == 0) && (info.getLen() == fileLength)) {
      FileUtil.fullyDelete(chunkFile);
    } else {
      String msg = String.format(
          "Trying to delete partial chunk %s from file %s with length %s",
          info, chunkFile, fileLength);
      LOG.error(msg);
      throw new StorageContainerException(msg, UNSUPPORTED_REQUEST);
    }
  }

  private static File getChunkFile(
      ChunkInfo info, KeyValueContainerData containerData) {
    return new File(containerData.getChunksPath(), info.getChunkName());
  }

  private static void wrapInStorageContainerException(Exception ex,
      ContainerProtos.Result result) throws StorageContainerException {
    throw new StorageContainerException("Internal error: ", ex, result);
  }

}
