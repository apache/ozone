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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
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

  private static final int DEFAULT_MAP_SIZE = 1 * 1024 * 1024; // 1MB

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private volatile MappedByteBuffer mapped;
  private volatile int mappedSize;
  private volatile Path backingFile;

  private void ensureMapped(int minSize)
      throws StorageContainerException {
    if (mapped != null && mappedSize >= minSize) {
      return;
    }

    lock.writeLock().lock();
    try {
      if (mapped != null && mappedSize >= minSize) {
        return;
      }

      int newSize = Math.max(DEFAULT_MAP_SIZE, minSize);
      if (backingFile == null) {
        backingFile = Files.createTempFile("ozone-dummy-chunk-", ".bin");
        backingFile.toFile().deleteOnExit();
      }

      try (FileChannel ch = FileChannel.open(backingFile,
          StandardOpenOption.READ, StandardOpenOption.WRITE)) {
        long currentSize = ch.size();
        if (currentSize < newSize) {
          ch.position(newSize - 1L);
          ch.write(ByteBuffer.wrap(new byte[]{0}));
        } else if (currentSize > newSize) {
          ch.truncate(newSize);
        }
        mapped = ch.map(FileChannel.MapMode.READ_ONLY, 0, newSize);
        mappedSize = newSize;
      }
    } catch (IOException e) {
      throw new StorageContainerException(
          "Failed to create mapped buffer for dummy chunk reads",
          e,
          ContainerProtos.Result.IO_EXCEPTION);
    } finally {
      lock.writeLock().unlock();
    }
  }

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

    long lenL = info.getLen();
    if (lenL > Integer.MAX_VALUE) {
      throw new StorageContainerException(
          "Chunk length too large: " + lenL, null);
    }
    int len = (int) lenL;

    ensureMapped(len);

    lock.readLock().lock();
    try {
      ByteBuffer dup = mapped.duplicate();
      dup.position(0);
      dup.limit(len);
      ByteBuffer slice = dup.slice();

      return ChunkBuffer.wrap(slice);
    } finally {
      lock.readLock().unlock();
    }
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
