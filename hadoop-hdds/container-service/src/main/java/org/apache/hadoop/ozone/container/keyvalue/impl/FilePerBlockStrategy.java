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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.Lists;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeIOStats;
import org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.common.interfaces.Container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Duration;
import java.util.concurrent.ExecutionException;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNSUPPORTED_REQUEST;
import static org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion.FILE_PER_BLOCK;
import static org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext.WriteChunkStage.COMMIT_DATA;
import static org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils.validateChunkForOverwrite;
import static org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils.verifyChunkFileExists;

/**
 * This class is for performing chunk related operations.
 */
public class FilePerBlockStrategy implements ChunkManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(FilePerBlockStrategy.class);

  private final boolean doSyncWrite;
  private final OpenFiles files = new OpenFiles();
  private final long defaultReadBufferCapacity;

  public FilePerBlockStrategy(boolean sync, BlockManager manager) {
    doSyncWrite = sync;
    this.defaultReadBufferCapacity = manager == null ? 0 :
        manager.getDefaultReadBufferCapacity();
  }

  private static void checkLayoutVersion(Container container) {
    Preconditions.checkArgument(
        container.getContainerData().getLayOutVersion() == FILE_PER_BLOCK);
  }

  @Override
  public void writeChunk(Container container, BlockID blockID, ChunkInfo info,
      ChunkBuffer data, DispatcherContext dispatcherContext)
      throws StorageContainerException {

    checkLayoutVersion(container);

    Preconditions.checkNotNull(dispatcherContext);
    DispatcherContext.WriteChunkStage stage = dispatcherContext.getStage();

    if (info.getLen() <= 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skip writing empty chunk {} in stage {}", info, stage);
      }
      return;
    }

    if (stage == COMMIT_DATA) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Ignore chunk {} in stage {}", info, stage);
      }
      return;
    }

    KeyValueContainerData containerData = (KeyValueContainerData) container
        .getContainerData();

    File chunkFile = getChunkFile(container, blockID, info);
    boolean overwrite = validateChunkForOverwrite(chunkFile, info);
    long len = info.getLen();
    long offset = info.getOffset();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Writing chunk {} (overwrite: {}) in stage {} to file {}",
          info, overwrite, stage, chunkFile);
    }

    HddsVolume volume = containerData.getVolume();
    VolumeIOStats volumeIOStats = volume.getVolumeIOStats();

    FileChannel channel = files.getChannel(chunkFile, doSyncWrite);
    ChunkUtils.writeData(channel, chunkFile.getName(), data, offset, len,
        volumeIOStats);

    containerData.updateWriteStats(len, overwrite);
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

    File chunkFile = getChunkFile(container, blockID, info);

    long len = info.getLen();
    long offset = info.getOffset();

    long bufferCapacity = 0;
    if (info.isReadDataIntoSingleBuffer()) {
      // Older client - read all chunk data into one single buffer.
      bufferCapacity = len;
    } else {
      // Set buffer capacity to checksum boundary size so that each buffer
      // corresponds to one checksum. If checksum is NONE, then set buffer
      // capacity to default (OZONE_CHUNK_READ_BUFFER_DEFAULT_SIZE_KEY = 64KB).
      ChecksumData checksumData = info.getChecksumData();

      if (checksumData != null) {
        if (checksumData.getChecksumType() ==
            ContainerProtos.ChecksumType.NONE) {
          bufferCapacity = defaultReadBufferCapacity;
        } else {
          bufferCapacity = checksumData.getBytesPerChecksum();
        }
      }
    }
    // If the buffer capacity is 0, set all the data into one ByteBuffer
    if (bufferCapacity == 0) {
      bufferCapacity = len;
    }

    ByteBuffer[] dataBuffers = BufferUtils.assignByteBuffers(len,
        bufferCapacity);

    ChunkUtils.readData(chunkFile, dataBuffers, offset, len, volumeIOStats);

    return ChunkBuffer.wrap(Lists.newArrayList(dataBuffers));
  }

  @Override
  public void deleteChunk(Container container, BlockID blockID, ChunkInfo info)
      throws StorageContainerException {
    deleteChunk(container, blockID, info, true);
  }

  @Override
  public void deleteChunks(Container container, BlockData blockData)
      throws StorageContainerException {
    deleteChunk(container, blockData.getBlockID(), null, false);
  }

  @Override
  public void finishWriteChunks(KeyValueContainer container,
      BlockData blockData) throws IOException {
    File chunkFile = getChunkFile(container, blockData.getBlockID(), null);
    files.close(chunkFile);
    verifyChunkFileExists(chunkFile);
  }

  private void deleteChunk(Container container, BlockID blockID,
      ChunkInfo info, boolean verifyLength)
      throws StorageContainerException {
    checkLayoutVersion(container);

    Preconditions.checkNotNull(blockID, "Block ID cannot be null.");

    File file = getChunkFile(container, blockID, info);

    // if the chunk file does not exist, it might have already been deleted.
    // The call might be because of reapply of transactions on datanode
    // restart.
    if (!file.exists()) {
      LOG.warn("Block file to be deleted does not exist: {}", file);
      return;
    }

    if (verifyLength) {
      Preconditions.checkNotNull(info, "Chunk info cannot be null for single " +
          "chunk delete");
      checkFullDelete(info, file);
    }

    FileUtil.fullyDelete(file);
    LOG.info("Deleted block file: {}", file);
  }

  private File getChunkFile(Container container, BlockID blockID,
      ChunkInfo info) throws StorageContainerException {
    return FILE_PER_BLOCK.getChunkFile(container.getContainerData(), blockID,
        info);
  }

  private static void checkFullDelete(ChunkInfo info, File chunkFile)
      throws StorageContainerException {
    long fileLength = chunkFile.length();
    if ((info.getOffset() > 0) || (info.getLen() != fileLength)) {
      String msg = String.format(
          "Trying to delete partial chunk %s from file %s with length %s",
          info, chunkFile, fileLength);
      LOG.error(msg);
      throw new StorageContainerException(msg, UNSUPPORTED_REQUEST);
    }
  }

  private static final class OpenFiles {

    private static final RemovalListener<String, OpenFile> ON_REMOVE =
        event -> close(event.getKey(), event.getValue());

    private final Cache<String, OpenFile> files = CacheBuilder.newBuilder()
        .expireAfterAccess(Duration.ofMinutes(10))
        .removalListener(ON_REMOVE)
        .build();

    public FileChannel getChannel(File file, boolean sync)
        throws StorageContainerException {
      try {
        return files.get(file.getPath(),
            () -> open(file, sync)).getChannel();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof IOException) {
          throw new UncheckedIOException((IOException) e.getCause());
        }
        throw new StorageContainerException(e.getCause(),
            ContainerProtos.Result.CONTAINER_INTERNAL_ERROR);
      }
    }

    private static OpenFile open(File file, boolean sync) {
      try {
        return new OpenFile(file, sync);
      } catch (FileNotFoundException e) {
        throw new UncheckedIOException(e);
      }
    }

    public void close(File file) {
      if (file != null) {
        files.invalidate(file.getPath());
      }
    }

    private static void close(String filename, OpenFile openFile) {
      if (openFile != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Closing file {}", filename);
        }
        openFile.close();
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("File {} not open", filename);
        }
      }
    }
  }

  private static final class OpenFile {

    private final RandomAccessFile file;

    private OpenFile(File file, boolean sync) throws FileNotFoundException {
      String mode = sync ? "rws" : "rw";
      this.file = new RandomAccessFile(file, mode);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Opened file {}", file);
      }
    }

    public FileChannel getChannel() {
      return file.getChannel();
    }

    public void close() {
      try {
        file.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

}
