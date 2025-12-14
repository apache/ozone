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

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CHUNK_FILE_INCONSISTENCY;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNSUPPORTED_REQUEST;
import static org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion.FILE_PER_BLOCK;
import static org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext.WriteChunkStage.COMMIT_DATA;
import static org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil.onFailure;
import static org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils.limitReadSize;
import static org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils.validateChunkForOverwrite;
import static org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils.verifyChunkFileExists;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.ChunkBufferToByteString;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.ratis.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is for performing chunk related operations.
 */
public class FilePerBlockStrategy implements ChunkManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(FilePerBlockStrategy.class);

  private final boolean doSyncWrite;
  private final OpenFiles files = new OpenFiles();
  private final int defaultReadBufferCapacity;
  private final int readMappedBufferThreshold;
  private final int readMappedBufferMaxCount;
  private final MappedBufferManager mappedBufferManager;

  private final boolean readNettyChunkedNioFile;

  public FilePerBlockStrategy(boolean sync, BlockManager manager) {
    doSyncWrite = sync;
    this.defaultReadBufferCapacity = manager == null ? 0 :
        manager.getDefaultReadBufferCapacity();
    this.readMappedBufferThreshold = manager == null ? 0
        : manager.getReadMappedBufferThreshold();
    this.readMappedBufferMaxCount = manager == null ? 0
        : manager.getReadMappedBufferMaxCount();
    LOG.info("ozone.chunk.read.mapped.buffer.max.count is load with {}", readMappedBufferMaxCount);
    if (this.readMappedBufferMaxCount > 0) {
      mappedBufferManager = new MappedBufferManager(this.readMappedBufferMaxCount);
    } else {
      mappedBufferManager = null;
    }

    this.readNettyChunkedNioFile = manager != null && manager.isReadNettyChunkedNioFile();
  }

  private static void checkLayoutVersion(Container container) {
    Preconditions.checkArgument(
        container.getContainerData().getLayoutVersion() == FILE_PER_BLOCK);
  }

  @Override
  public String streamInit(Container container, BlockID blockID)
      throws StorageContainerException {
    checkLayoutVersion(container);
    final File chunkFile = getChunkFile(container, blockID);
    return chunkFile.getAbsolutePath();
  }

  @Override
  public StateMachine.DataChannel getStreamDataChannel(
          Container container, BlockID blockID, ContainerMetrics metrics)
          throws StorageContainerException {
    checkLayoutVersion(container);
    final File chunkFile = getChunkFile(container, blockID);
    return new KeyValueStreamDataChannel(chunkFile,
        container.getContainerData(), metrics);
  }

  @Override
  public void writeChunk(Container container, BlockID blockID, ChunkInfo info,
      ChunkBuffer data, DispatcherContext dispatcherContext)
      throws StorageContainerException {

    checkLayoutVersion(container);

    Objects.requireNonNull(dispatcherContext, "dispatcherContext == null");
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

    final File chunkFile = getChunkFile(container, blockID);
    long chunkLength = info.getLen();
    long offset = info.getOffset();

    HddsVolume volume = containerData.getVolume();

    FileChannel channel = null;
    boolean overwrite;
    try {
      channel = files.getChannel(chunkFile, doSyncWrite);
      overwrite = validateChunkForOverwrite(channel, info);
    } catch (IOException e) {
      onFailure(volume);
      throw e;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Writing chunk {} (overwrite: {}) in stage {} to file {}",
          info, overwrite, stage, chunkFile);
    }

    // check whether offset matches block file length if its an overwrite
    if (!overwrite) {
      ChunkUtils.validateChunkSize(channel, info, chunkFile.getName());
    }

    long fileLengthBeforeWrite;
    try {
      fileLengthBeforeWrite = channel.size();
    } catch (IOException e) {
      throw new StorageContainerException("Encountered an error while getting the file size for "
          + chunkFile.getName(), CHUNK_FILE_INCONSISTENCY);
    }

    ChunkUtils.writeData(channel, chunkFile.getName(), data, offset, chunkLength, volume);

    // When overwriting, update the bytes used if the new length is greater than the old length
    // This is to ensure that the bytes used is updated correctly when overwriting a smaller chunk
    // with a larger chunk at the end of the block.
    if (overwrite) {
      long fileLengthAfterWrite = offset + chunkLength;
      if (fileLengthAfterWrite > fileLengthBeforeWrite) {
        containerData.getStatistics().updateWrite(fileLengthAfterWrite - fileLengthBeforeWrite, false);
      }
    }

    containerData.updateWriteStats(chunkLength, overwrite);
  }

  @Override
  public ChunkBufferToByteString readChunk(Container container, BlockID blockID,
      ChunkInfo info, DispatcherContext dispatcherContext)
      throws StorageContainerException {

    checkLayoutVersion(container);

    if (info.getLen() <= 0) {
      LOG.debug("Skip reading empty chunk {}", info);
      return ChunkBuffer.wrap(ByteBuffer.wrap(new byte[0]));
    }

    limitReadSize(info.getLen());

    KeyValueContainerData containerData = (KeyValueContainerData) container
        .getContainerData();

    HddsVolume volume = containerData.getVolume();

    final File chunkFile = getChunkFile(container, blockID);

    final long len = info.getLen();
    long offset = info.getOffset();
    int bufferCapacity = ChunkManager.getBufferCapacityForChunkRead(info,
        defaultReadBufferCapacity);

    if (readNettyChunkedNioFile && dispatcherContext != null && dispatcherContext.isReleaseSupported()) {
      return ChunkUtils.readData(chunkFile, bufferCapacity, offset, len, volume, dispatcherContext);
    }
    return ChunkUtils.readData(len, bufferCapacity, chunkFile, offset, volume,
        readMappedBufferThreshold, readMappedBufferMaxCount > 0, mappedBufferManager);
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
    final File chunkFile = getChunkFile(container, blockData.getBlockID());
    try {
      files.close(chunkFile);
      verifyChunkFileExists(chunkFile);
    } catch (IOException e) {
      onFailure(container.getContainerData().getVolume());
      throw e;
    }
  }

  @Override
  public void finalizeWriteChunk(KeyValueContainer container,
      BlockID blockId) throws IOException {
    synchronized (container) {
      File chunkFile = getChunkFile(container, blockId);
      try {
        if (files.isOpen(chunkFile)) {
          files.close(chunkFile);
        }
        verifyChunkFileExists(chunkFile);
      } catch (IOException e) {
        onFailure(container.getContainerData().getVolume());
        throw e;
      }
    }
  }

  private void deleteChunk(Container container, BlockID blockID,
      ChunkInfo info, boolean verifyLength)
      throws StorageContainerException {
    checkLayoutVersion(container);

    Objects.requireNonNull(blockID, "blockID == null");

    final File file = getChunkFile(container, blockID);

    // if the chunk file does not exist, it might have already been deleted.
    // The call might be because of reapply of transactions on datanode
    // restart.
    if (!file.exists()) {
      LOG.warn("Block file to be deleted does not exist: {}", file);
      return;
    }

    if (verifyLength) {
      checkFullDelete(info, file);
    }

    FileUtil.fullyDelete(file);
    LOG.info("Deleted block file: {}", file);
  }

  private static File getChunkFile(Container container, BlockID blockID) throws StorageContainerException {
    return FILE_PER_BLOCK.getChunkFile(container.getContainerData(), blockID, null);
  }

  private static void checkFullDelete(ChunkInfo info, File chunkFile)
      throws StorageContainerException {
    Objects.requireNonNull(info, "info == null");
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

    public boolean isOpen(File file) {
      return file != null &&
          files.getIfPresent(file.getPath()) != null;
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
