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

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.OzoneConsts;
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

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.IO_EXCEPTION;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNABLE_TO_FIND_CHUNK;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNSUPPORTED_REQUEST;
import static org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion.FILE_PER_CHUNK;

/**
 * This class is for performing chunk related operations.
 */
public class FilePerChunkStrategy implements ChunkManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(FilePerChunkStrategy.class);

  private final boolean doSyncWrite;
  private final BlockManager blockManager;
  private final long defaultReadBufferCapacity;

  public FilePerChunkStrategy(boolean sync, BlockManager manager) {
    doSyncWrite = sync;
    blockManager = manager;
    this.defaultReadBufferCapacity = manager == null ? 0 :
        manager.getDefaultReadBufferCapacity();
  }

  private static void checkLayoutVersion(Container container) {
    Preconditions.checkArgument(
        container.getContainerData().getLayOutVersion() == FILE_PER_CHUNK);
  }

  /**
   * writes a given chunk.
   *
   * @param container - Container for the chunk
   * @param blockID - ID of the block
   * @param info - ChunkInfo
   * @param data - data of the chunk
   * @param dispatcherContext - dispatcherContextInfo
   * @throws StorageContainerException
   */
  @Override
  public void writeChunk(Container container, BlockID blockID, ChunkInfo info,
      ChunkBuffer data, DispatcherContext dispatcherContext)
      throws StorageContainerException {

    checkLayoutVersion(container);

    Preconditions.checkNotNull(dispatcherContext);
    DispatcherContext.WriteChunkStage stage = dispatcherContext.getStage();
    try {
      KeyValueContainer kvContainer = (KeyValueContainer) container;
      KeyValueContainerData containerData = kvContainer.getContainerData();
      HddsVolume volume = containerData.getVolume();
      VolumeIOStats volumeIOStats = volume.getVolumeIOStats();

      File chunkFile = getChunkFile(kvContainer, blockID, info);

      boolean isOverwrite = ChunkUtils.validateChunkForOverwrite(
          chunkFile, info);
      File tmpChunkFile = getTmpChunkFile(chunkFile, dispatcherContext);
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "writing chunk:{} chunk stage:{} chunk file:{} tmp chunk file:{}",
            info.getChunkName(), stage, chunkFile, tmpChunkFile);
      }

      long len = info.getLen();
      long offset = 0; // ignore offset in chunk info
      switch (stage) {
      case WRITE_DATA:
        if (isOverwrite) {
          // if the actual chunk file already exists here while writing the temp
          // chunk file, then it means the same ozone client request has
          // generated two raft log entries. This can happen either because
          // retryCache expired in Ratis (or log index mismatch/corruption in
          // Ratis). This can be solved by two approaches as of now:
          // 1. Read the complete data in the actual chunk file ,
          //    verify the data integrity and in case it mismatches , either
          // 2. Delete the chunk File and write the chunk again. For now,
          //    let's rewrite the chunk file
          // TODO: once the checksum support for write chunks gets plugged in,
          // the checksum needs to be verified for the actual chunk file and
          // the data to be written here which should be efficient and
          // it matches we can safely return without rewriting.
          LOG.warn("ChunkFile already exists {}. Deleting it.", chunkFile);
          FileUtil.fullyDelete(chunkFile);
        }
        if (tmpChunkFile.exists()) {
          // If the tmp chunk file already exists it means the raft log got
          // appended, but later on the log entry got truncated in Ratis leaving
          // behind garbage.
          // TODO: once the checksum support for data chunks gets plugged in,
          // instead of rewriting the chunk here, let's compare the checkSums
          LOG.warn("tmpChunkFile already exists {}. Overwriting it.",
                  tmpChunkFile);
        }
        // Initially writes to temporary chunk file.
        ChunkUtils.writeData(tmpChunkFile, data, offset, len, volumeIOStats,
            doSyncWrite);
        // No need to increment container stats here, as still data is not
        // committed here.
        break;
      case COMMIT_DATA:
        // commit the data, means move chunk data from temporary chunk file
        // to actual chunk file.
        if (isOverwrite) {
          // if the actual chunk file already exists , it implies the write
          // chunk transaction in the containerStateMachine is getting
          // reapplied. This can happen when a node restarts.
          // TODO: verify the checkSums for the existing chunkFile and the
          // chunkInfo to be committed here
          LOG.warn("ChunkFile already exists {}", chunkFile);
          return;
        }
        // While committing a chunk , just rename the tmp chunk file which has
        // the same term and log index appended as the current transaction
        commitChunk(tmpChunkFile, chunkFile);
        // Increment container stats here, as we commit the data.
        containerData.updateWriteStats(len, isOverwrite);
        break;
      case COMBINED:
        // directly write to the chunk file
        ChunkUtils.writeData(chunkFile, data, offset, len, volumeIOStats,
            doSyncWrite);
        containerData.updateWriteStats(len, isOverwrite);
        break;
      default:
        throw new IOException("Can not identify write operation.");
      }
    } catch (StorageContainerException ex) {
      throw ex;
    } catch (IOException ex) {
      throw new StorageContainerException("Internal error: ", ex,
          IO_EXCEPTION);
    }
  }

  /**
   * reads the data defined by a chunk.
   *
   * @param container - Container for the chunk
   * @param blockID - ID of the block.
   * @param info - ChunkInfo.
   * @param dispatcherContext dispatcher context info.
   * @return byte array
   * @throws StorageContainerException
   * TODO: Right now we do not support partial reads and writes of chunks.
   * TODO: Explore if we need to do that for ozone.
   */
  @Override
  public ChunkBuffer readChunk(Container container, BlockID blockID,
      ChunkInfo info, DispatcherContext dispatcherContext)
      throws StorageContainerException {

    checkLayoutVersion(container);

    KeyValueContainer kvContainer = (KeyValueContainer) container;
    KeyValueContainerData containerData = kvContainer.getContainerData();

    HddsVolume volume = containerData.getVolume();
    VolumeIOStats volumeIOStats = volume.getVolumeIOStats();

    // In version1, we verify checksum if it is available and return data
    // of the chunk file.
    File finalChunkFile = getChunkFile(kvContainer, blockID, info);

    List<File> possibleFiles = new ArrayList<>();
    possibleFiles.add(finalChunkFile);
    if (dispatcherContext != null && dispatcherContext.isReadFromTmpFile()) {
      possibleFiles.add(getTmpChunkFile(finalChunkFile, dispatcherContext));
      // HDDS-2372. Read finalChunkFile after tmpChunkFile to solve race
      // condition between read and commit.
      possibleFiles.add(finalChunkFile);
    }

    long len = info.getLen();

    long bufferCapacity;
    if (info.isReadDataIntoSingleBuffer()) {
      // Older client - read all chunk data into one single buffer.
      bufferCapacity = len;
    } else {
      // Set buffer capacity to checksum boundary size so that each buffer
      // corresponds to one checksum. If checksum is NONE, then set buffer
      // capacity to default (OZONE_CHUNK_READ_BUFFER_DEFAULT_SIZE_KEY = 64KB).
      ChecksumData checksumData = info.getChecksumData();

      if (checksumData.getChecksumType() == ContainerProtos.ChecksumType.NONE) {
        bufferCapacity = defaultReadBufferCapacity;
      } else {
        bufferCapacity = checksumData.getBytesPerChecksum();
      }
    }
    // If the buffer capacity is 0, set all the data into one ByteBuffer
    if (bufferCapacity == 0) {
      bufferCapacity = len;
    }

    ByteBuffer[] dataBuffers = BufferUtils.assignByteBuffers(len,
        bufferCapacity);

    long chunkFileOffset = 0;
    if (info.getOffset() != 0) {
      try {
        BlockData blockData = blockManager.getBlock(kvContainer, blockID);
        List<ContainerProtos.ChunkInfo> chunks = blockData.getChunks();
        String chunkName = info.getChunkName();
        boolean found = false;
        for (ContainerProtos.ChunkInfo chunk : chunks) {
          if (chunk.getChunkName().equals(chunkName)) {
            chunkFileOffset = chunk.getOffset();
            found = true;
            break;
          }
        }
        if (!found) {
          throw new StorageContainerException(
              "Cannot find chunk " + chunkName + " in block " +
                  blockID.toString(), UNABLE_TO_FIND_CHUNK);
        }
      } catch (IOException e) {
        throw new StorageContainerException(
            "Cannot find block " + blockID.toString() + " for chunk " +
                info.getChunkName(), UNABLE_TO_FIND_CHUNK);
      }
    }

    for (File file : possibleFiles) {
      try {
        if (file.exists()) {
          long offset = info.getOffset() - chunkFileOffset;
          Preconditions.checkState(offset >= 0);
          ChunkUtils.readData(file, dataBuffers, offset, len, volumeIOStats);
          return ChunkBuffer.wrap(Lists.newArrayList(dataBuffers));
        }
      } catch (StorageContainerException ex) {
        //UNABLE TO FIND chunk is not a problem as we will try with the
        //next possible location
        if (ex.getResult() != UNABLE_TO_FIND_CHUNK) {
          throw ex;
        }
        dataBuffers = null;
      }
    }
    throw new StorageContainerException(
        "Chunk file can't be found " + possibleFiles.toString(),
        UNABLE_TO_FIND_CHUNK);
  }

  /**
   * Deletes a given chunk.
   *
   * @param container - Container for the chunk
   * @param blockID - ID of the block
   * @param info - Chunk Info
   * @throws StorageContainerException
   */
  @Override
  public void deleteChunk(Container container, BlockID blockID, ChunkInfo info)
      throws StorageContainerException {

    checkLayoutVersion(container);

    Preconditions.checkNotNull(blockID, "Block ID cannot be null.");
    KeyValueContainer kvContainer = (KeyValueContainer) container;

    // In version1, we have only chunk file.
    File chunkFile = getChunkFile(kvContainer, blockID, info);

    // if the chunk file does not exist, it might have already been deleted.
    // The call might be because of reapply of transactions on datanode
    // restart.
    if (!chunkFile.exists()) {
      LOG.warn("Chunk file not found for chunk {}", info);
      return;
    }

    long chunkFileSize = chunkFile.length();
    boolean allowed = info.getLen() == chunkFileSize
        // chunk written by new client to old datanode, expected
        // file length is offset + real chunk length; see HDDS-3644
        || info.getLen() + info.getOffset() == chunkFileSize;
    if (allowed) {
      FileUtil.fullyDelete(chunkFile);
      LOG.info("Deleted chunk file {} (size {}) for chunk {}",
          chunkFile, chunkFileSize, info);
    } else {
      LOG.error("Not Supported Operation. Trying to delete a " +
          "chunk that is in shared file. chunk info : {}", info);
      throw new StorageContainerException("Not Supported Operation. " +
          "Trying to delete a chunk that is in shared file. chunk info : "
          + info, UNSUPPORTED_REQUEST);
    }
  }

  @Override
  public void deleteChunks(Container container, BlockData blockData)
      throws StorageContainerException {
    for (ContainerProtos.ChunkInfo chunk : blockData.getChunks()) {
      try {
        ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(chunk);
        deleteChunk(container, blockData.getBlockID(), chunkInfo);
      } catch (IOException e) {
        throw new StorageContainerException(
            e, ContainerProtos.Result.INVALID_ARGUMENT);
      }
    }
  }

  private static File getChunkFile(KeyValueContainer container, BlockID blockID,
      ChunkInfo info) throws StorageContainerException {
    return FILE_PER_CHUNK.getChunkFile(container.getContainerData(), blockID,
        info);
  }

  /**
   * Returns the temporary chunkFile path.
   * @param chunkFile chunkFileName
   * @param dispatcherContext dispatcher context info
   * @return temporary chunkFile path
   * @throws StorageContainerException
   */
  private File getTmpChunkFile(File chunkFile,
      DispatcherContext dispatcherContext)  {
    return new File(chunkFile.getParent(),
        chunkFile.getName() +
            OzoneConsts.CONTAINER_CHUNK_NAME_DELIMITER +
            OzoneConsts.CONTAINER_TEMPORARY_CHUNK_PREFIX +
            OzoneConsts.CONTAINER_CHUNK_NAME_DELIMITER +
            dispatcherContext.getTerm() +
            OzoneConsts.CONTAINER_CHUNK_NAME_DELIMITER +
            dispatcherContext.getLogIndex());
  }

  /**
   * Commit the chunk by renaming the temporary chunk file to chunk file.
   * @param tmpChunkFile
   * @param chunkFile
   * @throws IOException
   */
  private void commitChunk(File tmpChunkFile, File chunkFile) throws
      IOException {
    Files.move(tmpChunkFile.toPath(), chunkFile.toPath(),
        StandardCopyOption.REPLACE_EXISTING);
  }

}
