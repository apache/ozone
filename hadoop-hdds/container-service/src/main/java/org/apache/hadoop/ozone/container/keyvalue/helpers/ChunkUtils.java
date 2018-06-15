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

package org.apache.hadoop.ozone.container.keyvalue.helpers;

import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ChunkManagerImpl;
import org.apache.hadoop.ozone.container.common.impl.KeyValueContainerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.*;

/**
 * Utility methods for chunk operations for KeyValue container.
 */
public final class ChunkUtils {

  /** Never constructed. **/
  private ChunkUtils() {

  }

  /**
   * Writes the data in chunk Info to the specified location in the chunkfile.
   *
   * @param chunkFile - File to write data to.
   * @param chunkInfo - Data stream to write.
   * @param data - The data buffer.
   * @throws StorageContainerException
   */
  public static void writeData(File chunkFile, ChunkInfo chunkInfo,
                               byte[] data) throws
      StorageContainerException, ExecutionException, InterruptedException,
      NoSuchAlgorithmException {

    Logger log = LoggerFactory.getLogger(ChunkManagerImpl.class);
    if (data.length != chunkInfo.getLen()) {
      String err = String.format("data array does not match the length " +
              "specified. DataLen: %d Byte Array: %d",
          chunkInfo.getLen(), data.length);
      log.error(err);
      throw new StorageContainerException(err, INVALID_WRITE_SIZE);
    }

    AsynchronousFileChannel file = null;
    FileLock lock = null;

    try {
      file =
          AsynchronousFileChannel.open(chunkFile.toPath(),
              StandardOpenOption.CREATE,
              StandardOpenOption.WRITE,
              StandardOpenOption.SPARSE,
              StandardOpenOption.SYNC);
      lock = file.lock().get();
      if (chunkInfo.getChecksum() != null &&
          !chunkInfo.getChecksum().isEmpty()) {
        verifyChecksum(chunkInfo, data, log);
      }
      int size = file.write(ByteBuffer.wrap(data), chunkInfo.getOffset()).get();
      if (size != data.length) {
        log.error("Invalid write size found. Size:{}  Expected: {} ", size,
            data.length);
        throw new StorageContainerException("Invalid write size found. " +
            "Size: " + size + " Expected: " + data.length, INVALID_WRITE_SIZE);
      }
    } catch (StorageContainerException ex) {
      throw ex;
    } catch(IOException e) {
      throw new StorageContainerException(e, IO_EXCEPTION);

    } finally {
      if (lock != null) {
        try {
          lock.release();
        } catch (IOException e) {
          log.error("Unable to release lock ??, Fatal Error.");
          throw new StorageContainerException(e, CONTAINER_INTERNAL_ERROR);

        }
      }
      if (file != null) {
        try {
          file.close();
        } catch (IOException e) {
          throw new StorageContainerException("Error closing chunk file",
              e, CONTAINER_INTERNAL_ERROR);
        }
      }
    }
  }

  /**
   * Reads data from an existing chunk file.
   *
   * @param chunkFile - file where data lives.
   * @param data - chunk definition.
   * @return ByteBuffer
   * @throws StorageContainerException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public static ByteBuffer readData(File chunkFile, ChunkInfo data) throws
      StorageContainerException, ExecutionException, InterruptedException,
      NoSuchAlgorithmException {
    Logger log = LoggerFactory.getLogger(ChunkManagerImpl.class);

    if (!chunkFile.exists()) {
      log.error("Unable to find the chunk file. chunk info : {}",
          data.toString());
      throw new StorageContainerException("Unable to find the chunk file. " +
          "chunk info " +
          data.toString(), UNABLE_TO_FIND_CHUNK);
    }

    AsynchronousFileChannel file = null;
    FileLock lock = null;
    try {
      file =
          AsynchronousFileChannel.open(chunkFile.toPath(),
              StandardOpenOption.READ);
      lock = file.lock(data.getOffset(), data.getLen(), true).get();

      ByteBuffer buf = ByteBuffer.allocate((int) data.getLen());
      file.read(buf, data.getOffset()).get();

      if (data.getChecksum() != null && !data.getChecksum().isEmpty()) {
        verifyChecksum(data, buf.array(), log);
      }

      return buf;
    } catch (IOException e) {
      throw new StorageContainerException(e, IO_EXCEPTION);
    } finally {
      if (lock != null) {
        try {
          lock.release();
        } catch (IOException e) {
          log.error("I/O error is lock release.");
        }
      }
      if (file != null) {
        IOUtils.closeStream(file);
      }
    }
  }

  /**
   * Verifies the checksum of a chunk against the data buffer.
   *
   * @param chunkInfo - Chunk Info.
   * @param data - data buffer
   * @param log - log
   * @throws NoSuchAlgorithmException
   * @throws StorageContainerException
   */
  private static void verifyChecksum(ChunkInfo chunkInfo, byte[] data, Logger
      log) throws NoSuchAlgorithmException, StorageContainerException {
    MessageDigest sha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
    sha.update(data);
    if (!Hex.encodeHexString(sha.digest()).equals(
        chunkInfo.getChecksum())) {
      log.error("Checksum mismatch. Provided: {} , computed: {}",
          chunkInfo.getChecksum(), DigestUtils.sha256Hex(sha.digest()));
      throw new StorageContainerException("Checksum mismatch. Provided: " +
          chunkInfo.getChecksum() + " , computed: " +
          DigestUtils.sha256Hex(sha.digest()), CHECKSUM_MISMATCH);
    }
  }

  /**
   * Validates chunk data and returns a file object to Chunk File that we are
   * expected to write data to.
   *
   * @param data - container data.
   * @param info - chunk info.
   * @return File
   * @throws StorageContainerException
   */
  public static File validateChunk(KeyValueContainerData data, ChunkInfo info)
      throws StorageContainerException {

    Logger log = LoggerFactory.getLogger(ChunkManagerImpl.class);

    File chunkFile = getChunkFile(data, info);
    if (isOverWriteRequested(chunkFile, info)) {
      if (!isOverWritePermitted(info)) {
        log.error("Rejecting write chunk request. Chunk overwrite " +
            "without explicit request. {}", info.toString());
        throw new StorageContainerException("Rejecting write chunk request. " +
            "OverWrite flag required." + info.toString(),
            OVERWRITE_FLAG_REQUIRED);
      }
    }
    return chunkFile;
  }

  /**
   * Validates that Path to chunk file exists.
   *
   * @param containerData - Container Data
   * @param info - Chunk info
   * @return - File.
   * @throws StorageContainerException
   */
  public static File getChunkFile(KeyValueContainerData containerData,
                                  ChunkInfo info) throws
      StorageContainerException {

    Preconditions.checkNotNull(containerData, "Container data can't be null");
    Logger log = LoggerFactory.getLogger(ChunkManagerImpl.class);

    String chunksPath = containerData.getChunksPath();
    if (chunksPath == null) {
      log.error("Chunks path is null in the container data");
      throw new StorageContainerException("Unable to get Chunks directory.",
          UNABLE_TO_FIND_DATA_DIR);
    }
    File chunksLoc = new File(chunksPath);
    if (!chunksLoc.exists()) {
      log.error("Chunks path does not exist");
      throw new StorageContainerException("Unable to get Chunks directory.",
          UNABLE_TO_FIND_DATA_DIR);
    }

    return chunksLoc.toPath().resolve(info.getChunkName()).toFile();
  }

  /**
   * Checks if we are getting a request to overwrite an existing range of
   * chunk.
   *
   * @param chunkFile - File
   * @param chunkInfo - Buffer to write
   * @return bool
   */
  public static boolean isOverWriteRequested(File chunkFile, ChunkInfo
      chunkInfo) {

    if (!chunkFile.exists()) {
      return false;
    }

    long offset = chunkInfo.getOffset();
    return offset < chunkFile.length();
  }

  /**
   * Overwrite is permitted if an only if the user explicitly asks for it. We
   * permit this iff the key/value pair contains a flag called
   * [OverWriteRequested, true].
   *
   * @param chunkInfo - Chunk info
   * @return true if the user asks for it.
   */
  public static boolean isOverWritePermitted(ChunkInfo chunkInfo) {
    String overWrite = chunkInfo.getMetadata().get(OzoneConsts.CHUNK_OVERWRITE);
    return (overWrite != null) &&
        (!overWrite.isEmpty()) &&
        (Boolean.valueOf(overWrite));
  }

}
