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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.security.NoSuchAlgorithmException;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.volume.VolumeIOStats;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;

import static java.nio.channels.FileChannel.open;
import static java.util.Collections.unmodifiableSet;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_INTERNAL_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.INVALID_WRITE_SIZE;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.IO_EXCEPTION;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.NO_SUCH_ALGORITHM;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNABLE_TO_FIND_CHUNK;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for chunk operations for KeyValue container.
 */
public final class ChunkUtils {

  private static final Set<Path> LOCKS = ConcurrentHashMap.newKeySet();

  private static final Logger LOG =
      LoggerFactory.getLogger(ChunkUtils.class);

  // skip SYNC and DSYNC to reduce contention on file.lock
  private static final Set<? extends OpenOption> WRITE_OPTIONS =
      unmodifiableSet(EnumSet.of(
          StandardOpenOption.CREATE,
          StandardOpenOption.WRITE,
          StandardOpenOption.SPARSE
      ));
  public static final Set<? extends OpenOption> READ_OPTIONS =
      unmodifiableSet(EnumSet.of(
          StandardOpenOption.READ
      ));
  public static final FileAttribute<?>[] NO_ATTRIBUTES = {};

  /** Never constructed. **/
  private ChunkUtils() {

  }

  /**
   * Writes the data in chunk Info to the specified location in the chunkfile.
   *  @param file - File to write data to.
   * @param data - The data buffer.
   * @param offset
   * @param len
   * @param volumeIOStats statistics collector
   * @param sync whether to do fsync or not
   */
  public static void writeData(File file, ChunkBuffer data,
      long offset, long len, VolumeIOStats volumeIOStats, boolean sync)
      throws StorageContainerException {

    writeData(data, file.getName(), offset, len, volumeIOStats,
        d -> writeDataToFile(file, d, offset, sync));
  }

  public static void writeData(FileChannel file, String filename,
      ChunkBuffer data, long offset, long len, VolumeIOStats volumeIOStats
  ) throws StorageContainerException {

    writeData(data, filename, offset, len, volumeIOStats,
        d -> writeDataToChannel(file, d, offset));
  }

  private static void writeData(ChunkBuffer data, String filename,
      long offset, long len, VolumeIOStats volumeIOStats,
      ToLongFunction<ChunkBuffer> writer) throws StorageContainerException {

    validateBufferSize(len, data.remaining());

    final long startTime = Time.monotonicNow();
    final long bytesWritten;
    try {
      bytesWritten = writer.applyAsLong(data);
    } catch (UncheckedIOException e) {
      throw wrapInStorageContainerException(e.getCause());
    }

    final long endTime = Time.monotonicNow();
    long elapsed = endTime - startTime;
    volumeIOStats.incWriteTime(elapsed);
    volumeIOStats.incWriteOpCount();
    volumeIOStats.incWriteBytes(bytesWritten);

    LOG.debug("Written {} bytes at offset {} to {} in {} ms",
        bytesWritten, offset, filename, elapsed);

    validateWriteSize(len, bytesWritten);
  }

  private static long writeDataToFile(File file, ChunkBuffer data,
      long offset, boolean sync) {
    final Path path = file.toPath();
    return processFileExclusively(path, () -> {
      FileChannel channel = null;
      try {
        channel = open(path, WRITE_OPTIONS, NO_ATTRIBUTES);

        try (FileLock ignored = channel.lock()) {
          return writeDataToChannel(channel, data, offset);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      } finally {
        closeFile(channel, sync);
      }
    });
  }

  private static long writeDataToChannel(FileChannel channel, ChunkBuffer data,
      long offset) {
    try {
      channel.position(offset);
      return data.writeTo(channel);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Reads data from an existing chunk file.
   *
   * @param file file where data lives
   */
  public static void readData(File file, ByteBuffer buf,
      long offset, long len, VolumeIOStats volumeIOStats)
      throws StorageContainerException {

    final Path path = file.toPath();
    final long startTime = Time.monotonicNow();
    final long bytesRead;

    try {
      bytesRead = processFileExclusively(path, () -> {
        try (FileChannel channel = open(path, READ_OPTIONS, NO_ATTRIBUTES);
             FileLock ignored = channel.lock(offset, len, true)) {

          return channel.read(buf, offset);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      });
    } catch (UncheckedIOException e) {
      throw wrapInStorageContainerException(e.getCause());
    }

    // Increment volumeIO stats here.
    long endTime = Time.monotonicNow();
    volumeIOStats.incReadTime(endTime - startTime);
    volumeIOStats.incReadOpCount();
    volumeIOStats.incReadBytes(bytesRead);

    LOG.debug("Read {} bytes starting at offset {} from {}",
        bytesRead, offset, file);

    validateReadSize(len, bytesRead);

    buf.flip();
  }

  /**
   * Validates chunk data and returns a file object to Chunk File that we are
   * expected to write data to.
   *
   * @param chunkFile - chunkFile to write data into.
   * @param info - chunk info.
   * @return true if the chunkFile exists and chunkOffset &lt; chunkFile length,
   *         false otherwise.
   */
  public static boolean validateChunkForOverwrite(File chunkFile,
      ChunkInfo info) {

    if (isOverWriteRequested(chunkFile, info)) {
      if (!isOverWritePermitted(info)) {
        LOG.warn("Duplicate write chunk request. Chunk overwrite " +
            "without explicit request. {}", info);
      }
      return true;
    }
    return false;
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
    return Boolean.parseBoolean(overWrite);
  }

  public static void verifyChunkFileExists(File file)
      throws StorageContainerException {
    if (!file.exists()) {
      throw new StorageContainerException(
          "Chunk file not found: " + file.getPath(),
          UNABLE_TO_FIND_CHUNK);
    }
  }

  @VisibleForTesting
  static <T> T processFileExclusively(Path path, Supplier<T> op) {
    for (;;) {
      if (LOCKS.add(path)) {
        break;
      }
    }

    try {
      return op.get();
    } finally {
      LOCKS.remove(path);
    }
  }

  private static void closeFile(FileChannel file, boolean sync) {
    if (file != null) {
      try {
        if (sync) {
          // ensure data and metadata is persisted
          file.force(true);
        }
        file.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  private static void validateReadSize(long expected, long actual)
      throws StorageContainerException {
    checkSize("read", expected, actual, CONTAINER_INTERNAL_ERROR);
  }

  private static void validateWriteSize(long expected, long actual)
      throws StorageContainerException {
    checkSize("write", expected, actual, INVALID_WRITE_SIZE);
  }

  public static void validateBufferSize(long expected, long actual)
      throws StorageContainerException {
    checkSize("buffer", expected, actual, INVALID_WRITE_SIZE);
  }

  private static void checkSize(String of, long expected, long actual,
      ContainerProtos.Result code) throws StorageContainerException {
    if (actual != expected) {
      String err = String.format(
          "Unexpected %s size. expected: %d, actual: %d",
          of, expected, actual);
      LOG.error(err);
      throw new StorageContainerException(err, code);
    }
  }

  private static StorageContainerException wrapInStorageContainerException(
      IOException e) {
    ContainerProtos.Result result = translate(e);
    return new StorageContainerException(e, result);
  }

  private static ContainerProtos.Result translate(Exception cause) {
    if (cause instanceof FileNotFoundException ||
        cause instanceof NoSuchFileException) {
      return UNABLE_TO_FIND_CHUNK;
    }

    if (cause instanceof IOException) {
      return IO_EXCEPTION;
    }

    if (cause instanceof NoSuchAlgorithmException) {
      return NO_SUCH_ALGORITHM;
    }

    return CONTAINER_INTERNAL_ERROR;
  }

}
