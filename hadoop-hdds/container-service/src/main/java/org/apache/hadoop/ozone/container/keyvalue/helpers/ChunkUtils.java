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
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;

import static java.nio.channels.FileChannel.open;
import static java.util.Collections.unmodifiableSet;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CHUNK_FILE_INCONSISTENCY;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_INTERNAL_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.INVALID_WRITE_SIZE;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.IO_EXCEPTION;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.NO_SUCH_ALGORITHM;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNABLE_TO_FIND_CHUNK;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNSUPPORTED_REQUEST;
import static org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil.onFailure;

import org.apache.ratis.util.function.CheckedFunction;
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
   * @param volume for statistics and checker
   * @param sync whether to do fsync or not
   */
  public static void writeData(File file, ChunkBuffer data,
      long offset, long len, HddsVolume volume, boolean sync)
      throws StorageContainerException {

    writeData(data, file.getName(), offset, len, volume,
        d -> writeDataToFile(file, d, offset, sync));
  }

  public static void writeData(FileChannel file, String filename,
      ChunkBuffer data, long offset, long len, HddsVolume volume)
      throws StorageContainerException {

    writeData(data, filename, offset, len, volume,
        d -> writeDataToChannel(file, d, offset));
  }

  private static void writeData(ChunkBuffer data, String filename,
      long offset, long len, HddsVolume volume,
      ToLongFunction<ChunkBuffer> writer) throws StorageContainerException {

    validateBufferSize(len, data.remaining());

    final long startTime = Time.monotonicNow();
    final long bytesWritten;
    try {
      bytesWritten = writer.applyAsLong(data);
    } catch (UncheckedIOException e) {
      if (!(e.getCause() instanceof InterruptedIOException)) {
        onFailure(volume);
      }
      throw wrapInStorageContainerException(e.getCause());
    }

    final long endTime = Time.monotonicNow();
    long elapsed = endTime - startTime;
    if (volume != null) {
      volume.getVolumeIOStats().incWriteTime(elapsed);
      volume.getVolumeIOStats().incWriteOpCount();
      volume.getVolumeIOStats().incWriteBytes(bytesWritten);
    }

    LOG.debug("Written {} bytes at offset {} to {} in {} ms",
        bytesWritten, offset, filename, elapsed);

    validateWriteSize(len, bytesWritten);
  }

  private static long writeDataToFile(File file, ChunkBuffer data,
      long offset, boolean sync) {
    final Path path = file.toPath();
    try {
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
    } catch (InterruptedException e) {
      throw new UncheckedIOException(new InterruptedIOException(
          "Interrupted while waiting to write file " + path));
    }
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

  public static ChunkBuffer readData(long len, int bufferCapacity,
      File file, long off, HddsVolume volume, int readMappedBufferThreshold)
      throws StorageContainerException {
    if (len > readMappedBufferThreshold) {
      return readData(file, bufferCapacity, off, len, volume);
    } else if (len == 0) {
      return ChunkBuffer.wrap(Collections.emptyList());
    }

    final ByteBuffer[] buffers = BufferUtils.assignByteBuffers(len,
        bufferCapacity);
    readData(file, off, len, c -> c.position(off).read(buffers), volume);
    Arrays.stream(buffers).forEach(ByteBuffer::flip);
    return ChunkBuffer.wrap(Arrays.asList(buffers));
  }

  private static void readData(File file, long offset, long len,
      CheckedFunction<FileChannel, Long, IOException> readMethod,
      HddsVolume volume) throws StorageContainerException {

    final Path path = file.toPath();
    final long startTime = Time.monotonicNow();
    final long bytesRead;

    try {
      bytesRead = processFileExclusively(path, () -> {
        try (FileChannel channel = open(path, READ_OPTIONS, NO_ATTRIBUTES);
             FileLock ignored = channel.lock(offset, len, true)) {
          return readMethod.apply(channel);
        } catch (IOException e) {
          onFailure(volume);
          throw new UncheckedIOException(e);
        }
      });
    } catch (UncheckedIOException e) {
      onFailure(volume);
      throw wrapInStorageContainerException(e.getCause());
    } catch (InterruptedException e) {
      throw wrapInStorageContainerException(e);
    }

    // Increment volumeIO stats here.
    long endTime = Time.monotonicNow();
    if (volume != null) {
      volume.getVolumeIOStats().incReadTime(endTime - startTime);
      volume.getVolumeIOStats().incReadOpCount();
      volume.getVolumeIOStats().incReadBytes(bytesRead);
    }

    LOG.debug("Read {} bytes starting at offset {} from {}",
        bytesRead, offset, file);

    validateReadSize(len, bytesRead);
  }

  /**
   * Read data from the given file using
   * {@link FileChannel#map(FileChannel.MapMode, long, long)},
   * whose javadoc recommends that it is generally only worth mapping
   * relatively large files (larger than a few tens of kilobytes)
   * into memory from the standpoint of performance.
   *
   * @return a list of {@link MappedByteBuffer} containing the data.
   */
  private static ChunkBuffer readData(File file, int chunkSize,
      long offset, long length, HddsVolume volume)
      throws StorageContainerException {

    final List<ByteBuffer> buffers = new ArrayList<>(
        Math.toIntExact((length - 1) / chunkSize) + 1);
    readData(file, offset, length, channel -> {
      long readLen = 0;
      while (readLen < length) {
        final int n = Math.toIntExact(Math.min(length - readLen, chunkSize));
        final ByteBuffer mapped = channel.map(
            FileChannel.MapMode.READ_ONLY, offset + readLen, n);
        LOG.debug("mapped: offset={}, readLen={}, n={}, {}",
            offset, readLen, n, mapped.getClass());
        readLen += mapped.remaining();
        buffers.add(mapped);
      }
      return readLen;
    }, volume);
    return ChunkBuffer.wrap(buffers);
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

    // TODO: when overwriting a chunk, we should ensure that the new chunk
    //  size is same as the old chunk size

    return false;
  }


  /**
   * Validates chunk data and returns a boolean value that indicates if the
   * chunk data should be overwritten.
   *
   * @param chunkFile - FileChannel of the chunkFile to write data into.
   * @param info - chunk info.
   * @return true if the chunkOffset is less than the chunkFile length,
   *         false otherwise.
   */
  public static boolean validateChunkForOverwrite(FileChannel chunkFile,
                                                  ChunkInfo info) {

    if (isOverWriteRequested(chunkFile, info)) {
      if (!isOverWritePermitted(info)) {
        LOG.warn("Duplicate write chunk request. Chunk overwrite " +
            "without explicit request. {}", info);
      }
      return true;
    }

    // TODO: when overwriting a chunk, we should ensure that the new chunk
    //  size is same as the old chunk size

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
   * Checks if a request to overwrite an existing range of a chunk has been
   * received.
   *
   * @param channel - FileChannel of the file to check
   * @param chunkInfo - Chunk information containing the offset
   * @return true if the offset is less than the file length, indicating
   *         a request to overwrite an existing range; false otherwise
   */
  public static boolean isOverWriteRequested(FileChannel channel, ChunkInfo
      chunkInfo) {
    long fileLen;
    try {
      fileLen = channel.size();
    } catch (IOException e) {
      String msg = "IO error encountered while getting the file size";
      LOG.error(msg, e.getMessage());
      throw new UncheckedIOException("IO error encountered while " +
          "getting the file size for ", e);
    }
    long offset = chunkInfo.getOffset();
    return offset < fileLen;
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
  static <T> T processFileExclusively(Path path, Supplier<T> op)
      throws InterruptedException {
    long period = 1;
    for (;;) {
      if (LOCKS.add(path)) {
        break;
      } else {
        Thread.sleep(period);
        // exponentially backoff until the sleep time is over 1 second.
        if (period < 1000) {
          period *= 2;
        }
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

  public static void limitReadSize(long len)
      throws StorageContainerException {
    if (len > OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE) {
      String err = String.format(
          "Oversize read. max: %d, actual: %d",
          OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE, len);
      LOG.error(err);
      throw new StorageContainerException(err, UNSUPPORTED_REQUEST);
    }
  }

  public static StorageContainerException wrapInStorageContainerException(
      Exception e) {
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

  /**
   * Checks if the block file length is equal to the chunk offset.
   *
   */
  public static void validateChunkSize(FileChannel fileChannel,
      ChunkInfo chunkInfo, String fileName)
      throws StorageContainerException {
    long offset = chunkInfo.getOffset();
    long fileLen;
    try {
      fileLen = fileChannel.size();
    } catch (IOException e) {
      throw new StorageContainerException("IO error encountered while " +
          "getting the file size for " + fileName + " at offset " + offset,
          CHUNK_FILE_INCONSISTENCY);
    }
    if (fileLen != offset) {
      throw new StorageContainerException("Chunk offset " + offset +
          " does not match length " + fileLen + " of blockFile " + fileName,
          CHUNK_FILE_INCONSISTENCY);
    }
  }
}
