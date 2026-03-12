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

package org.apache.hadoop.ozone.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to compute and verify checksums for chunks.
 *
 * This class is not thread safe.
 */
public class Checksum {
  private static final Logger LOG = LoggerFactory.getLogger(Checksum.class);

  private final ChecksumType checksumType;

  private final int bytesPerChecksum;

  /**
   * Caches computeChecksum() result when requested.
   * This must be manually cleared when a new block chunk has been started.
   */
  private final ChecksumCache checksumCache;

  private static Function<ByteBuffer, ByteString> newMessageDigestFunction(
      String algorithm) {
    final MessageDigest md;
    try {
      md = MessageDigest.getInstance(algorithm);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(
          "Failed to get MessageDigest for " + algorithm,  e);
    }
    return data -> {
      md.reset();
      md.update(data);
      return ByteString.copyFrom(md.digest());
    };
  }

  public static ByteString int2ByteString(int n) {
    return UnsafeByteOperations.unsafeWrap(Ints.toByteArray(n));
  }

  private static Function<ByteBuffer, ByteString> newChecksumByteBufferFunction(
      Supplier<ChecksumByteBuffer> constructor) {
    final ChecksumByteBuffer algorithm = constructor.get();
    return data -> {
      algorithm.reset();
      algorithm.update(data);
      return int2ByteString((int)algorithm.getValue());
    };
  }

  /** The algorithms for {@link ChecksumType}. */
  enum Algorithm {
    NONE(() -> data -> ByteString.EMPTY),
    CRC32(() ->
        newChecksumByteBufferFunction(ChecksumByteBufferFactory::crc32Impl)),
    CRC32C(() ->
        newChecksumByteBufferFunction(ChecksumByteBufferFactory::crc32CImpl)),
    SHA256(() -> newMessageDigestFunction("SHA-256")),
    MD5(() -> newMessageDigestFunction("MD5"));

    private final Supplier<Function<ByteBuffer, ByteString>> constructor;

    static Algorithm valueOf(ChecksumType type) {
      return valueOf(type.name());
    }

    Algorithm(Supplier<Function<ByteBuffer, ByteString>> constructor) {
      this.constructor = constructor;
    }

    Function<ByteBuffer, ByteString> newChecksumFunction() {
      return constructor.get();
    }
  }

  /**
   * BlockOutputStream needs to call this method to clear the checksum cache
   * whenever a block chunk has been established.
   */
  public boolean clearChecksumCache() {
    if (checksumCache != null) {
      checksumCache.clear();
      return true;
    }
    return false;
  }

  /**
   * Constructs a Checksum object.
   * @param type type of Checksum
   * @param bytesPerChecksum number of bytes of data per checksum
   */
  public Checksum(ChecksumType type, int bytesPerChecksum) {
    this.checksumType = type;
    this.bytesPerChecksum = bytesPerChecksum;
    this.checksumCache = null;
  }

  /**
   * Constructs a Checksum object.
   * @param type type of Checksum
   * @param bytesPerChecksum number of bytes of data per checksum
   * @param allowChecksumCache true to enable checksum cache
   */
  public Checksum(ChecksumType type, int bytesPerChecksum, boolean allowChecksumCache) {
    this.checksumType = type;
    this.bytesPerChecksum = bytesPerChecksum;
    LOG.debug("allowChecksumCache = {}", allowChecksumCache);
    if (allowChecksumCache) {
      this.checksumCache = new ChecksumCache(bytesPerChecksum);
    } else {
      this.checksumCache = null;
    }
  }

  /**
   * Computes checksum for give data.
   * @param data input data.
   * @return ChecksumData computed for input data.
   */
  public ChecksumData computeChecksum(byte[] data, int off, int len)
      throws OzoneChecksumException {
    return computeChecksum(ByteBuffer.wrap(data, off, len));
  }

  /**
   * Computes checksum for give data.
   * @param data input data in the form of byte array.
   * @return ChecksumData computed for input data.
   */
  public ChecksumData computeChecksum(byte[] data)
      throws OzoneChecksumException {
    return computeChecksum(ByteBuffer.wrap(data));
  }

  /**
   * The default implementation of computeChecksum(ByteBuffer) that does not use cache, even if cache is initialized.
   * This is a stop-gap solution before the protocol change.
   * @param data ByteBuffer
   * @return ChecksumData
   * @throws OzoneChecksumException
   */
  public ChecksumData computeChecksum(ByteBuffer data)
      throws OzoneChecksumException {
    return computeChecksum(data, false);
  }

  /**
   * Computes checksum for give data.
   * @param data input data.
   * @return ChecksumData computed for input data.
   * @throws OzoneChecksumException thrown when ChecksumType is not recognized
   */
  public ChecksumData computeChecksum(ByteBuffer data, boolean useChecksumCache)
      throws OzoneChecksumException {
    // If type is set to NONE, we do not need to compute the checksums. We also
    // need to avoid unnecessary conversions.
    if (checksumType == ChecksumType.NONE) {
      return new ChecksumData(checksumType, bytesPerChecksum);
    }
    if (!data.isReadOnly()) {
      data = data.asReadOnlyBuffer();
    }
    return computeChecksum(ChunkBuffer.wrap(data), useChecksumCache);
  }

  public ChecksumData computeChecksum(List<ByteString> byteStrings)
      throws OzoneChecksumException {
    final List<ByteBuffer> buffers =
        BufferUtils.getReadOnlyByteBuffers(byteStrings);
    return computeChecksum(ChunkBuffer.wrap(buffers));
  }

  /**
   * The default implementation of computeChecksum(ChunkBuffer) that does not use cache, even if cache is initialized.
   * This is a stop-gap solution before the protocol change.
   * @param data ChunkBuffer
   * @return ChecksumData
   * @throws OzoneChecksumException
   */
  public ChecksumData computeChecksum(ChunkBuffer data)
      throws OzoneChecksumException {
    return computeChecksum(data, false);
  }

  public ChecksumData computeChecksum(ChunkBuffer data, boolean useCache)
      throws OzoneChecksumException {
    if (checksumType == ChecksumType.NONE) {
      // Since type is set to NONE, we do not need to compute the checksums
      return new ChecksumData(checksumType, bytesPerChecksum);
    }

    final Function<ByteBuffer, ByteString> function;
    try {
      function = Algorithm.valueOf(checksumType).newChecksumFunction();
    } catch (Exception e) {
      throw new OzoneChecksumException("Failed to get the checksum function for " + checksumType, e);
    }

    final List<ByteString> checksumList;
    if (checksumCache == null || !useCache) {
      // When checksumCache is not enabled:
      // Checksum is computed for each bytesPerChecksum number of bytes of data
      // starting at offset 0. The last checksum might be computed for the
      // remaining data with length less than bytesPerChecksum.
      checksumList = new ArrayList<>();
      for (ByteBuffer b : data.iterate(bytesPerChecksum)) {
        checksumList.add(computeChecksum(b, function, bytesPerChecksum));  // merge this?
      }
    } else {
      // When checksumCache is enabled:
      // We only need to update the last checksum in the cache, then pass it along.
      checksumList = checksumCache.computeChecksum(data, function);
    }
    return new ChecksumData(checksumType, bytesPerChecksum, checksumList);
  }

  /**
   * Compute checksum using the algorithm for the data upto the max length.
   * @param data input data
   * @param function the checksum function
   * @param maxLength the max length of data
   * @return computed checksum ByteString
   */
  protected static ByteString computeChecksum(ByteBuffer data,
      Function<ByteBuffer, ByteString> function, int maxLength) {
    final int limit = data.limit();
    try {
      final int maxIndex = data.position() + maxLength;
      if (limit > maxIndex) {
        data.limit(maxIndex);
      }
      return function.apply(data);
    } finally {
      data.limit(limit);
    }
  }

  public static void verifySingleChecksum(ByteBuffer buffer, int offset, int bytesPerChecksum,
      ByteString checksum, ChecksumType checksumType) throws OzoneChecksumException {
    final ByteBuffer duplicated = buffer.duplicate();
    duplicated.position(offset).limit(offset + bytesPerChecksum);
    final ChecksumData cd = new ChecksumData(checksumType, bytesPerChecksum, Collections.singletonList(checksum));
    verifyChecksum(duplicated, cd, 0);
  }

  /**
   * Computes the ChecksumData for the input data and verifies that it
   * matches with that of the input checksumData.
   * @param data input data
   * @param checksumData checksumData to match with
   * @throws OzoneChecksumException is thrown if checksums do not match
   */
  public static void verifyChecksum(ByteBuffer data,
      ChecksumData checksumData, int startIndex) throws OzoneChecksumException {
    verifyChecksum(ChunkBuffer.wrap(data), checksumData, startIndex);
  }

  /**
   * Computes the ChecksumData for the input data and verifies that it
   * matches with that of the input checksumData.
   * @param data input data
   * @param checksumData checksumData to match with
   * @param startIndex index of first checksum in checksumData to match with
   *                   data's computed checksum.
   * @throws OzoneChecksumException is thrown if checksums do not match
   */
  public static void verifyChecksum(ChunkBuffer data,
      ChecksumData checksumData,
      int startIndex) throws OzoneChecksumException {
    ChecksumType checksumType = checksumData.getChecksumType();
    if (checksumType == ChecksumType.NONE) {
      // Checksum is set to NONE. No further verification is required.
      return;
    }

    int bytesPerChecksum = checksumData.getBytesPerChecksum();
    Checksum checksum = new Checksum(checksumType, bytesPerChecksum);
    final ChecksumData computed = checksum.computeChecksum(data);
    checksumData.verifyChecksumDataMatches(startIndex, computed);
  }

  /**
   * Computes the ChecksumData for the input byteStrings and verifies that
   * the checksums match with that of the input checksumData.
   * @param byteStrings input data buffers list. Each byteString should
   *                    correspond to one checksum.
   * @param checksumData checksumData to match with
   * @param startIndex index of first checksum in checksumData to match with
   *                   data's computed checksum.
   * @throws OzoneChecksumException is thrown if checksums do not match
   */
  public static void verifyChecksum(List<ByteString> byteStrings, ChecksumData checksumData, int startIndex)
      throws OzoneChecksumException {
    ChecksumType checksumType = checksumData.getChecksumType();
    if (checksumType == ChecksumType.NONE) {
      // Checksum is set to NONE. No further verification is required.
      return;
    }

    if (byteStrings.size() == 1) {
      // Optimization for a single ByteString.
      // Note that the old format (V0) also only has a single ByteString.
      verifyChecksum(byteStrings.get(0).asReadOnlyByteBuffer(), checksumData, startIndex);
      return;
    }

    // The data is a list of ByteStrings. Each ByteString length should be
    // the same as the number of bytes per checksum (except the last
    // ByteString which could be smaller).
    final List<ByteBuffer> buffers =
        BufferUtils.getReadOnlyByteBuffers(byteStrings);

    int bytesPerChecksum = checksumData.getBytesPerChecksum();
    Checksum checksum = new Checksum(checksumType, bytesPerChecksum);
    final ChecksumData computed = checksum.computeChecksum(
        ChunkBuffer.wrap(buffers));
    checksumData.verifyChecksumDataMatches(startIndex, computed);
  }

  /**
   * Returns a ChecksumData with type NONE for testing.
   */
  @VisibleForTesting
  public static ContainerProtos.ChecksumData getNoChecksumDataProto() {
    return new ChecksumData(ChecksumType.NONE, 0).getProtoBufMessage();
  }
}
