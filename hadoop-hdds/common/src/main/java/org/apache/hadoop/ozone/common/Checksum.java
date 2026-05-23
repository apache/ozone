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
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.utils.db.IntegerCodec;
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

  private static MessageDigest newMessageDigest(String algorithm) {
    try {
      return MessageDigest.getInstance(algorithm);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(
          "Failed to get MessageDigest for " + algorithm,  e);
    }
  }

  public static ByteString int2ByteString(int n) {
    return UnsafeByteOperations.unsafeWrap(IntegerCodec.get().toByteArray(n));
  }

  /**
   * Streaming checksum strategy: feed multiple ByteBuffer slices via
   * {@link #update}, then read the result via {@link #finish}, then
   * {@link #reset} to start a new window. Used by both the no-cache and
   * cache compute paths to avoid the {@code byte[bytesPerChecksum]}
   * allocation that {@code ChunkBuffer.iterate} performs whenever a
   * checksum window straddles multiple underlying buffers - the
   * {@link ChecksumByteBuffer#update(ByteBuffer)} and
   * {@link MessageDigest#update(ByteBuffer)} contracts both define
   * incremental updates as byte-equivalent to a single update over the
   * concatenation.
   */
  interface StreamingChecksum {
    void reset();

    void update(ByteBuffer slice);

    ByteString finish();
  }

  private static StreamingChecksum streamingCrc(
      Supplier<ChecksumByteBuffer> ctor) {
    final ChecksumByteBuffer cb = ctor.get();
    return new StreamingChecksum() {
      @Override
      public void reset() {
        cb.reset();
      }

      @Override
      public void update(ByteBuffer slice) {
        cb.update(slice);
      }

      @Override
      public ByteString finish() {
        return int2ByteString((int) cb.getValue());
      }
    };
  }

  private static StreamingChecksum streamingDigest(String algorithm) {
    final MessageDigest md = newMessageDigest(algorithm);
    return new StreamingChecksum() {
      @Override
      public void reset() {
        md.reset();
      }

      @Override
      public void update(ByteBuffer slice) {
        md.update(slice);
      }

      @Override
      public ByteString finish() {
        return UnsafeByteOperations.unsafeWrap(md.digest());
      }
    };
  }

  /** The algorithms for {@link ChecksumType}. */
  enum Algorithm {
    // NONE is reachable via Algorithm.valueOf(ChecksumType.NONE) only if
    // computeChecksum's NONE short-circuit is bypassed; throw to surface
    // such a misuse rather than silently producing empty checksums.
    NONE(() -> {
      throw new UnsupportedOperationException(
          "ChecksumType.NONE has no StreamingChecksum");
    }),
    CRC32(() -> streamingCrc(ChecksumByteBufferFactory::crc32Impl)),
    CRC32C(() -> streamingCrc(ChecksumByteBufferFactory::crc32CImpl)),
    SHA256(() -> streamingDigest("SHA-256")),
    MD5(() -> streamingDigest("MD5"));

    private final Supplier<StreamingChecksum> constructor;

    static Algorithm valueOf(ChecksumType type) {
      return valueOf(type.name());
    }

    Algorithm(Supplier<StreamingChecksum> constructor) {
      this.constructor = constructor;
    }

    StreamingChecksum newStreamingChecksum() {
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

  /**
   * @implNote The position of {@code data}'s underlying buffers is not
   * advanced by this method - both the no-cache and cache paths slice via
   * {@link ByteBuffer#duplicate()}.
   */
  public ChecksumData computeChecksum(ChunkBuffer data, boolean useCache)
      throws OzoneChecksumException {
    if (checksumType == ChecksumType.NONE) {
      return new ChecksumData(checksumType, bytesPerChecksum);
    }

    final StreamingChecksum algo;
    try {
      algo = Algorithm.valueOf(checksumType).newStreamingChecksum();
    } catch (Exception e) {
      throw new OzoneChecksumException(
          "Failed to get the checksum function for " + checksumType, e);
    }

    final List<ByteString> checksumList = (checksumCache == null || !useCache)
        ? computeChecksumDirect(data, algo)
        : checksumCache.computeChecksum(data, algo, bytesPerChecksum);
    return new ChecksumData(checksumType, bytesPerChecksum, checksumList);
  }

  /**
   * Walk {@code data}'s underlying ByteBuffer list, slicing each window of
   * {@link #bytesPerChecksum} bytes via {@link ByteBuffer#duplicate()} and
   * feeding slices to {@code algo}.  No linearization byte[] is allocated
   * when a window straddles multiple buffers.
   */
  private List<ByteString> computeChecksumDirect(ChunkBuffer data,
      StreamingChecksum algo) {
    final int dataLimit = data.limit();
    final List<ByteString> result = new ArrayList<>(
        (dataLimit + bytesPerChecksum - 1) / bytesPerChecksum);
    int windowRemaining = bytesPerChecksum;
    algo.reset();

    for (ByteBuffer src : data.asByteBufferList()) {
      int srcPos = src.position();
      final int srcLim = src.limit();
      while (srcPos < srcLim) {
        final int n = Math.min(srcLim - srcPos, windowRemaining);
        algo.update(BufferUtils.slice(src, srcPos, n));
        srcPos += n;
        windowRemaining -= n;
        if (windowRemaining == 0) {
          result.add(algo.finish());
          algo.reset();
          windowRemaining = bytesPerChecksum;
        }
      }
    }
    if (windowRemaining < bytesPerChecksum) {
      // Unaligned trailing window.
      result.add(algo.finish());
    }
    return result;
  }

  public static void verifySingleChecksum(ByteBuffer buffer, int offset, int bytesPerChecksum,
      ByteString checksum, ChecksumType checksumType) throws OzoneChecksumException {
    final ChecksumData cd = new ChecksumData(checksumType, bytesPerChecksum, Collections.singletonList(checksum));
    verifyChecksum(BufferUtils.slice(buffer, offset, bytesPerChecksum), cd, 0);
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
