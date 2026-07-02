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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.ozone.common.Checksum.StreamingChecksum;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache previous checksums to avoid recomputing them.
 * This is a stop-gap solution to reduce checksum calc overhead inside critical section
 * without having to do a major refactoring/overhaul over protobuf and interfaces.
 * This is only supposed to be used by BlockOutputStream, for now.
 * <p>
 * Each BlockOutputStream has its own Checksum instance.
 * Each block chunk (4 MB default) is divided into 16 KB (default) each for checksum calculation.
 * For CRC32/CRC32C, each checksum takes 4 bytes. Thus each block chunk has 4 MB / 16 KB * 4 B = 1 KB of checksum data.
 */
public class ChecksumCache {
  private static final Logger LOG = LoggerFactory.getLogger(ChecksumCache.class);

  private final int bytesPerChecksum;
  private final List<ByteString> checksums;
  // Chunk length last time the checksum is computed
  private int prevChunkLength;
  // This only serves as a hint for array list initial allocation. The array list will still grow as needed.
  private static final int BLOCK_CHUNK_SIZE = 4 * 1024 * 1024; // 4 MB

  public ChecksumCache(int bytesPerChecksum) {
    LOG.debug("Initializing ChecksumCache with bytesPerChecksum = {}", bytesPerChecksum);
    this.prevChunkLength = 0;
    this.bytesPerChecksum = bytesPerChecksum;
    // Set initialCapacity to avoid costly resizes
    this.checksums = new ArrayList<>(BLOCK_CHUNK_SIZE / bytesPerChecksum);
  }

  /**
   * Clear cached checksums. And reset the written index.
   */
  public void clear() {
    prevChunkLength = 0;
    checksums.clear();
  }

  public List<ByteString> getChecksums() {
    return checksums;
  }

  /**
   * Recompute checksums for the windows that have changed since the last
   * call: bytes {@code [ciStart * bytesPerChecksum, currChunkLength)} where
   * {@code ciStart = prevChunkLength / bytesPerChecksum} (the index of the
   * first window whose result may have changed - either the previously-
   * partial last window now has more bytes, or new full windows have been
   * appended).
   *
   * <p>Walks {@code data}'s underlying buffer list directly (no
   * {@code iterate()} byte[] linearization) and feeds slices to {@code algo}
   * incrementally; the cached prefix is skipped via index arithmetic so
   * those bytes are never re-fed.
   */
  public List<ByteString> computeChecksum(ChunkBuffer data,
      StreamingChecksum algo, int chksumSize) {
    if (chksumSize != bytesPerChecksum) {
      throw new IllegalArgumentException("bytesPerChecksum mismatch: cache="
          + bytesPerChecksum + " call=" + chksumSize);
    }
    final int currChunkLength = data.limit();

    if (currChunkLength == prevChunkLength) {
      LOG.debug("ChunkBuffer data limit same as last time ({}). No new checksums need to be computed", prevChunkLength);
      return checksums;
    }
    if (currChunkLength < prevChunkLength) {
      // Indicates a bug: BOS did not clear() the cache before starting a new chunk.
      throw new IllegalArgumentException("ChunkBuffer data limit (" + currChunkLength + ")"
          + " must not be smaller than last time (" + prevChunkLength + ")");
    }

    // Index of the first window that needs (re)computing.
    final int ciStart = prevChunkLength / bytesPerChecksum;
    final int ciEnd = currChunkLength / bytesPerChecksum
        + (currChunkLength % bytesPerChecksum == 0 ? 0 : 1);
    // Bytes to skip (the cached full windows preceding ciStart).
    long bytesToSkip = (long) ciStart * bytesPerChecksum;

    int i = ciStart;
    int windowRemaining = bytesPerChecksum;
    algo.reset();
    long position = 0;

    for (ByteBuffer src : data.asByteBufferList()) {
      int srcPos = src.position();
      final int srcLim = src.limit();
      final int srcLen = srcLim - srcPos;

      // Fast-forward through buffers that lie entirely within the cached
      // prefix.
      if (position + srcLen <= bytesToSkip) {
        position += srcLen;
        continue;
      }
      // First buffer that crosses into the not-yet-cached region: advance
      // srcPos to the boundary.
      if (position < bytesToSkip) {
        srcPos += (int) (bytesToSkip - position);
        position = bytesToSkip;
      }

      while (srcPos < srcLim) {
        final int n = Math.min(srcLim - srcPos, windowRemaining);
        algo.update(BufferUtils.slice(src, srcPos, n));
        srcPos += n;
        position += n;
        windowRemaining -= n;
        if (windowRemaining == 0) {
          storeChecksum(i++, algo.finish());
          algo.reset();
          windowRemaining = bytesPerChecksum;
        }
      }
    }
    if (windowRemaining < bytesPerChecksum) {
      // Unaligned trailing window.
      storeChecksum(i++, algo.finish());
    }

    if (i != ciEnd) {
      throw new IllegalStateException("ChecksumCache: Checksum index end does not match expectation");
    }

    prevChunkLength = currChunkLength;
    return checksums;
  }

  private void storeChecksum(int i, ByteString cs) {
    // i can either point to the last cached element (recompute - the
    // previously-partial trailing window) or one past it (append a new
    // checksum for newly-arrived bytes).
    assert i == checksums.size() - 1 || i == checksums.size();
    if (i == checksums.size()) {
      checksums.add(cs);
    } else {
      checksums.set(i, cs);
    }
  }
}
