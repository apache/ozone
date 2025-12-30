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
import java.util.function.Function;
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

  public List<ByteString> computeChecksum(ChunkBuffer data, Function<ByteBuffer, ByteString> function) {
    // Indicates how much data the current chunk buffer holds
    final int currChunkLength = data.limit();

    if (currChunkLength == prevChunkLength) {
      LOG.debug("ChunkBuffer data limit same as last time ({}). No new checksums need to be computed", prevChunkLength);
      return checksums;
    }

    // Sanity check
    if (currChunkLength < prevChunkLength) {
      // If currChunkLength <= lastChunkLength, it indicates a bug that needs to be addressed.
      // It means BOS has not properly clear()ed the cache when a new chunk is started in that code path.
      throw new IllegalArgumentException("ChunkBuffer data limit (" + currChunkLength + ")" +
          " must not be smaller than last time (" + prevChunkLength + ")");
    }

    // One or more checksums need to be computed

    // Start of the checksum index that need to be (re)computed
    final int ciStart = prevChunkLength / bytesPerChecksum;
    final int ciEnd = currChunkLength / bytesPerChecksum + (currChunkLength % bytesPerChecksum == 0 ? 0 : 1);
    int i = 0;
    for (ByteBuffer b : data.iterate(bytesPerChecksum)) {
      if (i < ciStart) {
        i++;
        continue;
      }

      // variable i can either point to:
      // 1. the last element in the list -- in which case the checksum needs to be updated
      // 2. one after the last element   -- in which case a new checksum needs to be added
      assert i == checksums.size() - 1 || i == checksums.size();

      // TODO: Furthermore for CRC32/CRC32C, it can be even more efficient by updating the last checksum byte-by-byte.
      final ByteString checksum = Checksum.computeChecksum(b, function, bytesPerChecksum);
      if (i == checksums.size()) {
        checksums.add(checksum);
      } else {
        checksums.set(i, checksum);
      }

      i++;
    }

    // Sanity check
    if (i != ciEnd) {
      throw new IllegalStateException("ChecksumCache: Checksum index end does not match expectation");
    }

    // Update last written index
    prevChunkLength = currChunkLength;
    return checksums;
  }
}
