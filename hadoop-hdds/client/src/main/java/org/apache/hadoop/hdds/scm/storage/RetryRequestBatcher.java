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

package org.apache.hadoop.hdds.scm.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sliding Window Request Optimizer for Ozone client retry handling.
 *
 * This optimizer combines consecutive failed requests before retry,
 * similar to TCP sliding window buffer management:
 * - Multiple writeChunk requests can be combined into one
 * - Only the latest putBlock metadata is needed
 * - Reduces network overhead and improves retry performance
 *
 * Example:
 * If requests fail: writeChunk1, putBlock2, writeChunk3, putBlock4
 * Optimized retry: writeChunk1+3, putBlock4
 */
public class RetryRequestBatcher {
  private static final Logger LOG =
      LoggerFactory.getLogger(RetryRequestBatcher.class);

  // Sliding window buffer: maintains inflight writeChunk and putBlock requests.
  private final List<PendingWriteChunk> inflightWriteChunks;
  private int writeChunkStartIndex;
  private final LinkedList<Long> inflightPutBlocks;
  private long acknowledgedOffset;
  private long sentOffset;

  /**
   * Represents a pending writeChunk request ordered by end offset.
   */
  private static final class PendingWriteChunk implements Comparable<PendingWriteChunk> {
    private final ChunkBuffer buffer;
    private final long offset;
    private final long length;
    private final long endOffset;

    PendingWriteChunk(ChunkBuffer buffer, long offset) {
      this.buffer = buffer;
      this.offset = offset;
      this.length = buffer != null ? buffer.position() : 0;
      this.endOffset = offset + this.length;
    }

    ChunkBuffer getBuffer() {
      return buffer;
    }

    long getLength() {
      return length;
    }

    long getEndOffset() {
      return endOffset;
    }

    @Override
    public int compareTo(PendingWriteChunk other) {
      if (this.endOffset == other.endOffset) {
        return Long.compare(this.offset, other.offset);
      }
      return Long.compare(this.endOffset, other.endOffset);
    }
  }

  /**
   * Represents optimized requests ready for retry.
   */
  public static class OptimizedRetryPlan {
    private final List<ChunkBuffer> combinedChunks;
    private final boolean needsPutBlock;
    private final long totalDataLength;

    public OptimizedRetryPlan(List<ChunkBuffer> combinedChunks, boolean needsPutBlock, long totalDataLength) {
      this.combinedChunks = combinedChunks;
      this.needsPutBlock = needsPutBlock;
      this.totalDataLength = totalDataLength;
    }

    public List<ChunkBuffer> getCombinedChunks() {
      return combinedChunks;
    }

    public boolean needsPutBlock() {
      return needsPutBlock;
    }

    public long getTotalDataLength() {
      return totalDataLength;
    }
  }

  public RetryRequestBatcher() {
    this.inflightWriteChunks = new ArrayList<>();
    this.writeChunkStartIndex = 0;
    this.inflightPutBlocks = new LinkedList<>();
    this.acknowledgedOffset = 0;
    this.sentOffset = 0;
  }

  /**
   * Track a write chunk request.
   *
   * @param buffer the chunk buffer
   * @param offset the offset of this chunk
  */
  public synchronized void trackInflightWriteChunkRequest(ChunkBuffer buffer, long offset) {
    PendingWriteChunk newRequest = new PendingWriteChunk(buffer, offset);

    int size = inflightWriteChunks.size();
    int activeStart = writeChunkStartIndex;
    int relativePos = Collections.binarySearch(
        inflightWriteChunks.subList(activeStart, size), newRequest);
    if (relativePos < 0) {
      relativePos = -relativePos - 1;
    }
    int insertPos = activeStart + relativePos;
    inflightWriteChunks.add(insertPos, newRequest);

    sentOffset = Math.max(sentOffset, newRequest.getEndOffset());
  }

  /**
   * Track a putBlock request.
   *
   * @param offset the offset up to which data should be committed
   */
  public synchronized void trackInflightPutBlockRequest(long offset) {
    if (!inflightPutBlocks.isEmpty()) {
      long currentHighestOffset = inflightPutBlocks.peekLast();
      if (offset <= currentHighestOffset) {
        LOG.debug("Ignoring putBlock request at offset {} as a higher offset {} is pending.",
            offset, currentHighestOffset);
        return;
      }
    }

    inflightPutBlocks.clear();
    inflightPutBlocks.add(offset);
  }

  /**
   * Mark requests up to the given offset as acknowledged.
   * This removes them from the sliding window buffer.
   *
   * @param offset the acknowledged offset
   */
  public synchronized void acknowledgeUpTo(long offset) {
    acknowledgedOffset = Math.max(acknowledgedOffset, offset);

    while (writeChunkStartIndex < inflightWriteChunks.size()
        && inflightWriteChunks.get(writeChunkStartIndex).getEndOffset() <= offset) {
      writeChunkStartIndex++;
    }
    trimAcknowledgedWriteChunks();

    while (!inflightPutBlocks.isEmpty() && inflightPutBlocks.peekFirst() <= offset) {
      inflightPutBlocks.removeFirst();
    }
  }

  /**
   * Optimize pending requests for retry.
   *
   * Optimization rules:
   * 1. Combine consecutive writeChunk requests into one
   * 2. Only keep the latest putBlock (earlier ones are stale)
   * 3. Remove acknowledged requests
   *
   * @return OptimizedRetryPlan containing the optimized request sequence
   */
  public synchronized OptimizedRetryPlan optimizeForRetry() {
    if (getActiveWriteChunkCount() == 0 && inflightPutBlocks.isEmpty()) {
      return new OptimizedRetryPlan(new ArrayList<>(), false, 0);
    }

    List<ChunkBuffer> combinedChunks = new ArrayList<>();
    boolean needsPutBlock = false;
    long totalDataLength = 0;

    // Merge writeChunk and putBlock sequences with two pointers to preserve ordering.
    int writeIndex = writeChunkStartIndex;
    int putIndex = 0;
    while (writeIndex < inflightWriteChunks.size() || putIndex < inflightPutBlocks.size()) {
      if (putIndex >= inflightPutBlocks.size()) {
        PendingWriteChunk writeChunk = inflightWriteChunks.get(writeIndex++);
        combinedChunks.add(writeChunk.getBuffer());
        totalDataLength += writeChunk.getLength();
      } else if (writeIndex >= inflightWriteChunks.size()) {
        needsPutBlock = true;
        break;
      } else {
        PendingWriteChunk writeChunk = inflightWriteChunks.get(writeIndex);
        long putBlockOffset = inflightPutBlocks.get(putIndex);
        if (writeChunk.getEndOffset() <= putBlockOffset) {
          combinedChunks.add(writeChunk.getBuffer());
          totalDataLength += writeChunk.getLength();
          writeIndex++;
        } else {
          needsPutBlock = true;
          putIndex++;
        }
      }
    }

    if (!needsPutBlock && putIndex < inflightPutBlocks.size()) {
      needsPutBlock = true;
    }

    LOG.debug("Optimized retry plan: {} combined chunks, needsPutBlock={}, totalDataLength={}",
        combinedChunks.size(), needsPutBlock, totalDataLength);

    return new OptimizedRetryPlan(combinedChunks, needsPutBlock, totalDataLength);
  }

  /**
   * Clear all pending requests.
   * Called when the stream is closed or reset.
   */
  public synchronized void clear() {
    inflightWriteChunks.clear();
    writeChunkStartIndex = 0;
    inflightPutBlocks.clear();
    acknowledgedOffset = 0;
    sentOffset = 0;
  }

  public synchronized int getPendingRequestCount() {
    return getActiveWriteChunkCount() + inflightPutBlocks.size();
  }

  public synchronized long getAcknowledgedOffset() {
    return acknowledgedOffset;
  }

  public synchronized long getSentOffset() {
    return sentOffset;
  }

  public synchronized long getUnacknowledgedDataLength() {
    return sentOffset - acknowledgedOffset;
  }

  private int getActiveWriteChunkCount() {
    return inflightWriteChunks.size() - writeChunkStartIndex;
  }

  private void trimAcknowledgedWriteChunks() {
    if (writeChunkStartIndex == 0) {
      return;
    }
    if (writeChunkStartIndex >= inflightWriteChunks.size()) {
      inflightWriteChunks.clear();
      writeChunkStartIndex = 0;
      return;
    }

    if (writeChunkStartIndex > 64
        || writeChunkStartIndex > inflightWriteChunks.size() / 2) {
      inflightWriteChunks.subList(0, writeChunkStartIndex).clear();
      writeChunkStartIndex = 0;
    }
  }
}
