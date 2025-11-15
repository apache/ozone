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

  /**
   * Minimum number of acknowledged chunks before triggering memory cleanup.
   * This threshold prevents frequent list operations for small acknowledgements.
   */
  private static final int MIN_CLEANUP_THRESHOLD = 64;

  /**
   * Cleanup trigger ratio: clean up when acknowledged chunks exceed this fraction
   * of the total list (e.g., 0.5 means clean up when >50% is acknowledged).
   */
  private static final double CLEANUP_RATIO_THRESHOLD = 0.5;

  /**
   * List of all inflight write chunk requests.
   * Uses writeChunkStartIndex to track which chunks are still pending (not yet acknowledged).
   */
  private final List<PendingWriteChunk> inflightWriteChunks;

  /**
   * Index pointing to the first non-acknowledged chunk in inflightWriteChunks.
   * Chunks before this index have been acknowledged and may be cleaned up.
   */
  private int writeChunkStartIndex;

  /**
   * Queue of pending putBlock requests, tracked by their commit offsets.
   * Only the latest putBlock is meaningful for retry.
   */
  private final LinkedList<Long> inflightPutBlocks;

  /** Highest offset that has been acknowledged by the server. */
  private long acknowledgedOffset;

  /** Highest offset that has been sent to the server. */
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

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      PendingWriteChunk that = (PendingWriteChunk) obj;
      return offset == that.offset && length == that.length && endOffset == that.endOffset;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(endOffset) * 31 + Long.hashCode(offset);
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

    // Offsets are strictly increasing and non-overlapping; append to the end.
    inflightWriteChunks.add(newRequest);

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
   *
   * This method performs fast O(1) tracking by incrementing writeChunkStartIndex.
   * Actual memory cleanup is deferred and handled lazily by trimAcknowledgedWriteChunks().
   *
   * @param offset the acknowledged offset
   */
  public synchronized void acknowledgeUpTo(long offset) {
    acknowledgedOffset = Math.max(acknowledgedOffset, offset);

    // Mark write chunks as acknowledged by advancing the start index
    while (writeChunkStartIndex < inflightWriteChunks.size()
        && inflightWriteChunks.get(writeChunkStartIndex).getEndOffset() <= offset) {
      writeChunkStartIndex++;
    }

    // Lazily clean up memory when worthwhile (see trimAcknowledgedWriteChunks for strategy)
    trimAcknowledgedWriteChunks();

    // Remove acknowledged putBlock requests (these are small, so we clean immediately)
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
    long totalDataLength = 0;
    for (int i = writeChunkStartIndex; i < inflightWriteChunks.size(); i++) {
      PendingWriteChunk writeChunk = inflightWriteChunks.get(i);
      combinedChunks.add(writeChunk.getBuffer());
      totalDataLength += writeChunk.getLength();
    }
    boolean needsPutBlock = !inflightPutBlocks.isEmpty();

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

  /**
   * Performs lazy cleanup of acknowledged write chunks to optimize memory usage.
   *
   * Strategy:
   * - Acknowledged chunks are tracked by incrementing writeChunkStartIndex (O(1) operation)
   * - Actual memory cleanup (removal from list) is deferred until it's worthwhile
   * - Cleanup triggers when EITHER:
   *   1. Acknowledged count exceeds MIN_CLEANUP_THRESHOLD (64 chunks), OR
   *   2. Acknowledged chunks represent more than CLEANUP_RATIO_THRESHOLD (50%) of the list
   *
   * This approach balances memory usage with performance:
   * - Avoids frequent O(n) list operations for each small acknowledgement
   * - Prevents unbounded memory growth from accumulated acknowledged chunks
   *
   * IMPORTANT: Only ACKNOWLEDGED chunks (before writeChunkStartIndex) are removed.
   * Active chunks (from writeChunkStartIndex onwards) are NEVER removed, so there's
   * no data loss regardless of how many requests accumulate.
   */
  private void trimAcknowledgedWriteChunks() {
    // No acknowledged chunks to clean up
    if (writeChunkStartIndex == 0) {
      return;
    }

    // All chunks are acknowledged - clear everything
    if (writeChunkStartIndex >= inflightWriteChunks.size()) {
      inflightWriteChunks.clear();
      writeChunkStartIndex = 0;
      return;
    }

    // Check if cleanup is warranted based on our thresholds
    int acknowledgedCount = writeChunkStartIndex;
    int totalCount = inflightWriteChunks.size();
    boolean exceedsMinThreshold = acknowledgedCount > MIN_CLEANUP_THRESHOLD;
    boolean exceedsRatioThreshold = acknowledgedCount > totalCount * CLEANUP_RATIO_THRESHOLD;

    if (exceedsMinThreshold || exceedsRatioThreshold) {
      // Remove ONLY the acknowledged chunks [0, writeChunkStartIndex)
      // Active chunks [writeChunkStartIndex, size) are preserved
      inflightWriteChunks.subList(0, writeChunkStartIndex).clear();
      writeChunkStartIndex = 0;
    }
    // If neither threshold is met, we skip cleanup to avoid the cost of list removal.
    // The acknowledged chunks remain in the list but are simply ignored (via the index).
  }
}
