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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Caches local block replicas obtained through short-circuit GetBlock, so that another stream opening the same
 * block reuses the block metadata and file descriptor instead of sending GetBlock to the datanode again.
 * <p>
 * Entries are keyed by container and local block ID only. A reader can only look up a block whose location it
 * received from OM, so the cache does not bypass OM authorization.
 * <p>
 * The datanode does not notify the client about deleted or repaired replicas, so entries expire a fixed time after
 * they were last accessed. A replica is closed only after it has been removed from the cache and released by every
 * stream.
 */
public class ShortCircuitStreamCache implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ShortCircuitStreamCache.class);

  private final Cache<ContainerBlockID, Replica> cache;
  private final ExecutorService closeExecutor;

  public ShortCircuitStreamCache(int maxSize, long expiryMs, ExecutorService closeExecutor,
      ScheduledExecutorService cleanupExecutor) {
    this.cache = Caffeine.newBuilder()
        .recordStats()
        .maximumSize(maxSize)
        .expireAfterWrite(expiryMs, MILLISECONDS)
        // run the maintenance and the removal listener on the calling thread, so that close() collects the clients
        .executor(Runnable::run)
        .removalListener((key, info, cause) -> onRemoval((Replica) info))
        .build();
    this.closeExecutor = closeExecutor;
    cleanupExecutor.scheduleAtFixedRate(this::cleanUp, expiryMs, expiryMs, MILLISECONDS);
    cleanupExecutor.scheduleAtFixedRate(this::logCacheStats, expiryMs, expiryMs, MILLISECONDS);
    LOG.info("{} is created with maxSize {}, expiryMs {}", this.hashCode(), maxSize, expiryMs);
  }

  /**
   * @return a cached replica that covers the given block commit sequence ID and length, or null.
   *         The caller must {@link #release} the returned replica.
   */
  public synchronized Replica acquire(BlockID blockID, long length) {
    final ContainerBlockID key = blockID.getContainerBlockID();
    final Replica replica = cache.getIfPresent(key);
    if (replica == null) {
      return null;
    }
    if (replica.getBlockCommitSequenceId() < blockID.getBlockCommitSequenceId()
        || replica.getBlockData().getSize() < length) {
      cache.asMap().remove(key, replica);
      return null;
    }
    replica.getRefCount().incrementAndGet();
    if (replica.isEvicted()) {
      release(replica);
      return null;
    }
    return replica;
  }

  /**
   * Add a replica, replacing any cached replica of the same block.
   * @return the added replica, already acquired by the caller, who must {@link #release} it.
   */
  public synchronized Replica put(BlockID blockID, BlockData blockData, FileInputStream stream) {
    final Replica replica = new Replica(blockID.getContainerBlockID(), blockData, stream);
    replica.getRefCount().incrementAndGet();
    cache.put(replica.blockID, replica);
    return replica;
  }

  public synchronized void release(Replica replica) {
    if (replica.getRefCount().decrementAndGet() == 0) {
      if (replica.shouldClose()) {
        closeAsync(replica);
      }
    }
    Preconditions.checkState(replica.getRefCount().get() >= 0, 
        "Replica %s is over-released", replica);
  }

  /**
   * Remove the replica so that no new stream uses it, e.g. after a read error.
   * The caller still holds the replica and must {@link #release} it.
   */
  public synchronized void invalidate(Replica replica) {
    cache.asMap().remove(replica.blockID, replica);
  }

  @Override
  public synchronized void close() {
    logCacheStats();
    cache.invalidateAll();
    cache.cleanUp();
  }

  public synchronized void cleanUp() {
    cache.cleanUp();
  }

  private void logCacheStats() {
    CacheStats stats = cache.stats();
    LOG.info("{} Hits: {}, Misses: {}, HitRate: {}, MissRate: {}, AverageLoadPenalty(Ns): {}", this.hashCode(),
        stats.hitCount(), stats.missCount(), stats.hitRate(), stats.missRate(), stats.averageLoadPenalty());
  }

  private void onRemoval(Replica info) {
    // Mark the entry as evicted
    if (info != null && info.setEvicted()) {
      closeAsync(info);
    }
  }

  private void closeAsync(Replica replica) {
    try {
      closeExecutor.execute(() -> replica.closeIfUnused());
    } catch (RejectedExecutionException e) {
      // this manager is already closed
      replica.closeIfUnused();
    }
  }

  /**
   * Block metadata and open file descriptor of a local block replica. Guarded by the cache lock.
   */
  public static final class Replica {
    private final ContainerBlockID blockID;
    private final BlockData blockData;
    private final FileInputStream stream;
    private final AtomicInteger refCount = new AtomicInteger(0);
    private volatile boolean evicted;
    private boolean closed;

    private Replica(ContainerBlockID key, BlockData blockData, FileInputStream stream) {
      this.blockID = key;
      this.blockData = blockData;
      this.stream = stream;
    }

    public BlockData getBlockData() {
      return blockData;
    }

    public FileInputStream getStream() {
      return stream;
    }

    long getBlockCommitSequenceId() {
      return blockData.getBlockID().getBlockCommitSequenceId();
    }

    public AtomicInteger getRefCount() {
      return refCount;
    }

    @VisibleForTesting
    public synchronized boolean isClosed() {
      return closed;
    }

    boolean isEvicted() {
      return evicted;
    }

    public boolean setEvicted() {
      evicted = true;
      return shouldClose();
    }

    private boolean shouldClose() {
      return refCount.get() == 0 && evicted;
    }

    private synchronized void closeIfUnused() {
      if (evicted && refCount.get() == 0 && !closed) {
        closed = true;
        try {
          stream.close();
        } catch (IOException e) {
          LOG.warn("Failed to close file InputStream for block {}", blockID, e);
        }
      }
    }

    @Override
    public String toString() {
      return "Replica: {" +
          "blockID=" + blockID +
          ", blockSize=" + blockData.getSize() +
          ", bcsid=" + blockData.getBlockID().getBlockCommitSequenceId() +
          ", refCount=" + refCount.get() +
          ", evicted=" + evicted +
          ", closed=" + closed +
          '}';
    }
  }
}
