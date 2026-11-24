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

package org.apache.hadoop.hdds.utils.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.utils.db.RocksDatabase.ColumnFamily;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scans a column family (optionally restricted to a prefix) with multiple
 * threads.
 *
 * <p>Design: split up front, never synchronize per element.
 * <ol>
 *   <li>Take one RocksDB snapshot shared by all splits.</li>
 *   <li>Compute disjoint split ranges from SST metadata
 *       ({@link RDBTableSplitter}).</li>
 *   <li>Each split gets its own {@link RDBSplitIterator} with native bounds,
 *       the shared snapshot and fillCache=false.</li>
 *   <li>Parallelism is min(requested, splits, configured cap).</li>
 *   <li>Every iterator is closed exactly once; the snapshot is released only
 *       after all split tasks have finished.</li>
 * </ol>
 * The consumer may be invoked concurrently from different threads (one thread
 * per split at a time), so it must be thread-safe. Keys within a split are
 * delivered in sorted order; there is no ordering across splits.
 */
public final class RDBParallelScanner {
  private static final Logger LOG =
      LoggerFactory.getLogger(RDBParallelScanner.class);
  /** Over-split a little so that uneven splits balance across threads. */
  static final int SPLITS_PER_THREAD = 4;

  private RDBParallelScanner() {
  }

  /** Summary of a scan, for metrics and tests. */
  public static final class Result {
    private final int splits;
    private final int threads;
    private final long keys;
    private final int iteratorsOpened;
    private final int iteratorsClosed;
    private final boolean snapshotReleased;

    Result(int splits, int threads, long keys, int iteratorsOpened,
        int iteratorsClosed, boolean snapshotReleased) {
      this.splits = splits;
      this.threads = threads;
      this.keys = keys;
      this.iteratorsOpened = iteratorsOpened;
      this.iteratorsClosed = iteratorsClosed;
      this.snapshotReleased = snapshotReleased;
    }

    public int getSplits() {
      return splits;
    }

    public int getThreads() {
      return threads;
    }

    public long getKeys() {
      return keys;
    }

    public int getIteratorsOpened() {
      return iteratorsOpened;
    }

    public int getIteratorsClosed() {
      return iteratorsClosed;
    }

    public boolean isSnapshotReleased() {
      return snapshotReleased;
    }

    @Override
    public String toString() {
      return "splits=" + splits + ", threads=" + threads + ", keys=" + keys
          + ", opened=" + iteratorsOpened + ", closed=" + iteratorsClosed;
    }
  }

  /** @return the key range covered by {@code prefix} (all keys if null). */
  static KeyRange prefixRange(byte[] prefix) {
    if (prefix == null || prefix.length == 0) {
      return new KeyRange(null, null);
    }
    return new KeyRange(prefix,
        RDBStoreAbstractIterator.getNextHigherPrefix(prefix));
  }

  /** Compute the split ranges for a scan, without scanning. */
  public static List<KeyRange> computeSplits(RocksDatabase db,
      ColumnFamily family, KeyRange range, int maxSplits)
      throws RocksDatabaseException {
    if (maxSplits <= 1) {
      return Collections.singletonList(range);
    }
    return RDBTableSplitter.split(db.getLiveFilesMetaData(), family.getName(),
        range, maxSplits);
  }

  /** Counts iterators opened and closed, to verify none leak. */
  private static final class Counters {
    private final AtomicInteger opened = new AtomicInteger();
    private final AtomicInteger closed = new AtomicInteger();
  }

  private static final class ScanContext {
    private final RocksDatabase db;
    private final ColumnFamily family;
    private final ManagedSnapshot snapshot;
    private final IteratorType type;
    private final Consumer<Table.KeyValue<byte[], byte[]>> consumer;
    private final Counters counters = new Counters();

    ScanContext(RocksDatabase db, ColumnFamily family, ManagedSnapshot snapshot,
        IteratorType type, Consumer<Table.KeyValue<byte[], byte[]>> consumer) {
      this.db = db;
      this.family = family;
      this.snapshot = snapshot;
      this.type = type;
      this.consumer = consumer;
    }
  }

  public static Result scan(RocksDatabase db, ColumnFamily family,
      byte[] prefix, int requestedThreads, ParallelScanConfig config,
      Consumer<Table.KeyValue<byte[], byte[]>> consumer)
      throws RocksDatabaseException {
    return scan(db, family, prefix, requestedThreads, IteratorType.KEY_AND_VALUE, config, consumer);
  }

  public static Result scan(RocksDatabase db, ColumnFamily family,
      byte[] prefix, int requestedThreads, IteratorType type,
      ParallelScanConfig config,
      Consumer<Table.KeyValue<byte[], byte[]>> consumer)
      throws RocksDatabaseException {
    final KeyRange range = prefixRange(prefix);
    final int cap = config.parallelism(requestedThreads, Integer.MAX_VALUE);
    final ManagedSnapshot snapshot = db.newSnapshot();
    final ScanContext ctx = new ScanContext(db, family, snapshot, type, consumer);
    final List<KeyRange> ranges;
    final int threads;
    long keys = 0;
    try {
      // Disabled or single-threaded: one range, i.e. a plain sequential scan.
      ranges = cap <= 1 ? Collections.singletonList(range)
          : computeSplits(db, family, range, cap * SPLITS_PER_THREAD);
      threads = config.parallelism(requestedThreads, ranges.size());
      LOG.debug("Parallel scan of {} {}: {} splits, {} threads",
          family.getName(), range, ranges.size(), threads);
      if (threads <= 1) {
        final AtomicBoolean aborted = new AtomicBoolean();
        for (KeyRange r : ranges) {
          keys += scanRange(ctx, r, aborted);
        }
      } else {
        keys = scanInParallel(ctx, ranges, threads);
      }
    } finally {
      // All split tasks have completed here, so this is after the last
      // iterator close.
      snapshot.close();
    }
    return new Result(ranges.size(), threads, keys, ctx.counters.opened.get(),
        ctx.counters.closed.get(), snapshot.isReleased());
  }

  private static long scanInParallel(ScanContext ctx, List<KeyRange> ranges,
      int threads) throws RocksDatabaseException {
    final AtomicInteger threadId = new AtomicInteger();
    final ExecutorService pool = Executors.newFixedThreadPool(threads, r -> {
      final Thread t = new Thread(r, "ParallelScan-" + ctx.family.getName()
          + "-" + threadId.getAndIncrement());
      t.setDaemon(true);
      return t;
    });
    final AtomicBoolean aborted = new AtomicBoolean();
    final List<Future<Long>> futures = new ArrayList<>(ranges.size());
    try {
      for (KeyRange r : ranges) {
        futures.add(pool.submit(() -> scanRange(ctx, r, aborted)));
      }
      long keys = 0;
      Throwable failure = null;
      for (Future<Long> f : futures) {
        try {
          keys += f.get();
        } catch (ExecutionException e) {
          aborted.set(true);
          if (failure == null) {
            failure = e.getCause();
          } else if (failure != e.getCause()) {
            // The same consumer exception may be thrown by several tasks.
            failure.addSuppressed(e.getCause());
          }
        } catch (InterruptedException e) {
          aborted.set(true);
          Thread.currentThread().interrupt();
          failure = failure == null ? e : failure;
          break;
        }
      }
      if (failure instanceof RocksDatabaseException) {
        throw (RocksDatabaseException) failure;
      } else if (failure instanceof RuntimeException) {
        throw (RuntimeException) failure;
      } else if (failure instanceof Error) {
        throw (Error) failure;
      } else if (failure != null) {
        throw new RocksDatabaseException("Parallel scan failed", (Exception) failure);
      }
      return keys;
    } finally {
      pool.shutdown();
      awaitQuietly(pool);
    }
  }

  private static void awaitQuietly(ExecutorService pool) {
    boolean interrupted = false;
    while (true) {
      try {
        if (pool.awaitTermination(1, TimeUnit.MINUTES)) {
          break;
        }
        LOG.warn("Waiting for parallel scan tasks to finish");
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private static long scanRange(ScanContext ctx, KeyRange range,
      AtomicBoolean aborted) throws RocksDatabaseException {
    if (aborted.get()) {
      return 0;
    }
    long count = 0;
    final RDBSplitIterator it = new RDBSplitIterator(
        ctx.db, ctx.family, ctx.snapshot, range, ctx.type);
    ctx.counters.opened.incrementAndGet();
    try {
      while (it.hasNext() && !aborted.get()) {
        ctx.consumer.accept(it.next());
        count++;
      }
      if (!aborted.get()) {
        it.checkStatus();
      }
    } catch (Throwable t) {
      aborted.set(true);
      throw t;
    } finally {
      it.close();
      ctx.counters.closed.incrementAndGet();
    }
    return count;
  }
}
