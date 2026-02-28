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

package org.apache.hadoop.ozone.recon.tasks.util;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.rocksdb.LiveFileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to iterate through a table in parallel by breaking table into multiple iterators.
 */
public class ParallelTableIteratorOperation<K extends Comparable<K>, V> implements Closeable {
  private final Table<K, V> table;
  private final Codec<K> keyCodec;

  // Thread Pools
  private final ExecutorService iteratorExecutor; // 5
  private final long maxNumberOfVals;
  private final OMMetadataManager metadataManager;
  private final int maxIteratorTasks;
  private final long logCountThreshold;

  private static final Logger LOG = LoggerFactory.getLogger(ParallelTableIteratorOperation.class);

  public ParallelTableIteratorOperation(OMMetadataManager metadataManager, Table<K, V> table, Codec<K> keyCodec,
                                        int iteratorCount, long logThreshold) {
    this.table = table;
    this.keyCodec = keyCodec;
    this.metadataManager = metadataManager;
    this.maxIteratorTasks = 2 * iteratorCount;  // Allow up to 10 pending iterator tasks

    // Create team of iterator threads with UNLIMITED queue
    // LinkedBlockingQueue() with no size = can hold infinite pending tasks
    this.iteratorExecutor = new ThreadPoolExecutor(iteratorCount, iteratorCount, 1, TimeUnit.MINUTES,
                    new LinkedBlockingQueue<>());
    this.logCountThreshold = logThreshold;
    this.maxNumberOfVals = Math.max(1000, this.logCountThreshold / (iteratorCount));
  }

  private List<K> getBounds(K startKey, K endKey) throws IOException {
    Set<K> keys = new HashSet<>();

    // Try to get SST file boundaries for optimal segmentation
    // In test/mock environments, this may not be available
    try {
      RDBStore store = (RDBStore) this.metadataManager.getStore();
      if (store != null && store.getDb() != null) {
        List<LiveFileMetaData> sstFiles = store.getDb().getSstFileList();
        String tableName = table.getName();

        // Only filter by column family if table name is available
        if (tableName != null && !tableName.isEmpty()) {
          byte[] tableNameBytes = tableName.getBytes(StandardCharsets.UTF_8);
          for (LiveFileMetaData sstFile : sstFiles) {
            // Filter SST files by column family to get bounds only for this specific table
            if (Arrays.equals(sstFile.columnFamilyName(), tableNameBytes)) {
              keys.add(this.keyCodec.fromPersistedFormat(sstFile.smallestKey()));
              keys.add(this.keyCodec.fromPersistedFormat(sstFile.largestKey()));
            }
          }
        }
      }
    } catch (Exception e) {
      // If we can't get SST files (test environment, permissions, etc.),
      // just use empty bounds and rely on fallback path
      LOG.debug("Unable to retrieve SST file boundaries, will use fallback iteration: {}", e.getMessage());
    }

    if (startKey != null) {
      keys.add(startKey);
    }
    if (endKey != null) {
      keys.add(endKey);
    }

    return keys.stream().sorted().filter(Objects::nonNull)
            .filter(key -> startKey == null || key.compareTo(startKey) >= 0)
            .filter(key -> endKey == null || endKey.compareTo(key) >= 0)
            .collect(Collectors.toList());
  }

  private void waitForQueueSize(Queue<Future<?>> futures, int expectedSize)
          throws ExecutionException, InterruptedException {
    while (!futures.isEmpty() && futures.size() > expectedSize) {
      Future<?> f = futures.poll();
      f.get();
    }
  }

  // Main parallelization logic
  public void performTaskOnTableVals(String taskName, K startKey, K endKey,
      Function<Table.KeyValue<K, V>, Void> keyOperation) throws IOException, ExecutionException, InterruptedException {
    List<K> bounds = getBounds(startKey, endKey);
    LOG.debug("Length of the bounds - {}", bounds.size());
    // Fallback for small tables (no SST files yet - data only in memtable)
    if (bounds.size() < 2) {
      try (TableIterator<K, ? extends Table.KeyValue<K, V>> iter = table.iterator()) {
        if (startKey != null) {
          iter.seek(startKey);
        }
        while (iter.hasNext()) {
          Table.KeyValue<K, V> kv = iter.next();
          if (endKey != null && kv.getKey().compareTo(endKey) > 0) {
            break;
          }
          keyOperation.apply(kv);
        }
      }
      return;
    }

    // ===== PARALLEL PROCESSING SETUP =====

    // Queue to track iterator threads
    Queue<Future<?>> iterFutures = new LinkedList<>();

    AtomicLong keyCounter = new AtomicLong();
    AtomicLong prevLogCounter = new AtomicLong();
    Object logLock = new Object();

    // ===== STEP 2: START ITERATOR THREADS =====
    // For each segment boundary, create an iterator thread
    // Example: If bounds = [0, 5M, 10M, 15M, 20M], this loop runs 4 times:
    //   idx=1: beg=0, end=5M
    //   idx=2: beg=5M, end=10M
    //   idx=3: beg=10M, end=15M
    //   idx=4: beg=15M, end=20M
    for (int idx = 1; idx < bounds.size(); idx++) {
      K beg = bounds.get(idx - 1);
      K end = bounds.get(idx);
      boolean inclusive = idx == bounds.size() - 1;
      waitForQueueSize(iterFutures, maxIteratorTasks - 1);

      // ===== STEP 3: SUBMIT ITERATOR TASK =====
      iterFutures.add(iteratorExecutor.submit(() -> {
        try (TableIterator<K, ? extends Table.KeyValue<K, V>> iter  = table.iterator()) {
          iter.seek(beg);
          int count = 0;
          while (iter.hasNext()) {
            Table.KeyValue<K, V> kv = iter.next();
            K key = kv.getKey();
            // Check if key is within this segment's range
            boolean withinBounds;
            if (inclusive) {
              // Last segment: include everything from beg onwards (or until endKey if specified)
              withinBounds = (endKey == null || key.compareTo(endKey) <= 0);
            } else {
              // Middle segment: include keys in range [beg, end)
              withinBounds = key.compareTo(end) < 0;
            }
            if (!withinBounds) {
              break;
            }
            // ===== STEP 4: APPLY OPERATION ON EACH KEY-VALUE =====
            keyOperation.apply(kv);
            // ===== STEP 5: EACH ITERATOR THREAD MAINTAINS ITS OWN LOCAL COUNT =====
            // Each iterator thread counts how many keys it processed locally.
            count++;
            if (count % maxNumberOfVals == 0) {
              // ===== STEP 6: UPDATE GLOBAL COUNT =====
              keyCounter.addAndGet(count);
              count = 0;
              if (keyCounter.get() - prevLogCounter.get() > logCountThreshold) {
                synchronized (logLock) {
                  if (keyCounter.get() - prevLogCounter.get() > logCountThreshold) {
                    long cnt = keyCounter.get();
                    LOG.debug("Iterated through {} keys while performing task: {}", keyCounter.get(), taskName);
                    prevLogCounter.set(cnt);
                  }
                }
              }
            }
          }
          keyCounter.addAndGet(count);
        } catch (IOException e) {
          LOG.error("IO error during parallel iteration on table {}", taskName, e);
          throw new RuntimeException("IO error during iteration", e);
        }
      }));
    }

    // ===== STEP 7: WAIT FOR EVERYONE TO FINISH =====
    // Wait for all iterator threads to finish reading
    waitForQueueSize(iterFutures, 0);
    
    LOG.info("{}: Parallel iteration completed - Total keys processed: {}", taskName, keyCounter.get());
  }

  @Override
  public void close() throws IOException {
    iteratorExecutor.shutdown();
    try {
      if (!iteratorExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
        iteratorExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      iteratorExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}



