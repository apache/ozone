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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
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
  private final ExecutorService iteratorExecutor;
  private final ExecutorService valueExecutors;
  private final int maxNumberOfVals;
  private final OMMetadataManager metadataManager;
  private final int maxIteratorTasks;
  private final int maxWorkerTasks;
  private final long logCountThreshold;

  private static final Logger LOG = LoggerFactory.getLogger(ParallelTableIteratorOperation.class);
  public ParallelTableIteratorOperation(OMMetadataManager metadataManager, Table<K, V> table, Codec<K> keyCodec,
                                        int iteratorCount, int workerCount, int maxNumberOfValsInMemory,
                                        long logThreshold) {
    this.table = table;
    this.keyCodec = keyCodec;
    this.metadataManager = metadataManager;
    this.maxIteratorTasks = 2 * iteratorCount;
    this.maxWorkerTasks = workerCount * 2;
    this.iteratorExecutor = new ThreadPoolExecutor(iteratorCount, iteratorCount, 1, TimeUnit.MINUTES,
                    new ArrayBlockingQueue<>(iteratorCount * 2),
                    new ThreadPoolExecutor.CallerRunsPolicy());
    this.valueExecutors = new ThreadPoolExecutor(workerCount, workerCount, 1, TimeUnit.MINUTES,
            new ArrayBlockingQueue<>(workerCount * 2),
            new ThreadPoolExecutor.CallerRunsPolicy());
    this.maxNumberOfVals = Math.max(10, maxNumberOfValsInMemory / (workerCount));
    this.logCountThreshold = logThreshold;
  }


  private List<K> getBounds(K startKey, K endKey) throws IOException {
    RDBStore store = (RDBStore) this.metadataManager.getStore();
    List<LiveFileMetaData> sstFiles = store.getDb().getSstFileList();
    Set<K> keys = new HashSet<>();
    String tableName = table.getName();
    byte[] tableNameBytes = tableName.getBytes(StandardCharsets.UTF_8);
    for (LiveFileMetaData sstFile : sstFiles) {
      // Filter SST files by column family to get bounds only for this specific table
      if (Arrays.equals(sstFile.columnFamilyName(), tableNameBytes)) {
        keys.add(this.keyCodec.fromPersistedFormat(sstFile.smallestKey()));
        keys.add(this.keyCodec.fromPersistedFormat(sstFile.largestKey()));
      }
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

  public void performTaskOnTableVals(String taskName, K startKey, K endKey,
      Function<Table.KeyValue<K, V>, Void> keyOperation) throws IOException, ExecutionException, InterruptedException {
    List<K> bounds = getBounds(startKey, endKey);
    
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
    
    Queue<Future<?>> iterFutures = new LinkedList<>();
    Queue<Future<?>> workerFutures = new ConcurrentLinkedQueue<>();
    AtomicLong keyCounter = new AtomicLong();
    AtomicLong prevLogCounter = new AtomicLong();
    for (int idx = 1; idx < bounds.size(); idx++) {
      K beg = bounds.get(idx - 1);
      K end = bounds.get(idx);
      boolean inclusive = idx == bounds.size() - 1;
      waitForQueueSize(iterFutures, maxIteratorTasks - 1);
      iterFutures.add(iteratorExecutor.submit(() -> {
        try (TableIterator<K, ? extends Table.KeyValue<K, V>> iter  = table.iterator()) {
          iter.seek(beg);
          while (iter.hasNext()) {
            List<Table.KeyValue<K, V>> keyValues = new ArrayList<>();
            boolean reachedEnd = false;
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
              
              if (withinBounds) {
                keyValues.add(kv);
              } else {
                reachedEnd = true;
                break;
              }
              if (keyValues.size() >= maxNumberOfVals) {
                break;
              }
            }
            if (!keyValues.isEmpty()) {
              waitForQueueSize(workerFutures, maxWorkerTasks - 10);
              workerFutures.add(valueExecutors.submit(() -> {
                for (Table.KeyValue<K, V> kv : keyValues) {
                  keyOperation.apply(kv);
                }
                keyCounter.addAndGet(keyValues.size());
                if (keyCounter.get() - prevLogCounter.get() > logCountThreshold) {
                  synchronized (keyCounter) {
                    if (keyCounter.get() - prevLogCounter.get() > logCountThreshold) {
                      long cnt = keyCounter.get();
                      LOG.info("Iterated through {} keys while performing task: {}", keyCounter.get(), taskName);
                      prevLogCounter.set(cnt);
                    }
                  }
                }
              }));
            }
            if (reachedEnd) {
              break;
            }
          }
        } catch (IOException | ExecutionException | InterruptedException e) {
          throw new RuntimeException(e);
        }
      }));
    }
    waitForQueueSize(iterFutures, 0);
    waitForQueueSize(workerFutures, 0);
    
    // Log final stats
    LOG.info("{}: Parallel iteration completed - Total keys processed: {}", taskName, keyCounter.get());
  }

  @Override
  public void close() throws IOException {
    iteratorExecutor.shutdown();
    valueExecutors.shutdown();
    try {
      if (!iteratorExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
        iteratorExecutor.shutdownNow();
      }
      if (!valueExecutors.awaitTermination(60, TimeUnit.SECONDS)) {
        valueExecutors.shutdownNow();
      }
    } catch (InterruptedException e) {
      iteratorExecutor.shutdownNow();
      valueExecutors.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}



