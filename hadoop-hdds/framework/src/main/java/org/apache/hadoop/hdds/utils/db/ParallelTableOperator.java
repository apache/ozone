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

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;

/**
 * Class to iterate through a table in parallel by breaking table into multiple iterators for RDB store.
 */
public abstract class ParallelTableOperator<TABLE extends Table<K, V>, K, V> {
  private final TABLE table;
  private final Codec<K> keyCodec;
  private final Comparator<K> comparator;
  private final ThrottledThreadpoolExecutor executor;

  public ParallelTableOperator(ThrottledThreadpoolExecutor throttledThreadpoolExecutor,
                               TABLE table, Codec<K> keyCodec) {
    this.executor = throttledThreadpoolExecutor;
    this.table = table;
    this.keyCodec = keyCodec;
    this.comparator = keyCodec.comparator();
  }

  /**
   * Provide all the bounds that fall in range [startKey, endKey] in a sorted list to facilitate efficiently
   * splitting table iteration of keys into multiple parallel iterators of the table.
   */
  protected abstract List<K> getBounds(K startKey, K endKey) throws IOException;

  @SuppressWarnings("parameternumber")
  private <THROWABLE extends Throwable> CompletableFuture<Void> submit(
      CheckedFunction<Table.KeyValue<K, V>, Void, THROWABLE> keyOperation, K beg, K end,
      AtomicLong keyCounter, AtomicLong prevLogCounter, long logCountThreshold, Logger log,
      AtomicBoolean cancelled) throws InterruptedException {
    return executor.submit(() -> {
      try (TableIterator<K, ? extends Table.KeyValue<K, V>> iter  = table.iterator()) {
        if (beg != null) {
          iter.seek(beg);
        } else {
          iter.seekToFirst();
        }
        while (iter.hasNext() && !cancelled.get()) {
          Table.KeyValue<K, V> kv = iter.next();
          if (end == null || Objects.compare(kv.getKey(), end, comparator) < 0) {
            keyOperation.apply(kv);
            keyCounter.incrementAndGet();
            if (keyCounter.get() - prevLogCounter.get() > logCountThreshold) {
              log.info("Iterated through table : {} {} keys while performing task.", table.getName(),
                  keyCounter.get());
              prevLogCounter.set(keyCounter.get());
            }
          } else {
            break;
          }
        }
      }
    });
  }

  public <THROWABLE extends Throwable> void performTaskOnTableVals(K startKey, K endKey,
                                     CheckedFunction<Table.KeyValue<K, V>, Void, THROWABLE> keyOperation,
                                     Logger log, int logPercentageThreshold)
      throws ExecutionException, InterruptedException, IOException, THROWABLE {
    List<K> bounds;
    long logCountThreshold = Math.max((table.getEstimatedKeyCount() * logPercentageThreshold) / 100, 1L);
    try {
      bounds = getBounds(startKey, endKey);
    } catch (IOException e) {
      log.warn("Error while getting bounds Table: {} startKey: {}, endKey: {}", table.getName(), startKey, endKey, e);
      bounds = Arrays.asList(startKey, endKey);
    }
    AtomicLong keyCounter = new AtomicLong();
    AtomicLong prevLogCounter = new AtomicLong();
    CompletableFuture<Void> iterFutures = CompletableFuture.completedFuture(null);
    AtomicBoolean cancelled = new AtomicBoolean(false);
    for (int idx = 1; idx < bounds.size(); idx++) {
      K beg = bounds.get(idx - 1);
      K end = bounds.get(idx);
      if (cancelled.get()) {
        break;
      }
      CompletableFuture<Void> future = submit(keyOperation, beg, end, keyCounter, prevLogCounter,
          logCountThreshold, log, cancelled);
      future.exceptionally((e -> {
        cancelled.set(true);
        return null;
      }));
      iterFutures = iterFutures.thenCombine(future, (v1, v2) -> null);
    }
    iterFutures.get();
  }

  protected TABLE getTable() {
    return table;
  }

  public Codec<K> getKeyCodec() {
    return keyCodec;
  }

  public Comparator<K> getComparator() {
    return comparator;
  }
}
