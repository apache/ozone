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
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.utils.db.RDBStoreAbstractIterator.AutoCloseableRawKeyValue;
import org.apache.ratis.util.ReferenceCountedObject;

/**
 * An abstract implementation of {@link Table.KeyValueSpliterator} designed
 * for iterating and splitting over raw key-value pairs retrieved via a
 * {@link TableIterator}.
 *
 * <p>This class manages asynchronous raw iterator resources and provides
 * functionality for converting raw key-value pairs into a structured
 * {@link Table.KeyValue} representation. It also allows controlled splitting
 * of the underlying iterator for parallel processing, while ensuring proper
 * resource handling and thread safety.
 *
 * @param <RAW>   The raw representation type of the key-value pair.
 * @param <KEY>   The type of key in the key-value pair.
 * @param <VALUE> The type of value in the key-value pair.
 */
abstract class RawSpliterator<RAW, KEY, VALUE> implements Table.KeyValueSpliterator<KEY, VALUE> {

  private ReferenceCountedObject<TableIterator<RAW, AutoCloseableRawKeyValue<RAW>>> rawIterator;
  private final KEY keyPrefix;
  private final KEY startKey;
  private final AtomicInteger maxNumberOfAdditionalSplits;
  private final Lock lock;
  private final AtomicReference<IOException> closeException = new AtomicReference<>();
  private boolean closed;
  private final boolean closeOnException;
  private boolean initialized;

  abstract Table.KeyValue<KEY, VALUE> convert(RawKeyValue<RAW> kv) throws IOException;

  abstract TableIterator<RAW, AutoCloseableRawKeyValue<RAW>> getRawIterator(
      KEY prefix, KEY start, int maxParallelism) throws IOException;

  RawSpliterator(KEY keyPrefix, KEY startKey, int maxParallelism, boolean closeOnException) {
    this.keyPrefix = keyPrefix;
    this.startKey = startKey;
    this.closeOnException = closeOnException;
    this.lock = new ReentrantLock();
    this.maxNumberOfAdditionalSplits = new AtomicInteger(maxParallelism);
    this.initialized = false;
    this.closed = false;
  }

  synchronized void initializeIterator() throws IOException {
    if (initialized) {
      return;
    }
    TableIterator<RAW, AutoCloseableRawKeyValue<RAW>> itr = getRawIterator(keyPrefix,
        startKey, maxNumberOfAdditionalSplits.decrementAndGet());
    try {
      this.rawIterator = ReferenceCountedObject.wrap(itr, () -> { },
          (completelyReleased) -> {
            if (completelyReleased) {
              closeRawIteratorWithLock();
            }
            this.maxNumberOfAdditionalSplits.incrementAndGet();
          });
      this.rawIterator.retain();
    } catch (Throwable e) {
      itr.close();
      throw e;
    }
    initialized = true;
  }

  @Override
  public boolean tryAdvance(Consumer<? super Table.KeyValue<KEY, VALUE>> action) {
    lock.lock();
    AutoCloseableRawKeyValue<RAW> kv;
    try {
      if (!closed && this.rawIterator.get().hasNext()) {
        kv = rawIterator.get().next();
      } else {
        closeRawIterator();
        return false;
      }
    } finally {
      lock.unlock();
    }
    try (AutoCloseableRawKeyValue<RAW> keyValue = kv) {
      if (keyValue != null) {
        action.accept(convert(keyValue));
        return true;
      }
      return false;
    } catch (Throwable e) {
      if (closeOnException) {
        closeRawIteratorWithLock();
      }
      throw new IllegalStateException("Failed next()", e);
    }
  }

  @Override
  public int characteristics() {
    return Spliterator.DISTINCT;
  }

  @Override
  public Spliterator<Table.KeyValue<KEY, VALUE>> trySplit() {
    int val = maxNumberOfAdditionalSplits.decrementAndGet();
    if (val >= 0) {
      try {
        this.rawIterator.retain();
      } catch (Exception e) {
        maxNumberOfAdditionalSplits.incrementAndGet();
        return null;
      }
      return this;
    } else {
      maxNumberOfAdditionalSplits.incrementAndGet();
    }
    return null;
  }

  private void closeRawIterator() {
    if (!closed) {
      try {
        closed = true;
        this.rawIterator.get().close();
      } catch (IOException e) {
        closeException.set(e);
      }
    }
  }

  private void closeRawIteratorWithLock() {
    if (!closed) {
      this.lock.lock();
      try {
        closeRawIterator();
      } finally {
        this.lock.unlock();
      }
    }
  }

  @Override
  public void close() throws IOException {
    this.rawIterator.release();
    if (closeException.get() != null) {
      throw closeException.get();
    }
  }

  @Override
  public long estimateSize() {
    return Long.MAX_VALUE;
  }
}
