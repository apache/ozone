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
 * A {@link Table.KeyValueIterator} backed by a raw iterator.
 *
 * @param <RAW> The raw type.
 */
abstract class RawSpliterator<RAW, KEY, VALUE> implements Table.KeyValueSpliterator<KEY, VALUE> {

  private final ReferenceCountedObject<TableIterator<RAW, AutoCloseableRawKeyValue<RAW>>> rawIterator;
  private final AtomicInteger maxNumberOfAdditionalSplits;
  private final Lock lock;
  private final AtomicReference<IOException> closeException = new AtomicReference<>();

  abstract Table.KeyValue<KEY, VALUE> convert(RawKeyValue<RAW> kv) throws IOException;

  abstract TableIterator<RAW, AutoCloseableRawKeyValue<RAW>> getRawIterator(
      KEY prefix, KEY startKey, int maxParallelism) throws IOException;

  RawSpliterator(KEY prefix, KEY startKey, int maxParallelism) throws IOException {
    TableIterator<RAW, AutoCloseableRawKeyValue<RAW>> itr = getRawIterator(prefix,
        startKey, maxParallelism);
    this.lock = new ReentrantLock();
    this.maxNumberOfAdditionalSplits = new AtomicInteger(maxParallelism - 1);
    this.rawIterator = ReferenceCountedObject.wrap(itr, () -> { },
        (completelyReleased) -> {
          if (completelyReleased) {
            lock.lock();
            try {
              itr.close();
            } catch (IOException e) {
              closeException.set(e);
            } finally {
              lock.unlock();
            }
          }
          this.maxNumberOfAdditionalSplits.incrementAndGet();
        });
    this.rawIterator.retain();
  }

  @Override
  public boolean tryAdvance(Consumer<? super Table.KeyValue<KEY, VALUE>> action) {
    lock.lock();
    AutoCloseableRawKeyValue<RAW> kv = null;
    try {
      if (this.rawIterator.get().hasNext()) {
        kv = rawIterator.get().next();
      }
    } finally {
      lock.unlock();
    }
    try {
      if (kv != null) {
        action.accept(convert(kv));
        return true;
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed next()", e);
    } finally {
      if (kv != null) {
        kv.close();
      }
    }
    return false;
  }

  @Override
  public int characteristics() {
    return Spliterator.DISTINCT;
  }

  @Override
  public Spliterator<Table.KeyValue<KEY, VALUE>> trySplit() {
    int val = maxNumberOfAdditionalSplits.decrementAndGet();
    if (val >= 1) {
      try {
        this.rawIterator.retain();
      } catch (Exception e) {
        maxNumberOfAdditionalSplits.incrementAndGet();
        return null;
      }
      return this;
    }
    return null;
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
