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

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract {@link Table.KeyValueIterator} to iterate raw {@link Table.KeyValue}s.
 *
 * @param <RAW> the raw type.
 */
abstract class RDBStoreAbstractIterator<RAW>
    implements Table.KeyValueIterator<RAW, RAW> {

  private static final Logger LOG =
      LoggerFactory.getLogger(RDBStoreAbstractIterator.class);

  private final ManagedRocksIterator rocksDBIterator;
  private final RDBTable rocksDBTable;
  private Table.KeyValue<RAW, RAW> currentEntry;
  // This is for schemas that use a fixed-length
  // prefix for each key.
  private final RAW prefix;
  private final IteratorType type;
  private final ManagedReadOptions readOptions;
  private final AtomicBoolean isIteratorClosed = new AtomicBoolean(false);

  /**
   * Constructor for RDBStoreAbstractIterator.
   * Callers must ensure that the iterator is always obtained using try-with-resources
   * or always closed in a finally block to ensure accurate refcounting.
   */
  RDBStoreAbstractIterator(ManagedRocksIterator iterator, RDBTable table, RAW prefix, IteratorType type) {
    this(iterator, table, prefix, type, null);
  }

  RDBStoreAbstractIterator(ManagedRocksIterator iterator, RDBTable table,
      RAW prefix, IteratorType type, ManagedReadOptions readOptions) {
    this.rocksDBIterator = iterator;
    this.rocksDBTable = table;
    this.prefix = prefix;
    this.type = type;
    this.readOptions = readOptions;
  }

  static byte[] copyNonEmpty(byte[] prefix) {
    return prefix == null || prefix.length == 0 ? null
        : Arrays.copyOf(prefix, prefix.length);
  }

  static byte[] getNextHigherPrefix(byte[] prefix) {
    if (prefix == null || prefix.length == 0) {
      return null;
    }
    for (int i = prefix.length - 1; i >= 0; i--) {
      if ((prefix[i] & 0xFF) != 0xFF) {
        byte[] upperBound = Arrays.copyOf(prefix, i + 1);
        upperBound[i]++;
        return upperBound;
      }
    }
    return null;
  }

  static ManagedReadOptions newReadOptions(byte[] prefix, boolean fillCache) {
    final byte[] lowerBound =
        prefix == null || prefix.length == 0 ? null : prefix;
    final ManagedReadOptions readOptions = new ManagedReadOptions(
        lowerBound, getNextHigherPrefix(lowerBound));
    try {
      readOptions.setFillCache(fillCache);
      return readOptions;
    } catch (RuntimeException | Error e) {
      try {
        readOptions.close();
      } catch (RuntimeException | Error closeException) {
        e.addSuppressed(closeException);
      }
      throw e;
    }
  }

  IteratorType getType() {
    return type;
  }

  /** @return the key for the current entry. */
  abstract RAW key();

  /** @return the {@link Table.KeyValue} for the current entry. */
  abstract Table.KeyValue<RAW, RAW> getKeyValue();

  /** Seek to the given key. */
  abstract void seek0(RAW key);

  /** Delete the given key. */
  abstract void delete(RAW key) throws RocksDatabaseException;

  /** Does the given key start with the prefix? */
  abstract boolean startsWithPrefix(RAW key);

  final ManagedRocksIterator getRocksDBIterator() {
    return rocksDBIterator;
  }

  final RDBTable getRocksDBTable() {
    return rocksDBTable;
  }

  final RAW getPrefix() {
    return prefix;
  }

  @Override
  public final void forEachRemaining(
      Consumer<? super Table.KeyValue<RAW, RAW>> action) {
    while (hasNext()) {
      action.accept(next());
    }
  }

  private boolean isDbClosed() {
    return rocksDBTable != null && rocksDBTable.isClosed();
  }

  private void setCurrentEntry() {
    if (rocksDBIterator.get().isValid()) {
      currentEntry = getKeyValue();
    } else {
      currentEntry = null;
    }
  }

  @Override
  public final boolean hasNext() {
    if (isDbClosed()) {
      return false;
    }
    return rocksDBIterator.get().isValid() &&
        (prefix == null || startsWithPrefix(key()));
  }

  @Override
  public final Table.KeyValue<RAW, RAW> next() {
    setCurrentEntry();
    if (currentEntry != null) {
      rocksDBIterator.get().next();
      return currentEntry;
    }
    throw new NoSuchElementException("RocksDB Store has no more elements");
  }

  @Override
  public final void seekToFirst() {
    if (prefix == null) {
      rocksDBIterator.get().seekToFirst();
    } else {
      seek0(prefix);
    }
    setCurrentEntry();
  }

  @Override
  public final void seekToLast() {
    if (prefix == null) {
      rocksDBIterator.get().seekToLast();
    } else {
      throw new UnsupportedOperationException("seekToLast: prefix != null");
    }
    setCurrentEntry();
  }

  @Override
  public final Table.KeyValue<RAW, RAW> seek(RAW key) {
    // Prefix-bounded RocksDB iterators must not seek below their native
    // lower bound. RocksDB documents that case as undefined.
    seek0(key);
    setCurrentEntry();
    return currentEntry;
  }

  @Override
  public final void removeFromDB() throws RocksDatabaseException, CodecException {
    if (rocksDBTable == null) {
      throw new UnsupportedOperationException("remove");
    }
    if (currentEntry != null) {
      delete(currentEntry.getKey());
    } else {
      LOG.info("Failed to removeFromDB: currentEntry == null");
    }
  }

  @Override
  public void close() {
    if (isIteratorClosed.compareAndSet(false, true)) {
      try {
        rocksDBIterator.close();
      } finally {
        if (readOptions != null) {
          readOptions.close();
        }
      }
    }
  }
}
