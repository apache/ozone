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
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.ratis.util.ReferenceCountedObject;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract {@link TableIterator} to iterate raw {@link Table.KeyValue}s.
 *
 * @param <RAW> the raw type.
 */
abstract class RDBStoreAbstractIterator<RAW>
    implements TableIterator<RAW, UncheckedAutoCloseableSupplier<RawKeyValue<RAW>>> {

  private static final Logger LOG =
      LoggerFactory.getLogger(RDBStoreAbstractIterator.class);

  private final ManagedRocksIterator rocksDBIterator;
  private final RDBTable rocksDBTable;
  private ReferenceCountedObject<RawKeyValue<RAW>> currentEntry;
  private RawKeyValue<RAW> previousKeyValue;
  // This is for schemas that use a fixed-length
  // prefix for each key.
  private final RAW prefix;
  private boolean hasNext;
  private boolean closed;

  RDBStoreAbstractIterator(ManagedRocksIterator iterator, RDBTable table,
      RAW prefix) {
    this.rocksDBIterator = iterator;
    this.rocksDBTable = table;
    this.prefix = prefix;
    this.currentEntry = null;
    this.hasNext = false;
    this.closed = false;
    this.previousKeyValue = null;
  }

  /** @return the {@link Table.KeyValue} for the current entry. */
  abstract ReferenceCountedObject<RawKeyValue<RAW>> getKeyValue();

  /** Seek to the given key. */
  abstract void seek0(RAW key);

  /** Delete the given key. */
  abstract void delete(RAW key) throws IOException;

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
      Consumer<? super UncheckedAutoCloseableSupplier<RawKeyValue<RAW>>> action) {
    while (hasNext()) {
      UncheckedAutoCloseableSupplier<RawKeyValue<RAW>> entry = next();
      action.accept(entry);
    }
  }

  private void setCurrentEntry() {
    if (currentEntry != null) {
      currentEntry.release();
    }

    boolean isValid = !closed && rocksDBIterator.get().isValid();
    if (isValid) {
      currentEntry = getKeyValue();
      currentEntry.retain();
    } else {
      currentEntry = null;
    }
    setHasNext(isValid, currentEntry);
  }

  public void setHasNext(boolean isValid, ReferenceCountedObject<RawKeyValue<RAW>> entry) {
    this.hasNext = isValid && (prefix == null || startsWithPrefix(entry.get().getKey()));
  }

  @Override
  public final boolean hasNext() {
    return hasNext;
  }

  @Override
  public final UncheckedAutoCloseableSupplier<RawKeyValue<RAW>> next() {
    if (hasNext()) {
      UncheckedAutoCloseableSupplier<RawKeyValue<RAW>> entry = currentEntry.retainAndReleaseOnClose();
      this.previousKeyValue = entry.get();
      rocksDBIterator.get().next();
      setCurrentEntry();
      return entry;
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
  public final UncheckedAutoCloseableSupplier<RawKeyValue<RAW>> seek(RAW key) {
    seek0(key);
    setCurrentEntry();
    // Current entry should be only closed when the next() and thus closing the returned entry should be a noop.
    if (hasNext()) {
      return currentEntry.retainAndReleaseOnClose();
    }
    return null;
  }

  @Override
  public final void removeFromDB() throws IOException {
    if (rocksDBTable == null) {
      throw new UnsupportedOperationException("remove");
    }
    if (previousKeyValue != null) {
      delete(previousKeyValue.getKey());
    } else {
      LOG.info("Failed to removeFromDB: currentEntry == null");
    }
  }

  @Override
  public void close() {
    rocksDBIterator.close();
    closed = true;
    setCurrentEntry();
  }
}
