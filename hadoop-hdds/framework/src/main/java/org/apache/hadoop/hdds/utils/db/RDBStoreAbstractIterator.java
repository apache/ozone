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

import com.google.common.primitives.UnsignedBytes;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract {@link Table.KeyValueIterator} to iterate raw {@link Table.KeyValue}s.
 * NOTE: This class only works with RocksDB when comparator is set to Rocksdb's ByteWiseComparator.
 * @param <RAW> the raw type.
 */
abstract class RDBStoreAbstractIterator<RAW, KV extends Table.KeyValue<RAW, RAW>> implements TableIterator<RAW, KV> {

  private static final Logger LOG =
      LoggerFactory.getLogger(RDBStoreAbstractIterator.class);

  private final ManagedReadOptions readOptions;
  private final ManagedRocksIterator rocksDBIterator;
  private final RDBTable rocksDBTable;
  private KV currentEntry;

  private final IteratorType type;

  RDBStoreAbstractIterator(
      CheckedFunction<ManagedReadOptions, ManagedRocksIterator, RocksDatabaseException> itrSupplier, RDBTable table,
      byte[] prefix, IteratorType type) throws RocksDatabaseException {
    this.readOptions = new ManagedReadOptions(false, prefix, getNextHigherPrefix(prefix));
    this.rocksDBIterator = itrSupplier.apply(readOptions);
    this.rocksDBTable = table;
    this.type = type;
  }

  private byte[] getNextHigherPrefix(byte[] prefix) {
    if (prefix == null) {
      return null;
    }
    for (int i = prefix.length - 1; i >= 0; i--) {
      if (UnsignedBytes.compare(prefix[i], UnsignedBytes.MAX_VALUE) != 0) {
        byte[] nextHigher = Arrays.copyOf(prefix, i + 1);
        nextHigher[i] = (byte) (prefix[i] + 1);
        return nextHigher;
      }
    }
    // No higher prefix exists since all bytes are MAX_VALUE.
    return null;
  }

  IteratorType getType() {
    return type;
  }

  /** @return the {@link Table.KeyValue} for the current entry. */
  abstract KV getKeyValue();

  /** Seek to the given key. */
  abstract void seek0(RAW key);

  /** Delete the given key. */
  abstract void delete(RAW key) throws RocksDatabaseException;

  final ManagedRocksIterator getRocksDBIterator() {
    return rocksDBIterator;
  }

  final RDBTable getRocksDBTable() {
    return rocksDBTable;
  }

  @Override
  public final void forEachRemaining(
      Consumer<? super KV> action) {
    while (hasNext()) {
      action.accept(next());
    }
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
    return rocksDBIterator.get().isValid();
  }

  @Override
  public final KV next() {
    setCurrentEntry();
    if (currentEntry != null) {
      rocksDBIterator.get().next();
      return currentEntry;
    }
    throw new NoSuchElementException("RocksDB Store has no more elements");
  }

  @Override
  public final void seekToFirst() {
    rocksDBIterator.get().seekToFirst();
  }

  @Override
  public final void seekToLast() {
    rocksDBIterator.get().seekToLast();
  }

  @Override
  public final KV seek(RAW key) {
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
    rocksDBIterator.close();
    readOptions.close();
  }
}
