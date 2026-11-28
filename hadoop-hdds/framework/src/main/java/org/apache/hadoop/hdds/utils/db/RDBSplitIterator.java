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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSnapshot;

/**
 * Iterates one {@link KeyRange} of a column family as of a snapshot.
 * The range is pushed down to RocksDB as native iterate lower and upper bounds
 * (see {@link ManagedReadOptions}), with fillCache disabled so a full scan
 * does not evict the hot block cache. Not thread-safe: each split is owned by
 * a single thread, so there is no per-element synchronization.
 */
public final class RDBSplitIterator
    implements Iterator<Table.KeyValue<byte[], byte[]>>, AutoCloseable {
  private final KeyRange range;
  private final IteratorType type;
  private final ManagedReadOptions readOptions;
  private final ManagedRocksIterator iterator;
  private final AtomicBoolean closed = new AtomicBoolean();

  RDBSplitIterator(RocksDatabase db, RocksDatabase.ColumnFamily family,
      ManagedSnapshot snapshot, KeyRange range) throws RocksDatabaseException {
    this(db, family, snapshot, range, IteratorType.KEY_AND_VALUE);
  }

  RDBSplitIterator(RocksDatabase db, RocksDatabase.ColumnFamily family,
      ManagedSnapshot snapshot, KeyRange range, IteratorType type)
      throws RocksDatabaseException {
    this.range = Objects.requireNonNull(range, "range == null");
    this.type = Objects.requireNonNull(type, "type == null");
    final ManagedReadOptions options =
        new ManagedReadOptions(range.getStart(), range.getEnd());
    ManagedRocksIterator rocksIterator = null;
    try {
      options.setFillCache(false);
      options.setTotalOrderSeek(true);
      if (snapshot != null) {
        options.setSnapshot(snapshot);
      }
      rocksIterator = db.newIterator(family, options);
      rocksIterator.get().seekToFirst();
    } catch (RocksDatabaseException | RuntimeException | Error t) {
      IOUtils.closeAndSuppress(t, rocksIterator, options);
      throw t;
    }
    this.readOptions = options;
    this.iterator = rocksIterator;
  }

  public KeyRange getRange() {
    return range;
  }

  @Override
  public boolean hasNext() {
    return !closed.get() && iterator.get().isValid();
  }

  @Override
  public Table.KeyValue<byte[], byte[]> next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more elements in " + range);
    }
    final byte[] key = type.readKey() ? iterator.get().key() : null;
    final byte[] value = type.readValue() ? iterator.get().value() : null;
    final Table.KeyValue<byte[], byte[]> kv = value == null
        ? Table.newKeyValue(key, null)
        : Table.newKeyValue(key, value, value.length);
    iterator.get().next();
    return kv;
  }

  /** Check iterator status, e.g. for I/O errors hidden by isValid()=false. */
  void checkStatus() throws RocksDatabaseException {
    RocksDatabase.checkStatus(iterator, range);
  }

  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      try {
        iterator.close();
      } finally {
        readOptions.close();
      }
    }
  }
}
