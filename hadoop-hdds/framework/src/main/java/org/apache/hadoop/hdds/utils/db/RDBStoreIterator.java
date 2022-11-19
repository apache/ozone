/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.utils.db;

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocksDB store iterator.
 */
public class RDBStoreIterator
    implements TableIterator<byte[], ByteArrayKeyValue> {

  private static final Logger LOG =
      LoggerFactory.getLogger(RDBStoreIterator.class);

  private final ManagedRocksIterator rocksDBIterator;
  private RDBTable rocksDBTable;
  private ByteArrayKeyValue currentEntry;
  // This is for schemas that use a fixed-length
  // prefix for each key.
  private byte[] prefix;

  public RDBStoreIterator(ManagedRocksIterator iterator) {
    this(iterator, null);
  }

  public RDBStoreIterator(ManagedRocksIterator iterator, RDBTable table) {
    this(iterator, table, null);
  }

  public RDBStoreIterator(ManagedRocksIterator iterator, RDBTable table,
      byte[] prefix) {
    this.rocksDBIterator = iterator;
    this.rocksDBTable = table;
    if (prefix != null) {
      this.prefix = Arrays.copyOf(prefix, prefix.length);
    }
    seekToFirst();
  }

  @Override
  public void forEachRemaining(
      Consumer<? super ByteArrayKeyValue> action) {
    while (hasNext()) {
      action.accept(next());
    }
  }

  private void setCurrentEntry() {
    if (rocksDBIterator.get().isValid()) {
      currentEntry = ByteArrayKeyValue.create(rocksDBIterator.get().key(),
          rocksDBIterator.get().value());
    } else {
      currentEntry = null;
    }
  }

  @Override
  public boolean hasNext() {
    return rocksDBIterator.get().isValid() &&
        (prefix == null || startsWith(prefix, rocksDBIterator.get().key()));
  }

  @Override
  public ByteArrayKeyValue next() {
    setCurrentEntry();
    if (currentEntry != null) {
      rocksDBIterator.get().next();
      return currentEntry;
    }
    throw new NoSuchElementException("RocksDB Store has no more elements");
  }

  @Override
  public void seekToFirst() {
    if (prefix == null) {
      rocksDBIterator.get().seekToFirst();
    } else {
      rocksDBIterator.get().seek(prefix);
    }
    setCurrentEntry();
  }

  @Override
  public void seekToLast() {
    if (prefix == null) {
      rocksDBIterator.get().seekToLast();
    } else {
      throw new UnsupportedOperationException("seekToLast");
    }
    setCurrentEntry();
  }

  @Override
  public ByteArrayKeyValue seek(byte[] key) {
    rocksDBIterator.get().seek(key);
    setCurrentEntry();
    return currentEntry;
  }

  @Override
  public void removeFromDB() throws IOException {
    if (rocksDBTable == null) {
      throw new UnsupportedOperationException("remove");
    }
    if (currentEntry != null) {
      rocksDBTable.delete(currentEntry.getKey());
    } else {
      LOG.info("Unable to delete currentEntry as it does not exist.");
    }
  }

  @Override
  public void close() throws IOException {
    rocksDBIterator.close();
  }

  private static boolean startsWith(byte[] prefix, byte[] value) {
    if (prefix == null) {
      return true;
    }
    if (value == null) {
      return false;
    }

    int length = prefix.length;
    if (value.length < length) {
      return false;
    }

    for (int i = 0; i < length; i++) {
      if (value[i] != prefix[i]) {
        return false;
      }
    }
    return true;
  }
}
