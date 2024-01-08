/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.snapshot;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.util.ClosableIterator;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

/**
 * Persistent list backed by RocksDB.
 */
public class RocksDbPersistentList<E> implements PersistentList<E> {

  private final ManagedRocksDB db;
  private final ColumnFamilyHandle columnFamilyHandle;
  private final CodecRegistry codecRegistry;
  private final Class<E> entryType;
  private int currentIndex;

  public RocksDbPersistentList(ManagedRocksDB db,
                               ColumnFamilyHandle columnFamilyHandle,
                               CodecRegistry codecRegistry,
                               Class<E> entryType) {
    this.db = db;
    this.columnFamilyHandle = columnFamilyHandle;
    this.codecRegistry = codecRegistry;
    this.entryType = entryType;
    this.currentIndex = 0;
  }

  @Override
  public boolean add(E entry) throws IOException {
    byte[] rawKey = codecRegistry.asRawData(currentIndex++);
    byte[] rawValue = codecRegistry.asRawData(entry);
    try {
      db.get().put(columnFamilyHandle, rawKey, rawValue);
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
    return true;
  }

  @Override
  public boolean addAll(PersistentList<E> from) throws IOException {
    try (ClosableIterator<E> iterator = from.iterator()) {
      try {
        while (iterator.hasNext()) {
          add(iterator.next());
        }
      } catch (UncheckedIOException ioException) {
        throw ioException.getCause();
      }
    }
    return true;
  }

  @Override
  public E get(int index) throws IOException {
    byte[] rawKey = codecRegistry.asRawData(index);
    byte[] rawValue = null;
    try {
      rawValue = db.get().get(columnFamilyHandle, rawKey);
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
    return codecRegistry.asObject(rawValue, entryType);
  }

  @Override
  public ClosableIterator<E> iterator() {
    ManagedRocksIterator managedRocksIterator
        = new ManagedRocksIterator(db.get().newIterator(columnFamilyHandle));
    managedRocksIterator.get().seekToFirst();

    return new ClosableIterator<E>() {
      @Override
      public boolean hasNext() {
        return managedRocksIterator.get().isValid();
      }

      @Override
      public E next() {
        byte[] rawKey = managedRocksIterator.get().value();
        managedRocksIterator.get().next();
        try {
          return codecRegistry.asObject(rawKey, entryType);
        } catch (IOException exception) {
          // TODO: [SNAPSHOT] Fail gracefully.
          throw new UncheckedIOException(exception);
        }
      }

      @Override
      public void close() {
        managedRocksIterator.close();
      }
    };
  }
}
