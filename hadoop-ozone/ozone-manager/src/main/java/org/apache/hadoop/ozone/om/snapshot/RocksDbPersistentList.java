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

package org.apache.hadoop.ozone.om.snapshot;

import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.ozone.util.ClosableIterator;
import org.rocksdb.ColumnFamilyHandle;

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
  public boolean add(E entry) {
    return SnapshotStorageException.wrap(
        "Failed to append list entry", () -> {
          byte[] rawKey = codecRegistry.asRawData(currentIndex++);
          byte[] rawValue = codecRegistry.asRawData(entry);
          db.get().put(columnFamilyHandle, rawKey, rawValue);
          return true;
        });
  }

  @Override
  public boolean addAll(PersistentList<E> from) {
    try (ClosableIterator<E> iterator = from.iterator()) {
      iterator.forEachRemaining(this::add);
    }
    return true;
  }

  @Override
  public E get(int index) {
    return SnapshotStorageException.wrap(
        "Failed to read list entry", () -> {
          byte[] rawKey = codecRegistry.asRawData(index);
          byte[] rawValue = db.get().get(columnFamilyHandle, rawKey);
          return codecRegistry.asObject(rawValue, entryType);
        });
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
        return SnapshotStorageException.wrap(
            "Failed to deserialize list entry",
            () -> codecRegistry.asObject(rawKey, entryType));
      }

      @Override
      public void close() {
        managedRocksIterator.close();
      }
    };
  }
}
