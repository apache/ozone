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
 * Persistent set backed by RocksDB.
 */
public class RocksDbPersistentSet<E> implements PersistentSet<E> {
  private final ManagedRocksDB db;
  private final ColumnFamilyHandle columnFamilyHandle;
  private final CodecRegistry codecRegistry;
  private final Class<E> entryType;
  private final byte[] emptyByteArray = new byte[0];

  public RocksDbPersistentSet(ManagedRocksDB db,
                              ColumnFamilyHandle columnFamilyHandle,
                              CodecRegistry codecRegistry,
                              Class<E> entryType) {
    this.db = db;
    this.columnFamilyHandle = columnFamilyHandle;
    this.codecRegistry = codecRegistry;
    this.entryType = entryType;
  }

  @Override
  public void add(E entry) {
    SnapshotStorageException.wrap(
        "Failed to add set entry", () -> {
          byte[] rawKey = codecRegistry.asRawData(entry);
          byte[] rawValue = codecRegistry.asRawData(emptyByteArray);
          db.get().put(columnFamilyHandle, rawKey, rawValue);
        });
  }

  @Override
  public ClosableIterator<E> iterator() {
    ManagedRocksIterator managedRocksIterator =
        new ManagedRocksIterator(db.get().newIterator(columnFamilyHandle));
    managedRocksIterator.get().seekToFirst();

    return new ClosableIterator<E>() {
      @Override
      public boolean hasNext() {
        return managedRocksIterator.get().isValid();
      }

      @Override
      public E next() {
        byte[] rawKey = managedRocksIterator.get().key();
        managedRocksIterator.get().next();
        return SnapshotStorageException.wrap(
            "Failed to deserialize set entry",
            () -> codecRegistry.asObject(rawKey, entryType));
      }

      @Override
      public void close() {
        managedRocksIterator.close();
      }
    };
  }
}
