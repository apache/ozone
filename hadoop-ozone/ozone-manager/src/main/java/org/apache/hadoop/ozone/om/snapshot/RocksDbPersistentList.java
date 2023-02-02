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
import java.util.Iterator;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 * Persistent list backed by RocksDB.
 */
public class RocksDbPersistentList<E> implements PersistentList<E> {

  private final RocksDB db;
  private final ColumnFamilyHandle columnFamilyHandle;
  private final CodecRegistry codecRegistry;
  private final Class<E> entryType;
  private int currentIndex;

  public RocksDbPersistentList(RocksDB db,
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
    try {
      byte[] rawKey = codecRegistry.asRawData(currentIndex++);
      byte[] rawValue = codecRegistry.asRawData(entry);
      db.put(columnFamilyHandle, rawKey, rawValue);
      return true;
    } catch (IOException | RocksDBException exception) {
      // TODO:: Fail gracefully.
      throw new RuntimeException(exception);
    }
  }

  @Override
  public boolean addAll(PersistentList<E> from) {
    from.iterator().forEachRemaining(this::add);
    return true;
  }

  @Override
  public E get(int index) {
    try {
      byte[] rawKey = codecRegistry.asRawData(index);
      byte[] rawValue = db.get(columnFamilyHandle, rawKey);
      return codecRegistry.asObject(rawValue, entryType);
    } catch (IOException | RocksDBException exception) {
      // TODO:: Fail gracefully.
      throw new RuntimeException(exception);
    }
  }

  @Override
  public Iterator<E> iterator() {
    RocksIterator rocksIterator = db.newIterator(columnFamilyHandle);
    rocksIterator.seekToFirst();

    return new Iterator<E>() {
      @Override
      public boolean hasNext() {
        return rocksIterator.isValid();
      }

      @Override
      public E next() {
        byte[] rawKey = rocksIterator.value();
        rocksIterator.next();
        try {
          return codecRegistry.asObject(rawKey, entryType);
        } catch (IOException exception) {
          // TODO:: Fail gracefully.
          throw new RuntimeException(exception);
        }
      }
    };
  }

  @Override
  public Iterator<E> iterator(int index) {
    byte[] target;
    try {
      target = codecRegistry.asRawData(index);
    } catch (IOException e) {
      // TODO:: Fail gracefully.
      throw new RuntimeException(e);
    }

    RocksIterator rocksIterator = db.newIterator(columnFamilyHandle);
    rocksIterator.seek(target);

    return new Iterator<E>() {
      @Override
      public boolean hasNext() {
        return rocksIterator.isValid();
      }

      @Override
      public E next() {
        byte[] rawKey = rocksIterator.value();
        rocksIterator.next();
        try {
          return codecRegistry.asObject(rawKey, entryType);
        } catch (IOException exception) {
          // TODO:: Fail gracefully.
          throw new RuntimeException(exception);
        }
      }
    };
  }
}
