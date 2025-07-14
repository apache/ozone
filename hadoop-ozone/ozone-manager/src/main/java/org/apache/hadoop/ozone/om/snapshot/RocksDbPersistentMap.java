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

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSlice;
import org.apache.hadoop.ozone.util.ClosableIterator;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

/**
 * Persistent map backed by RocksDB.
 */
public class RocksDbPersistentMap<K, V> implements PersistentMap<K, V> {
  private final ManagedRocksDB db;
  private final ColumnFamilyHandle columnFamilyHandle;
  private final CodecRegistry codecRegistry;
  private final Class<K> keyType;
  private final Class<V> valueType;

  public RocksDbPersistentMap(@Nonnull ManagedRocksDB db,
                              @Nonnull ColumnFamilyHandle columnFamilyHandle,
                              @Nonnull CodecRegistry codecRegistry,
                              @Nonnull Class<K> keyType,
                              @Nonnull Class<V> valueType) {
    this.db = db;
    this.columnFamilyHandle = columnFamilyHandle;
    this.codecRegistry = codecRegistry;
    this.keyType = keyType;
    this.valueType = valueType;
  }

  @Override
  public V get(K key) {
    try {
      byte[] rawKey = codecRegistry.asRawData(key);
      byte[] rawValue = db.get().get(columnFamilyHandle, rawKey);
      return codecRegistry.asObject(rawValue, valueType);
    } catch (IOException | RocksDBException exception) {
      // TODO: [SNAPSHOT] Fail gracefully.
      throw new RuntimeException(exception);
    }
  }

  @Override
  public void put(K key, V value) {
    try {
      byte[] rawKey = codecRegistry.asRawData(key);
      byte[] rawValue = codecRegistry.asRawData(value);
      db.get().put(columnFamilyHandle, rawKey, rawValue);
    } catch (IOException | RocksDBException exception) {
      // TODO: [SNAPSHOT] Fail gracefully.
      throw new RuntimeException(exception);
    }
  }

  @Override
  public void remove(K key) {
    try {
      byte[] rawKey = codecRegistry.asRawData(key);
      db.get().delete(columnFamilyHandle, rawKey);
    } catch (IOException | RocksDBException exception) {
      // TODO: [SNAPSHOT] Fail gracefully.
      throw new RuntimeException(exception);
    }
  }

  @Override
  public ClosableIterator<Map.Entry<K, V>> iterator(Optional<K> lowerBound,
                                                    Optional<K> upperBound) {
    final ManagedReadOptions readOptions = new ManagedReadOptions();
    ManagedRocksIterator iterator;
    final ManagedSlice lowerBoundSlice;
    final ManagedSlice upperBoundSlice;
    try {
      if (lowerBound.isPresent()) {
        lowerBoundSlice = new ManagedSlice(
            codecRegistry.asRawData(lowerBound.get()));
        readOptions.setIterateLowerBound(lowerBoundSlice);
      } else {
        lowerBoundSlice = null;
      }

      if (upperBound.isPresent()) {
        upperBoundSlice = new ManagedSlice(
            codecRegistry.asRawData(upperBound.get()));
        readOptions.setIterateUpperBound(upperBoundSlice);
      } else {
        upperBoundSlice = null;
      }
    } catch (IOException exception) {
      // TODO: [SNAPSHOT] Fail gracefully.
      throw new RuntimeException(exception);
    }

    iterator = ManagedRocksIterator.managed(
        db.get().newIterator(columnFamilyHandle, readOptions));

    iterator.get().seekToFirst();

    return new ClosableIterator<Map.Entry<K, V>>() {
      @Override
      public boolean hasNext() {
        return iterator.get().isValid();
      }

      @Override
      public Map.Entry<K, V> next() {
        if (!hasNext()) {
          throw new NoSuchElementException("No more elements in the map.");
        }
        K key;
        V value;

        try {
          key = codecRegistry.asObject(iterator.get().key(), keyType);
          value = codecRegistry.asObject(iterator.get().value(), valueType);
        } catch (IOException exception) {
          // TODO: [SNAPSHOT] Fail gracefully.
          throw new RuntimeException(exception);
        }

        // Move iterator to the next.
        iterator.get().next();

        return new Map.Entry<K, V>() {
          @Override
          public K getKey() {
            return key;
          }

          @Override
          public V getValue() {
            return value;
          }

          @Override
          public V setValue(V value) {
            throw new IllegalStateException("setValue is not implemented.");
          }
        };
      }

      @Override
      public void close() {
        iterator.close();
        readOptions.close();
        if (upperBoundSlice != null) {
          upperBoundSlice.close();
        }
        if (lowerBoundSlice != null) {
          lowerBoundSlice.close();
        }
      }
    };
  }
}
