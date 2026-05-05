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

import java.io.Closeable;
import java.util.Iterator;
import java.util.function.Function;
import org.apache.hadoop.hdds.utils.db.Table.KeyValueIterator;

/**
 * To iterate a {@link Table}.
 *
 * @param <KEY> The key type to support {@link #seek(Object)}.
 * @param <T> The type to be iterated.
 */
public interface TableIterator<KEY, T> extends Iterator<T>, Closeable {
  @Override
  void close() throws RocksDatabaseException;

  /**
   * seek to first entry.
   */
  void seekToFirst();

  /**
   * seek to last entry.
   */
  void seekToLast();

  /**
   * Seek to the specific key.
   *
   * @param key - Bytes that represent the key.
   * @return VALUE.
   */
  T seek(KEY key) throws RocksDatabaseException, CodecException;

  /**
   * Remove the actual value of the iterator from the database table on
   * which the iterator is working on.
   */
  void removeFromDB() throws RocksDatabaseException, CodecException;

  /**
   * Convert the given {@link KeyValueIterator} to a {@link TableIterator} using the given converter.
   *
   * @param <K> The key type of both the input and the output iterators
   * @param <INPUT> The value type of the input iterator
   * @param <OUTPUT> The value type of the output iterator
   */
  static <K, INPUT, OUTPUT> TableIterator<K, OUTPUT> convert(KeyValueIterator<K, INPUT> i,
      Function<Table.KeyValue<K, INPUT>, OUTPUT> converter) throws RocksDatabaseException, CodecException {
    return new TableIterator<K, OUTPUT>() {
      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public OUTPUT next() {
        return converter.apply(i.next());
      }

      @Override
      public void close() throws RocksDatabaseException {
        i.close();
      }

      @Override
      public void seekToFirst() {
        i.seekToFirst();
      }

      @Override
      public void seekToLast() {
        i.seekToLast();
      }

      @Override
      public OUTPUT seek(K key) throws RocksDatabaseException, CodecException {
        return converter.apply(i.seek(key));
      }

      @Override
      public void removeFromDB() throws RocksDatabaseException, CodecException {
        i.removeFromDB();
      }
    };
  }
}
