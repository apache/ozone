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

package org.apache.hadoop.ozone.om.snapshot.util;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.ozone.util.ClosableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TableMergeIterator is an implementation of an iterator that merges multiple table iterators
 * and filters the data based on the keys provided by another iterator.
 *
 * This iterator allows sequential traversal of all keys and their corresponding values
 * across multiple tables. For each key, it gathers the associated value from each table.
 * If a particular table does not have the entry for the given key, a null value is included in its place.
 *
 * This class is primarily designed to support efficient merging and filtering operations
 * across multiple key-value tables.
 *
 * @param <K> The type of keys, which must be comparable.
 * @param <V> The type of values.
 */
public class TableMergeIterator<K extends Comparable<K>, V> implements ClosableIterator<KeyValue<K, List<V>>> {

  private static final Logger LOG = LoggerFactory.getLogger(TableMergeIterator.class);

  private final Iterator<K> keysToFilter;
  private final Table.KeyValueIterator<K, V>[] itrs;
  private final List<KeyValue<K, V>> kvs;
  private final List<V> nextValues;

  public TableMergeIterator(Iterator<K> keysToFilter, K prefix, Table<K, V>... tables)
      throws RocksDatabaseException, CodecException {
    this.itrs = new Table.KeyValueIterator[tables.length];
    for (int i = 0; i < tables.length; i++) {
      this.itrs[i] = tables[i].iterator(prefix);
    }
    this.kvs = new ArrayList<>(Collections.nCopies(tables.length, null));
    this.nextValues = new ArrayList<>(Collections.nCopies(tables.length, null));
    this.keysToFilter = keysToFilter;
  }

  @Override
  public void close() {
    IOUtils.close(LOG, itrs);
  }

  @Override
  public boolean hasNext() {
    return keysToFilter.hasNext();
  }

  private V updateAndGetValueAtIndex(K key, int index) {
    Table.KeyValueIterator<K, V> itr = itrs[index];
    if (itr.hasNext() && (kvs.get(index) == null || key.compareTo(kvs.get(index).getKey()) > 0)) {
      try {
        itr.seek(key);
      } catch (RocksDatabaseException | CodecException e) {
        throw new UncheckedIOException("Error while seeking to key " + key, e);
      }
      Table.KeyValue<K, V> kv = itr.hasNext() ? itr.next() : null;
      kvs.set(index, kv);
    }
    // Return the value only if the key matches & not null.
    return key == null || kvs.get(index) == null || kvs.get(index).getKey().compareTo(key) != 0 ? null :
        kvs.get(index).getValue();
  }

  /**
   * Gets the next key and corresponding values from all tables. Note: the values array returned is not Immutable and
   * will be modified on the next call to next().
   * @return KeyValue containing the next key and corresponding values from all tables. The value would be null if
   * the key is not present in that table otherwise the value from that table corresponding to the next key.
   */
  @Override
  public KeyValue<K, List<V>> next() {
    K nextKey = keysToFilter.next();
    nextValues.clear();
    for (int idx = 0; idx < itrs.length; idx++) {
      nextValues.add(updateAndGetValueAtIndex(nextKey, idx));
    }
    return Table.newKeyValue(nextKey, nextValues);
  }
}
