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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSnapshot;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.RocksDBException;

/**
 * Class to iterate through a table in parallel by breaking table into multiple iterators for RDB store.
 */
public class RDBSplitTableIteratorOp<K, V> extends SplitTableIteratorOp<BaseRDBTable<K, V>, K, V> implements Closeable {

  private final ManagedSnapshot snapshot;

  public RDBSplitTableIteratorOp(
      ThrottledThreadpoolExecutor throttledThreadpoolExecutor,
      BaseRDBTable<K, V> table, Codec<K> keyCodec) throws IOException {
    super(throttledThreadpoolExecutor, table, keyCodec);
    this.snapshot = getTable().takeTableSnapshot();
  }

  @Override
  protected List<K> getBounds(K startKey, K endKey) throws IOException {
    Set<K> keys = new HashSet<>();
    for (LiveFileMetaData sstFile : this.getTable().getTableSstFiles()) {
      keys.add(this.getKeyCodec().fromPersistedFormat(sstFile.smallestKey()));
      keys.add(this.getKeyCodec().fromPersistedFormat(sstFile.largestKey()));
    }
    List<K> boundKeys = new ArrayList<>();
    boundKeys.add(startKey);
    boundKeys.addAll(keys.stream().sorted().filter(Objects::nonNull)
            .filter(key -> startKey == null || getComparator().compare(key, startKey) > 0)
            .filter(key -> endKey == null || getComparator().compare(endKey, key) > 0)
            .collect(Collectors.toList()));
    boundKeys.add(endKey);
    return boundKeys;
  }

  @Override
  protected final TableIterator<K, ? extends Table.KeyValue<K, V>> newIterator() throws IOException {
    return getTable().iterator(this.snapshot);
  }

  @Override
  public void close() throws IOException {
    this.snapshot.close();
  }
}
