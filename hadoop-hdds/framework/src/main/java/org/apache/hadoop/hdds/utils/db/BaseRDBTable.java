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

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSnapshot;
import org.rocksdb.LiveFileMetaData;

/**
 * Base table interface for Rocksdb.
 */
public interface BaseRDBTable<KEY, VALUE> extends Table<KEY, VALUE> {
  List<LiveFileMetaData> getTableSstFiles() throws IOException;

  /**
   * Take snapshot of the table.
   */
  ManagedSnapshot takeTableSnapshot() throws IOException;

  /**
   * Returns the iterator for this metadata store from a rocksdb snapshot.
   *
   * @return MetaStoreIterator
   * @throws IOException on failure.
   */
  TableIterator<KEY, ? extends KeyValue<KEY, VALUE>> iterator(ManagedSnapshot snapshot)
      throws IOException;

  /**
   * Returns a prefixed iterator for this metadata store to read from a snapshot.
   * @param prefix
   * @return MetaStoreIterator
   */
  TableIterator<KEY, ? extends KeyValue<KEY, VALUE>> iterator(KEY prefix, ManagedSnapshot snapshot)
      throws IOException;

}
