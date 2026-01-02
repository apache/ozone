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

import static org.apache.hadoop.hdds.utils.db.Table.newKeyValue;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.cache.TableCache;
import org.apache.hadoop.hdds.utils.db.cache.TableCache.CacheType;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCompactRangeOptions;
import org.apache.hadoop.ozone.util.ClosableIterator;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.apache.ratis.util.UncheckedAutoCloseable;

/**
 * The DBStore interface provides the ability to create Tables, which store
 * a specific type of Key-Value pair. Some DB interfaces like LevelDB will not
 * be able to do this. In those case a Table creation will map to a default
 * store.
 *
 */
@InterfaceStability.Evolving
public interface DBStore extends UncheckedAutoCloseable, BatchOperationHandler {

  /**
   * Gets an existing TableStore.
   *
   * @param name - Name of the TableStore to get
   * @return - TableStore.
   */
  Table<byte[], byte[]> getTable(String name) throws RocksDatabaseException;

  /** The same as getTable(name, keyCodec, valueCodec, CacheType.PARTIAL_CACHE). */
  default <KEY, VALUE> Table<KEY, VALUE> getTable(String name, Codec<KEY> keyCodec, Codec<VALUE> valueCodec)
      throws RocksDatabaseException, CodecException {
    return getTable(name, keyCodec, valueCodec, CacheType.PARTIAL_CACHE);
  }

  /**
   * Gets table store with implict key/value conversion.
   *
   * @param name - table name
   * @param keyCodec - key codec
   * @param valueCodec - value codec
   * @param cacheType - cache type
   * @return - Table Store
   */
  <KEY, VALUE> Table<KEY, VALUE> getTable(
      String name, Codec<KEY> keyCodec, Codec<VALUE> valueCodec, TableCache.CacheType cacheType)
      throws RocksDatabaseException, CodecException;

  /**
   * Lists the Known list of Tables in a DB.
   *
   * @return List of Tables, in case of Rocks DB and LevelDB we will return at
   * least one entry called DEFAULT.
   */
  List<Table<?, ?>> listTables();

  /**
   * Flush the DB buffer onto persistent storage.
   */
  void flushDB() throws RocksDatabaseException;

  /**
   * Flush the outstanding I/O operations of the DB.
   * @param sync if true will sync the outstanding I/Os to the disk.
   */
  void flushLog(boolean sync) throws RocksDatabaseException;

  /**
   * Returns the RocksDB checkpoint differ.
   */
  RocksDBCheckpointDiffer getRocksDBCheckpointDiffer();

  /**
   * Compact the entire database.
   */
  void compactDB() throws RocksDatabaseException;

  /**
   * Compact the specific table.
   *
   * @param tableName - Name of the table to compact.
   */
  void compactTable(String tableName) throws RocksDatabaseException;

  /**
   * Compact the specific table.
   *
   * @param tableName - Name of the table to compact.
   * @param options - Options for the compact operation.
   */
  void compactTable(String tableName, ManagedCompactRangeOptions options) throws RocksDatabaseException;

  /**
   * Returns an estimated count of keys in this DB.
   *
   * @return long, estimate of keys in the DB.
   */
  long getEstimatedKeyCount() throws RocksDatabaseException;


  /**
   * Get current snapshot of DB store as an artifact stored on
   * the local filesystem.
   * @return An object that encapsulates the checkpoint information along with
   * location.
   */
  DBCheckpoint getCheckpoint(boolean flush) throws RocksDatabaseException;

  /**
   * Get current snapshot of DB store as an artifact stored on
   * the local filesystem with different parent path.
   * @return An object that encapsulates the checkpoint information along with
   * location.
   */
  DBCheckpoint getCheckpoint(String parentDir, boolean flush) throws RocksDatabaseException;

  /**
   * Get DB Store location.
   * @return DB file location.
   */
  File getDbLocation();

  /**
   * Get List of Index to Table Names.
   * (For decoding table from column family index)
   * @return Map of Index -&gt; TableName
   */
  Map<Integer, String> getTableNames();

  /**
   * Drop the specific table.
   * @param tableName - Name of the table to truncate.
   */
  void dropTable(String tableName) throws RocksDatabaseException;

  /**
   * Get data written to DB since a specific sequence number.
   */
  DBUpdatesWrapper getUpdatesSince(long sequenceNumber)
      throws SequenceNumberNotFoundException;

  /**
   * Get limited data written to DB since a specific sequence number.
   */
  DBUpdatesWrapper getUpdatesSince(long sequenceNumber, long limitCount)
      throws SequenceNumberNotFoundException;

  /**
   * Return if the underlying DB is closed. This call is thread safe.
   * @return true if the DB is closed.
   */
  boolean isClosed();

  String getSnapshotsParentDir();

  /**
   * Creates an iterator that merges multiple tables into a single iterator,
   * grouping values with the same key across the tables.
   *
   * @param <KEY> the type of keys for the tables
   * @param keyComparator the comparator used to compare keys from different tables
   * @param prefix the prefix used to filter entries of each table
   * @param table one or more tables to merge
   * @return a closable iterator over merged key-value pairs, where each key corresponds
   *         to a collection of values from the tables
   */
  default <KEY> ClosableIterator<KeyValue<KEY, List<Object>>> getMergeIterator(
      Comparator<KEY> keyComparator, KEY prefix, Table<KEY, Object>... table) {
    List<Object> tableValues = IntStream.range(0, table.length).mapToObj(i -> null).collect(Collectors.toList());
    KeyValue<KEY, Object> defaultNullValue = newKeyValue(null, null);
    Comparator<KeyValue<KEY, Object>> comparator = Comparator.comparing(KeyValue::getKey, keyComparator);
    return new MinHeapMergeIterator<KeyValue<KEY, Object>, Table.KeyValueIterator<KEY, Object>,
        KeyValue<KEY, List<Object>>>(table.length, comparator) {
      @Override
      protected Table.KeyValueIterator<KEY, Object> getIterator(int idx) throws IOException {
        return table[idx].iterator(prefix);
      }

      @Override
      protected KeyValue<KEY, List<Object>> merge(Map<Integer, KeyValue<KEY, Object>> keysToMerge) {
        KEY key = keysToMerge.values().stream().findAny()
            .orElseThrow(() -> new NoSuchElementException("No keys found")).getKey();
        for (int i = 0; i < tableValues.size(); i++) {
          tableValues.set(i, keysToMerge.getOrDefault(i, defaultNullValue).getValue());
        }
        return newKeyValue(key, tableValues);
      }
    };
  }
}
