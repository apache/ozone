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

package org.apache.hadoop.ozone.container.metadata;

import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.CollectionUtils;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfoList;

/**
 * This class allows support of the schema version one RocksDB layout for
 * datanode, where
 * all data is kept in the same default table. Clients can use this class as
 * if the database is in the new format (which has separate column families for
 * block data, metadata and deleted block IDs), even
 * though all tables map back to the default table in this implementation.
 */
public class DatanodeSchemaOneDBDefinition
        extends AbstractDatanodeDBDefinition {
  // In the underlying database, tables are retrieved by name, and then the
  // codecs/classes are applied on top. By defining different DBDefinitions
  // with different codecs that all map to the default table, clients are
  // unaware they are using the same table for both interpretations of the data.

  // Note that the current RDBStore implementation requires all column
  // families to use the same codec instance for each key/value type.
  public static final DBColumnFamilyDefinition<String, BlockData>
      BLOCK_DATA =
      new DBColumnFamilyDefinition<>(
          StringUtils.bytes2String(DEFAULT_COLUMN_FAMILY),
          SchemaOneKeyCodec.get(),
          BlockData.getCodec());

  public static final DBColumnFamilyDefinition<String, Long>
        METADATA =
        new DBColumnFamilyDefinition<>(
            StringUtils.bytes2String(DEFAULT_COLUMN_FAMILY),
            SchemaOneKeyCodec.get(),
            LongCodec.get());

  public static final DBColumnFamilyDefinition<String, ChunkInfoList>
        DELETED_BLOCKS =
        new DBColumnFamilyDefinition<>(
            StringUtils.bytes2String(DEFAULT_COLUMN_FAMILY),
            SchemaOneKeyCodec.get(),
            SchemaOneChunkInfoListCodec.get());

  private static final Map<String, List<DBColumnFamilyDefinition<?, ?>>>
      COLUMN_FAMILIES = DBColumnFamilyDefinition.newUnmodifiableMultiMap(
          BLOCK_DATA,
          METADATA,
          DELETED_BLOCKS);

  public DatanodeSchemaOneDBDefinition(String dbPath,
      ConfigurationSource config) {
    super(dbPath, config);
  }

  @Override
  public DBColumnFamilyDefinition<String, BlockData>
      getBlockDataColumnFamily() {
    return BLOCK_DATA;
  }

  @Override
  public DBColumnFamilyDefinition<String, Long> getMetadataColumnFamily() {
    return METADATA;
  }

  public DBColumnFamilyDefinition<String, ChunkInfoList>
      getDeletedBlocksColumnFamily() {
    return DELETED_BLOCKS;
  }

  @Override
  public DBColumnFamilyDefinition<String, BlockData>
      getLastChunkInfoColumnFamily() {
    return null;
  }

  @Override
  public List<DBColumnFamilyDefinition<?, ?>> getColumnFamilies(String name) {
    return COLUMN_FAMILIES.get(name);
  }

  @Override
  public Iterable<DBColumnFamilyDefinition<?, ?>> getColumnFamilies() {
    return () -> CollectionUtils.newIterator(COLUMN_FAMILIES.values());
  }
}
