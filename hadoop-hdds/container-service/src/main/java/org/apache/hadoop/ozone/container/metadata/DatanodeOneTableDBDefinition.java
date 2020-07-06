/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.metadata;

import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.rocksdb.RocksDB;

import java.io.File;

/**
 * This class allows support of the old RocksDB layout for datanode, where block data and
 * metadata were kept in the same default table. Clients can use this class as if the database is
 * in the new format (which has separate column families for block data and metadata), even
 * though they both map back to the default table in this implementation.
 */
public class DatanodeOneTableDBDefinition implements DBDefinition {
  // In the underlying database, tables are retrieved by name, and then the codecs/classes are
  // applied on top.
  // By defining two different DBDefinitions with different codecs that both map to the default
  // table, clients are unaware they are using the same table for both interpretations of the data.
  public static final DBColumnFamilyDefinition<String, BlockData>
          BLOCK_DATA =
          new DBColumnFamilyDefinition<>(
                  StringUtils.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY),
                  String.class,
                  new StringCodec(),
                  BlockData.class,
                  new BlockDataCodec());

  public static final DBColumnFamilyDefinition<String, Long>
          METADATA =
          new DBColumnFamilyDefinition<>(
                  StringUtils.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY),
                  String.class,
                  new StringCodec(),
                  Long.class,
                  new LongCodec());

  private File dbDir;

  public DatanodeOneTableDBDefinition(String dbPath) {
    this.dbDir = new File(dbPath);
  }

  @Override
  public String getName() {
    return dbDir.getName();
  }

  @Override
  public String getLocationConfigKey() {
    return dbDir.getParentFile().getParentFile().getAbsolutePath();
  }

  @Override
  public DBColumnFamilyDefinition[] getColumnFamilies() {
    return new DBColumnFamilyDefinition[] {BLOCK_DATA, METADATA};
  }
}
