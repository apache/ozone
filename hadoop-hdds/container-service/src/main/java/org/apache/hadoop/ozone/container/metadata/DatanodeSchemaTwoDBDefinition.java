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

import java.util.Map;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.FixedLengthStringCodec;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;

/**
 * This class defines the RocksDB structure for datanodes following schema
 * version 2, where the block data, metadata, and transactions which are to be
 * deleted are put in their own separate column families.
 */
public class DatanodeSchemaTwoDBDefinition extends AbstractDatanodeDBDefinition
    implements DBDefinition.WithMapInterface {

  public static final DBColumnFamilyDefinition<String, BlockData>
          BLOCK_DATA =
          new DBColumnFamilyDefinition<>(
                  "block_data",
                  StringCodec.get(),
                  BlockData.getCodec());

  public static final DBColumnFamilyDefinition<String, Long>
          METADATA =
          new DBColumnFamilyDefinition<>(
          "metadata",
          StringCodec.get(),
          LongCodec.get());

  public static final DBColumnFamilyDefinition<Long, DeletedBlocksTransaction>
      DELETE_TRANSACTION =
      new DBColumnFamilyDefinition<>(
          "delete_txns",
          LongCodec.get(),
          Proto2Codec.get(DeletedBlocksTransaction.getDefaultInstance()));

  public static final DBColumnFamilyDefinition<String, Long>
      FINALIZE_BLOCKS =
      new DBColumnFamilyDefinition<>(
          "finalize_blocks",
          FixedLengthStringCodec.get(),
          LongCodec.get());

  public static final DBColumnFamilyDefinition<String, BlockData>
      LAST_CHUNK_INFO =
      new DBColumnFamilyDefinition<>(
          "last_chunk_info",
          FixedLengthStringCodec.get(),
          BlockData.getCodec());

  private static final Map<String, DBColumnFamilyDefinition<?, ?>>
      COLUMN_FAMILIES = DBColumnFamilyDefinition.newUnmodifiableMap(
          BLOCK_DATA,
          METADATA,
          DELETE_TRANSACTION,
          FINALIZE_BLOCKS,
          LAST_CHUNK_INFO);

  public DatanodeSchemaTwoDBDefinition(String dbPath, ConfigurationSource config) {
    super(dbPath, config);
  }

  @Override
  public Map<String, DBColumnFamilyDefinition<?, ?>> getMap() {
    return COLUMN_FAMILIES;
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

  @Override
  public DBColumnFamilyDefinition<String, BlockData>
      getLastChunkInfoColumnFamily() {
    return LAST_CHUNK_INFO;
  }

  public DBColumnFamilyDefinition<Long, DeletedBlocksTransaction>
      getDeleteTransactionsColumnFamily() {
    return DELETE_TRANSACTION;
  }

  @Override
  public DBColumnFamilyDefinition<String, Long> getFinalizeBlocksColumnFamily() {
    return FINALIZE_BLOCKS;
  }
}
