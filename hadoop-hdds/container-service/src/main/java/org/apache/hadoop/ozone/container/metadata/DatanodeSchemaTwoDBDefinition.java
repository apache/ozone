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

import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfoList;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;

/**
 * This class defines the RocksDB structure for datanodes following schema
 * version 2, where the block data, metadata, and transactions which are to be
 * deleted are put in their own separate column families.
 */
public class DatanodeSchemaTwoDBDefinition extends
        AbstractDatanodeDBDefinition {

  public static final DBColumnFamilyDefinition<String, BlockData>
          BLOCK_DATA =
          new DBColumnFamilyDefinition<>(
                  "block_data",
                  String.class,
                  new StringCodec(),
                  BlockData.class,
                  new BlockDataCodec());

  public static final DBColumnFamilyDefinition<String, Long>
          METADATA =
          new DBColumnFamilyDefinition<>(
          "metadata",
          String.class,
          new StringCodec(),
          Long.class,
          new LongCodec());

  public static final DBColumnFamilyDefinition<String, ChunkInfoList>
          DELETED_BLOCKS =
          new DBColumnFamilyDefinition<>(
                  "deleted_blocks",
                  String.class,
                  new StringCodec(),
                  ChunkInfoList.class,
                  new ChunkInfoListCodec());

  public static final DBColumnFamilyDefinition<Long, DeletedBlocksTransaction>
      DELETE_TRANSACTION =
      new DBColumnFamilyDefinition<>(
          "delete_txns",
          Long.class,
          new LongCodec(),
          StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction.class,
          new DeletedBlocksTransactionCodec());

  public DatanodeSchemaTwoDBDefinition(String dbPath) {
    super(dbPath);
  }

  @Override
  public DBColumnFamilyDefinition[] getColumnFamilies() {
    return new DBColumnFamilyDefinition[] {getBlockDataColumnFamily(),
        getMetadataColumnFamily(), getDeletedBlocksColumnFamily(),
        getDeleteTransactionsColumnFamily()};
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
  public DBColumnFamilyDefinition<String, ChunkInfoList>
      getDeletedBlocksColumnFamily() {
    return DELETED_BLOCKS;
  }

  public DBColumnFamilyDefinition<Long, DeletedBlocksTransaction>
      getDeleteTransactionsColumnFamily() {
    return DELETE_TRANSACTION;
  }
}
