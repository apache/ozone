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

import com.google.common.primitives.Longs;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.FixedLengthStringUtils;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.FixedLengthStringCodec;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfoList;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.utils.db.DatanodeDBProfile;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.hdds.utils.db.DBStoreBuilder.HDDS_DEFAULT_DB_PROFILE;

/**
 * This class defines the RocksDB structure for datanode following schema
 * version 3, where the block data, metadata, and transactions which are to be
 * deleted are put in their own separate column families and with containerID
 * as key prefix.
 *
 * Some key format illustrations for the column families:
 * - block_data:     containerID | blockID
 * - metadata:       containerID | #BLOCKCOUNT
 *                   containerID | #BYTESUSED
 *                   ...
 * - deleted_blocks: containerID | blockID
 * - delete_txns:    containerID | TransactionID
 *
 * The keys would be encoded in a fix-length encoding style in order to
 * utilize the "Prefix Seek" feature from Rocksdb to optimize seek.
 */
public class DatanodeSchemaThreeDBDefinition
    extends AbstractDatanodeDBDefinition {
  public static final DBColumnFamilyDefinition<String, BlockData>
      BLOCK_DATA =
      new DBColumnFamilyDefinition<>(
          "block_data",
          String.class,
          new FixedLengthStringCodec(),
          BlockData.class,
          new BlockDataCodec());

  public static final DBColumnFamilyDefinition<String, Long>
      METADATA =
      new DBColumnFamilyDefinition<>(
          "metadata",
          String.class,
          new FixedLengthStringCodec(),
          Long.class,
          new LongCodec());

  public static final DBColumnFamilyDefinition<String, ChunkInfoList>
      DELETED_BLOCKS =
      new DBColumnFamilyDefinition<>(
          "deleted_blocks",
          String.class,
          new FixedLengthStringCodec(),
          ChunkInfoList.class,
          new ChunkInfoListCodec());

  public static final DBColumnFamilyDefinition<String, DeletedBlocksTransaction>
      DELETE_TRANSACTION =
      new DBColumnFamilyDefinition<>(
          "delete_txns",
          String.class,
          new FixedLengthStringCodec(),
          DeletedBlocksTransaction.class,
          new DeletedBlocksTransactionCodec());

  private static String separator = "";

  public DatanodeSchemaThreeDBDefinition(String dbPath,
      ConfigurationSource config) {
    super(dbPath, config);

    DatanodeConfiguration dc = config.getObject(DatanodeConfiguration.class);
    setSeparator(dc.getContainerSchemaV3KeySeparator());

    // Get global ColumnFamilyOptions first.
    DatanodeDBProfile dbProfile = DatanodeDBProfile
        .getProfile(config.getEnum(HDDS_DB_PROFILE, HDDS_DEFAULT_DB_PROFILE));

    ManagedColumnFamilyOptions cfOptions =
        dbProfile.getColumnFamilyOptions(config);
    // Use prefix seek to mitigating seek overhead.
    // See: https://github.com/facebook/rocksdb/wiki/Prefix-Seek
    cfOptions.useFixedLengthPrefixExtractor(getContainerKeyPrefixLength());

    BLOCK_DATA.setCfOptions(cfOptions);
    METADATA.setCfOptions(cfOptions);
    DELETED_BLOCKS.setCfOptions(cfOptions);
    DELETE_TRANSACTION.setCfOptions(cfOptions);
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

  public DBColumnFamilyDefinition<String, DeletedBlocksTransaction>
      getDeleteTransactionsColumnFamily() {
    return DELETE_TRANSACTION;
  }

  public static String getContainerKeyPrefix(long containerID) {
    // NOTE: Rocksdb normally needs a fixed length prefix.
    return FixedLengthStringUtils.bytes2String(Longs.toByteArray(containerID))
        + separator;
  }

  public static int getContainerKeyPrefixLength() {
    return FixedLengthStringUtils.string2Bytes(
        getContainerKeyPrefix(0L)).length;
  }

  private void setSeparator(String keySeparator) {
    separator = keySeparator;
  }
}