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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.hdds.utils.db.DBStoreBuilder.HDDS_DEFAULT_DB_PROFILE;

import com.google.common.primitives.Longs;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBConfigFromFile;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.FixedLengthStringCodec;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.utils.db.DatanodeDBProfile;
import org.rocksdb.RocksDBException;

/**
 * This class defines the RocksDB structure for datanode following schema
 * version 3, where the block data, metadata, and transactions which are to be
 * deleted are put in their own separate column families and with containerID
 * as key prefix.
 * <p>
 * Some key format illustrations for the column families:
 * - block_data:     containerID | blockID
 * - metadata:       containerID | #BLOCKCOUNT
 *                   containerID | #BYTESUSED
 *                   ...
 * - deleted_blocks: containerID | blockID
 * - delete_txns:    containerID | TransactionID
 * <p>
 * The keys would be encoded in a fix-length encoding style in order to
 * utilize the "Prefix Seek" feature from Rocksdb to optimize seek.
 */
public class DatanodeSchemaThreeDBDefinition extends AbstractDatanodeDBDefinition
    implements DBDefinition.WithMapInterface {

  public static final DBColumnFamilyDefinition<String, BlockData>
      BLOCK_DATA =
      new DBColumnFamilyDefinition<>(
          "block_data",
          FixedLengthStringCodec.get(),
          BlockData.getCodec());

  public static final DBColumnFamilyDefinition<String, Long>
      METADATA =
      new DBColumnFamilyDefinition<>(
          "metadata",
          FixedLengthStringCodec.get(),
          LongCodec.get());

  public static final DBColumnFamilyDefinition<String, DeletedBlocksTransaction>
      DELETE_TRANSACTION =
      new DBColumnFamilyDefinition<>(
          "delete_txns",
          FixedLengthStringCodec.get(),
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

  private static String separator = "";

  private static final Map<String, DBColumnFamilyDefinition<?, ?>>
      COLUMN_FAMILIES = DBColumnFamilyDefinition.newUnmodifiableMap(
         BLOCK_DATA,
         METADATA,
         DELETE_TRANSACTION,
         FINALIZE_BLOCKS,
         LAST_CHUNK_INFO);

  public DatanodeSchemaThreeDBDefinition(String dbPath,
      ConfigurationSource config) {
    super(dbPath, config);

    DatanodeConfiguration dc = config.getObject(DatanodeConfiguration.class);
    setSeparator(dc.getContainerSchemaV3KeySeparator());

    // Get global ColumnFamilyOptions first.
    DatanodeDBProfile dbProfile = DatanodeDBProfile
        .getProfile(config.getEnum(HDDS_DB_PROFILE, HDDS_DEFAULT_DB_PROFILE));

    Path optionsPath = Paths.get(
        config.get(HddsConfigKeys.DATANODE_DB_CONFIG_PATH, HddsConfigKeys.DATANODE_DB_CONFIG_PATH_DEFAULT));

    setCfOptions(config, dbProfile, optionsPath, BLOCK_DATA);
    setCfOptions(config, dbProfile, optionsPath, METADATA);
    setCfOptions(config, dbProfile, optionsPath, DELETE_TRANSACTION);
    setCfOptions(config, dbProfile, optionsPath, FINALIZE_BLOCKS);
    setCfOptions(config, dbProfile, optionsPath, LAST_CHUNK_INFO);
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

  public DBColumnFamilyDefinition<String, DeletedBlocksTransaction>
      getDeleteTransactionsColumnFamily() {
    return DELETE_TRANSACTION;
  }

  @Override
  public DBColumnFamilyDefinition<String, Long>
      getFinalizeBlocksColumnFamily() {
    return FINALIZE_BLOCKS;
  }

  public static int getContainerKeyPrefixLength() {
    return FixedLengthStringCodec.string2Bytes(
        getContainerKeyPrefix(0L)).length;
  }

  public static String getContainerKeyPrefix(long containerID) {
    // NOTE: Rocksdb normally needs a fixed length prefix.
    return FixedLengthStringCodec.bytes2String(Longs.toByteArray(containerID))
        + separator;
  }

  public static byte[] getContainerKeyPrefixBytes(long containerID) {
    // NOTE: Rocksdb normally needs a fixed length prefix.
    return FixedLengthStringCodec.string2Bytes(
        getContainerKeyPrefix(containerID));
  }

  public static String getKeyWithoutPrefix(String keyWithPrefix) {
    return keyWithPrefix.substring(keyWithPrefix.indexOf(separator) + 1);
  }

  /**
   *
   * @param key rocksDB original key
   * @return containerID
   */
  public static long getContainerId(String key) {
    int index = getContainerKeyPrefixLength();
    String cid = key.substring(0, index);
    return Longs.fromByteArray(FixedLengthStringCodec.string2Bytes(cid));
  }

  private void setSeparator(String keySeparator) {
    separator = keySeparator;
  }

  private void setCfOptions(ConfigurationSource config, DatanodeDBProfile dbProfile, Path pathToOptions,
      DBColumnFamilyDefinition<?, ?> definition) {
    // Use prefix seek to mitigating seek overhead.
    // See: https://github.com/facebook/rocksdb/wiki/Prefix-Seek
    ManagedColumnFamilyOptions cfOptions = null;
    try {
      cfOptions = DBConfigFromFile.readCFOptionsFromFile(pathToOptions, definition.getName());
    } catch (RocksDBException e) {
      LOG.error("Error while reading column family options from file: {}", pathToOptions);
    }
    if (cfOptions == null) {
      cfOptions = dbProfile.getColumnFamilyOptions(config);
    }
    cfOptions.useFixedLengthPrefixExtractor(getContainerKeyPrefixLength());
    definition.setCfOptions(cfOptions);
  }
}
