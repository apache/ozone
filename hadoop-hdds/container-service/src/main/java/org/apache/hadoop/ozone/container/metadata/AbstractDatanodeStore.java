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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.hdds.utils.db.BatchOperationHandler;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedStatistics;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfoList;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.utils.db.DatanodeDBProfile;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.StatsLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.hdds.utils.db.DBStoreBuilder.HDDS_DEFAULT_DB_PROFILE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS_OFF;

/**
 * Implementation of the {@link DatanodeStore} interface that contains
 * functionality common to all more derived datanode store implementations.
 */
public abstract class AbstractDatanodeStore implements DatanodeStore {

  private Table<String, Long> metadataTable;

  private Table<String, BlockData> blockDataTable;

  private Table<String, BlockData> blockDataTableWithIterator;

  private Table<String, ChunkInfoList> deletedBlocksTable;

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractDatanodeStore.class);
  private volatile DBStore store;
  private final AbstractDatanodeDBDefinition dbDef;
  private final ManagedColumnFamilyOptions cfOptions;

  private static DatanodeDBProfile dbProfile;
  private final boolean openReadOnly;

  /**
   * Constructs the metadata store and starts the DB services.
   *
   * @param config - Ozone Configuration.
   * @throws IOException - on Failure.
   */
  protected AbstractDatanodeStore(ConfigurationSource config,
      AbstractDatanodeDBDefinition dbDef, boolean openReadOnly)
      throws IOException {

    dbProfile = DatanodeDBProfile
        .getProfile(config.getEnum(HDDS_DB_PROFILE, HDDS_DEFAULT_DB_PROFILE));

    // The same config instance is used on each datanode, so we can share the
    // corresponding column family options, providing a single shared cache
    // for all containers on a datanode.
    cfOptions = dbProfile.getColumnFamilyOptions(config);

    this.dbDef = dbDef;
    this.openReadOnly = openReadOnly;
    start(config);
  }

  @Override
  public void start(ConfigurationSource config)
      throws IOException {
    if (this.store == null) {
      ManagedDBOptions options = dbProfile.getDBOptions();
      options.setCreateIfMissing(true);
      options.setCreateMissingColumnFamilies(true);

      if (this.dbDef instanceof DatanodeSchemaOneDBDefinition ||
          this.dbDef instanceof DatanodeSchemaTwoDBDefinition) {
        long maxWalSize = DBProfile.toLong(StorageUnit.MB.toBytes(2));
        options.setMaxTotalWalSize(maxWalSize);
      }

      if (this.dbDef instanceof DatanodeSchemaThreeDBDefinition) {
        DatanodeConfiguration dc =
            config.getObject(DatanodeConfiguration.class);
        // Config user log files
        InfoLogLevel level = InfoLogLevel.valueOf(
            dc.getRocksdbLogLevel() + "_LEVEL");
        options.setInfoLogLevel(level);
        options.setMaxLogFileSize(dc.getRocksdbMaxFileSize());
        options.setKeepLogFileNum(dc.getRocksdbMaxFileNum());
        options.setDeleteObsoleteFilesPeriodMicros(
            dc.getRocksdbDeleteObsoleteFilesPeriod());
      }

      String rocksDbStat = config.getTrimmed(
              OZONE_METADATA_STORE_ROCKSDB_STATISTICS,
              OZONE_METADATA_STORE_ROCKSDB_STATISTICS_DEFAULT);

      if (!rocksDbStat.equals(OZONE_METADATA_STORE_ROCKSDB_STATISTICS_OFF)) {
        ManagedStatistics statistics = new ManagedStatistics();
        statistics.setStatsLevel(StatsLevel.valueOf(rocksDbStat));
        options.setStatistics(statistics);
      }

      this.store = DBStoreBuilder.newBuilder(config, dbDef)
              .setDBOptions(options)
              .setDefaultCFOptions(cfOptions)
              .setOpenReadOnly(openReadOnly)
              .build();

      // Use the DatanodeTable wrapper to disable the table iterator on
      // existing Table implementations retrieved from the DBDefinition.
      // See the DatanodeTable's Javadoc for an explanation of why this is
      // necessary.
      metadataTable = new DatanodeTable<>(
              dbDef.getMetadataColumnFamily().getTable(this.store));
      checkTableStatus(metadataTable, metadataTable.getName());

      // The block iterator this class returns will need to use the table
      // iterator internally, so construct a block data table instance
      // that does not have the iterator disabled by DatanodeTable.
      blockDataTableWithIterator =
              dbDef.getBlockDataColumnFamily().getTable(this.store);

      blockDataTable = new DatanodeTable<>(blockDataTableWithIterator);
      checkTableStatus(blockDataTable, blockDataTable.getName());

      deletedBlocksTable = new DatanodeTable<>(
              dbDef.getDeletedBlocksColumnFamily().getTable(this.store));
      checkTableStatus(deletedBlocksTable, deletedBlocksTable.getName());
    }
  }

  @Override
  public synchronized void stop() throws Exception {
    if (store != null) {
      store.close();
      store = null;
    }
  }

  @Override
  public DBStore getStore() {
    return this.store;
  }

  @Override
  public BatchOperationHandler getBatchHandler() {
    return this.store;
  }

  @Override
  public Table<String, Long> getMetadataTable() {
    return metadataTable;
  }

  @Override
  public Table<String, BlockData> getBlockDataTable() {
    return blockDataTable;
  }

  @Override
  public Table<String, ChunkInfoList> getDeletedBlocksTable() {
    return deletedBlocksTable;
  }

  @Override
  public BlockIterator<BlockData> getBlockIterator(long containerID)
      throws IOException {
    return new KeyValueBlockIterator(containerID,
            blockDataTableWithIterator.iterator());
  }

  @Override
  public BlockIterator<BlockData> getBlockIterator(long containerID,
      KeyPrefixFilter filter) throws IOException {
    return new KeyValueBlockIterator(containerID,
            blockDataTableWithIterator.iterator(), filter);
  }

  @Override
  public synchronized boolean isClosed() {
    if (this.store == null) {
      return true;
    }
    return this.store.isClosed();
  }

  @Override
  public void close() throws IOException {
    this.store.close();
    this.cfOptions.close();
  }

  @Override
  public void flushDB() throws IOException {
    store.flushDB();
  }

  @Override
  public void flushLog(boolean sync) throws IOException {
    store.flushLog(sync);
  }

  @Override
  public void compactDB() throws IOException {
    store.compactDB();
  }

  @VisibleForTesting
  public DatanodeDBProfile getDbProfile() {
    return dbProfile;
  }

  protected AbstractDatanodeDBDefinition getDbDef() {
    return this.dbDef;
  }

  protected Table<String, BlockData> getBlockDataTableWithIterator() {
    return this.blockDataTableWithIterator;
  }

  private static void checkTableStatus(Table<?, ?> table, String name)
          throws IOException {
    String logMessage = "Unable to get a reference to %s table. Cannot " +
            "continue.";
    String errMsg = "Inconsistent DB state, Table - %s. Please check the" +
            " logs for more info.";
    if (table == null) {
      LOG.error(String.format(logMessage, name));
      throw new IOException(String.format(errMsg, name));
    }
  }

  /**
   * Block Iterator for KeyValue Container. This block iterator returns blocks
   * which match with the {@link MetadataKeyFilters.KeyPrefixFilter}. If no
   * filter is specified, then default filter used is
   * {@link MetadataKeyFilters#getUnprefixedKeyFilter()}
   */
  @InterfaceAudience.Public
  public static class KeyValueBlockIterator implements
          BlockIterator<BlockData>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(
            KeyValueBlockIterator.class);

    private final TableIterator<String, ? extends Table.KeyValue<String,
            BlockData>>
            blockIterator;
    private static final KeyPrefixFilter DEFAULT_BLOCK_FILTER =
            MetadataKeyFilters.getUnprefixedKeyFilter();
    private final KeyPrefixFilter blockFilter;
    private BlockData nextBlock;
    private final long containerID;

    /**
     * KeyValueBlockIterator to iterate unprefixed blocks in a container.
     * @param iterator - The underlying iterator to apply the block filter to.
     */
    KeyValueBlockIterator(long containerID,
            TableIterator<String, ? extends Table.KeyValue<String, BlockData>>
                    iterator) {
      this.containerID = containerID;
      this.blockIterator = iterator;
      this.blockFilter = DEFAULT_BLOCK_FILTER;
    }

    /**
     * KeyValueBlockIterator to iterate blocks in a container.
     * @param iterator - The underlying iterator to apply the block filter to.
     * @param filter - Block filter, filter to be applied for blocks
     */
    KeyValueBlockIterator(long containerID,
            TableIterator<String, ? extends Table.KeyValue<String, BlockData>>
                    iterator, KeyPrefixFilter filter) {
      this.containerID = containerID;
      this.blockIterator = iterator;
      this.blockFilter = filter;
    }

    /**
     * This method returns blocks matching with the filter.
     * @return next block or null if no more blocks
     * @throws IOException
     */
    @Override
    public BlockData nextBlock() throws IOException, NoSuchElementException {
      if (nextBlock != null) {
        BlockData currentBlock = nextBlock;
        nextBlock = null;
        return currentBlock;
      }
      if (hasNext()) {
        return nextBlock();
      }
      throw new NoSuchElementException("Block Iterator reached end for " +
              "ContainerID " + containerID);
    }

    @Override
    public boolean hasNext() throws IOException {
      if (nextBlock != null) {
        return true;
      }
      while (blockIterator.hasNext()) {
        Table.KeyValue<String, BlockData> keyValue = blockIterator.next();
        byte[] keyBytes = StringUtils.string2Bytes(keyValue.getKey());
        if (blockFilter.filterKey(null, keyBytes, null)) {
          nextBlock = keyValue.getValue();
          if (LOG.isTraceEnabled()) {
            LOG.trace("Block matching with filter found: blockID is : {} for " +
                    "containerID {}", nextBlock.getLocalID(), containerID);
          }
          return true;
        }
      }
      return false;
    }

    @Override
    public void seekToFirst() {
      nextBlock = null;
      blockIterator.seekToFirst();
    }

    @Override
    public void seekToLast() {
      nextBlock = null;
      blockIterator.seekToLast();
    }

    @Override
    public void close() throws IOException {
      blockIterator.close();
    }
  }
}
