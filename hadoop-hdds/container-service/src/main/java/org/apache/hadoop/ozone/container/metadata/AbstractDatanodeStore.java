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

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.BatchOperationHandler;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfoList;
import org.rocksdb.DBOptions;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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

  private Table<String, ChunkInfoList> deletedBlocksTable;

  private static final Logger LOG =
          LoggerFactory.getLogger(AbstractDatanodeStore.class);
  private DBStore store;
  private final AbstractDatanodeDBDefinition dbDef;

  /**
   * Constructs the metadata store and starts the DB services.
   *
   * @param config - Ozone Configuration.
   * @throws IOException - on Failure.
   */
  protected AbstractDatanodeStore(ConfigurationSource config,
                                  AbstractDatanodeDBDefinition dbDef)
          throws IOException {
    this.dbDef = dbDef;
    start(config);
  }

  @Override
  public void start(ConfigurationSource config)
          throws IOException {
    if (this.store == null) {
      DBOptions options = new DBOptions();
      options.setCreateIfMissing(true);
      options.setCreateMissingColumnFamilies(true);

      String rocksDbStat = config.getTrimmed(
              OZONE_METADATA_STORE_ROCKSDB_STATISTICS,
              OZONE_METADATA_STORE_ROCKSDB_STATISTICS_DEFAULT);

      if (!rocksDbStat.equals(OZONE_METADATA_STORE_ROCKSDB_STATISTICS_OFF)) {
        Statistics statistics = new Statistics();
        statistics.setStatsLevel(StatsLevel.valueOf(rocksDbStat));
        options.setStatistics(statistics);
      }

      this.store = DBStoreBuilder.newBuilder(config, dbDef)
              .setDBOption(options)
              .build();

      metadataTable = new DatanodeTable<>(
              dbDef.getMetadataColumnFamily().getTable(this.store));
      checkTableStatus(metadataTable, metadataTable.getName());

      blockDataTable = new DatanodeTable<>(
              dbDef.getBlockDataColumnFamily().getTable(this.store));
      checkTableStatus(blockDataTable, blockDataTable.getName());

      deletedBlocksTable = new DatanodeTable<>(
              dbDef.getDeletedBlocksColumnFamily().getTable(this.store));
      checkTableStatus(deletedBlocksTable, deletedBlocksTable.getName());
    }
  }

  @Override
  public void stop() throws Exception {
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
}
