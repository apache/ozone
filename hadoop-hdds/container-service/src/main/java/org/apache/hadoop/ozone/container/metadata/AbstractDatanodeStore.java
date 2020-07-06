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
import org.apache.hadoop.hdds.utils.db.*;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class AbstractDatanodeStore implements DatanodeStore {

  private Table<String, Long> metadataTable;

  private Table<String, BlockData> blockDataTable;

  private static final Logger LOG =
          LoggerFactory.getLogger(AbstractDatanodeStore.class);
  private DBStore store;
  private AbstractDatanodeDBDefinition dbDef;
  private final ConfigurationSource configuration;

  /**
   * Constructs the metadata store and starts the DB services.
   *
   * @param config - Ozone Configuration.
   * @throws IOException - on Failure.
   */
  protected AbstractDatanodeStore(ConfigurationSource config, AbstractDatanodeDBDefinition dbDef)
          throws IOException {
    this.configuration = config;
    this.dbDef = dbDef;
    start(config);
  }

  @Override
  public void start(ConfigurationSource config)
          throws IOException {
    if (this.store == null) {
      // TODO : Determine how to make sure the DB is created if missing.
      this.store = DBStoreBuilder.createDBStore(config, dbDef);

      metadataTable = dbDef.getMetadataColumnFamily().getTable(this.store);

      checkTableStatus(metadataTable, metadataTable.getName());

      blockDataTable = dbDef.getBlockDataColumnFamily().getTable(this.store);

      checkTableStatus(metadataTable, blockDataTable.getName());
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

  static void checkTableStatus(Table table, String name) throws IOException {
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
