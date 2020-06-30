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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DatanodeStoreRDBImpl implements DatanodeStore {

  private Table<Long, Long> metadataTable;

  private Table<Long, Long> blockDataTable;

  private static final Logger LOG =
          LoggerFactory.getLogger(DatanodeStoreRDBImpl.class);
  private DBStore store;
  private final OzoneConfiguration configuration;

  /**
   * Constructs the metadata store and starts the DB Services.
   *
   * @param config - Ozone Configuration.
   * @throws IOException - on Failure.
   */
  public DatanodeStoreRDBImpl(OzoneConfiguration config)
          throws IOException {
    this.configuration = config;
    start(this.configuration);
  }

  @Override
  public void start(OzoneConfiguration config)
          throws IOException {
    if (this.store == null) {

      // TODO : Determine how to get the path to the DB needed to init this instance.
      this.store = DBStoreBuilder.createDBStore(config, new DatanodeTwoTableDBDefinition());

      metadataTable =
              DatanodeTwoTableDBDefinition.METADATA.getTable(this.store);

      checkTableStatus(metadataTable,
              DatanodeTwoTableDBDefinition.METADATA.getName());

      blockDataTable =
              DatanodeTwoTableDBDefinition.BLOCK_DATA.getTable(this.store);

      checkTableStatus(metadataTable,
              DatanodeTwoTableDBDefinition.BLOCK_DATA.getName());

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
  public Table<Long, Long> getMetadataTable() {
    return metadataTable;
  }

  @Override
  public Table<Long, Long> getBlockDataTable() {
    return blockDataTable;
  }

  private void checkTableStatus(Table table, String name) throws IOException {
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
