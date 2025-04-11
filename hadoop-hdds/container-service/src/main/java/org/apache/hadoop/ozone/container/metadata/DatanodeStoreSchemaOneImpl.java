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

import java.io.IOException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfoList;

/**
 * Constructs a datanode store in accordance with schema version 1, which
 * places all data in the default column family.
 */
public class DatanodeStoreSchemaOneImpl extends AbstractDatanodeStore {

  private Table<String, ChunkInfoList> deletedBlocksTable;

  /**
   * Constructs the metadata store and starts the DB Services.
   *
   * @param config - Ozone Configuration.
   * @throws IOException - on Failure.
   */
  public DatanodeStoreSchemaOneImpl(ConfigurationSource config, String dbPath,
      boolean openReadOnly) throws IOException {
    super(config, new DatanodeSchemaOneDBDefinition(dbPath, config),
        openReadOnly);
    deletedBlocksTable = new DatanodeTable<>(
        ((DatanodeSchemaOneDBDefinition) getDbDef()).getDeletedBlocksColumnFamily().getTable(getStore()));
    checkTableStatus(deletedBlocksTable, deletedBlocksTable.getName());
  }

  @Override
  public Table<String, ChunkInfoList> getDeletedBlocksTable() {
    // Return a wrapper around the deleted blocks table to handle prefixes
    // when all data is stored in a single table.
    return new SchemaOneDeletedBlocksTable(deletedBlocksTable);
  }
}
