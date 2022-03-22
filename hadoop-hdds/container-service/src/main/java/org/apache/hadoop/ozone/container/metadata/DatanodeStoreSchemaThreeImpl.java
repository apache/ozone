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
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;

import java.io.IOException;

import static org.apache.hadoop.ozone.container.metadata.DatanodeSchemaThreeDBDefinition.getContainerKeyPrefix;

/**
 * Constructs a datanode store in accordance with schema version 3, which uses
 * three column families/tables:
 * 1. A block data table.
 * 2. A metadata table.
 * 3. A Delete Transaction Table.
 *
 * This is different from schema version 2 from these points:
 * - All keys have containerID as prefix.
 * - The table 3 has String as key instead of Long since we want to use prefix.
 */
public class DatanodeStoreSchemaThreeImpl extends AbstractDatanodeStore {

  private final Table<String, DeletedBlocksTransaction> deleteTransactionTable;

  public DatanodeStoreSchemaThreeImpl(ConfigurationSource config,
      String dbPath, boolean openReadOnly) throws IOException {
    super(config, new DatanodeSchemaThreeDBDefinition(dbPath, config),
        openReadOnly);
    this.deleteTransactionTable = ((DatanodeSchemaThreeDBDefinition) getDbDef())
        .getDeleteTransactionsColumnFamily().getTable(getStore());
  }

  public Table<String, DeletedBlocksTransaction> getDeleteTransactionTable() {
    return this.deleteTransactionTable;
  }

  @Override
  public BlockIterator<BlockData> getBlockIterator(long containerID)
      throws IOException {
    // Here we need to filter the keys with containerID as prefix
    // and followed by metadata prefixes such as #deleting#.
    return new KeyValueBlockIterator(containerID,
        getBlockDataTableWithIterator()
            .iterator(getContainerKeyPrefix(containerID)),
        new MetadataKeyFilters.KeyPrefixFilter().addFilter(
            getContainerKeyPrefix(containerID) + "#", true));
  }

  @Override
  public BlockIterator<BlockData> getBlockIterator(long containerID,
      MetadataKeyFilters.KeyPrefixFilter filter) throws IOException {
    return new KeyValueBlockIterator(containerID,
        getBlockDataTableWithIterator()
            .iterator(getContainerKeyPrefix(containerID)), filter);
  }
}