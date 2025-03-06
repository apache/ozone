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
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.utils.db.Table;

/**
 * Constructs a datanode store in accordance with schema version 2, which uses
 * three column families/tables:
 * 1. A block data table.
 * 2. A metadata table.
 * 3. A Delete Transaction Table.
 */
public class DatanodeStoreSchemaTwoImpl extends DatanodeStoreWithIncrementalChunkList
    implements DeleteTransactionStore<Long> {

  private final Table<Long, DeletedBlocksTransaction>
      deleteTransactionTable;

  /**
   * Constructs the datanode store and starts the DB Services.
   *
   * @param config - Ozone Configuration.
   * @throws IOException - on Failure.
   */
  public DatanodeStoreSchemaTwoImpl(ConfigurationSource config, String dbPath,
      boolean openReadOnly) throws IOException {
    super(config, new DatanodeSchemaTwoDBDefinition(dbPath, config),
        openReadOnly);
    this.deleteTransactionTable = ((DatanodeSchemaTwoDBDefinition) getDbDef())
        .getDeleteTransactionsColumnFamily().getTable(getStore());
  }

  @Override
  public Table<Long, DeletedBlocksTransaction> getDeleteTransactionTable() {
    return deleteTransactionTable;
  }
}
