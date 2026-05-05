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

package org.apache.hadoop.ozone.recon.spi.impl;

import static org.apache.hadoop.ozone.recon.spi.impl.ReconDBDefinition.GLOBAL_STATS;

import java.io.IOException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.recon.spi.ReconGlobalStatsManager;
import org.apache.hadoop.ozone.recon.tasks.GlobalStatsValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the Recon Global Stats DB Service.
 */
@Singleton
public class ReconGlobalStatsManagerImpl implements ReconGlobalStatsManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconGlobalStatsManagerImpl.class);

  private Table<String, GlobalStatsValue> globalStatsTable;
  private DBStore globalStatsDbStore;

  @Inject
  public ReconGlobalStatsManagerImpl(ReconDBProvider reconDBProvider) {
    this(reconDBProvider.getDbStore());
  }

  private ReconGlobalStatsManagerImpl(DBStore reconDBStore) {
    globalStatsDbStore = reconDBStore;
    initializeTables();
  }

  @Override
  public ReconGlobalStatsManager getStagedReconGlobalStatsManager(
      DBStore stagedReconDbStore) {
    return new ReconGlobalStatsManagerImpl(stagedReconDbStore);
  }

  @Override
  public void reinitialize(ReconDBProvider reconDBProvider) {
    globalStatsDbStore = reconDBProvider.getDbStore();
    initializeTables();
  }

  /**
   * Initialize the global stats DB tables.
   */
  private void initializeTables() {
    try {
      this.globalStatsTable = GLOBAL_STATS.getTable(globalStatsDbStore);
    } catch (IOException e) {
      LOG.error("Unable to create Global Stats table.", e);
    }
  }

  @Override
  public void batchStoreGlobalStats(BatchOperation batch,
                                    String key,
                                    GlobalStatsValue value) throws IOException {
    globalStatsTable.putWithBatch(batch, key, value);
  }

  @Override
  public GlobalStatsValue getGlobalStatsValue(String key) throws IOException {
    return globalStatsTable.get(key);
  }

  @Override
  public Table<String, GlobalStatsValue> getGlobalStatsTable() {
    return globalStatsTable;
  }

  @Override
  public void commitBatchOperation(RDBBatchOperation rdbBatchOperation)
      throws IOException {
    globalStatsDbStore.commitBatchOperation(rdbBatchOperation);
  }
}
