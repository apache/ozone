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

package org.apache.hadoop.ozone.recon.spi;

import java.io.IOException;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider;
import org.apache.hadoop.ozone.recon.tasks.GlobalStatsValue;

/**
 * The Recon Global Stats DB Service interface.
 */
@InterfaceStability.Unstable
public interface ReconGlobalStatsManager {

  /**
   * Returns staged DB global stats manager.
   *
   * @param stagedReconDbStore staged Recon DB store
   * @return ReconGlobalStatsManager
   */
  ReconGlobalStatsManager getStagedReconGlobalStatsManager(DBStore stagedReconDbStore);

  /**
   * reinitialize the ReconGlobalStatsManager.
   *
   * @param reconDBProvider recon DB provider to reinitialize with.
   */
  void reinitialize(ReconDBProvider reconDBProvider);

  /**
   * Store the global stats value into a batch.
   *
   * @param batch the batch operation we store into
   * @param key   the global stats key.
   * @param value the global stats value.
   */
  void batchStoreGlobalStats(BatchOperation batch,
                             String key,
                             GlobalStatsValue value) throws IOException;

  /**
   * Get the stored global stats value for the given key.
   *
   * @param key the global stats key.
   * @return the global stats value.
   */
  GlobalStatsValue getGlobalStatsValue(String key) throws IOException;

  /**
   * Get the entire globalStatsTable.
   *
   * @return globalStatsTable
   */
  Table<String, GlobalStatsValue> getGlobalStatsTable();

  /**
   * Commit a batch operation into the globalStatsDbStore.
   *
   * @param rdbBatchOperation batch operation we want to commit
   */
  void commitBatchOperation(RDBBatchOperation rdbBatchOperation)
      throws IOException;
}
