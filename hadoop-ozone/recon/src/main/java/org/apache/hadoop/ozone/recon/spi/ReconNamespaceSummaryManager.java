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
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider;

/**
 * Interface for DB operations on NSSummary.
 */
@InterfaceStability.Unstable
public interface ReconNamespaceSummaryManager {

  ReconNamespaceSummaryManager getStagedNsSummaryManager(DBStore dbStore) throws IOException;

  void reinitialize(ReconDBProvider reconDBProvider) throws IOException;

  void clearNSSummaryTable() throws IOException;

  @Deprecated
  void storeNSSummary(long objectId, NSSummary nsSummary) throws IOException;

  void batchStoreNSSummaries(BatchOperation batch, long objectId,
                             NSSummary nsSummary) throws IOException;

  void batchDeleteNSSummaries(BatchOperation batch, long objectId) throws IOException;

  void deleteNSSummary(long objectId) throws IOException;

  NSSummary getNSSummary(long objectId) throws IOException;

  void commitBatchOperation(RDBBatchOperation rdbBatchOperation)
      throws IOException;
}
