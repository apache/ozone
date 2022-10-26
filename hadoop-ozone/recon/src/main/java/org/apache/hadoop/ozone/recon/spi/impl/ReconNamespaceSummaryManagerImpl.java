/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.spi.impl;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import static org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider.truncateTable;

import javax.inject.Inject;
import static org.apache.hadoop.ozone.recon.spi.impl.ReconDBDefinition.NAMESPACE_SUMMARY;

import java.io.IOException;

/**
 * Wrapper functions for DB operations on recon namespace summary metadata.
 */
public class ReconNamespaceSummaryManagerImpl
        implements ReconNamespaceSummaryManager {

  private Table<Long, NSSummary> nsSummaryTable;
  private DBStore namespaceDbStore;

  @Inject
  public ReconNamespaceSummaryManagerImpl(ReconDBProvider reconDBProvider)
          throws IOException {
    namespaceDbStore = reconDBProvider.getDbStore();
    this.nsSummaryTable = NAMESPACE_SUMMARY.getTable(namespaceDbStore);
  }

  @Override
  public void clearNSSummaryTable() throws IOException {
    truncateTable(nsSummaryTable);
  }

  @Override
  public void storeNSSummary(long objectId, NSSummary nsSummary)
          throws IOException {
    nsSummaryTable.put(objectId, nsSummary);
  }

  @Override
  public void batchStoreNSSummaries(BatchOperation batch,
                                    long objectId, NSSummary nsSummary)
      throws IOException {
    nsSummaryTable.putWithBatch(batch, objectId, nsSummary);
  }

  @Override
  public void deleteNSSummary(long objectId) throws IOException {
    nsSummaryTable.delete(objectId);
  }

  @Override
  public NSSummary getNSSummary(long objectId) throws IOException {
    return nsSummaryTable.get(objectId);
  }

  @Override
  public void commitBatchOperation(RDBBatchOperation rdbBatchOperation)
      throws IOException {
    this.namespaceDbStore.commitBatchOperation(rdbBatchOperation);
  }

  public Table getNSSummaryTable() {
    return nsSummaryTable;
  }
}
