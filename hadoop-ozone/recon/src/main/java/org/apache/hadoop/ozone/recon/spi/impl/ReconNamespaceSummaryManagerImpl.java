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

import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import static org.apache.hadoop.ozone.recon.spi.impl.ReconRocksDB.clearTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import static org.apache.hadoop.ozone.recon.spi.impl.ReconDBDefinition.NAMESPACE_SUMMARY;

import java.io.IOException;

/**
 * Wrapper functions for DB operations on recon namespace summary metadata.
 */
public class ReconNamespaceSummaryManagerImpl
        implements ReconNamespaceSummaryManager {

  private static final Logger LOG =
          LoggerFactory.getLogger(ReconNamespaceSummaryManagerImpl.class);
  private Table<Long, NSSummary> nsSummaryTable;
  private DBStore namespaceDbStore;

  // TODO: compute disk usage here?
  @Inject
  public ReconNamespaceSummaryManagerImpl(ReconRocksDB reconRocksDB) {
    namespaceDbStore = reconRocksDB.getDbStore();
    initializeTable();
  }

  @Override
  public void initNSSummaryTable() throws IOException {
    initializeTable();
  }

  @Override
  public void storeNSSummary(long objectId, NSSummary nsSummary)
          throws IOException {
    nsSummaryTable.put(objectId, nsSummary);
  }

  @Override
  public NSSummary getNSSummary(long objectId) throws IOException {
    return nsSummaryTable.get(objectId);
  }

  private void initializeTable() {
    try {
      clearTable(this.nsSummaryTable);
      this.nsSummaryTable = NAMESPACE_SUMMARY.getTable(namespaceDbStore);
    } catch (IOException e) {
      LOG.error("cannot initialize table");
    }
  }

  public Table getNSSummaryTable() {
    return nsSummaryTable;
  }
}
