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
package org.apache.hadoop.hdds.datanode.metadata;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hdds.datanode.metadata.CRLDBDefinition.CRL_SEQUENCE_ID;
import static org.apache.hadoop.hdds.datanode.metadata.CRLDBDefinition.PENDING_CRLS;
import static org.apache.hadoop.ozone.OzoneConsts.CRL_SEQUENCE_ID_KEY;

/**
 * A RocksDB based implementation of the Datanode CRL Store.
 */
public class DatanodeCRLStoreImpl implements DatanodeCRLStore {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeCRLStore.class);
  private DBStore store;
  private Table<String, Long> crlSequenceIdTable;
  private Table<Long, CRLInfo> pendingCRLsTable;

  /**
   * Constructs the metadata store and starts the DB Services.
   *
   * @param configuration - Ozone Configuration.
   * @throws IOException - on Failure.
   */
  public DatanodeCRLStoreImpl(OzoneConfiguration configuration)
      throws IOException {
    start(configuration);
  }

  @Override
  public void start(OzoneConfiguration configuration) throws IOException {
    if (this.store == null) {
      this.store = DBStoreBuilder.createDBStore(configuration,
          new CRLDBDefinition());

      crlSequenceIdTable = CRL_SEQUENCE_ID.getTable(store);
      checkTableStatus(crlSequenceIdTable, CRL_SEQUENCE_ID.getName());

      pendingCRLsTable = PENDING_CRLS.getTable(store);
      checkTableStatus(pendingCRLsTable, PENDING_CRLS.getName());
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
  public Table<String, Long> getCRLSequenceIdTable() {
    return crlSequenceIdTable;
  }

  @Override
  public Table<Long, CRLInfo> getPendingCRLsTable() {
    return pendingCRLsTable;
  }

  @Override
  public Long getLatestCRLSequenceID() throws IOException {
    Long sequenceId = crlSequenceIdTable.get(CRL_SEQUENCE_ID_KEY);
    // If the CRL_SEQUENCE_ID_KEY does not exist in DB return 0
    if (sequenceId == null) {
      return 0L;
    }
    return sequenceId;
  }

  @Override
  public List<CRLInfo> getPendingCRLs() throws IOException {
    try (TableIterator<Long, ? extends Table.KeyValue<Long, CRLInfo>> iter =
        pendingCRLsTable.iterator()) {
      List<CRLInfo> pendingCRLs = new ArrayList<>();
      while (iter.hasNext()) {
        pendingCRLs.add(iter.next().getValue());
      }
      return pendingCRLs;
    }
  }

  private void checkTableStatus(Table table, String name) throws IOException {
    String logMessage = "Unable to get a reference to %s table. Cannot " +
        "continue.";
    String errMsg = "Inconsistent DB state, Table - %s. Please check the logs" +
        "for more info.";
    if (table == null) {
      LOG.error(String.format(logMessage, name));
      throw new IOException(String.format(errMsg, name));
    }
  }
}
