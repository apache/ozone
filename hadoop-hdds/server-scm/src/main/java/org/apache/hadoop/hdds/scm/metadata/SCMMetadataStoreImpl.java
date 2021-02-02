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
package org.apache.hadoop.hdds.scm.metadata;

import java.io.IOException;
import java.math.BigInteger;
import java.security.cert.X509Certificate;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.utils.db.BatchOperationHandler;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;

import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.CONTAINERS;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.DELETED_BLOCKS;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.PIPELINES;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.REVOKED_CERTS;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.VALID_CERTS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A RocksDB based implementation of SCM Metadata Store.
 *
 */
public class SCMMetadataStoreImpl implements SCMMetadataStore {

  private Table<Long, DeletedBlocksTransaction> deletedBlocksTable;

  private Table<BigInteger, X509Certificate> validCertsTable;

  private Table<BigInteger, X509Certificate> revokedCertsTable;

  private Table<ContainerID, ContainerInfo> containerTable;

  private Table<PipelineID, Pipeline> pipelineTable;

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMMetadataStoreImpl.class);
  private DBStore store;
  private final OzoneConfiguration configuration;

  /**
   * Constructs the metadata store and starts the DB Services.
   *
   * @param config - Ozone Configuration.
   * @throws IOException - on Failure.
   */
  public SCMMetadataStoreImpl(OzoneConfiguration config)
      throws IOException {
    this.configuration = config;
    start(this.configuration);
  }

  @Override
  public void start(OzoneConfiguration config)
      throws IOException {
    if (this.store == null) {

      this.store = DBStoreBuilder.createDBStore(config, new SCMDBDefinition());

      deletedBlocksTable =
          DELETED_BLOCKS.getTable(this.store);

      checkTableStatus(deletedBlocksTable,
          DELETED_BLOCKS.getName());

      validCertsTable = VALID_CERTS.getTable(store);

      checkTableStatus(validCertsTable, VALID_CERTS.getName());

      revokedCertsTable = REVOKED_CERTS.getTable(store);

      checkTableStatus(revokedCertsTable, REVOKED_CERTS.getName());

      pipelineTable = PIPELINES.getTable(store);

      containerTable = CONTAINERS.getTable(store);
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
  public Table<Long, DeletedBlocksTransaction> getDeletedBlocksTXTable() {
    return deletedBlocksTable;
  }


  @Override
  public Table<BigInteger, X509Certificate> getValidCertsTable() {
    return validCertsTable;
  }

  @Override
  public Table<BigInteger, X509Certificate> getRevokedCertsTable() {
    return revokedCertsTable;
  }

  @Override
  public TableIterator getAllCerts(CertificateStore.CertType certType) {
    if(certType == CertificateStore.CertType.VALID_CERTS) {
      return validCertsTable.iterator();
    }

    if(certType == CertificateStore.CertType.REVOKED_CERTS) {
      return revokedCertsTable.iterator();
    }

    return null;
  }

  @Override
  public Table<PipelineID, Pipeline> getPipelineTable() {
    return pipelineTable;
  }

  @Override
  public BatchOperationHandler getBatchHandler() {
    return this.store;
  }

  @Override
  public Table<ContainerID, ContainerInfo> getContainerTable() {
    return containerTable;
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
