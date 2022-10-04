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

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.MoveDataNodePair;
import org.apache.hadoop.hdds.security.x509.certificate.CertInfo;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperationHandler;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;

import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.CONTAINERS;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.CRLS;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.CRL_SEQUENCE_ID;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.DELETED_BLOCKS;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.MOVE;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.PIPELINES;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.REVOKED_CERTS;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.REVOKED_CERTS_V2;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.STATEFUL_SERVICE_CONFIG;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.TRANSACTIONINFO;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.VALID_CERTS;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.VALID_SCM_CERTS;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.SEQUENCE_ID;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.META;
import static org.apache.hadoop.ozone.OzoneConsts.DB_TRANSIENT_MARKER;

import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A RocksDB based implementation of SCM Metadata Store.
 *
 */
public class SCMMetadataStoreImpl implements SCMMetadataStore {

  public static final Set<DBColumnFamilyDefinition<?, ?>> COLUMN_FAMILIES =
      new HashSet<>(Arrays.asList(
          DELETED_BLOCKS,
          VALID_CERTS,
          VALID_SCM_CERTS,
          REVOKED_CERTS,
          REVOKED_CERTS_V2,
          CONTAINERS,
          PIPELINES,
          TRANSACTIONINFO,
          CRLS,
          SEQUENCE_ID,
          MOVE,
          META,
          STATEFUL_SERVICE_CONFIG
      ));

  private Table<Long, DeletedBlocksTransaction> deletedBlocksTable;

  private Table<BigInteger, X509Certificate> validCertsTable;

  private Table<BigInteger, X509Certificate> validSCMCertsTable;

  private Table<BigInteger, X509Certificate> revokedCertsTable;

  private Table<BigInteger, CertInfo> revokedCertsV2Table;

  private Table<ContainerID, ContainerInfo> containerTable;

  private Table<PipelineID, Pipeline> pipelineTable;

  private Table<String, TransactionInfo> transactionInfoTable;

  private Table<Long, CRLInfo> crlInfoTable;

  private Table<String, Long> crlSequenceIdTable;

  private Table<String, Long> sequenceIdTable;

  private Table<ContainerID, MoveDataNodePair> moveTable;

  private Table<String, String> metaTable;

  private Table<String, ByteString> statefulServiceConfigTable;

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMMetadataStoreImpl.class);
  private DBStore store;
  private final OzoneConfiguration configuration;

  private SCMMetadataStoreMetrics metrics;
  private Map<String, Table<?, ?>> tableMap = new ConcurrentHashMap<>();

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

      File metaDir = HAUtils.getMetaDir(new SCMDBDefinition(), configuration);
      // Check if there is a DB Inconsistent Marker in the metaDir. This
      // marker indicates that the DB is in an inconsistent state and hence
      // the OM process should be terminated.
      File markerFile = new File(metaDir, DB_TRANSIENT_MARKER);
      if (markerFile.exists()) {
        LOG.error("File {} marks that SCM DB is in an inconsistent state.",
            markerFile);
        // Note - The marker file should be deleted only after fixing the DB.
        // In an HA setup, this can be done by replacing this DB with a
        // checkpoint from another SCM.
        String errorMsg = "Cannot load SCM DB as it is in an inconsistent " +
            "state.";
        ExitUtils.terminate(1, errorMsg, LOG);
      }


      this.store = DBStoreBuilder.createDBStore(config, new SCMDBDefinition());

      deletedBlocksTable =
          DELETED_BLOCKS.getTable(this.store);

      checkAndPopulateTable(deletedBlocksTable, DELETED_BLOCKS.getName());

      validCertsTable = VALID_CERTS.getTable(store);

      checkAndPopulateTable(validCertsTable, VALID_CERTS.getName());

      validSCMCertsTable = VALID_SCM_CERTS.getTable(store);

      checkAndPopulateTable(validSCMCertsTable, VALID_SCM_CERTS.getName());

      revokedCertsTable = REVOKED_CERTS.getTable(store);

      checkAndPopulateTable(revokedCertsTable, REVOKED_CERTS.getName());

      revokedCertsV2Table = REVOKED_CERTS_V2.getTable(store);

      checkAndPopulateTable(revokedCertsV2Table, REVOKED_CERTS_V2.getName());

      pipelineTable = PIPELINES.getTable(store);

      checkAndPopulateTable(pipelineTable, PIPELINES.getName());

      containerTable = CONTAINERS.getTable(store);

      checkAndPopulateTable(containerTable, CONTAINERS.getName());

      transactionInfoTable = TRANSACTIONINFO.getTable(store);

      checkAndPopulateTable(transactionInfoTable, TRANSACTIONINFO.getName());

      crlInfoTable = CRLS.getTable(store);

      checkAndPopulateTable(crlInfoTable, CRLS.getName());

      crlSequenceIdTable = CRL_SEQUENCE_ID.getTable(store);

      checkAndPopulateTable(crlInfoTable, CRL_SEQUENCE_ID.getName());

      sequenceIdTable = SEQUENCE_ID.getTable(store);

      checkAndPopulateTable(sequenceIdTable, SEQUENCE_ID.getName());

      moveTable = MOVE.getTable(store);

      checkAndPopulateTable(moveTable, MOVE.getName());

      metaTable = META.getTable(store);

      checkAndPopulateTable(moveTable, META.getName());

      statefulServiceConfigTable = STATEFUL_SERVICE_CONFIG.getTable(store);

      checkAndPopulateTable(statefulServiceConfigTable,
          STATEFUL_SERVICE_CONFIG.getName());

      metrics = SCMMetadataStoreMetrics.create(this);
    }
  }

  @Override
  public void stop() throws Exception {
    if (store != null) {
      store.close();
      store = null;
    }
    if (metrics != null) {
      metrics.unRegister();
      metrics = null;
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
  public Table<BigInteger, X509Certificate> getValidSCMCertsTable() {
    return validSCMCertsTable;
  }

  @Override
  public Table<BigInteger, X509Certificate> getRevokedCertsTable() {
    return revokedCertsTable;
  }

  @Override
  public Table<BigInteger, CertInfo> getRevokedCertsV2Table() {
    return revokedCertsV2Table;
  }

  /**
   * A table that maintains X509 Certificate Revocation Lists and its metadata.
   *
   * @return Table.
   */
  @Override
  public Table<Long, CRLInfo> getCRLInfoTable() {
    return crlInfoTable;
  }

  /**
   * A table that maintains the last CRL SequenceId. This helps to make sure
   * that the CRL Sequence Ids are monotonically increasing.
   *
   * @return Table.
   */
  @Override
  public Table<String, Long> getCRLSequenceIdTable() {
    return crlSequenceIdTable;
  }

  @Override
  public TableIterator getAllCerts(CertificateStore.CertType certType) {
    if (certType == CertificateStore.CertType.VALID_CERTS) {
      return validCertsTable.iterator();
    }

    if (certType == CertificateStore.CertType.REVOKED_CERTS) {
      return revokedCertsTable.iterator();
    }

    return null;
  }

  @Override
  public Table<PipelineID, Pipeline> getPipelineTable() {
    return pipelineTable;
  }

  @Override
  public Table<String, TransactionInfo> getTransactionInfoTable() {
    return transactionInfoTable;
  }

  @Override
  public BatchOperationHandler getBatchHandler() {
    return this.store;
  }

  @Override
  public Table<ContainerID, ContainerInfo> getContainerTable() {
    return containerTable;
  }

  @Override
  public Table<String, Long> getSequenceIdTable() {
    return sequenceIdTable;
  }

  @Override
  public Table<ContainerID, MoveDataNodePair> getMoveTable() {
    return moveTable;
  }

  @Override
  public Table<String, String> getMetaTable() {
    return metaTable;
  }

  public Table<String, ByteString> getStatefulServiceConfigTable() {
    return statefulServiceConfigTable;
  }

  private void checkAndPopulateTable(Table table, String name)
      throws IOException {
    String logMessage = "Unable to get a reference to %s table. Cannot " +
        "continue.";
    String errMsg = "Inconsistent DB state, Table - %s. Please check the" +
        " logs for more info.";
    if (table == null) {
      LOG.error(String.format(logMessage, name));
      throw new IOException(String.format(errMsg, name));
    }
    tableMap.put(name, table);
  }

  public String getEstimatedKeyCountStr() {
    Gson gson = new Gson();
    return gson.toJson(tableMap.entrySet().stream().map(e -> {
      try {
        return e.getKey() + " : " + e.getValue().getEstimatedKeyCount();
      } catch (IOException ex) {
        ex.printStackTrace();
      }
      return "N/A";
    }).collect(Collectors.toList()));
  }

  Map<String, Table<?, ?>> getTableMap() {
    return tableMap;
  }

  SCMMetadataStoreMetrics getMetrics() {
    return metrics;
  }
}
