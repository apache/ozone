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

package org.apache.hadoop.hdds.scm.metadata;

import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.CONTAINERS;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.DELETED_BLOCKS;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.META;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.MOVE;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.PIPELINES;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.SEQUENCE_ID;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.STATEFUL_SERVICE_CONFIG;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.TRANSACTIONINFO;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.VALID_CERTS;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.VALID_SCM_CERTS;
import static org.apache.hadoop.ozone.OzoneConsts.DB_TRANSIENT_MARKER;

import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.MoveDataNodePair;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperationHandler;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A RocksDB based implementation of SCM Metadata Store.
 *
 */
public class SCMMetadataStoreImpl implements SCMMetadataStore {

  private Table<Long, DeletedBlocksTransaction> deletedBlocksTable;

  private Table<BigInteger, X509Certificate> validCertsTable;

  private Table<BigInteger, X509Certificate> validSCMCertsTable;

  private Table<ContainerID, ContainerInfo> containerTable;

  private Table<PipelineID, Pipeline> pipelineTable;

  private Table<String, TransactionInfo> transactionInfoTable;

  private Table<String, Long> sequenceIdTable;

  private Table<ContainerID, MoveDataNodePair> moveTable;

  private Table<String, String> metaTable;

  private Table<String, ByteString> statefulServiceConfigTable;

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMMetadataStoreImpl.class);
  private DBStore store;
  private final OzoneConfiguration configuration;

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
      final SCMDBDefinition scmdbDefinition = SCMDBDefinition.get();
      File metaDir = HAUtils.getMetaDir(scmdbDefinition, configuration);
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

      this.store = DBStoreBuilder.createDBStore(config, scmdbDefinition);

      deletedBlocksTable =
          DELETED_BLOCKS.getTable(this.store);

      checkAndPopulateTable(deletedBlocksTable, DELETED_BLOCKS.getName());

      validCertsTable = VALID_CERTS.getTable(store);

      checkAndPopulateTable(validCertsTable, VALID_CERTS.getName());

      validSCMCertsTable = VALID_SCM_CERTS.getTable(store);

      checkAndPopulateTable(validSCMCertsTable, VALID_SCM_CERTS.getName());

      pipelineTable = PIPELINES.getTable(store);

      checkAndPopulateTable(pipelineTable, PIPELINES.getName());

      containerTable = CONTAINERS.getTable(store);

      checkAndPopulateTable(containerTable, CONTAINERS.getName());

      transactionInfoTable = TRANSACTIONINFO.getTable(store);

      checkAndPopulateTable(transactionInfoTable, TRANSACTIONINFO.getName());

      sequenceIdTable = SEQUENCE_ID.getTable(store);

      checkAndPopulateTable(sequenceIdTable, SEQUENCE_ID.getName());

      moveTable = MOVE.getTable(store);

      checkAndPopulateTable(moveTable, MOVE.getName());

      metaTable = META.getTable(store);

      checkAndPopulateTable(metaTable, META.getName());

      statefulServiceConfigTable = STATEFUL_SERVICE_CONFIG.getTable(store);

      checkAndPopulateTable(statefulServiceConfigTable,
          STATEFUL_SERVICE_CONFIG.getName());
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
  public Table<BigInteger, X509Certificate> getValidSCMCertsTable() {
    return validSCMCertsTable;
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

  @Override
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

  Map<String, Table<?, ?>> getTableMap() {
    return tableMap;
  }
}
