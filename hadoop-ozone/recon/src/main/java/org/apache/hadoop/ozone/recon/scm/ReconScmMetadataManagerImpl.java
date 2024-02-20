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

package org.apache.hadoop.ozone.recon.scm;

import com.google.protobuf.ByteString;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.MoveDataNodePair;
import org.apache.hadoop.hdds.scm.metadata.BigIntegerCodec;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.scm.metadata.X509CertificateCodec;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.security.x509.certificate.CertInfo;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.ByteStringCodec;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.security.cert.X509Certificate;

import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.CONTAINERS;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.CRLS;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.CRL_SEQUENCE_ID;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.DELETED_BLOCKS;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.META;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.MOVE;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.PIPELINES;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.REVOKED_CERTS;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.REVOKED_CERTS_V2;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.SEQUENCE_ID;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.STATEFUL_SERVICE_CONFIG;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.TRANSACTIONINFO;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.VALID_CERTS;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.VALID_SCM_CERTS;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_SCM_SNAPSHOT_DB;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_DB_DIR;

/**
 * Recon's implementation of the SCM Metadata manager. By extending and
 * relying on the SCMMetadataStoreImpl, we can make sure all changes made to
 * schema in SCM will be automatically picked up by Recon.
 */
@Singleton
public class ReconScmMetadataManagerImpl extends SCMMetadataStoreImpl
    implements ReconScmMetadataManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconScmMetadataManagerImpl.class);

  private OzoneConfiguration ozoneConfiguration;
  private ReconUtils reconUtils;
  private boolean scmTablesInitialized = false;

  @Inject
  public ReconScmMetadataManagerImpl(OzoneConfiguration configuration,
                                     ReconUtils reconUtils) throws IOException {
    this.reconUtils = reconUtils;
    this.ozoneConfiguration = configuration;
  }

  @Override
  public void start(OzoneConfiguration configuration) throws IOException {
    LOG.info("Starting ReconScmMetadataManagerImpl...");
    File reconDbDir =
        reconUtils.getReconDbDir(configuration, OZONE_RECON_SCM_DB_DIR);
    File lastKnownSCMSnapshot =
        reconUtils.getLastKnownDB(reconDbDir, RECON_SCM_SNAPSHOT_DB);
    if (lastKnownSCMSnapshot != null) {
      LOG.info("Last known snapshot for SCM : {}",
          lastKnownSCMSnapshot.getAbsolutePath());
      initializeNewRdbStore(lastKnownSCMSnapshot);
    }
  }

  /**
   * Replace existing DB instance with new one.
   *
   * @param dbFile new DB file location.
   */
  private void initializeNewRdbStore(File dbFile) throws IOException {
    try {
      DBStoreBuilder dbStoreBuilder =
          DBStoreBuilder.newBuilder(ozoneConfiguration)
          .setName(dbFile.getName())
          .setPath(dbFile.toPath().getParent());
      addScmTablesAndCodecs(dbStoreBuilder);
      setStore(dbStoreBuilder.build());
      LOG.info("Created SCM DB handle from snapshot at {}.",
          dbFile.getAbsolutePath());
    } catch (IOException ioEx) {
      LOG.error("Unable to initialize Recon SCM DB snapshot store.", ioEx);
    }
    if (getStore() != null) {
      initializeScmTables();
      scmTablesInitialized = true;
    }
  }

  @Override
  public void updateScmDB(File newDbLocation) throws IOException {
    if (getStore() != null) {
      File oldDBLocation = getStore().getDbLocation();
      if (oldDBLocation.exists()) {
        LOG.info("Cleaning up old SCM snapshot db at {}.",
            oldDBLocation.getAbsolutePath());
        FileUtils.deleteDirectory(oldDBLocation);
      }
    }
    DBStore current = getStore();
    try {
      initializeNewRdbStore(newDbLocation);
    } finally {
      // Always close DBStore if it's replaced.
      if (current != null && current != getStore()) {
        current.close();
      }
    }
  }

  /**
   * Get SCM metadata RocksDB's latest sequence number.
   * @return latest sequence number.
   */
  @Override
  public long getLastSequenceNumberFromDB() {
    RDBStore rocksDBStore = (RDBStore) getStore();
    if (null == rocksDBStore) {
      return 0;
    } else {
      try {
        return rocksDBStore.getDb().getLatestSequenceNumber();
      } catch (IOException e) {
        return 0;
      }
    }
  }

  /**
   * Check if SCM tables are initialized.
   * @return true if SCM Tables are initialized, otherwise false.
   */
  @Override
  public boolean isScmTablesInitialized() {
    return scmTablesInitialized;
  }

  public DBStoreBuilder addScmTablesAndCodecs(DBStoreBuilder builder) throws IOException {

    return builder.addTable(DELETED_BLOCKS.getName())
        .addTable(VALID_CERTS.getName())
        .addTable(VALID_SCM_CERTS.getName())
        .addTable(REVOKED_CERTS.getName())
        .addTable(REVOKED_CERTS_V2.getName())
        .addTable(CONTAINERS.getName())
        .addTable(PIPELINES.getName())
        .addTable(TRANSACTIONINFO.getName())
        .addTable(CRLS.getName())
        .addTable(CRL_SEQUENCE_ID.getName())
        .addTable(SEQUENCE_ID.getName())
        .addTable(MOVE.getName())
        .addTable(META.getName())
        .addTable(STATEFUL_SERVICE_CONFIG.getName())
        .addCodec(Long.class, LongCodec.get())
        .addCodec(CRLInfo.class, CRLInfo.getCodec())
        .addProto2Codec(StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction.getDefaultInstance())
        .addCodec(BigInteger.class, BigIntegerCodec.get())
        .addCodec(X509Certificate.class, X509CertificateCodec.get())
        .addCodec(CertInfo.class, CertInfo.getCodec())
        .addCodec(PipelineID.class, PipelineID.getCodec())
        .addCodec(Pipeline.class, Pipeline.getCodec())
        .addCodec(ContainerID.class, ContainerID.getCodec())
        .addCodec(MoveDataNodePair.class, MoveDataNodePair.getCodec())
        .addCodec(ContainerInfo.class, ContainerInfo.getCodec())
        .addCodec(String.class, StringCodec.get())
        .addCodec(ByteString.class, ByteStringCodec.get())
        .addCodec(TransactionInfo.class, TransactionInfo.getCodec());
  }

  /**
   * Return table mapped to the specified table name.
   *
   * @param tableName
   * @return Table
   */
  @Override
  public Table getTable(String tableName) {
    Table table = getTableMap().get(tableName);
    if (table == null) {
      throw  new IllegalArgumentException("Unknown table " + tableName);
    }
    return table;
  }
}
