/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import com.google.common.base.Preconditions;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocolPB.SecretKeyProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.RemoveSCMRequest;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.scm.metadata.SCMDBTransactionBufferImpl;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.security.SecretKeyManagerService;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.utils.HAUtils;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.hdds.ExitManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.apache.hadoop.hdds.utils.HddsServerUtil.getSecretKeyClientForScm;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL_DEFAULT;

/**
 * SCMHAManagerImpl uses Apache Ratis for HA implementation. We will have 2N+1
 * node Ratis ring. The Ratis ring will have one Leader node and 2N follower
 * nodes.
 *
 * TODO
 *
 */
public class SCMHAManagerImpl implements SCMHAManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMHAManagerImpl.class);

  private final SCMRatisServer ratisServer;
  private final ConfigurationSource conf;
  private final OzoneConfiguration ozoneConf;
  private final SecurityConfig securityConfig;
  private final DBTransactionBuffer transactionBuffer;
  private final SCMSnapshotProvider scmSnapshotProvider;
  private final StorageContainerManager scm;
  private ExitManager exitManager;

  // this should ideally be started only in a ratis leader
  private final InterSCMGrpcProtocolService grpcServer;
  private BackgroundSCMService trxBufferMonitorService = null;

  /**
   * Creates SCMHAManager instance.
   */
  public SCMHAManagerImpl(final ConfigurationSource conf,
      final SecurityConfig securityConfig,
      final StorageContainerManager scm) throws IOException {
    this.conf = conf;
    this.ozoneConf = OzoneConfiguration.of(conf);
    this.securityConfig = securityConfig;
    this.scm = scm;
    this.exitManager = new ExitManager();
    if (SCMHAUtils.isSCMHAEnabled(conf)) {
      this.transactionBuffer = new SCMHADBTransactionBufferImpl(scm);
      this.ratisServer = new SCMRatisServerImpl(conf, scm,
          (SCMHADBTransactionBuffer) transactionBuffer);
      this.scmSnapshotProvider = newScmSnapshotProvider(scm);
      grpcServer = new InterSCMGrpcProtocolService(conf, scm);
    } else {
      this.transactionBuffer = new SCMDBTransactionBufferImpl();
      this.scmSnapshotProvider = null;
      this.grpcServer = null;
      this.ratisServer = null;
    }

  }

  @VisibleForTesting
  protected SCMSnapshotProvider newScmSnapshotProvider(StorageContainerManager storageContainerManager) {
    return new SCMSnapshotProvider(storageContainerManager.getConfiguration(),
        storageContainerManager.getSCMHANodeDetails().getPeerNodeDetails(),
        storageContainerManager.getScmCertificateClient());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void start() throws IOException {
    if (ratisServer == null) {
      return;
    }
    ratisServer.start();
    if (ratisServer.getDivision().getGroup().getPeers().isEmpty()) {
      // this is a bootstrapped node
      // It will first try to add itself to existing ring
      final SCMNodeDetails nodeDetails =
          scm.getSCMHANodeDetails().getLocalNodeDetails();
      final boolean success = HAUtils.addSCM(ozoneConf,
          new AddSCMRequest.Builder().setClusterId(scm.getClusterId())
              .setScmId(scm.getScmId())
              .setRatisAddr(nodeDetails
                  // TODO : Should we use IP instead of hostname??
                  .getRatisHostPortStr()).build(), scm.getSCMNodeId());
      if (!success) {
        throw new IOException("Adding SCM to existing HA group failed");
      } else {
        LOG.info("Successfully added SCM {} to group {}",
            nodeDetails.getNodeId(), ratisServer.getDivision().getGroup());
      }
    } else {
      LOG.info(" scm role is {} peers {}",
          ratisServer.getDivision().getInfo().getCurrentRole(),
          ratisServer.getDivision().getGroup().getPeers());
    }
    grpcServer.start();
    createStartTransactionBufferMonitor();
  }

  private void createStartTransactionBufferMonitor() {
    long interval = conf.getTimeDuration(
        OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL,
        OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    SCMHATransactionBufferMonitorTask monitorTask
        = new SCMHATransactionBufferMonitorTask(
        (SCMHADBTransactionBuffer) transactionBuffer, ratisServer, interval);
    trxBufferMonitorService =
        new BackgroundSCMService.Builder().setClock(scm.getSystemClock())
            .setScmContext(scm.getScmContext())
            .setServiceName("SCMHATransactionMonitor")
            .setIntervalInMillis(interval)
            .setWaitTimeInMillis(interval)
            .setPeriodicalTask(monitorTask).build();
    scm.getSCMServiceManager().register(trxBufferMonitorService);
    trxBufferMonitorService.start();
  }

  public SCMRatisServer getRatisServer() {
    return ratisServer;
  }

  @Override
  public DBTransactionBuffer getDBTransactionBuffer() {
    return transactionBuffer;
  }

  @Override
  public SCMSnapshotProvider getSCMSnapshotProvider() {
    return scmSnapshotProvider;
  }

  @Override
  public SCMHADBTransactionBuffer asSCMHADBTransactionBuffer() {
    Preconditions
        .checkArgument(transactionBuffer instanceof SCMHADBTransactionBuffer);
    return (SCMHADBTransactionBuffer)transactionBuffer;

  }

  @Override
  public DBCheckpoint downloadCheckpointFromLeader(String leaderId) {
    if (scmSnapshotProvider == null) {
      LOG.error("SCM Snapshot Provider is not configured as there are no peer "
          + "nodes.");
      return null;
    }

    DBCheckpoint dBCheckpoint = getDBCheckpointFromLeader(leaderId);
    if (dBCheckpoint != null) {
      LOG.info("Downloaded checkpoint from Leader {} to the location {}",
          leaderId, dBCheckpoint.getCheckpointLocation());
    }
    return dBCheckpoint;
  }

  @Override
  public List<ManagedSecretKey> getSecretKeysFromLeader(String leaderID)
      throws IOException {
    if (!SecretKeyManagerService.isSecretKeyEnable(securityConfig)) {
      return null;
    }

    LOG.info("Getting secret keys from leader {}.", leaderID);
    try (SecretKeyProtocolClientSideTranslatorPB secretKeyProtocol =
             getSecretKeyClientForScm(conf, leaderID,
                 UserGroupInformation.getLoginUser())) {
      return secretKeyProtocol.getAllSecretKeys();
    }
  }

  private TransactionInfo getTransactionInfoFromCheckpoint(Path checkpointLocation) throws IOException {
    return HAUtils.getTrxnInfoFromCheckpoint(
        ozoneConf, checkpointLocation, SCMDBDefinition.get());
  }

  @Override
  public TermIndex verifyCheckpointFromLeader(String leaderId,
                                              DBCheckpoint checkpoint) {
    try {
      Path checkpointLocation = checkpoint.getCheckpointLocation();
      final TransactionInfo checkpointTxnInfo = getTransactionInfoFromCheckpoint(checkpointLocation);
      LOG.info("{}: Verify checkpoint {} from leader {}", scm.getScmId(), checkpointTxnInfo, leaderId);

      TermIndex termIndex =
          getRatisServer().getSCMStateMachine().getLastAppliedTermIndex();
      long lastAppliedIndex = termIndex.getIndex();

      // Check if current applied log index is smaller than the downloaded
      // checkpoint transaction index. If yes, proceed by stopping the ratis
      // server so that the SCM state can be re-initialized. If no then do not
      // proceed with installSnapshot.
      boolean canProceed = HAUtils.verifyTransactionInfo(checkpointTxnInfo,
          lastAppliedIndex, leaderId, checkpointLocation, LOG);

      if (!canProceed) {
        LOG.warn("Cannot proceed with InstallSnapshot as SCM is at TermIndex {}"
            + " and checkpoint has lower TermIndex {}. Reloading old"
            + " state of SCM.", termIndex, checkpointTxnInfo.getTermIndex());
        return null;
      }
      return checkpointTxnInfo.getTermIndex();
    } catch (Exception ex) {
      LOG.error("Failed to install snapshot from Leader SCM.", ex);
      return null;
    }
  }

  /**
   * Download the latest SCM DB checkpoint from the leader SCM.
   *
   * @param leaderId SCMNodeID of the leader SCM node.
   * @return latest DB checkpoint from leader SCM.
   */
  private DBCheckpoint getDBCheckpointFromLeader(String leaderId) {
    LOG.info("Downloading checkpoint from leader SCM {} and reloading state " +
        "from the checkpoint.", leaderId);

    try {
      return scmSnapshotProvider.getSCMDBSnapshot(leaderId);
    } catch (Exception e) {
      LOG.error("Failed to download checkpoint from SCM leader {}", leaderId,
          e);
    }
    return null;
  }

  @Override
  public TermIndex installCheckpoint(DBCheckpoint dbCheckpoint)
      throws Exception {

    Path checkpointLocation = dbCheckpoint.getCheckpointLocation();
    final TransactionInfo checkpointTrxnInfo = getTransactionInfoFromCheckpoint(checkpointLocation);

    LOG.info("{}: Install checkpoint {}", scm.getScmId(), checkpointTrxnInfo);

    return installCheckpoint(checkpointLocation, checkpointTrxnInfo);
  }

  public TermIndex installCheckpoint(Path checkpointLocation,
      TransactionInfo checkpointTxnInfo) throws Exception {
    // we have passed verification of the termIndex of checkpoint
    // in verifyCheckpointFromLeader
    TermIndex termIndex =
        getRatisServer().getSCMStateMachine().getLastAppliedTermIndex();
    if (checkpointTxnInfo.getTermIndex().compareTo(termIndex) < 0) {
      LOG.warn("Cannot proceed with InstallSnapshot as SCM is at TermIndex {} "
          + "and checkpoint has lower TermIndex {}. Reloading old "
          + "state of SCM.", termIndex, checkpointTxnInfo.getTermIndex());
      throw new IOException("checkpoint is too older to install.");
    }

    long term = checkpointTxnInfo.getTerm();
    long lastAppliedIndex = checkpointTxnInfo.getTransactionIndex();

    File oldDBLocation = scm.getScmMetadataStore().getStore().getDbLocation();

    try {
      // Stop services
      stopServices();
    } catch (Exception e) {
      LOG.error("Failed to stop/ pause the services. Cannot proceed with "
          + "installing the new checkpoint.");
      startServices();
      throw e;
    }

    File dbBackup;
    try {
      dbBackup = HAUtils
          .replaceDBWithCheckpoint(lastAppliedIndex, oldDBLocation,
              checkpointLocation, OzoneConsts.SCM_DB_BACKUP_PREFIX);
      LOG.info("Replaced DB with checkpoint, term: {}, index: {}",
          term, lastAppliedIndex);
    } catch (Exception e) {
      // If we are not able to install latest checkpoint we should throw
      // this exception. In this way reinitialize can throw exception to
      // ratis to handle properly.
      LOG.error("Failed to install Snapshot as SCM failed to replace"
          + " DB with downloaded checkpoint. Checkpoint transaction {}", e,
          checkpointTxnInfo.getTransactionIndex());
      throw e;
    }

    // Reload the DB store with the new checkpoint.
    try {
      reloadSCMState();
      LOG.info("Reloaded SCM state with Term: {} and Index: {}", term,
          lastAppliedIndex);
    } catch (Exception ex) {
      LOG.info("Failed to reload SCM state with Term: {} and Index: {}", term,
          lastAppliedIndex);
      // revert to the old db, since the new db may be a corrupted one
      // so that SCM can restart from the old db.
      try {
        if (dbBackup != null) {
          dbBackup =
              HAUtils.replaceDBWithCheckpoint(lastAppliedIndex, oldDBLocation,
                  dbBackup.toPath(), OzoneConsts.SCM_DB_BACKUP_PREFIX);
          LOG.error("Replacing SCM state with Term : {} and Index:",
              termIndex.getTerm(), termIndex.getTerm());
          // This is being done to check before stop with old db
          // try to reload and then finally terminate and also test has
          // assumption for re-verify after corrupt DB loading without
          // reloadSCMState call test fails with NPE when finding db location.
          reloadSCMState();
        }
      } finally {
        String errorMsg = "Failed to reload SCM state and instantiate " +
            "services.";
        exitManager.exitSystem(1, errorMsg, ex, LOG);
      }
    }

    // Delete the backup DB
    try {
      if (dbBackup != null) {
        FileUtils.deleteFully(dbBackup);
      }
    } catch (Exception e) {
      LOG.error("Failed to delete the backup of the original DB {}",
          dbBackup);
    }

    return checkpointTxnInfo.getTermIndex();
  }


  /**
   * Re-instantiate MetadataManager with new DB checkpoint.
   * All the classes which use/ store MetadataManager should also be updated
   * with the new MetadataManager instance.
   */
  void reloadSCMState()
      throws IOException {
    startServices();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void stop() throws IOException {
    if (ratisServer != null) {
      ratisServer.stop();
      grpcServer.stop();
      close();
    }
    if (trxBufferMonitorService != null) {
      trxBufferMonitorService.stop();
    }
  }

  /**
   * Releases resources that are allocated even if not {@link #start()}ed.
   */
  @Override
  public void close() {
    IOUtils.close(LOG, transactionBuffer);
  }

  @Override
  public boolean addSCM(AddSCMRequest request) throws IOException {
    String clusterId = scm.getClusterId();
    if (!request.getClusterId().equals(scm.getClusterId())) {
      throw new IOException(
          "SCM " + request.getScmId() + " with addr " + request.getRatisAddr()
              + " has cluster Id " + request.getClusterId()
              + " but leader SCM cluster id is " + clusterId);
    }
    Preconditions.checkNotNull(
        getRatisServer().getDivision().getGroup().getGroupId());
    return getRatisServer().addSCM(request);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean removeSCM(RemoveSCMRequest request) throws IOException {

    String clusterId = scm.getClusterId();
    if (!request.getClusterId().equals(scm.getClusterId())) {
      throw new IOException("SCM " + request.getScmId() +
          " with address " + request.getRatisAddr() +
          " has cluster Id " + request.getClusterId() +
          " but leader SCM cluster id is " + clusterId);
    }
    Preconditions.checkNotNull(ratisServer.getDivision().getGroup());
    return ratisServer.removeSCM(request);
  }


  void stopServices() throws Exception {

    // just stop the SCMMetaData store. All other background
    // services will be in pausing state in the follower.
    scm.getScmMetadataStore().stop();
  }

  @VisibleForTesting
   public void startServices() throws IOException {

   // TODO: Fix the metrics ??
    final SCMMetadataStore metadataStore = scm.getScmMetadataStore();
    metadataStore.start(ozoneConf);
    scm.getSequenceIdGen().reinitialize(metadataStore.getSequenceIdTable());
    scm.getPipelineManager().reinitialize(metadataStore.getPipelineTable());
    scm.getContainerManager().reinitialize(metadataStore.getContainerTable());
    scm.getScmBlockManager().getDeletedBlockLog().reinitialize(
        metadataStore.getDeletedBlocksTXTable());
    scm.getStatefulServiceStateManager().reinitialize(
        metadataStore.getStatefulServiceConfigTable());
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      if (scm.getRootCertificateServer() != null) {
        scm.getRootCertificateServer().reinitialize(metadataStore);
      }
      scm.getScmCertificateServer().reinitialize(metadataStore);
    }
    // This call also performs upgrade finalization if the new table contains a
    // higher metadata layout version than the SCM's current one.
    scm.getFinalizationManager().reinitialize(metadataStore.getMetaTable());
  }

  @VisibleForTesting
  public void setExitManagerForTesting(ExitManager exitManagerForTesting) {
    this.exitManager = exitManagerForTesting;
  }

  @VisibleForTesting
  public void stopGrpcService() {
    grpcServer.stop();
  }

  @VisibleForTesting
  public static Logger getLogger() {
    return LOG;
  }
}
