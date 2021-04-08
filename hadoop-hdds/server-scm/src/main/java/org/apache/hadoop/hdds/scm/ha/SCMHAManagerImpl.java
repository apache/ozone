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
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.scm.metadata.SCMDBTransactionBufferImpl;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.HAUtils;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.util.ExitManager;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

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
  private final DBTransactionBuffer transactionBuffer;
  private final SCMSnapshotProvider scmSnapshotProvider;
  private final StorageContainerManager scm;
  private ExitManager exitManager;

  // this should ideally be started only in a ratis leader
  private final InterSCMGrpcProtocolService grpcServer;

  /**
   * Creates SCMHAManager instance.
   */
  public SCMHAManagerImpl(final ConfigurationSource conf,
      final StorageContainerManager scm) throws IOException {
    this.conf = conf;
    this.scm = scm;
    if (SCMHAUtils.isSCMHAEnabled(conf)) {
      this.transactionBuffer = new SCMHADBTransactionBufferImpl(scm);
      this.ratisServer = new SCMRatisServerImpl(conf, scm,
          (SCMHADBTransactionBuffer) transactionBuffer);
      this.scmSnapshotProvider = new SCMSnapshotProvider(conf,
          scm.getSCMHANodeDetails().getPeerNodeDetails());
      grpcServer = new InterSCMGrpcProtocolService(conf, scm);
    } else {
      this.transactionBuffer = new SCMDBTransactionBufferImpl();
      this.scmSnapshotProvider = null;
      this.grpcServer = null;
      this.ratisServer = null;
    }

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
      boolean success = HAUtils.addSCM(OzoneConfiguration.of(conf),
          new AddSCMRequest.Builder().setClusterId(scm.getClusterId())
              .setScmId(scm.getScmId())
              .setRatisAddr(scm.getSCMHANodeDetails().getLocalNodeDetails()
                  // TODO : Should we use IP instead of hostname??
                  .getRatisHostPortStr()).build(), scm.getSCMNodeId());
      if (!success) {
        throw new IOException("Adding SCM to existing HA group failed");
      }
    } else {
      LOG.info(" scm role is {} peers {}",
          ratisServer.getDivision().getInfo().getCurrentRole(),
          ratisServer.getDivision().getGroup().getPeers());
    }
    grpcServer.start();
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
  /**
   * Download and install latest checkpoint from leader SCM.
   *
   * @param leaderId peerNodeID of the leader SCM
   * @return If checkpoint is installed successfully, return the
   *         corresponding termIndex. Otherwise, return null.
   */
  public TermIndex installSnapshotFromLeader(String leaderId) {
    if (scmSnapshotProvider == null) {
      LOG.error("SCM Snapshot Provider is not configured as there are no peer "
          + "nodes.");
      return null;
    }

    DBCheckpoint dBCheckpoint = getDBCheckpointFromLeader(leaderId);
    LOG.info("Downloaded checkpoint from Leader {} to the location {}",
        leaderId, dBCheckpoint.getCheckpointLocation());

    TermIndex termIndex = null;
    try {
      termIndex = installCheckpoint(leaderId, dBCheckpoint);
    } catch (Exception ex) {
      LOG.error("Failed to install snapshot from Leader SCM.", ex);
    }
    return termIndex;
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
    } catch (IOException e) {
      LOG.error("Failed to download checkpoint from SCM leader {}", leaderId,
          e);
    }
    return null;
  }

  /**
   * Install checkpoint. If the checkpoints snapshot index is greater than
   * SCM's last applied transaction index, then re-initialize the SCM
   * state via this checkpoint. Before re-initializing SCM state, the SCM Ratis
   * server should be stopped so that no new transactions can be applied.
   */
  @VisibleForTesting
  public TermIndex installCheckpoint(String leaderId, DBCheckpoint dbCheckpoint)
      throws Exception {

    Path checkpointLocation = dbCheckpoint.getCheckpointLocation();
    TransactionInfo checkpointTrxnInfo = HAUtils
        .getTrxnInfoFromCheckpoint(OzoneConfiguration.of(conf),
            checkpointLocation, new SCMDBDefinition());

    LOG.info("Installing checkpoint with SCMTransactionInfo {}",
        checkpointTrxnInfo);

    return installCheckpoint(leaderId, checkpointLocation, checkpointTrxnInfo);
  }

  public TermIndex installCheckpoint(String leaderId, Path checkpointLocation,
      TransactionInfo checkpointTrxnInfo) throws Exception {

    File dbBackup = null;
    TermIndex termIndex =
        getRatisServer().getSCMStateMachine().getLastAppliedTermIndex();
    long term = termIndex.getTerm();
    long lastAppliedIndex = termIndex.getIndex();
    // Check if current applied log index is smaller than the downloaded
    // checkpoint transaction index. If yes, proceed by stopping the ratis
    // server so that the SCM state can be re-initialized. If no then do not
    // proceed with installSnapshot.
    boolean canProceed = HAUtils
        .verifyTransactionInfo(checkpointTrxnInfo, lastAppliedIndex, leaderId,
            checkpointLocation, LOG);
    File oldDBLocation = scm.getScmMetadataStore().getStore().getDbLocation();
    if (canProceed) {
      try {
        // Stop services
        stopServices();

        // Pause the State Machine so that no new transactions can be applied.
        // This action also clears the SCM Double Buffer so that if there
        // are any pending transactions in the buffer, they are discarded.
        getRatisServer().getSCMStateMachine().pause();
      } catch (Exception e) {
        LOG.error("Failed to stop/ pause the services. Cannot proceed with "
            + "installing the new checkpoint.");
        startServices();
        throw e;
      }
      try {
        dbBackup = HAUtils
            .replaceDBWithCheckpoint(lastAppliedIndex, oldDBLocation,
                checkpointLocation, OzoneConsts.SCM_DB_BACKUP_PREFIX);
        term = checkpointTrxnInfo.getTerm();
        lastAppliedIndex = checkpointTrxnInfo.getTransactionIndex();
        LOG.info(
            "Replaced DB with checkpoint from SCM: {}, term: {}, index: {}",
            leaderId, term, lastAppliedIndex);
      } catch (Exception e) {
        LOG.error("Failed to install Snapshot from {} as SCM failed to replace"
            + " DB with downloaded checkpoint. Reloading old SCM state.", e);
      }
      // Reload the DB store with the new checkpoint.
      // Restart (unpause) the state machine and update its last applied index
      // to the installed checkpoint's snapshot index.
      try {
        reloadSCMState();
        getRatisServer().getSCMStateMachine().unpause(term, lastAppliedIndex);
        LOG.info("Reloaded SCM state with Term: {} and Index: {}", term,
            lastAppliedIndex);
      } catch (Exception ex) {
        String errorMsg =
            "Failed to reload SCM state and instantiate services.";
        exitManager.exitSystem(1, errorMsg, ex, LOG);
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
    } else {
      LOG.warn("Cannot proceed with InstallSnapshot as SCM is at TermIndex {} "
          + "and checkpoint has lower TermIndex {}. Reloading old "
          + "state of SCM.", termIndex, checkpointTrxnInfo.getTermIndex());
    }

    if (lastAppliedIndex != checkpointTrxnInfo.getTransactionIndex()) {
      // Install Snapshot failed and old state was reloaded. Return null to
      // Ratis to indicate that installation failed.
      return null;
    }

    TermIndex newTermIndex = TermIndex.valueOf(term, lastAppliedIndex);
    return newTermIndex;
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
  public void shutdown() throws IOException {
    if (ratisServer != null) {
      ratisServer.stop();
      ratisServer.getSCMStateMachine().close();
      grpcServer.stop();
    }
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

  void stopServices() throws Exception {

    // just stop the SCMMetaData store. All other background
    // services will be in pausing state in the follower.
    scm.getScmMetadataStore().stop();
  }

  @VisibleForTesting
   public void startServices() throws IOException {

   // TODO: Fix the metrics ??
    final SCMMetadataStore metadataStore = scm.getScmMetadataStore();
    metadataStore.start(OzoneConfiguration.of(conf));
    scm.getPipelineManager().reinitialize(metadataStore.getPipelineTable());
    scm.getContainerManager().reinitialize(metadataStore.getContainerTable());
    scm.getScmBlockManager().getDeletedBlockLog().reinitialize(
        metadataStore.getDeletedBlocksTXTable());
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      if (scm.getRootCertificateServer() != null) {
        scm.getRootCertificateServer().reinitialize(metadataStore);
      }
      scm.getScmCertificateServer().reinitialize(metadataStore);
    }
  }

  @VisibleForTesting
  public void setExitManagerForTesting(ExitManager exitManagerForTesting) {
    this.exitManager = exitManagerForTesting;
  }

  @VisibleForTesting
  public static Logger getLogger() {
    return LOG;
  }
}
