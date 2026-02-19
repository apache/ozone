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

package org.apache.hadoop.hdds.scm.server.upgrade;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.scm.metadata.Replicate;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the state of finalization in SCM.
 */
public class FinalizationStateManagerImpl implements FinalizationStateManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(FinalizationStateManagerImpl.class);

  private Table<String, String> finalizationStore;
  private final DBTransactionBuffer transactionBuffer;
  private final HDDSLayoutVersionManager versionManager;
  // Ensures that we are not in the process of updating checkpoint state as
  // we read it to determine the current checkpoint.
  private final ReadWriteLock checkpointLock;
  // SCM transaction buffer flushes asynchronously, so we must keep the most
  // up-to-date DB information in memory as well for reads.
  private volatile boolean hasFinalizingMark;
  private SCMUpgradeFinalizationContext upgradeContext;
  private final SCMUpgradeFinalizer upgradeFinalizer;

  protected FinalizationStateManagerImpl(Builder builder) throws IOException {
    this.finalizationStore = builder.finalizationStore;
    this.transactionBuffer = builder.transactionBuffer;
    this.upgradeFinalizer = builder.upgradeFinalizer;
    this.versionManager = this.upgradeFinalizer.getVersionManager();
    this.checkpointLock = new ReentrantReadWriteLock();
    initialize();
  }

  private void initialize() throws IOException {
    this.hasFinalizingMark =
        finalizationStore.isExist(OzoneConsts.FINALIZING_KEY);
  }

  private void publishCheckpoint(FinalizationCheckpoint checkpoint) {
    // Move the upgrade status according to this checkpoint. This is sent
    // back to the client if they query for the current upgrade status.
    versionManager.setUpgradeState(checkpoint.getStatus());

    // Check whether this checkpoint change requires us to move node state.
    // If this is necessary, it must be done before unfreezing pipeline
    // creation to make sure nodes are not added to pipelines based on
    // outdated layout information.
    // This operation is not idempotent.
    if (checkpoint == FinalizationCheckpoint.MLV_EQUALS_SLV) {
      upgradeContext.getNodeManager().forceNodesToHealthyReadOnly();
    }

    // Check whether this checkpoint change requires us to freeze pipeline
    // creation. These are idempotent operations.
    PipelineManager pipelineManager = upgradeContext.getPipelineManager();
    if (FinalizationManager.shouldCreateNewPipelines(checkpoint) &&
        pipelineManager.isPipelineCreationFrozen()) {
      pipelineManager.resumePipelineCreation();
    } else if (!FinalizationManager.shouldCreateNewPipelines(checkpoint) &&
          !pipelineManager.isPipelineCreationFrozen()) {
      pipelineManager.freezePipelineCreation();
    }

    // Set the checkpoint in the SCM context so other components can read it.
    upgradeContext.getSCMContext().setFinalizationCheckpoint(checkpoint);
  }

  @Override
  public void setUpgradeContext(SCMUpgradeFinalizationContext context) {
    this.upgradeContext = context;
    FinalizationCheckpoint checkpoint = getFinalizationCheckpoint();
    upgradeContext.getSCMContext().setFinalizationCheckpoint(checkpoint);
    // Set the version manager's upgrade status (sent back to the client to
    // identify upgrade progress) based on the current checkpoint.
    versionManager.setUpgradeState(checkpoint.getStatus());
  }

  @Override
  public void addFinalizingMark() throws IOException {
    checkpointLock.writeLock().lock();
    try {
      hasFinalizingMark = true;
    } finally {
      checkpointLock.writeLock().unlock();
    }
    transactionBuffer.addToBuffer(finalizationStore,
        OzoneConsts.FINALIZING_KEY, "");
    publishCheckpoint(FinalizationCheckpoint.FINALIZATION_STARTED);
  }

  @Override
  public void finalizeLayoutFeatures(Integer startLayoutVersion, Integer endLayoutersion) throws IOException {
    for (int version = startLayoutVersion; version <= endLayoutersion; version++) {
      finalizeLayoutFeatureLocal(version);
    }
  }

  /**
   * A version of finalizeLayoutFeature without the {@link Replicate}
   * annotation that can be called by followers to finalize from a snapshot.
   */
  private void finalizeLayoutFeatureLocal(Integer layoutVersion)
      throws IOException {
    checkpointLock.writeLock().lock();
    try {
      // The VERSION file is the source of truth for the current layout
      // version. This is updated in the replicated finalization steps.
      // Layout version will be written to the DB as well so followers can
      // finalize from a snapshot.
      HDDSLayoutFeature feature =
          (HDDSLayoutFeature)versionManager.getFeature(layoutVersion);
      upgradeFinalizer.replicatedFinalizationSteps(feature, upgradeContext);
    } finally {
      checkpointLock.writeLock().unlock();
    }

    if (!versionManager.needsFinalization()) {
      publishCheckpoint(FinalizationCheckpoint.MLV_EQUALS_SLV);
    }
    transactionBuffer.addToBuffer(finalizationStore,
        OzoneConsts.LAYOUT_VERSION_KEY, String.valueOf(layoutVersion));
  }

  @Override
  public void removeFinalizingMark() throws IOException {
    checkpointLock.writeLock().lock();
    try {
      hasFinalizingMark = false;
    } finally {
      checkpointLock.writeLock().unlock();
    }
    transactionBuffer.removeFromBuffer(finalizationStore,
        OzoneConsts.FINALIZING_KEY);

    // All prior checkpoints should have been crossed when this method is
    // called, leaving us at the finalization complete checkpoint.
    // If this is not the case, this SCM (leader or follower) has encountered
    // a bug leaving it in an inconsistent upgrade finalization state.
    // It should terminate to avoid further damage.
    FinalizationCheckpoint checkpoint = getFinalizationCheckpoint();
    if (checkpoint != FinalizationCheckpoint.FINALIZATION_COMPLETE) {
      String errorMessage = String.format("SCM upgrade finalization " +
              "is in an unknown state. Expected %s but was %s",
          FinalizationCheckpoint.FINALIZATION_COMPLETE, checkpoint);
      ExitUtils.terminate(1, errorMessage, LOG);
    }

    publishCheckpoint(FinalizationCheckpoint.FINALIZATION_COMPLETE);
  }

  @Override
  public boolean crossedCheckpoint(FinalizationCheckpoint query) {
    return getFinalizationCheckpoint().hasCrossed(query);
  }

  @Override
  public FinalizationCheckpoint getFinalizationCheckpoint() {
    // Get a point-in-time snapshot of the finalization state under the lock,
    // then use this to determine which checkpoint we were on at that time.
    boolean mlvBehindSlvSnapshot;
    boolean hasFinalizingMarkSnapshot;
    checkpointLock.readLock().lock();
    try {
      mlvBehindSlvSnapshot = versionManager.needsFinalization();
      hasFinalizingMarkSnapshot = hasFinalizingMark;
    } finally {
      checkpointLock.readLock().unlock();
    }

    FinalizationCheckpoint currentCheckpoint = null;
    for (FinalizationCheckpoint checkpoint: FinalizationCheckpoint.values()) {
      if (checkpoint.isCurrent(hasFinalizingMarkSnapshot,
          mlvBehindSlvSnapshot)) {
        currentCheckpoint = checkpoint;
        break;
      }
    }

    // SCM cannot function if it does not know which finalization checkpoint
    // it is on, so it must terminate. This should only happen in the case of
    // a serious bug.
    if (currentCheckpoint == null) {
      String errorMessage = String.format("SCM upgrade finalization " +
              "is in an unknown state.%nFinalizing mark present? %b%n" +
              "Metadata layout version behind software layout version? %b",
          hasFinalizingMarkSnapshot, mlvBehindSlvSnapshot);
      ExitUtils.terminate(1, errorMessage, LOG);
    }

    return currentCheckpoint;
  }

  /**
   * Called on snapshot installation.
   */
  @Override
  public void reinitialize(Table<String, String> newFinalizationStore)
      throws IOException {
    checkpointLock.writeLock().lock();
    try {
      this.finalizationStore = newFinalizationStore;
      initialize();

      int dbLayoutVersion = getDBLayoutVersion();
      int currentLayoutVersion = versionManager.getMetadataLayoutVersion();
      if (currentLayoutVersion < dbLayoutVersion) {
        // Snapshot contained a higher metadata layout version. Finalize this
        // follower SCM as a result.
        LOG.info("New SCM snapshot received with metadata layout version {}, " +
                "which is higher than this SCM's metadata layout version {}." +
                "Attempting to finalize current SCM to that version.",
            dbLayoutVersion, currentLayoutVersion);
        // Since the SCM is finalizing from a snapshot, it is a follower, and
        // does not need to run the leader only finalization driving actions
        // that the UpgradeFinalizationExecutor contains. Just run the
        // upgrade actions for the layout features, set the finalization
        // checkpoint, and increase the version in the VERSION file.
        for (int version = currentLayoutVersion + 1; version <= dbLayoutVersion;
             version++) {
          finalizeLayoutFeatureLocal(version);
        }
      }
      publishCheckpoint(getFinalizationCheckpoint());
    } catch (Exception ex) {
      LOG.error("Failed to reinitialize finalization state", ex);
      throw new IOException(ex);
    } finally {
      checkpointLock.writeLock().unlock();
    }
  }

  /**
   * Gets the metadata layout version from the SCM RocksDB. This is used for
   * Ratis snapshot based finalization in a slow follower. In all other
   * cases, the VERSION file should be the source of truth.
   *
   * MLV was not stored in RocksDB until SCM HA supported snapshot based
   * finalization, which was after a few HDDS layout features
   * were introduced. If the SCM has not finalized since this code
   * was added, the layout version will not be there. Defer to the MLV in the
   * VERSION file in this case, since finalization is not ongoing. The key will
   * be added once finalization is started with this software version.
   */
  private int getDBLayoutVersion() throws IOException {
    String dbLayoutVersion = finalizationStore.get(
        OzoneConsts.LAYOUT_VERSION_KEY);
    if (dbLayoutVersion == null) {
      return versionManager.getMetadataLayoutVersion();
    } else {
      try {
        return Integer.parseInt(dbLayoutVersion);
      } catch (NumberFormatException ex) {
        String msg = String.format(
            "Failed to read layout version from SCM DB. Found string %s",
            dbLayoutVersion);
        LOG.error(msg, ex);
        throw new IOException(msg, ex);
      }
    }
  }

  /**
   * Builds a {@link FinalizationManagerImpl}.
   */
  public static class Builder {
    private Table<String, String> finalizationStore;
    private DBTransactionBuffer transactionBuffer;
    private SCMRatisServer scmRatisServer;
    private SCMUpgradeFinalizer upgradeFinalizer;

    public Builder() {
    }

    public Builder setUpgradeFinalizer(final SCMUpgradeFinalizer finalizer) {
      upgradeFinalizer = finalizer;
      return this;
    }

    public Builder setRatisServer(final SCMRatisServer ratisServer) {
      scmRatisServer = ratisServer;
      return this;
    }

    public Builder setFinalizationStore(
        Table<String, String> finalizationStore) {
      this.finalizationStore = finalizationStore;
      return this;
    }

    public Builder setTransactionBuffer(DBTransactionBuffer transactionBuffer) {
      this.transactionBuffer = transactionBuffer;
      return this;
    }

    public FinalizationStateManager build() throws IOException {
      Objects.requireNonNull(finalizationStore, "finalizationStore == null");
      Objects.requireNonNull(transactionBuffer, "transactionBuffer == null");
      Objects.requireNonNull(upgradeFinalizer, "upgradeFinalizer == null");

      return scmRatisServer.getProxyHandler(SCMRatisProtocol.RequestType.FINALIZE,
        FinalizationStateManager.class, new FinalizationStateManagerImpl(this));
    }
  }
}
