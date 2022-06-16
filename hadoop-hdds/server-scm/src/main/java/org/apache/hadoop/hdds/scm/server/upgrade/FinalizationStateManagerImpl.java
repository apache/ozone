/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.server.upgrade;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol;
import org.apache.hadoop.hdds.scm.ha.SCMHAInvocationHandler;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.scm.metadata.Replicate;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import com.google.common.base.Preconditions;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages the state of finalization in SCM.
 */
public class FinalizationStateManagerImpl implements FinalizationStateManager {

  @VisibleForTesting
  public static final Logger LOG =
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
  private final SCMServiceManager serviceManager;

  protected FinalizationStateManagerImpl(Builder builder) throws IOException {
    this.finalizationStore = builder.finalizationStore;
    this.transactionBuffer = builder.transactionBuffer;
    this.upgradeFinalizer = builder.upgradeFinalizer;
    this.serviceManager = builder.serviceManager;
    this.versionManager = this.upgradeFinalizer.getVersionManager();
    this.checkpointLock = new ReentrantReadWriteLock();
    initialize();
  }

  private void initialize() throws IOException {
    this.hasFinalizingMark =
        finalizationStore.isExist(OzoneConsts.FINALIZING_KEY);
  }

  private void setCheckpoint(FinalizationCheckpoint checkpoint) {
    upgradeContext.getSCMContext().setFinalizationCheckpoint(checkpoint);
    serviceManager.notifyEventTriggered(
        SCMService.Event.FINALIZATION_CHECKPOINT_CROSSED);
  }

  @Override
  public void setUpgradeContext(SCMUpgradeFinalizationContext context) {
    this.upgradeContext = context;
  }

  @Override
  public void addFinalizingMark() throws IOException {
    checkpointLock.writeLock().lock();
    try {
      hasFinalizingMark = true;
    } finally {
      checkpointLock.writeLock().unlock();
    }
    setCheckpoint(FinalizationCheckpoint.FINALIZATION_STARTED);
    transactionBuffer.addToBuffer(finalizationStore,
        OzoneConsts.FINALIZING_KEY, "");
  }

  @Override
  public void finalizeLayoutFeature(Integer layoutVersion) throws IOException {
    finalizeLayoutFeatureLocal(layoutVersion);
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
      setCheckpoint(FinalizationCheckpoint.MLV_EQUALS_SLV);
      // If we just finalized the last layout feature, don't wait for next
      // heartbeat from datanodes in order to move them to
      // Healthy - Readonly state. Force them to Healthy ReadOnly state so that
      // we can resume pipeline creation right away.
      upgradeContext.getNodeManager().forceNodesToHealthyReadOnly();
      // This may be a no-op on a follower who gets all finalization steps
      // from a snapshot, since the background pipeline creator thread was
      // never stopped.
      upgradeContext.getPipelineManager().resumePipelineCreation();
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

    setCheckpoint(FinalizationCheckpoint.FINALIZATION_COMPLETE);
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

    String errorMessage = String.format("SCM upgrade finalization " +
            "is in an unknown state.%nFinalizing mark present? %b%n" +
            "Metadata layout version behind software layout version? %b",
        hasFinalizingMarkSnapshot, mlvBehindSlvSnapshot);
    Preconditions.checkNotNull(currentCheckpoint, errorMessage);
    return currentCheckpoint;
  }

  @Override
  public void reinitialize(Table<String, String> newFinalizationStore)
      throws IOException {
    checkpointLock.writeLock().lock();
    try {
      newFinalizationStore.close();
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
      FinalizationCheckpoint newCheckpoint = getFinalizationCheckpoint();
      upgradeContext.getSCMContext().setFinalizationCheckpoint(newCheckpoint);
    } catch (Exception ex) {
      LOG.error("Failed to reinitialize finalization state", ex);
      throw new IOException(ex);
    } finally {
      checkpointLock.writeLock().unlock();
    }
  }

  /**
   * Gets the metadata layout version from the SCM RocksDB. If there is no
   * layout version in the DB, the first layout version will be returned.
   * This may happen if working with an older SCM instance before the upgrade
   * framework was present.
   */
  private int getDBLayoutVersion() throws IOException {
    String dbLayoutVersion = finalizationStore.get(
        OzoneConsts.LAYOUT_VERSION_KEY);
    if (dbLayoutVersion == null) {
      return HDDSLayoutFeature.INITIAL_VERSION.layoutVersion();
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
    private SCMServiceManager serviceManager;

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

    public Builder setServiceManager(SCMServiceManager serviceManager) {
      this.serviceManager = serviceManager;
      return this;
    }

    public FinalizationStateManager build() throws IOException {
      Preconditions.checkNotNull(finalizationStore);
      Preconditions.checkNotNull(transactionBuffer);
      Preconditions.checkNotNull(upgradeFinalizer);
      final SCMHAInvocationHandler invocationHandler =
          new SCMHAInvocationHandler(SCMRatisProtocol.RequestType.FINALIZE,
              new FinalizationStateManagerImpl(this),
              scmRatisServer);

      return (FinalizationStateManager) Proxy.newProxyInstance(
          SCMHAInvocationHandler.class.getClassLoader(),
          new Class<?>[]{FinalizationStateManager.class}, invocationHandler);
    }
  }
}
