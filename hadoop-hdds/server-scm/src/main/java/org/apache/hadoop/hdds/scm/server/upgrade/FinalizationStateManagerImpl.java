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
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.scm.metadata.Replicate;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
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
  private SCMUpgradeFinalizationContext upgradeContext;
  private final SCMUpgradeFinalizer upgradeFinalizer;

  protected FinalizationStateManagerImpl(Builder builder) throws IOException {
    this.finalizationStore = builder.finalizationStore;
    this.transactionBuffer = builder.transactionBuffer;
    this.upgradeFinalizer = builder.upgradeFinalizer;
    this.versionManager = this.upgradeFinalizer.getVersionManager();
  }

  @Override
  public void setUpgradeContext(SCMUpgradeFinalizationContext context) {
    this.upgradeContext = context;
  }

  @Override
  public synchronized void finalizeLayoutFeatures(Integer toVersion) throws IOException {
    for (LayoutFeature feature : versionManager.unfinalizedFeatures()) {
      finalizeLayoutFeatureLocal((HDDSLayoutFeature) feature);
    }
  }

  /**
   * A version of finalizeLayoutFeature without the {@link Replicate}
   * annotation that can be called by followers to finalize from a snapshot.
   */
  private void finalizeLayoutFeatureLocal(HDDSLayoutFeature layoutFeature)
      throws IOException {
    // The VERSION file is the source of truth for the current layout
    // version. This is updated in the replicated finalization steps.
    // Layout version will be written to the DB as well so followers can
    // finalize from a snapshot.
    if (versionManager.getMetadataLayoutVersion() >= layoutFeature.layoutVersion()) {
      LOG.warn("Attempting to finalize layout feature for layout version {}, but " +
          "current metadata layout version is {}. Skipping finalization for this layout version.",
          layoutFeature.layoutVersion(), versionManager.getMetadataLayoutVersion());
    } else {
      upgradeFinalizer.replicatedFinalizationSteps(layoutFeature, upgradeContext);
    }
    transactionBuffer.addToBuffer(finalizationStore,
        OzoneConsts.LAYOUT_VERSION_KEY, String.valueOf(layoutFeature.layoutVersion()));
  }

  /**
   * Called on snapshot installation.
   */
  @Override
  public synchronized void reinitialize(Table<String, String> newFinalizationStore)
      throws IOException {
    try {
      this.finalizationStore = newFinalizationStore;

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
        finalizeLayoutFeatures(dbLayoutVersion);
      }
    } catch (Exception ex) {
      LOG.error("Failed to reinitialize finalization state", ex);
      throw new IOException(ex);
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
