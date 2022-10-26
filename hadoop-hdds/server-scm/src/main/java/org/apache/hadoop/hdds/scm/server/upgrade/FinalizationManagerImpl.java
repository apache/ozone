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
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.DefaultUpgradeFinalizationExecutor;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizationExecutor;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.Executors;

/**
 * Class to initiate SCM finalization and query its progress.
 */
public class FinalizationManagerImpl implements FinalizationManager {
  private static final Logger LOG = LoggerFactory
      .getLogger(FinalizationManagerImpl.class);

  private SCMUpgradeFinalizer upgradeFinalizer;
  private SCMUpgradeFinalizationContext context;
  private SCMStorageConfig storage;
  private OzoneConfiguration conf;
  private HDDSLayoutVersionManager versionManager;
  private final FinalizationStateManager finalizationStateManager;

  /**
   * For test classes to inject their own state manager.
   */
  @VisibleForTesting
  protected FinalizationManagerImpl(Builder builder,
      FinalizationStateManager stateManager) throws IOException {
    initCommonFields(builder);
    this.finalizationStateManager = stateManager;

  }

  private FinalizationManagerImpl(Builder builder) throws IOException {
    initCommonFields(builder);
    this.finalizationStateManager = new FinalizationStateManagerImpl.Builder()
        .setUpgradeFinalizer(this.upgradeFinalizer)
        .setFinalizationStore(builder.finalizationStore)
        .setTransactionBuffer(builder.scmHAManager.getDBTransactionBuffer())
        .setRatisServer(builder.scmHAManager.getRatisServer())
        .build();
  }

  private void initCommonFields(Builder builder) {
    this.storage = builder.storage;
    this.versionManager = builder.versionManager;
    this.conf = builder.conf;
    this.upgradeFinalizer = new SCMUpgradeFinalizer(this.versionManager,
        builder.executor);
  }

  @Override
  public void buildUpgradeContext(NodeManager nodeManager,
                                  PipelineManager pipelineManager,
                                  SCMContext scmContext) {
    this.context = new SCMUpgradeFinalizationContext.Builder()
            .setStorage(this.storage)
            .setFinalizationStateManager(finalizationStateManager)
            .setConfiguration(conf)
            .setNodeManager(nodeManager)
            .setPipelineManager(pipelineManager)
            .setLayoutVersionManager(versionManager)
            .setSCMContext(scmContext)
            .build();

    finalizationStateManager.setUpgradeContext(this.context);
  }

  @Override
  public UpgradeFinalizer.StatusAndMessages finalizeUpgrade(
      String upgradeClientID)
      throws IOException {
    Preconditions.checkNotNull(context, "Cannot finalize upgrade without " +
        "first building the upgrade context.");
    return upgradeFinalizer.finalize(upgradeClientID, context);
  }

  @Override
  public UpgradeFinalizer.StatusAndMessages queryUpgradeFinalizationProgress(
      String upgradeClientID, boolean takeover, boolean readonly
  ) throws IOException {
    if (readonly) {
      return new UpgradeFinalizer.StatusAndMessages(
          upgradeFinalizer.getStatus(), Collections.emptyList());
    }
    return upgradeFinalizer.reportStatus(upgradeClientID, takeover);
  }

  @Override
  public BasicUpgradeFinalizer<SCMUpgradeFinalizationContext,
      HDDSLayoutVersionManager> getUpgradeFinalizer() {
    return upgradeFinalizer;
  }

  @Override
  public void runPrefinalizeStateActions() throws IOException  {
    upgradeFinalizer.runPrefinalizeStateActions(storage, context);
  }

  @Override
  public boolean crossedCheckpoint(FinalizationCheckpoint checkpoint) {
    return finalizationStateManager.crossedCheckpoint(checkpoint);
  }

  @Override
  public FinalizationCheckpoint getCheckpoint() {
    return finalizationStateManager.getFinalizationCheckpoint();
  }

  @Override
  public void reinitialize(Table<String, String> finalizationStore)
      throws IOException {
    finalizationStateManager.reinitialize(finalizationStore);
  }

  @Override
  public void onLeaderReady() {
    // Launch a background thread to drive finalization.
    Executors.newSingleThreadExecutor().submit(() -> {
      FinalizationCheckpoint currentCheckpoint = getCheckpoint();
      if (currentCheckpoint.hasCrossed(
          FinalizationCheckpoint.FINALIZATION_STARTED) &&
          !currentCheckpoint.hasCrossed(
              FinalizationCheckpoint.FINALIZATION_COMPLETE)) {
        LOG.info("SCM became leader. Resuming upgrade finalization from" +
            " current checkpoint {}.", currentCheckpoint);
        try {
          finalizeUpgrade("resume-finalization-as-leader");
        } catch (IOException ex) {
          ExitUtils.terminate(1,
              "Resuming upgrade finalization failed on SCM leader change.",
              ex, true, LOG);
        }
      } else if (LOG.isDebugEnabled()) {
        LOG.debug("SCM became leader. No upgrade finalization action" +
            " required for current checkpoint {}", currentCheckpoint);
      }
    });
  }

  /**
   * Builds a {@link FinalizationManagerImpl}.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static class Builder {
    private OzoneConfiguration conf;
    private HDDSLayoutVersionManager versionManager;
    private SCMStorageConfig storage;
    private SCMHAManager scmHAManager;
    private Table<String, String> finalizationStore;
    private UpgradeFinalizationExecutor<SCMUpgradeFinalizationContext> executor;

    public Builder() {
      executor = new DefaultUpgradeFinalizationExecutor<>();
    }

    public Builder setConfiguration(OzoneConfiguration conf) {
      this.conf = conf;
      return this;
    }

    public Builder setLayoutVersionManager(
        HDDSLayoutVersionManager versionManager) {
      this.versionManager = versionManager;
      return this;
    }

    public Builder setStorage(SCMStorageConfig storage) {
      this.storage = storage;
      return this;
    }

    public Builder setHAManager(SCMHAManager scmHAManager) {
      this.scmHAManager = scmHAManager;
      return this;
    }

    public Builder setFinalizationStore(
        Table<String, String> finalizationStore) {
      this.finalizationStore = finalizationStore;
      return this;
    }

    public Builder setFinalizationExecutor(
        UpgradeFinalizationExecutor<SCMUpgradeFinalizationContext> executor) {
      this.executor = executor;
      return this;
    }

    public FinalizationManagerImpl build() throws IOException {
      Preconditions.checkNotNull(conf);
      Preconditions.checkNotNull(versionManager);
      Preconditions.checkNotNull(storage);
      Preconditions.checkNotNull(scmHAManager);
      Preconditions.checkNotNull(finalizationStore);
      Preconditions.checkNotNull(executor);

      return new FinalizationManagerImpl(this);
    }
  }
}
