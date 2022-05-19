/**
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

import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.CLOSED;

import java.io.IOException;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManager.FinalizationCheckpoint;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizationExecutor;

/**
 * UpgradeFinalizer for the Storage Container Manager service.
 */
public class SCMUpgradeFinalizer extends
    BasicUpgradeFinalizer<SCMUpgradeFinalizer.SCMUpgradeFinalizationContext, HDDSLayoutVersionManager> {

  public SCMUpgradeFinalizer(HDDSLayoutVersionManager versionManager) {
    super(versionManager);
  }

  public SCMUpgradeFinalizer(HDDSLayoutVersionManager versionManager,
      UpgradeFinalizationExecutor<SCMUpgradeFinalizationContext> executor) {
    super(versionManager, executor);
  }

  // This should be called in the context of a separate finalize upgrade thread.
  // This function can block indefinitely till the conditions are met to safely
  // finalize Upgrade.
  @Override
  public void preFinalizeUpgrade(SCMUpgradeFinalizationContext context)
      throws IOException {

    FinalizationStateManager stateManager =
        context.getFinalizationStateManager();
    if (stateManager.getFinalizationCheckpoint() == FinalizationCheckpoint.FINALIZATION_REQUIRED) {
      context.getFinalizationStateManager().addFinalizingMark();
    }
    if (stateManager.getFinalizationCheckpoint() == FinalizationCheckpoint.FINALIZATION_STARTED) {
      closePipelinesBeforeFinalization(context.getPipelineManager());
    }
  }

  @Override
  public void finalizeLayoutFeature(LayoutFeature lf,
       SCMUpgradeFinalizationContext context) throws UpgradeException {
    // Run upgrade actions, update VERSION file, and update layout version in
    // DB.
    try {
      context.getFinalizationStateManager()
          .finalizeLayoutFeature(lf.layoutVersion());
    } catch (IOException ex) {
      throw new UpgradeException(ex,
          UpgradeException.ResultCodes.LAYOUT_FEATURE_FINALIZATION_FAILED);
    }
  }

  void replicatedFinalizationSteps(LayoutFeature lf,
      SCMUpgradeFinalizationContext context) throws UpgradeException {
    // Run upgrade actions and update VERSION file.
    super.finalizeLayoutFeature(lf, context.getStorage());
    if (!getVersionManager().needsFinalization()) {
      // Don 't wait for next heartbeat from datanodes in order to move them to
      // Healthy - Readonly state. Force them to Healthy ReadOnly state so that
      // we can resume pipeline creation right away.
      context.getNodeManager().forceNodesToHealthyReadOnly();
    }
  }

  public void postFinalizeUpgrade(SCMUpgradeFinalizationContext context)
      throws IOException {
    FinalizationStateManager stateManager =
        context.getFinalizationStateManager();
    if (stateManager.getFinalizationCheckpoint() == FinalizationCheckpoint.MLV_EQUALS_SLV) {
        createPipelinesAfterFinalization(context.getPipelineManager());
    }
    stateManager.removeFinalizingMark();
  }

  @Override
  public void runPrefinalizeStateActions(Storage storage,
    SCMUpgradeFinalizationContext context) throws IOException {
    super.runPrefinalizeStateActions(
        lf -> ((HDDSLayoutFeature) lf)::scmAction, storage, context);
  }

  private void closePipelinesBeforeFinalization(PipelineManager pipelineManager)
      throws IOException {
    /*
     * Before we can call finalize the feature, we need to make sure that
     * all existing pipelines are closed and pipeline Manger would freeze
     * all new pipeline creation.
     */
    String msg = "  Existing pipelines and containers will be closed " +
        "during Upgrade.";
    msg += "\n  New pipelines creation will remain frozen until Upgrade " +
        "is finalized.";

    // Pipeline creation will remain frozen until postFinalizeUpgrade()
    pipelineManager.freezePipelineCreation();

    for (Pipeline pipeline : pipelineManager.getPipelines()) {
      if (pipeline.getPipelineState() != CLOSED) {
        pipelineManager.closePipeline(pipeline, true);
      }
    }

    // We can not yet move all the existing data nodes to HEALTHY-READONLY
    // state since the next heartbeat will move them back to HEALTHY state.
    // This has to wait till postFinalizeUpgrade, when SCM MLV version is
    // already upgraded as part of finalize processing.
    // While in this state, it should be safe to do finalize processing for
    // all new features. This will also update ondisk mlv version. Any
    // disrupting upgrade can add a hook here to make sure that SCM is in a
    // consistent state while finalizing the upgrade.

    logAndEmit(msg);
  }

  private void createPipelinesAfterFinalization(PipelineManager pipelineManager) {
    pipelineManager.resumePipelineCreation();

    // Wait for at least one pipeline to be created before finishing
    // finalization, so clients can write.
    boolean hasPipeline = false;
    while (!hasPipeline) {
      ReplicationConfig ratisThree =
          ReplicationConfig.fromProtoTypeAndFactor(
              HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.THREE);
      int pipelineCount =
          pipelineManager.getPipelines(ratisThree, Pipeline.PipelineState.OPEN)
              .size();

      hasPipeline = (pipelineCount >= 1);
      if (!hasPipeline) {
        LOG.info("Waiting for at least one open pipeline after SCM " +
            "finalization.");
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          // Try again on next loop iteration.
          Thread.currentThread().interrupt();
        }
      } else {
        LOG.info("Open pipeline found after SCM finalization");
      }
    }
    emitFinishedMsg();
  }

  public static class SCMUpgradeFinalizationContext {
    private final PipelineManager pipelineManager;
    private final NodeManager nodeManager;
    private final FinalizationStateManager finalizationStateManager;
    private final SCMStorageConfig storage;
    private final HDDSLayoutVersionManager versionManager;
    private final OzoneConfiguration conf;

    private SCMUpgradeFinalizationContext(Builder builder) {
      pipelineManager = builder.pipelineManager;
      nodeManager = builder.nodeManager;
      finalizationStateManager = builder.finalizationStateManager;
      storage = builder.storage;
      versionManager = builder.versionManager;
      conf = builder.conf;
    }

    public NodeManager getNodeManager() {
      return nodeManager;
    }

    public PipelineManager getPipelineManager() {
      return pipelineManager;
    }

    public FinalizationStateManager getFinalizationStateManager() {
      return finalizationStateManager;
    }

    public OzoneConfiguration getConfiguration() {
      return conf;
    }

    public HDDSLayoutVersionManager getLayoutVersionManager() {
      return versionManager;
    }

    public SCMStorageConfig getStorage() {
      return storage;
    }

    public static final class Builder {
      private PipelineManager pipelineManager;
      private NodeManager nodeManager;
      private FinalizationStateManager finalizationStateManager;
      private SCMStorageConfig storage;
      private HDDSLayoutVersionManager versionManager;
      private OzoneConfiguration conf;

      public Builder() {
      }

      public Builder setPipelineManager(PipelineManager pipelineManager) {
        this.pipelineManager = pipelineManager;
        return this;
      }

      public Builder setNodeManager(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
        return this;
      }

      public Builder setFinalizationStateManager(FinalizationStateManager finalizationStateManager) {
        this.finalizationStateManager = finalizationStateManager;
        return this;
      }

      public Builder setStorage(SCMStorageConfig storage) {
        this.storage = storage;
        return this;
      }

      public Builder setLayoutVersionManager(HDDSLayoutVersionManager versionManager) {
        this.versionManager = versionManager;
        return this;
      }

      public Builder setConfiguration(OzoneConfiguration conf) {
        this.conf = conf;
        return this;
      }

      public SCMUpgradeFinalizationContext build() {
        return new SCMUpgradeFinalizationContext(this);
      }
    }
  }
}
