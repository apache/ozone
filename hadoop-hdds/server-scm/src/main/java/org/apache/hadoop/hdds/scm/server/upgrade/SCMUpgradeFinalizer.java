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

import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.CLOSED;

import java.io.IOException;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizationExecutor;
import org.apache.ratis.protocol.exceptions.NotLeaderException;

/**
 * UpgradeFinalizer for the Storage Container Manager service.
 *
 * This class contains the actions to drive finalization on the leader SCM,
 * while followers are updated through replicated methods in
 * {@link FinalizationStateManager}.
 */
public class SCMUpgradeFinalizer extends
    BasicUpgradeFinalizer<SCMUpgradeFinalizationContext,
        HDDSLayoutVersionManager> {

  public SCMUpgradeFinalizer(HDDSLayoutVersionManager versionManager) {
    super(versionManager);
  }

  public SCMUpgradeFinalizer(HDDSLayoutVersionManager versionManager,
      UpgradeFinalizationExecutor<SCMUpgradeFinalizationContext> executor) {
    super(versionManager, executor);
  }

  private void logCheckpointCrossed(FinalizationCheckpoint checkpoint) {
    LOG.info("SCM Finalization has crossed checkpoint {}", checkpoint);
  }

  @Override
  public void preFinalizeUpgrade(SCMUpgradeFinalizationContext context)
      throws IOException {
    FinalizationStateManager stateManager =
        context.getFinalizationStateManager();
    if (!stateManager.crossedCheckpoint(
        FinalizationCheckpoint.FINALIZATION_STARTED)) {
      context.getFinalizationStateManager().addFinalizingMark();
    }
    logCheckpointCrossed(FinalizationCheckpoint.FINALIZATION_STARTED);

    if (!stateManager.crossedCheckpoint(
        FinalizationCheckpoint.MLV_EQUALS_SLV)) {
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

  /**
   * Run on each SCM (leader and follower) when a layout feature is being
   * finalized to run its finalization actions, update the VERSION file, and
   * move the state of its in memory datanodes to healthy readonly.
   *
   * @param lf The layout feature that is being finalized.
   * @param context Supplier of objects needed to run the steps.
   * @throws UpgradeException
   */
  void replicatedFinalizationSteps(HDDSLayoutFeature lf,
      SCMUpgradeFinalizationContext context) throws UpgradeException {
    // Run upgrade actions and update VERSION file.
    super.finalizeLayoutFeature(lf,
        lf.scmAction(LayoutFeature.UpgradeActionType.ON_FINALIZE),
        context.getStorage());
  }

  @Override
  public void postFinalizeUpgrade(SCMUpgradeFinalizationContext context)
      throws IOException {
    // If we reached this phase of finalization, all layout features should
    // be finalized.
    logCheckpointCrossed(FinalizationCheckpoint.MLV_EQUALS_SLV);
    FinalizationStateManager stateManager =
        context.getFinalizationStateManager();
    if (!stateManager.crossedCheckpoint(
        FinalizationCheckpoint.FINALIZATION_COMPLETE)) {
      createPipelinesAfterFinalization(context);
      stateManager.removeFinalizingMark();
    }
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

    // Pipeline creation should already be frozen when the finalization state
    // manager set the checkpoint.
    if (!pipelineManager.isPipelineCreationFrozen()) {
      throw new SCMException("Error during finalization. Pipeline creation" +
          "should have been frozen before closing existing pipelines.",
          SCMException.ResultCodes.INTERNAL_ERROR);
    }

    for (Pipeline pipeline : pipelineManager.getPipelines()) {
      if (pipeline.getPipelineState() != CLOSED) {
        pipelineManager.closePipeline(pipeline.getId());
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

  private void createPipelinesAfterFinalization(
      SCMUpgradeFinalizationContext context) throws SCMException,
      NotLeaderException {
    // Pipeline creation should already be resumed when the finalization state
    // manager set the checkpoint.
    PipelineManager pipelineManager = context.getPipelineManager();
    if (pipelineManager.isPipelineCreationFrozen()) {
      throw new SCMException("Error during finalization. Pipeline creation " +
          "should have been resumed before waiting for new pipelines.",
          SCMException.ResultCodes.INTERNAL_ERROR);
    }

    // Wait for at least one pipeline to be created before finishing
    // finalization, so clients can write.
    boolean hasPipeline = false;
    while (!hasPipeline) {
      // Break out of the wait and step down from driving finalization if this
      // SCM is no longer the leader by throwing NotLeaderException.
      context.getSCMContext().getTermOfLeader();

      ReplicationConfig ratisThree =
          ReplicationConfig.fromProtoTypeAndFactor(
              HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.THREE);
      int pipelineCount =
          pipelineManager.getPipelines(ratisThree, Pipeline.PipelineState.OPEN)
              .size();

      hasPipeline = (pipelineCount >= 1);
      if (!hasPipeline) {
        LOG.info("Waiting for at least one open Ratis 3 pipeline after SCM " +
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
  }
}
