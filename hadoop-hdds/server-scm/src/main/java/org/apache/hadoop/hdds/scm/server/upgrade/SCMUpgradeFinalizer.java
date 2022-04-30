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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.UpgradeException;

/**
 * UpgradeFinalizer for the Storage Container Manager service.
 */
public class SCMUpgradeFinalizer extends
    BasicUpgradeFinalizer<StorageContainerManager, HDDSLayoutVersionManager> {

  public SCMUpgradeFinalizer(HDDSLayoutVersionManager versionManager) {
    super(versionManager);
  }

  // This should be called in the context of a separate finalize upgrade thread.
  // This function can block indefinitely till the conditions are met to safely
  // finalize Upgrade.
  @Override
  public void preFinalizeUpgrade(StorageContainerManager scm)
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

    PipelineManager pipelineManager = scm.getPipelineManager();

    // Pipeline creation will remain frozen until postFinalizeUpgrade()
    pipelineManager.freezePipelineCreation();

    waitForAllPipelinesToDestroy(pipelineManager);


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

  @Override
  public void finalizeUpgrade(StorageContainerManager scm)
      throws UpgradeException {
    super.finalizeUpgrade(lf -> ((HDDSLayoutFeature) lf)::scmAction,
        scm.getScmStorageConfig());
  }

  public void postFinalizeUpgrade(StorageContainerManager scm)
      throws IOException {


    // Don 't wait for next heartbeat from datanodes in order to move them to
    // Healthy - Readonly state. Force them to Healthy ReadOnly state so that
    // we can resume pipeline creation right away.
    scm.getScmNodeManager().forceNodesToHealthyReadOnly();

    PipelineManager pipelineManager = scm.getPipelineManager();

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

  @Override
  public void runPrefinalizeStateActions(Storage storage,
                                         StorageContainerManager scm)
      throws IOException {
    runPrefinalizeStateActions(
        lf -> ((HDDSLayoutFeature) lf)::scmAction, storage, scm);
  }

  /**
   * Ask all the existing data nodes to close any open containers and
   * destroy existing pipelines.
   */
  private void waitForAllPipelinesToDestroy(PipelineManager pipelineManager)
      throws IOException {
    boolean pipelineFound = true;
    while (pipelineFound) {
      pipelineFound = false;
      for (Pipeline pipeline : pipelineManager.getPipelines()) {
        if (pipeline.getPipelineState() != CLOSED) {
          pipelineFound = true;
          pipelineManager.closePipeline(pipeline, true);
        }
      }
      try {
        if (pipelineFound) {
          LOG.info("Waiting for all pipelines to close.");
          Thread.sleep(5000);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        continue;
      }
    }
  }
}
