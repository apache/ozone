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
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
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
   * @param lf      The layout feature that is being finalized.
   * @param context Supplier of objects needed to run the steps.
   * @throws UpgradeException
   */
  void replicatedFinalizationSteps(HDDSLayoutFeature lf,
                                   SCMUpgradeFinalizationContext context) throws UpgradeException {
    // Run upgrade actions and update VERSION file.
    super.finalizeLayoutFeature(lf,
        lf.scmAction(),
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
      waitForDatanodesToFinalize(context);
      stateManager.removeFinalizingMark();
    }
  }

  /**
   * Wait for all HEALTHY datanodes to complete finalization before finishing
   * SCM finalization. This ensures that when the client receives a
   * FINALIZATION_DONE status, all healthy datanodes have also finalized.
   *
   * A datanode is considered finalized when its metadata layout version (MLV)
   * equals its software layout version (SLV), indicating it has completed
   * processing all layout features.
   *
   * @param context The finalization context containing node manager reference
   * @throws SCMException if waiting is interrupted or SCM loses leadership
   * @throws NotLeaderException if SCM is no longer the leader
   */
  private void waitForDatanodesToFinalize(SCMUpgradeFinalizationContext context)
      throws SCMException, NotLeaderException {
    NodeManager nodeManager = context.getNodeManager();

    LOG.info("Waiting for all HEALTHY datanodes to complete finalization " +
        "before finishing SCM finalization.");

    boolean allDatanodesFinalized = false;
    while (!allDatanodesFinalized) {
      // Break out of the wait and step down from driving finalization if this
      // SCM is no longer the leader by throwing NotLeaderException.
      context.getSCMContext().getTermOfLeader();

      allDatanodesFinalized = true;
      int totalHealthyNodes = 0;
      int finalizedNodes = 0;
      int unfinalizedNodes = 0;

      for (DatanodeDetails dn : nodeManager.getAllNodes()) {
        try {
          // Only check HEALTHY nodes. STALE/DEAD nodes will be told to
          // finalize when they recover.
          if (nodeManager.getNodeStatus(dn).isHealthy()) {
            totalHealthyNodes++;
            DatanodeInfo datanodeInfo = nodeManager.getDatanodeInfo(dn);
            if (datanodeInfo == null) {
              LOG.warn("Could not get DatanodeInfo for {}, skipping in " +
                  "finalization wait.", dn.getHostName());
              continue;
            }

            LayoutVersionProto dnLayout = datanodeInfo.getLastKnownLayoutVersion();
            int dnMlv = dnLayout.getMetadataLayoutVersion();
            int dnSlv = dnLayout.getSoftwareLayoutVersion();

            if (dnMlv < dnSlv) {
              // Datanode has not yet finalized
              allDatanodesFinalized = false;
              unfinalizedNodes++;
              LOG.debug("Datanode {} has not yet finalized: MLV={}, SLV={}",
                  dn.getHostName(), dnMlv, dnSlv);
            } else {
              finalizedNodes++;
            }
          }
        } catch (NodeNotFoundException e) {
          // Node was removed while we were iterating. This is OK, skip it.
          LOG.debug("Node {} not found while waiting for finalization, " +
              "skipping.", dn);
        }
      }

      if (!allDatanodesFinalized) {
        LOG.info("Waiting for datanodes to finalize. Status: {}/{} healthy " +
                "datanodes have finalized ({} remaining).",
            finalizedNodes, totalHealthyNodes, unfinalizedNodes);
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SCMException("Interrupted while waiting for datanodes to " +
              "finalize.", SCMException.ResultCodes.INTERNAL_ERROR);
        }
      } else {
        LOG.info("All {} HEALTHY datanodes have completed finalization.",
            totalHealthyNodes);
      }
    }
  }
}
