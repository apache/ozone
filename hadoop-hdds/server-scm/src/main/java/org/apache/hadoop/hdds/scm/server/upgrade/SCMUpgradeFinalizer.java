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
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizationExecutor;

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
      stateManager.removeFinalizingMark();
    }
  }

}
