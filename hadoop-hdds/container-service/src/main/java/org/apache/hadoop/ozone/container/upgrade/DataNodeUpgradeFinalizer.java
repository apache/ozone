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

package org.apache.hadoop.ozone.container.upgrade;

import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_DONE;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_IN_PROGRESS;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_REQUIRED;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeatureCatalog.HDDSLayoutFeature;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;

/**
 * UpgradeFinalizer for the DataNode.
 */
public class DataNodeUpgradeFinalizer extends
    BasicUpgradeFinalizer<DatanodeStateMachine, DataNodeLayoutVersionManager> {

  public DataNodeUpgradeFinalizer(DataNodeLayoutVersionManager versionManager) {
    super(versionManager);
  }

  @Override
  public StatusAndMessages finalize(String upgradeClientID,
                                    DatanodeStateMachine dsm)
      throws IOException {
    StatusAndMessages response = preFinalize(upgradeClientID, dsm);
    if (response.status() != FINALIZATION_REQUIRED) {
      return response;
    }
    new Worker(dsm).call();
    return STARTING_MSG;
  }

  private class Worker implements Callable<Void> {
    private DatanodeStateMachine datanodeStateMachine;

    /**
     * Initiates the Worker, for the specified DataNode instance.
     * @param dsm the DataNodeStateMachine instance on which to finalize the
     *           new LayoutFeatures.
     */
    Worker(DatanodeStateMachine dsm) {
      datanodeStateMachine = dsm;
    }

    @Override
    public Void call() throws IOException {
      if(!datanodeStateMachine.preFinalizeUpgrade()) {
      // datanode is not yet ready to finalize.
      // Reset the Finalization state.
        versionManager.setUpgradeState(FINALIZATION_REQUIRED);
        return null;
      }
      try {
        emitStartingMsg();
        versionManager.setUpgradeState(FINALIZATION_IN_PROGRESS);
        /*
         * Before we can call finalize the feature, we need to make sure that
         * all existing pipelines are closed and pipeline Manger would freeze
         * all new pipeline creation.
         */

        for (HDDSLayoutFeature f : versionManager.unfinalizedFeatures()) {
          Optional<? extends LayoutFeature.UpgradeAction> action =
              f.onFinalizeDataNodeAction();
          finalizeFeature(f, datanodeStateMachine.getDataNodeStorageConfig(),
              action);
          updateLayoutVersionInVersionFile(f,
              datanodeStateMachine.getDataNodeStorageConfig());
          versionManager.finalized(f);
        }
        versionManager.completeFinalization();
        datanodeStateMachine.postFinalizeUpgrade();
        emitFinishedMsg();
        return null;
      } finally {
        versionManager.setUpgradeState(FINALIZATION_DONE);
        isDone = true;
      }
    }
  }
}
