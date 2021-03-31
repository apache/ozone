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

import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.ON_FINALIZE;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_DONE;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_IN_PROGRESS;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_REQUIRED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.UpgradeTestInjectionPoints.AfterCompleteFinalization;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.UpgradeTestInjectionPoints.AfterPostFinalizeUpgrade;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.UpgradeTestInjectionPoints.AfterPreFinalizeUpgrade;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.UpgradeTestInjectionPoints.BeforeCompleteFinalization;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.UpgradeTestInjectionPoints.BeforePreFinalizeUpgrade;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeAction;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UpgradeFinalizer for the DataNode.
 */
public class DataNodeUpgradeFinalizer extends
    BasicUpgradeFinalizer<DatanodeStateMachine, HDDSLayoutVersionManager> {
  static final Logger LOG =
      LoggerFactory.getLogger(DataNodeUpgradeFinalizer.class);

  public DataNodeUpgradeFinalizer(HDDSLayoutVersionManager versionManager,
                                  String optionalClientID) {
    super(versionManager);
    clientID = optionalClientID;
  }

  @Override
  public StatusAndMessages finalize(String upgradeClientID,
                                    DatanodeStateMachine dsm)
      throws IOException {
    StatusAndMessages response = preFinalize(upgradeClientID, dsm);
    if (response.status() != FINALIZATION_REQUIRED) {
      return response;
    }
    try {
      new Worker(dsm).call();
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
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
    public Void call() throws Exception {
      injectTestFunctionAtThisPoint(BeforePreFinalizeUpgrade);
      if(!datanodeStateMachine.preFinalizeUpgrade()) {
      // DataNode is not yet ready to finalize.
      // Reset the Finalization state.
        versionManager.setUpgradeState(FINALIZATION_REQUIRED);
        return null;
      }
      injectTestFunctionAtThisPoint(AfterPreFinalizeUpgrade);
      try {
        emitStartingMsg();
        versionManager.setUpgradeState(FINALIZATION_IN_PROGRESS);
        for (HDDSLayoutFeature f : versionManager.unfinalizedFeatures()) {
          Optional<? extends UpgradeAction> action =
              f.datanodeAction(ON_FINALIZE);
          finalizeFeature(f, datanodeStateMachine.getLayoutStorage(), action);
          updateLayoutVersionInVersionFile(f,
              datanodeStateMachine.getLayoutStorage());
          versionManager.finalized(f);
        }
        injectTestFunctionAtThisPoint(BeforeCompleteFinalization);
        versionManager.completeFinalization();
        injectTestFunctionAtThisPoint(AfterCompleteFinalization);
        datanodeStateMachine.postFinalizeUpgrade();
        injectTestFunctionAtThisPoint(AfterPostFinalizeUpgrade);
        emitFinishedMsg();
        return null;
      } catch (Exception e) {
        e.printStackTrace();
        if (versionManager.needsFinalization()) {
          versionManager.setUpgradeState(FINALIZATION_REQUIRED);
        }
      } finally {
        isDone = true;
      }
      return null;
    }
  }

  @Override
  public void runPrefinalizeStateActions(Storage storage,
                                         DatanodeStateMachine dsm)
      throws IOException {
    super.runPrefinalizeStateActions(
        lf -> ((HDDSLayoutFeature) lf)::datanodeAction, storage, dsm);
  }
}
