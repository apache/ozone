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

import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.ON_FINALIZE;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_REQUIRED;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeAction;
import org.apache.hadoop.ozone.upgrade.UpgradeException;

/**
 * UpgradeFinalizer for the Storage Container Manager service.
 */
public class SCMUpgradeFinalizer extends
    BasicUpgradeFinalizer<StorageContainerManager, HDDSLayoutVersionManager> {

  private StorageContainerManager storageContainerManager;

  public SCMUpgradeFinalizer(HDDSLayoutVersionManager versionManager) {
    super(versionManager);
  }

  @Override
  public StatusAndMessages finalize(String upgradeClientID,
                                    StorageContainerManager scm)
      throws IOException {
    storageContainerManager = scm;
    StatusAndMessages response = preFinalize(upgradeClientID, scm);
    if (response.status() != FINALIZATION_REQUIRED) {
      return response;
    }
    try {
      getFinalizationExecutor().execute(scm.getScmStorageConfig(), this);
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
    return STARTING_MSG;
  }

  @Override
  public boolean preFinalizeUpgrade() throws IOException {
    /*
     * Before we can call finalize the feature, we need to make sure that
     * all existing pipelines are closed and pipeline Manger would freeze
     * all new pipeline creation.
     */
    String msg = "  Existing pipelines and containers will be closed " +
        "during Upgrade.";
    msg += "\n  New pipelines creation will remain frozen until Upgrade " +
        "is finalized.";

    storageContainerManager.preFinalizeUpgrade();
    logAndEmit(msg);
    return true;
  }

  @Override
  protected void finalizeUpgrade(Storage storageConfig)
      throws UpgradeException {
    for (HDDSLayoutFeature f : versionManager.unfinalizedFeatures()) {
      Optional<? extends UpgradeAction> action = f.scmAction(ON_FINALIZE);
      finalizeFeature(f, storageConfig, action);
      updateLayoutVersionInVersionFile(f, storageConfig);
      versionManager.finalized(f);
    }
    versionManager.completeFinalization();
  }

  public void postFinalizeUpgrade() throws IOException {
    storageContainerManager.postFinalizeUpgrade();
    emitFinishedMsg();
  }

  @Override
  public void runPrefinalizeStateActions(Storage storage,
                                         StorageContainerManager scm)
      throws IOException {
    super.runPrefinalizeStateActions(
        lf -> ((HDDSLayoutFeature) lf)::scmAction, storage, scm);
  }
}
