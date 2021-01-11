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

import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_DONE;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_IN_PROGRESS;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_REQUIRED;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;

/**
 * UpgradeFinalizer for the Storage Container Manager service.
 */
public class SCMUpgradeFinalizer extends
    BasicUpgradeFinalizer<StorageContainerManager, HDDSLayoutVersionManager> {

  public SCMUpgradeFinalizer(HDDSLayoutVersionManager versionManager) {
    super(versionManager);
  }

  @Override
  public StatusAndMessages finalize(String upgradeClientID,
                                    StorageContainerManager scm)
      throws IOException {
    StatusAndMessages response = preFinalize(upgradeClientID, scm);
    if (response.status() != FINALIZATION_REQUIRED) {
      return response;
    }
    new Worker(scm).call();
    return STARTING_MSG;
  }

  private class Worker implements Callable<Void> {
    private StorageContainerManager storageContainerManager;

    /**
     * Initiates the Worker, for the specified SCM instance.
     * @param scm the StorageContainerManager instance on which to finalize the
     *           new LayoutFeatures.
     */
    Worker(StorageContainerManager scm) {
      storageContainerManager = scm;
    }

    @Override
    public Void call() throws IOException {
      try {
        emitStartingMsg();
        versionManager.setUpgradeState(FINALIZATION_IN_PROGRESS);
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

        for (HDDSLayoutFeature f : versionManager.unfinalizedFeatures()) {
          Optional<? extends LayoutFeature.UpgradeAction> action =
              f.onFinalizeSCMAction();
          finalizeFeature(f, storageContainerManager.getScmStorageConfig(),
              action);
          updateLayoutVersionInVersionFile(f,
              storageContainerManager.getScmStorageConfig());
          versionManager.finalized(f);
        }
        versionManager.completeFinalization();
        storageContainerManager.postFinalizeUpgrade();
        emitFinishedMsg();
        return null;
      } finally {
        versionManager.setUpgradeState(FINALIZATION_DONE);
        isDone = true;
      }
    }
  }
}
