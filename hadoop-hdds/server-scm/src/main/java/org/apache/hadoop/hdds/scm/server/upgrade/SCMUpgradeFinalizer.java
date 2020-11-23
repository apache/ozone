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

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds
    .upgrade.HDDSLayoutFeatureCatalog.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;

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
    if (!versionManager.needsFinalization()) {
      return FINALIZED_MSG;
    }
    clientID = upgradeClientID;
    component = scm;

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
        /*
         * Before we can call finalize the feature, we need to make sure that
         * all existing pipelines are closed and pipeline Manger would freeze
         * all new pipeline creation.
         */
        storageContainerManager.preFinalizeUpgrade();

        for (HDDSLayoutFeature f : versionManager.unfinalizedFeatures()) {
          finalizeFeature(f, storageContainerManager.getScmStorageConfig());
          updateLayoutVersionInVersionFile(f,
              storageContainerManager.getScmStorageConfig());
          versionManager.finalized(f);
        }
        versionManager.completeFinalization();
        storageContainerManager.postFinalizeUpgrade();
        emitFinishedMsg();
      } finally {
        isDone = true;
      }
      return null;
    }
  }
}
