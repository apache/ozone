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

package org.apache.hadoop.ozone.upgrade;

import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_IN_PROGRESS;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_REQUIRED;

import org.apache.hadoop.ozone.common.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DefaultUpgradeFinalizationExecutor for driving the main part of finalization.
 * Unit/Integration tests can override this to provide error injected version
 * of this class.
 */

@SuppressWarnings("checkstyle:VisibilityModifier")
public class DefaultUpgradeFinalizationExecutor {
  static final Logger LOG =
      LoggerFactory.getLogger(DefaultUpgradeFinalizationExecutor.class);

  public DefaultUpgradeFinalizationExecutor() {
  }

  public Void execute(Storage storageConfig,
                      BasicUpgradeFinalizer basicUpgradeFinalizer)
      throws Exception {
    try {
      basicUpgradeFinalizer.emitStartingMsg();
      basicUpgradeFinalizer.getVersionManager()
          .setUpgradeState(FINALIZATION_IN_PROGRESS);

      /*
       * Before we can call finalize the feature, we need to make sure that
       * all existing pipelines are closed and pipeline Manger would freeze
       * all new pipeline creation.
       */
      if(!basicUpgradeFinalizer.preFinalizeUpgrade()) {
        return null;
      }

      basicUpgradeFinalizer.finalizeUpgrade(storageConfig);

      basicUpgradeFinalizer.postFinalizeUpgrade();

      basicUpgradeFinalizer.emitFinishedMsg();
      return null;
    } catch (Exception e) {
      LOG.warn("Upgrade Finalization failed with following Exception:");
      e.printStackTrace();
      if (basicUpgradeFinalizer.getVersionManager().needsFinalization()) {
        basicUpgradeFinalizer.getVersionManager()
            .setUpgradeState(FINALIZATION_REQUIRED);
        throw (e);
      }
    } finally {
      basicUpgradeFinalizer.markFinalizationDone();
    }
    return null;
  }
}