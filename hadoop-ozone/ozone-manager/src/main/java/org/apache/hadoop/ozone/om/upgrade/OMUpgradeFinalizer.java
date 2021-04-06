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

package org.apache.hadoop.ozone.om.upgrade;

import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.ON_FINALIZE;
import static org.apache.hadoop.ozone.OzoneConsts.LAYOUT_VERSION_KEY;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_REQUIRED;

import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeAction;
import org.apache.hadoop.ozone.upgrade.UpgradeException;

/**
 * UpgradeFinalizer implementation for the Ozone Manager service.
 */
public class OMUpgradeFinalizer extends BasicUpgradeFinalizer<OzoneManager,
    OMLayoutVersionManager> {
  private  static final OmUpgradeAction NOOP = a -> {};
  private OzoneManager ozoneManager;

  public OMUpgradeFinalizer(OMLayoutVersionManager versionManager) {
    super(versionManager);
  }

  @Override
  public StatusAndMessages finalize(String upgradeClientID, OzoneManager om)
      throws IOException {
    ozoneManager = om;
    StatusAndMessages response = preFinalize(upgradeClientID, om);
    if (response.status() != FINALIZATION_REQUIRED) {
      return response;
    }
    // This requires some more investigation on how to do it properly while
    // requests are on the fly, and post finalize features one by one.
    // Until that is done, monitoring is not really doing anything meaningful
    // but this is a tradoff we can take for the first iteration either if
    // needed, as the finalization of the first few features should not take
    // that long. Follow up JIRA is in HDDS-4286
    //    String threadName = "OzoneManager-Upgrade-Finalizer";
    //    ExecutorService executor =
    //        Executors.newSingleThreadExecutor(r -> new Thread(threadName));
    //    executor.submit(new Worker(om));
    try {
      getFinalizationExecutor().execute(ozoneManager.getOmStorage(),
          this);
    } catch (Exception e) {
      e.printStackTrace();
      throw (IOException) e;
    }
    return STARTING_MSG;
  }

  @Override
  protected void postFinalizeUpgrade() throws IOException {
    return;
  }

  @Override
  protected void finalizeUpgrade(Storage storageConfig)
      throws UpgradeException {
    for (OMLayoutFeature f : versionManager.unfinalizedFeatures()) {
      Optional<? extends UpgradeAction> action = f.action(ON_FINALIZE);
      finalizeFeature(f, storageConfig, action);
      updateLayoutVersionInVersionFile(f, storageConfig);
      versionManager.finalized(f);
    }
    versionManager.completeFinalization();
  }

  @Override
  protected boolean preFinalizeUpgrade() throws IOException {
    return true;
  }

  public void runPrefinalizeStateActions(Storage storage, OzoneManager om)
      throws IOException {
    super.runPrefinalizeStateActions(
        lf -> ((OMLayoutFeature) lf)::action, storage, om);
  }

  /**
   * Write down Layout version of a finalized feature to DB on finalization.
   * @param f layout feature
   * @param om OM instance
   * @throws IOException on Error.
   */
  public void updateLayoutVersionInDB(OMLayoutVersionManager lvm,
                                      OzoneManager om)
      throws IOException {
    OMMetadataManager omMetadataManager = om.getMetadataManager();
    omMetadataManager.getMetaTable().put(LAYOUT_VERSION_KEY,
        String.valueOf(lvm.getMetadataLayoutVersion()));
  }
}
