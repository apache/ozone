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

import static org.apache.hadoop.ozone.OzoneConsts.LAYOUT_VERSION_KEY;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_DONE;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_IN_PROGRESS;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_REQUIRED;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;

/**
 * UpgradeFinalizer implementation for the Ozone Manager service.
 */
public class OMUpgradeFinalizer extends BasicUpgradeFinalizer<OzoneManager,
    OMLayoutVersionManager> {
  private  static final OmUpgradeAction NOOP = a -> {};

  public OMUpgradeFinalizer(OMLayoutVersionManager versionManager) {
    super(versionManager);
  }

  @Override
  public StatusAndMessages finalize(String upgradeClientID, OzoneManager om)
      throws IOException {
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
    new Worker(om).call();
    return STARTING_MSG;
  }

  /**
   * This class implements the finalization logic applied to every
   * LayoutFeature that needs to be finalized.
   *
   * For the first approach this happens synchronously within the state machine
   * during the FinalizeUpgrade request, but ideally this has to be moved to
   * individual calls that are going into the StateMaching one by one.
   * The prerequisits for this to happen in the background are the following:
   * - more fine grained control for LayoutFeatures to prepare the
   *    finalization outside the state machine, do the switch from old to new
   *    logic inside the statemachine and apply the finalization, and then do
   *    any cleanup necessary outside the state machine
   * - a way to post a request to the state machine that is not part of the
   *    client API, so basically not an OMRequest, but preferably an internal
   *    request, which is posted from the leader OM to the follower OMs only.
   * - ensure that there is a possibility to implement a rollback logic if
   *    something goes wrong inside the state machine, to avoid OM stuck in an
   *    intermediate state due to an error.
   */
  private class Worker implements Callable<Void> {
    private OzoneManager ozoneManager;

    /**
     * Initiates the Worker, for the specified OM instance.
     * @param om the OzoneManager instance on which to finalize the new
     *           LayoutFeatures.
     */
    Worker(OzoneManager om) {
      ozoneManager = om;
    }

    @Override
    public Void call() throws IOException {
      try {
        emitStartingMsg();
        versionManager.setUpgradeState(FINALIZATION_IN_PROGRESS);

        for (OMLayoutFeature f : versionManager.unfinalizedFeatures()) {
          finalizeFeature(f);
          updateLayoutVersionInVersionFile(f, ozoneManager.getOmStorage());
          versionManager.finalized(f);
        }

        versionManager.completeFinalization();
        emitFinishedMsg();
        return null;
      } finally {
        versionManager.setUpgradeState(FINALIZATION_DONE);
        isDone = true;
      }
    }

    private void finalizeFeature(OMLayoutFeature feature)
        throws IOException {
      OmUpgradeAction action = feature.onFinalizeAction().orElse(NOOP);

      if (action == NOOP) {
        emitNOOPMsg(feature.name());
        return;
      }

      putFinalizationMarkIntoVersionFile(feature, ozoneManager.getOmStorage());

      emitStartingFinalizationActionMsg(feature.name());
      try {
        action.executeAction(ozoneManager);
      } catch (Exception e) {
        logFinalizationFailureAndThrow(e, feature.name());
      }
      emitFinishFinalizationActionMsg(feature.name());

      removeFinalizationMarkFromVersionFile(feature,
          ozoneManager.getOmStorage());
    }
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
