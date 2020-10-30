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

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.*;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PERSIST_UPGRADE_TO_LAYOUT_VERSION_FAILED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.REMOVE_UPGRADE_TO_LAYOUT_VERSION_FAILED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.UPDATE_LAYOUT_VERSION_FAILED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_DONE;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_IN_PROGRESS;

/**
 * UpgradeFinalizer implementation for the Ozone Manager service.
 */
public class OMUpgradeFinalizer implements UpgradeFinalizer<OzoneManager> {

  private  static final OmUpgradeAction NOOP = a -> {};

  private OMLayoutVersionManagerImpl versionManager;
  private String clientID;

  private Queue<String> msgs = new ConcurrentLinkedQueue<>();
  private boolean isDone = false;

  public OMUpgradeFinalizer(OMLayoutVersionManagerImpl versionManager) {
    this.versionManager = versionManager;
  }

  @Override
  public StatusAndMessages finalize(String upgradeClientID, OzoneManager om)
      throws IOException {
    if (!versionManager.needsFinalization()) {
      return FINALIZED_MSG;
    }
    clientID = upgradeClientID;

// This requires some more investigation on how to do it properly while
// requests are on the fly, and post finalize features one by one.
// Until that is done, monitoring is not really doing anything meaningful
// but this is a tradoff we can take for the first iteration either if needed,
// as the finalization of the first few features should not take that long.
// Follow up JIRA is in HDDS-4286
//    String threadName = "OzoneManager-Upgrade-Finalizer";
//    ExecutorService executor =
//        Executors.newSingleThreadExecutor(r -> new Thread(threadName));
//    executor.submit(new Worker(om));
    new Worker(om).call();
    return STARTING_MSG;
  }

  @Override
  public synchronized StatusAndMessages reportStatus(
      String upgradeClientID, boolean takeover
  ) throws IOException {
    if (takeover) {
      clientID = upgradeClientID;
    }
    assertClientId(upgradeClientID);
    List<String> returningMsgs = new ArrayList<>(msgs.size()+10);
    Status status = isDone ? FINALIZATION_DONE : FINALIZATION_IN_PROGRESS;
    while (msgs.size() > 0) {
      returningMsgs.add(msgs.poll());
    }
    return new StatusAndMessages(status, returningMsgs);
  }

  private void assertClientId(String id) throws OMException {
    if (!this.clientID.equals(id)) {
      throw new OMException("Unknown client tries to get finalization status.\n"
          + "The requestor is not the initiating client of the finalization,"
          + " if you want to take over, and get unsent status messages, check"
          + " -takeover option.", INVALID_REQUEST);
    }
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
    public Void call() throws OMException {
      try {
        emitStartingMsg();

        for (OMLayoutFeature f : versionManager.unfinalizedFeatures()) {
          finalizeFeature(f);
          updateLayoutVersionInVersionFile(f);
          versionManager.finalized(f);
        }

        versionManager.completeFinalization();
        emitFinishedMsg();
      } finally {
        isDone = true;
      }
      return null;
    }

    private void finalizeFeature(OMLayoutFeature feature)
        throws OMException {
      OmUpgradeAction action = feature.onFinalizeAction().orElse(NOOP);

      if (action == NOOP) {
        emitNOOPMsg(feature.name());
        return;
      }

      putFinalizationMarkIntoVersionFile(feature);

      emitStartingFinalizationActionMsg(feature.name());
      try {
        action.executeAction(ozoneManager);
      } catch (Exception e) {
        logFinalizationFailureAndThrow(e, feature.name());
      }
      emitFinishFinalizationActionMsg(feature.name());

      removeFinalizationMarkFromVersionFile(feature);
    }

    private void updateLayoutVersionInVersionFile(OMLayoutFeature feature)
        throws OMException {
      int prevLayoutVersion = currentStoredLayoutVersion();

      updateStorageLayoutVersion(feature.layoutVersion());
      try {
        persistStorage();
      } catch (IOException e) {
        updateStorageLayoutVersion(prevLayoutVersion);
        logLayoutVersionUpdateFailureAndThrow(e);
      }
    }

    private void putFinalizationMarkIntoVersionFile(OMLayoutFeature feature)
        throws OMException {
      try {
        emitUpgradeToLayoutVersionPersistingMsg(feature.name());

        setUpgradeToLayoutVersionInStorage(feature.layoutVersion());
        persistStorage();

        emitUpgradeToLayoutVersionPersistedMsg();
      } catch (IOException e) {
        logUpgradeToLayoutVersionPersistingFailureAndThrow(feature.name(), e);
      }
    }

    private void removeFinalizationMarkFromVersionFile(OMLayoutFeature feature)
        throws OMException {
      try {
        emitRemovingUpgradeToLayoutVersionMsg(feature.name());

        unsetUpgradeToLayoutVersionInStorage();
        persistStorage();

        emitRemovedUpgradeToLayoutVersionMsg();
      } catch (IOException e) {
        logUpgradeToLayoutVersionRemovalFailureAndThrow(feature.name(), e);
      }
    }





    private void setUpgradeToLayoutVersionInStorage(int version) {
      ozoneManager.getOmStorage().setUpgradeToLayoutVersion(version);
    }

    private void unsetUpgradeToLayoutVersionInStorage() {
      ozoneManager.getOmStorage().unsetUpgradeToLayoutVersion();
    }

    private int currentStoredLayoutVersion() {
      return ozoneManager.getOmStorage().getLayoutVersion();
    }

    private void updateStorageLayoutVersion(int version) {
      ozoneManager.getOmStorage().setLayoutVersion(version);
    }

    private void persistStorage() throws IOException {
      ozoneManager.getOmStorage().persistCurrentState();
    }

    private void emitNOOPMsg(String feature) {
      String msg = "No finalization work defined for feature: " + feature + ".";
      String msg2 = "Skipping.";

      logAndEmit(msg);
      logAndEmit(msg2);
    }

    private void emitStartingMsg() {
      String msg = "Finalization started.";
      logAndEmit(msg);
    }

    private void emitFinishedMsg() {
      String msg = "Finalization is done.";
      logAndEmit(msg);
    }

    private void emitStartingFinalizationActionMsg(String feature) {
      String msg = "Executing finalization of feature: " + feature + ".";
      logAndEmit(msg);
    }

    private void emitFinishFinalizationActionMsg(String feature) {
      String msg = "The feature " + feature + " is finalized.";
      logAndEmit(msg);
    }

    private void emitUpgradeToLayoutVersionPersistingMsg(String feature) {
      String msg = "Mark finalization of " + feature + " in VERSION file.";
      logAndEmit(msg);
    }

    private void emitUpgradeToLayoutVersionPersistedMsg() {
      String msg = "Finalization mark placed.";
      logAndEmit(msg);
    }

    private void emitRemovingUpgradeToLayoutVersionMsg(String feature) {
      String msg = "Remove finalization mark of " + feature
          + " feature from VERSION file.";
      logAndEmit(msg);
    }

    private void emitRemovedUpgradeToLayoutVersionMsg() {
      String msg = "Finalization mark removed.";
      logAndEmit(msg);
    }

    private void logAndEmit(String msg) {
      LOG.info(msg);
      msgs.offer(msg);
    }

    private void logFinalizationFailureAndThrow(Exception e, String feature)
        throws OMException {
      String msg = "Error during finalization of " + feature + ".";
      logAndThrow(e, msg, LAYOUT_FEATURE_FINALIZATION_FAILED);
    }

    private void logLayoutVersionUpdateFailureAndThrow(IOException e)
        throws OMException {
      String msg = "Updating the LayoutVersion in the VERSION file failed.";
      logAndThrow(e, msg, UPDATE_LAYOUT_VERSION_FAILED);
    }

    private void logUpgradeToLayoutVersionPersistingFailureAndThrow(
        String feature, IOException e
    ) throws OMException {
      String msg = "Failed to update VERSION file with the upgrading feature: "
          + feature + ".";
      logAndThrow(e, msg, PERSIST_UPGRADE_TO_LAYOUT_VERSION_FAILED);
    }

    private void logUpgradeToLayoutVersionRemovalFailureAndThrow(
        String feature, IOException e) throws OMException {
      String msg =
          "Failed to unmark finalization of " + feature + " LayoutFeature.";
      logAndThrow(e, msg, REMOVE_UPGRADE_TO_LAYOUT_VERSION_FAILED);
    }

    private void logAndThrow(Exception e, String msg, ResultCodes resultCode)
        throws OMException {
      LOG.error(msg, e);
      throw new OMException(msg, e, resultCode);
    }
  }
}
