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

import static org.apache.hadoop.ozone.upgrade.UpgradeException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.upgrade.UpgradeException.ResultCodes.LAYOUT_FEATURE_FINALIZATION_FAILED;
import static org.apache.hadoop.ozone.upgrade.UpgradeException.ResultCodes.PERSIST_UPGRADE_TO_LAYOUT_VERSION_FAILED;
import static org.apache.hadoop.ozone.upgrade.UpgradeException.ResultCodes.REMOVE_UPGRADE_TO_LAYOUT_VERSION_FAILED;
import static org.apache.hadoop.ozone.upgrade.UpgradeException.ResultCodes.UPDATE_LAYOUT_VERSION_FAILED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_DONE;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_IN_PROGRESS;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_REQUIRED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.STARTING_FINALIZATION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeAction;
import org.apache.hadoop.ozone.upgrade.UpgradeException.ResultCodes;

/**
 * UpgradeFinalizer implementation for the Storage Container Manager service.
 */
@SuppressWarnings("checkstyle:VisibilityModifier")
public class BasicUpgradeFinalizer<T, V extends AbstractLayoutVersionManager>
    implements UpgradeFinalizer<T> {

  protected V versionManager;
  protected String clientID;
  protected T component;

  private Queue<String> msgs = new ConcurrentLinkedQueue<>();
  protected boolean isDone = false;

  public BasicUpgradeFinalizer(V versionManager) {
    this.versionManager = versionManager;
  }

  public boolean isFinalizationDone() {
    return isDone;
  }

  public synchronized StatusAndMessages preFinalize(String upgradeClientID,
                                                    T id)
      throws UpgradeException {
    switch (versionManager.getUpgradeState()) {
    case STARTING_FINALIZATION:
      return STARTING_MSG;
    case FINALIZATION_IN_PROGRESS:
      return FINALIZATION_IN_PROGRESS_MSG;
    case FINALIZATION_DONE:
    case ALREADY_FINALIZED:
      return FINALIZED_MSG;
    default:
      if (!versionManager.needsFinalization()) {
        throw new UpgradeException("Upgrade found in inconsistent state. " +
            "Upgrade state is FINALIZATION_REQUIRED while MLV has been " +
            "upgraded to SLV.", INVALID_REQUEST);
      }
      versionManager.setUpgradeState(STARTING_FINALIZATION);

      clientID = upgradeClientID;
      this.component = id;
      return FINALIZATION_REQUIRED_MSG;
    }
  }

  /*
   * This method must be overriden by the component implementing the
   * finalization logic.
   */
  public StatusAndMessages finalize(String upgradeClientID, T id)
      throws IOException {
    StatusAndMessages response = preFinalize(upgradeClientID, id);
    if (response.status() != FINALIZATION_REQUIRED) {
      return response;
    }

    /**
     * Overriding class should schedule actual finalization logic
     * in a separate thread here.
     */
    return STARTING_MSG;
  }

  @Override
  public synchronized StatusAndMessages reportStatus(
      String upgradeClientID, boolean takeover
  ) throws UpgradeException {
    if (takeover) {
      clientID = upgradeClientID;
    }
    assertClientId(upgradeClientID);
    List<String> returningMsgs = new ArrayList<>(msgs.size()+10);
    Status status = versionManager.getUpgradeState();
    while (msgs.size() > 0) {
      returningMsgs.add(msgs.poll());
    }
    return new StatusAndMessages(status, returningMsgs);
  }

  private void assertClientId(String id) throws UpgradeException {
    if (!this.clientID.equals(id)) {
      throw new UpgradeException("Unknown client tries to get finalization " +
          "status.\n The requestor is not the initiating client of the " +
          "finalization, if you want to take over, and get unsent status " +
          "messages, check -takeover option.", INVALID_REQUEST);
    }
  }

  protected void finalizeFeature(LayoutFeature feature, Storage config)
      throws UpgradeException {
    Optional<? extends UpgradeAction> action = feature.onFinalizeAction();

    if (!action.isPresent()) {
      emitNOOPMsg(feature.name());
      return;
    }

    putFinalizationMarkIntoVersionFile(feature, config);

    emitStartingFinalizationActionMsg(feature.name());
    try {
      UpgradeAction<T> newaction = action.get();
      newaction.executeAction(component);
    } catch (Exception e) {
      logFinalizationFailureAndThrow(e, feature.name());
    }
    emitFinishFinalizationActionMsg(feature.name());

    removeFinalizationMarkFromVersionFile(feature, config);
  }

  protected void updateLayoutVersionInVersionFile(LayoutFeature feature,
                                                  Storage config)
      throws UpgradeException {
    int prevLayoutVersion = currentStoredLayoutVersion(config);

    updateStorageLayoutVersion(feature.layoutVersion(), config);
    try {
      persistStorage(config);
    } catch (IOException e) {
      updateStorageLayoutVersion(prevLayoutVersion, config);
      logLayoutVersionUpdateFailureAndThrow(e);
    }
  }

  protected void putFinalizationMarkIntoVersionFile(LayoutFeature feature,
                                                  Storage config)
      throws UpgradeException {
    try {
      emitUpgradeToLayoutVersionPersistingMsg(feature.name());

      setUpgradeToLayoutVersionInStorage(feature.layoutVersion(), config);
      persistStorage(config);

      emitUpgradeToLayoutVersionPersistedMsg();
    } catch (IOException e) {
      logUpgradeToLayoutVersionPersistingFailureAndThrow(feature.name(), e);
    }
  }

  protected void removeFinalizationMarkFromVersionFile(
      LayoutFeature feature, Storage config) throws UpgradeException {
    try {
      emitRemovingUpgradeToLayoutVersionMsg(feature.name());

      unsetUpgradeToLayoutVersionInStorage(config);
      persistStorage(config);

      emitRemovedUpgradeToLayoutVersionMsg();
    } catch (IOException e) {
      logUpgradeToLayoutVersionRemovalFailureAndThrow(feature.name(), e);
    }
  }

  private void setUpgradeToLayoutVersionInStorage(int version,
                                                  Storage config) {
    config.setUpgradeToLayoutVersion(version);
  }

  private void unsetUpgradeToLayoutVersionInStorage(Storage config) {
    config.unsetUpgradeToLayoutVersion();
  }

  private int currentStoredLayoutVersion(Storage config) {
    return config.getLayoutVersion();
  }

  private void updateStorageLayoutVersion(int version, Storage config) {
    config.setLayoutVersion(version);
  }

  private void persistStorage(Storage config) throws IOException {
    config.persistCurrentState();
  }

  protected void emitNOOPMsg(String feature) {
    String msg = "No finalization work defined for feature: " + feature + ".";
    String msg2 = "Skipping.";

    logAndEmit(msg);
    logAndEmit(msg2);
  }

  protected void emitStartingMsg() {
    String msg = "Finalization started.";
    logAndEmit(msg);
  }

  protected void emitFinishedMsg() {
    String msg = "Finalization is done.";
    logAndEmit(msg);
  }

  protected void emitStartingFinalizationActionMsg(String feature) {
    String msg = "Executing finalization of feature: " + feature + ".";
    logAndEmit(msg);
  }

  protected void emitFinishFinalizationActionMsg(String feature) {
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

  protected void logFinalizationFailureAndThrow(Exception e, String feature)
      throws UpgradeException {
    String msg = "Error during finalization of " + feature + ".";
    logAndThrow(e, msg, LAYOUT_FEATURE_FINALIZATION_FAILED);
  }

  private void logLayoutVersionUpdateFailureAndThrow(IOException e)
      throws UpgradeException {
    String msg = "Updating the LayoutVersion in the VERSION file failed.";
    logAndThrow(e, msg, UPDATE_LAYOUT_VERSION_FAILED);
  }

  private void logUpgradeToLayoutVersionPersistingFailureAndThrow(
      String feature, IOException e
  ) throws UpgradeException {
    String msg = "Failed to update VERSION file with the upgrading feature: "
        + feature + ".";
    logAndThrow(e, msg, PERSIST_UPGRADE_TO_LAYOUT_VERSION_FAILED);
  }

  private void logUpgradeToLayoutVersionRemovalFailureAndThrow(
      String feature, IOException e) throws UpgradeException {
    String msg =
        "Failed to unmark finalization of " + feature + " LayoutFeature.";
    logAndThrow(e, msg, REMOVE_UPGRADE_TO_LAYOUT_VERSION_FAILED);
  }

  private void logAndThrow(Exception e, String msg, ResultCodes resultCode)
      throws UpgradeException {
    LOG.error(msg, e);
    throw new UpgradeException(msg, e, resultCode);
  }
}
