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

import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.ON_FIRST_UPGRADE_START;
import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.VALIDATE_IN_PREFINALIZE;
import static org.apache.hadoop.ozone.upgrade.UpgradeException.ResultCodes.FIRST_UPGRADE_START_ACTION_FAILED;
import static org.apache.hadoop.ozone.upgrade.UpgradeException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.upgrade.UpgradeException.ResultCodes.LAYOUT_FEATURE_FINALIZATION_FAILED;
import static org.apache.hadoop.ozone.upgrade.UpgradeException.ResultCodes.PREFINALIZE_ACTION_VALIDATION_FAILED;
import static org.apache.hadoop.ozone.upgrade.UpgradeException.ResultCodes.UPDATE_LAYOUT_VERSION_FAILED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_DONE;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_REQUIRED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.STARTING_FINALIZATION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeAction;
import org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType;
import org.apache.hadoop.ozone.upgrade.UpgradeException.ResultCodes;

import com.google.common.annotations.VisibleForTesting;

/**
 * UpgradeFinalizer implementation for the Storage Container Manager service.
 */
public abstract class BasicUpgradeFinalizer
    <T, V extends AbstractLayoutVersionManager> implements UpgradeFinalizer<T> {

  private final V versionManager;
  private String clientID;
  private T component;
  private UpgradeFinalizationExecutor<T> finalizationExecutor;
  // Ensures that there is only one finalization thread running at a time.
  private final Lock finalizationLock;

  private final Queue<String> msgs = new ConcurrentLinkedQueue<>();
  private boolean isDone = false;

  public BasicUpgradeFinalizer(V versionManager) {
    this(versionManager, new DefaultUpgradeFinalizationExecutor<>());
  }

  public BasicUpgradeFinalizer(V versionManager,
                               UpgradeFinalizationExecutor<T> executor) {
    this.versionManager = versionManager;
    this.finalizationExecutor = executor;
    this.finalizationLock = new ReentrantLock();
  }

  public StatusAndMessages finalize(String upgradeClientID, T service)
      throws IOException {
    // In some components, finalization can be driven asynchronously by a
    // thread, not a single serialized Ratis request.
    // A second request could closely follow the first before it
    // sets the finalization status to FINALIZATION_IN_PROGRESS.
    // Therefore, a lock is used to make sure only one finalization thread is
    // running at a time.
    StatusAndMessages response = initFinalize(upgradeClientID, service);
    if (finalizationLock.tryLock()) {
      try {
        // Even if the status indicates finalization completed, the component
        // may not have finished all its specific steps if finalization was
        // interrupted, so we should re-invoke them here.
        if (response.status() == FINALIZATION_REQUIRED ||
            !componentFinishedFinalizationSteps(service)) {
          finalizationExecutor.execute(service, this);
          response = STARTING_MSG;
        }
      } finally {
        finalizationLock.unlock();
      }
    } else {
      // We could not acquire the lock, so either finalization is ongoing, or
      // it already finished but we received multiple requests to
      // run it at the same time.
      if (!isFinalized(response.status())) {
        response = FINALIZATION_IN_PROGRESS_MSG;
      }
    }
    return response;
  }

  public synchronized StatusAndMessages reportStatus(
      String upgradeClientID, boolean takeover) throws UpgradeException {
    if (takeover) {
      clientID = upgradeClientID;
    }
    assertClientId(upgradeClientID);
    List<String> returningMsgs = new ArrayList<>(msgs.size() + 10);
    Status status = versionManager.getUpgradeState();
    while (msgs.size() > 0) {
      returningMsgs.add(msgs.poll());
    }
    return new StatusAndMessages(status, returningMsgs);
  }

  @Override
  public synchronized Status getStatus() {
    return versionManager.getUpgradeState();
  }

  protected void preFinalizeUpgrade(T service) throws IOException {
    // No Op by default.
  }

  protected void postFinalizeUpgrade(T service) throws IOException {
    // No Op by default.
  }

  @Override
  public void finalizeAndWaitForCompletion(
      String upgradeClientID, T service, long maxTimeToWaitInSeconds)
      throws IOException {

    StatusAndMessages response = finalize(upgradeClientID, service);
    LOG.info("Finalization Messages : {} ", response.msgs());
    if (isFinalized(response.status())) {
      return;
    }

    boolean success = false;
    long endTime = System.currentTimeMillis() +
        TimeUnit.SECONDS.toMillis(maxTimeToWaitInSeconds);
    while (System.currentTimeMillis() < endTime) {
      try {
        response = reportStatus(upgradeClientID, false);
        LOG.info("Finalization Messages : {} ", response.msgs());
        if (isFinalized(response.status())) {
          success = true;
          break;
        }
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Finalization Wait thread interrupted!");
      }
    }
    if (!success) {
      throw new IOException(
          String.format("Unable to finalize after waiting for %d seconds",
          maxTimeToWaitInSeconds));
    }
  }

  @VisibleForTesting
  public boolean isFinalizationDone() {
    return isDone;
  }

  @VisibleForTesting
  public void markFinalizationDone() {
    isDone = true;
  }

  public V getVersionManager() {
    return versionManager;
  }

  private synchronized StatusAndMessages initFinalize(
      String upgradeClientID, T id) throws UpgradeException {
    switch (versionManager.getUpgradeState()) {
    case STARTING_FINALIZATION:
      return STARTING_MSG;
    case FINALIZATION_IN_PROGRESS:
      return FINALIZATION_IN_PROGRESS_MSG;
    case FINALIZATION_DONE:
    case ALREADY_FINALIZED:
      if (versionManager.needsFinalization()) {
        throw new UpgradeException("Upgrade found in inconsistent state. " +
            "Upgrade state is FINALIZATION Complete while MLV has not been " +
            "upgraded to SLV.", INVALID_REQUEST);
      }
      return FINALIZED_MSG;
    default:
      if (!versionManager.needsFinalization()) {
        throw new UpgradeException("Upgrade found in inconsistent state. " +
            "Upgrade state is FINALIZATION_REQUIRED while MLV has been " +
            "upgraded to SLV.", INVALID_REQUEST);
      }
      versionManager.setUpgradeState(STARTING_FINALIZATION);

      this.clientID = upgradeClientID;
      this.component = id;
      return FINALIZATION_REQUIRED_MSG;
    }
  }

  private void assertClientId(String id) throws UpgradeException {
    if (this.clientID == null || !this.clientID.equals(id)) {
      throw new UpgradeException("Unknown client tries to get finalization " +
          "status.\n The requestor is not the initiating client of the " +
          "finalization, if you want to take over, and get unsent status " +
          "messages, check -takeover option.", INVALID_REQUEST);
    }
  }

  private static boolean isFinalized(Status status) {
    return status.equals(Status.ALREADY_FINALIZED)
        || status.equals(FINALIZATION_DONE);
  }

  /**
   * Child classes that have additional finalization steps can override this
   * method to check component specific state to determine whether
   * finalization still needs to be run.
   */
  protected boolean componentFinishedFinalizationSteps(T service) {
    return true;
  }

  public abstract void finalizeLayoutFeature(LayoutFeature lf, T context)
      throws UpgradeException;

  protected void finalizeLayoutFeature(LayoutFeature lf, Optional<?
      extends UpgradeAction> action, Storage storage)
      throws UpgradeException {
    runFinalizationAction(lf, action);
    updateLayoutVersionInVersionFile(lf, storage);
    versionManager.finalized(lf);
  }

  protected void runFinalizationAction(LayoutFeature feature,
      Optional<?extends UpgradeAction> action) throws UpgradeException {

    if (!action.isPresent()) {
      emitNOOPMsg(feature.name());
    } else {
      LOG.info("Running finalization actions for layout feature: {}", feature);
      try {
        action.get().execute(component);
      } catch (Exception e) {
        logFinalizationFailureAndThrow(e, feature.name());
      }
    }
  }

  @VisibleForTesting
  protected void runPrefinalizeStateActions(Function<LayoutFeature,
      Function<UpgradeActionType, Optional<? extends UpgradeAction>>> aFunction,
      Storage storage, T service) throws IOException {

    if (!versionManager.needsFinalization()) {
      return;
    }
    this.component = service;
    LOG.info("Running pre-finalized state validations for unfinalized " +
        "layout features.");
    for (Object obj : versionManager.unfinalizedFeatures()) {
      LayoutFeature lf = (LayoutFeature) obj;
      Function<UpgradeActionType, Optional<? extends UpgradeAction>> function =
          aFunction.apply(lf);
      Optional<? extends UpgradeAction> action =
          function.apply(VALIDATE_IN_PREFINALIZE);
      if (action.isPresent()) {
        runValidationAction(lf, action.get());
      }
    }

    LOG.info("Running first upgrade commands for unfinalized layout features.");
    for (Object obj : versionManager.unfinalizedFeatures()) {
      LayoutFeature lf = (LayoutFeature) obj;
      Function<UpgradeActionType, Optional<? extends UpgradeAction>> function =
          aFunction.apply(lf);
      Optional<? extends UpgradeAction> action =
          function.apply(ON_FIRST_UPGRADE_START);
      if (action.isPresent()) {
        runFirstUpgradeAction(lf, action.get(), storage);
      }
    }
  }

  private void runValidationAction(LayoutFeature f, UpgradeAction action)
      throws UpgradeException {
    try {
      LOG.info("Executing pre finalize state validation {}", action.name());
      action.execute(component);
    } catch (Exception ex) {
      String msg = "Exception while running pre finalize state validation " +
          "for feature %s";
      LOG.error(String.format(msg, f.name()));
      throw new UpgradeException(
          String.format(msg, f.name()), ex,
          PREFINALIZE_ACTION_VALIDATION_FAILED);
    }
  }

  private void runFirstUpgradeAction(LayoutFeature f, UpgradeAction action,
                                     Storage storage) throws IOException {
    try {
      int versionOnDisk = storage.getFirstUpgradeActionLayoutVersion();
      if (f.layoutVersion() > versionOnDisk) {
        LOG.info("Executing first upgrade start action {}", action.name());
        action.execute(component);

        storage.setFirstUpgradeActionLayoutVersion(f.layoutVersion());
        persistStorage(storage);
      } else {
        LOG.info("Skipping action {} since it has already been run.",
            action.name());
      }
    } catch (Exception ex) {
      String msg = "Exception while running first upgrade run actions " +
          "for feature %s";
      LOG.error(String.format(msg, f.name()));
      throw new UpgradeException(
          String.format(msg, f.name()), ex, FIRST_UPGRADE_START_ACTION_FAILED);
    }
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
    String msg = "No onFinalize work defined for feature: " + feature + ".";

    logAndEmit(msg);
  }

  protected void emitStartingMsg() {
    String msg = "Finalization started.";
    logAndEmit(msg);
  }

  protected void emitFinishedMsg() {
    String msg = "Finalization is done.";
    logAndEmit(msg);
  }

  protected void logAndEmit(String msg) {
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

  private void logAndThrow(Exception e, String msg, ResultCodes resultCode)
      throws UpgradeException {
    LOG.error(msg, e);
    throw new UpgradeException(msg, e, resultCode);
  }

  @VisibleForTesting
  public void setFinalizationExecutor(DefaultUpgradeFinalizationExecutor<T>
                                            executor) {
    finalizationExecutor = executor;
  }
}
