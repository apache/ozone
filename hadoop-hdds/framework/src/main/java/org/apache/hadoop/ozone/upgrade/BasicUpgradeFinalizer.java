/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import static org.apache.hadoop.ozone.upgrade.UpgradeException.ResultCodes.UPDATE_LAYOUT_VERSION_FAILED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.FINALIZATION_IN_PROGRESS_MSG;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.FINALIZATION_REQUIRED_MSG;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.FINALIZED_MSG;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.STARTING_MSG;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status.FINALIZATION_DONE;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status.FINALIZATION_IN_PROGRESS;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status.FINALIZATION_REQUIRED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status.STARTING_FINALIZATION;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.upgrade.UpgradeException.ResultCodes;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.StatusAndMessages;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.exceptions.NotLeaderException;

/**
 * Base UpgradeFinalizer implementation to be extended by services.
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

  @Override
  public StatusAndMessages finalize(String upgradeClientID, T service)
      throws IOException {
    // In some components, finalization can be driven asynchronously by a
    // thread, not a single serialized Ratis request.
    // A second request could closely follow the first before it
    // sets the finalization status to FINALIZATION_IN_PROGRESS.
    // Therefore, a lock is used to make sure only one finalization thread is
    // running at a time.
    if (isFinalized(versionManager.getUpgradeState())) {
      return FINALIZED_MSG;
    }
    if (finalizationLock.tryLock()) {
      try {
        StatusAndMessages response = initFinalize(upgradeClientID, service);
        // If we were able to enter the lock and finalization status is "in
        // progress", we should resume finalization because the last attempt
        // was interrupted. If an attempt was currently ongoing, the lock
        // would have been held.
        if (response.status() == FINALIZATION_REQUIRED ||
            response.status() == FINALIZATION_IN_PROGRESS) {
          finalizationExecutor.execute(service, this);
          return STARTING_MSG;
        }
        // Else, the initial response we got from initFinalize can be used,
        // since we do not need to start/resume finalization.
        return response;
      } catch (NotLeaderException e) {
        LOG.info("Leader change encountered during finalization. This " +
            "component will continue finalization as directed by the new " +
            "leader.", e);
        return FINALIZATION_IN_PROGRESS_MSG;
      } finally {
        finalizationLock.unlock();
      }
    } else {
      // Finalization has not completed, but another thread holds the lock to
      // run finalization.
      return FINALIZATION_IN_PROGRESS_MSG;
    }
  }

  @Override
  public synchronized StatusAndMessages reportStatus(
      String upgradeClientID, boolean takeover) throws UpgradeException {
    if (takeover) {
      clientID = upgradeClientID;
    }
    assertClientId(upgradeClientID);
    List<String> returningMsgs = new ArrayList<>(msgs.size() + 10);
    Status status = versionManager.getUpgradeState();
    while (!msgs.isEmpty()) {
      returningMsgs.add(msgs.poll());
    }
    return new StatusAndMessages(status, returningMsgs);
  }

  @Override
  public synchronized Status getStatus() {
    return versionManager.getUpgradeState();
  }

  /**
   * Child classes may override this method to set when finalization has
   * begun progress.
   */
  protected void preFinalizeUpgrade(T service) throws IOException {
    versionManager.setUpgradeState(FINALIZATION_IN_PROGRESS);
  }

  /**
   * Child classes may override this method to delay finalization being
   * marked done until a set of post finalize actions complete.
   */
  protected void postFinalizeUpgrade(T service) throws IOException {
    versionManager.setUpgradeState(FINALIZATION_DONE);
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
    long endTime = Time.monotonicNow() +
        TimeUnit.SECONDS.toMillis(maxTimeToWaitInSeconds);
    while (Time.monotonicNow() < endTime) {
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
          "status.\n The requester is not the initiating client of the " +
          "finalization, if you want to take over, and get unsent status " +
          "messages, check -takeover option.", INVALID_REQUEST);
    }
  }

  private static boolean isFinalized(Status status) {
    return status.equals(Status.ALREADY_FINALIZED)
        || status.equals(FINALIZATION_DONE);
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
      Optional<? extends UpgradeAction> action) throws UpgradeException {

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
