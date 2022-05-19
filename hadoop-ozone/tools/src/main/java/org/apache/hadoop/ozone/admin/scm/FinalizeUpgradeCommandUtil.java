/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.admin.scm;

import static org.apache.hadoop.ozone.upgrade.UpgradeException
    .ResultCodes.INVALID_REQUEST;

import java.io.IOException;

import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;

/**
 * Base class to help with Upgrade finalization command.
 */

public final class FinalizeUpgradeCommandUtil {

  private FinalizeUpgradeCommandUtil() {

  }

  public static void handleInvalidRequestAfterInitiatingFinalization(
      boolean force, UpgradeException e) throws IOException {
    if (e.getResult().equals(INVALID_REQUEST)) {
      if (force) {
        return;
      }
      System.err.println("Finalization is already in progress, it is not"
          + "possible to initiate it again.");
      e.printStackTrace(System.err);
      System.err.println("If you want to track progress from a new client"
          + "for any reason, use --takeover, and the status update will be"
          + "received by the new client. Note that with forcing to monitor"
          + "progress from a new client, the old one initiated the upgrade"
          + "will not be able to monitor the progress further and exit.");
      throw new IOException("Exiting...");
    } else {
      throw e;
    }
  }

  public static void emitExitMsg() {
    System.out.println("Exiting...");
  }

  public static boolean isFinalized(UpgradeFinalizer.Status status) {
    return status.equals(UpgradeFinalizer.Status.ALREADY_FINALIZED);
  }

  public static boolean isDone(UpgradeFinalizer.Status status) {
    return status.equals(UpgradeFinalizer.Status.FINALIZATION_DONE);
  }

  public static boolean isInprogress(UpgradeFinalizer.Status status) {
    return status.equals(UpgradeFinalizer.Status.FINALIZATION_IN_PROGRESS);
  }

  public static boolean isStarting(UpgradeFinalizer.Status status) {
    return status.equals(UpgradeFinalizer.Status.STARTING_FINALIZATION);
  }

  public static void emitGeneralErrorMsg() {
    System.err.println("Finalization was not successful.");
  }

  public static void emitFinishedMsg(String component) {
    System.out.println("Finalization of " + component + "'s metadata upgrade "
        + "finished.");
  }

  public static void emitCancellationMsg(String component) {
    System.err.println("Finalization command was cancelled. Note that, this"
        + "will not cancel finalization in " + component + ". Progress can be"
        + "monitored in the Ozone Manager's log.");
  }
}
