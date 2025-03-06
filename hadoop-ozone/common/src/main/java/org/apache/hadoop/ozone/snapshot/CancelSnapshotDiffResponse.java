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

package org.apache.hadoop.ozone.snapshot;

/**
 * POJO for Cancel Snapshot Diff Response.
 */
public class CancelSnapshotDiffResponse {
  private final String message;

  public CancelSnapshotDiffResponse(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public String toString() {
    return message;
  }

  /**
   * Snapshot diff cancel message.
   */
  public enum CancelMessage {
    CANCEL_SUCCEEDED("Snapshot diff job has been cancelled."),
    CANCEL_FAILED("Failed to cancel the job. Its state has been updated in " +
        "between cancel flow. Please retry."),
    CANCEL_JOB_NOT_EXIST("Snapshot diff job doesn't exist for given" +
        " parameters."),
    CANCEL_ALREADY_DONE_JOB("Snapshot diff job has already completed."),
    CANCEL_ALREADY_CANCELLED_JOB(
        "Snapshot diff job has been cancelled already."),
    CANCEL_ALREADY_FAILED_JOB(
        "Snapshot diff job has been failed."),
    CANCEL_NON_CANCELLABLE(
        "Snapshot diff job is not in cancellable state.");

    private final String message;

    CancelMessage(String message) {
      this.message = message;
    }

    public String getMessage() {
      return message;
    }
  }
}
