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

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus;
import org.junit.jupiter.api.Test;

class TestSubmitSnapshotDiffResponse {

  @Test
  void testSubmitResponseForQueuedJob() {
    SubmitSnapshotDiffResponse response =
        new SubmitSnapshotDiffResponse(1000L, JobStatus.QUEUED, null);

    String message = response.getResponse();
    assertTrue(message.contains("Submitting a new job"));
    assertTrue(message.contains("--get-report"));
  }

  @Test
  void testSubmitResponseForInProgressJob() {
    SubmitSnapshotDiffResponse response =
        new SubmitSnapshotDiffResponse(1000L, JobStatus.IN_PROGRESS, null);

    String message = response.getResponse();
    assertTrue(message.contains("Previous snapshot diff attempt found"));
    assertTrue(message.contains("IN_PROGRESS"));
    assertTrue(message.contains("--get-report"));
  }

  @Test
  void testSubmitResponseForDoneJob() {
    SubmitSnapshotDiffResponse response =
        new SubmitSnapshotDiffResponse(1000L, JobStatus.DONE, null);

    String message = response.getResponse();
    assertTrue(message.contains("Previous snapshot diff attempt found"));
    assertTrue(message.contains("DONE"));
    assertTrue(message.contains("--get-report"));
  }

  @Test
  void testSubmitResponseForFailedJobIncludesReason() {
    SubmitSnapshotDiffResponse response =
        new SubmitSnapshotDiffResponse(1000L, JobStatus.FAILED, "previous failure");

    String message = response.getResponse();
    assertTrue(message.contains("Previous snapshot diff attempt found"));
    assertTrue(message.contains("FAILED"));
    assertTrue(message.contains("previous failure"));
    assertTrue(message.contains("Submitting a new job"));
    assertTrue(message.contains("--get-report"));
  }

  @Test
  void testSubmitResponseForRejectedJobIncludesReason() {
    SubmitSnapshotDiffResponse response =
        new SubmitSnapshotDiffResponse(1000L, JobStatus.REJECTED, "previously rejected");

    String message = response.getResponse();
    assertTrue(message.contains("Previous snapshot diff attempt found"));
    assertTrue(message.contains("REJECTED"));
    assertTrue(message.contains("previously rejected"));
    assertTrue(message.contains("Submitting a new job"));
    assertTrue(message.contains("--get-report"));
  }

  @Test
  void testSubmitResponseForCancelledJobIncludesReason() {
    SubmitSnapshotDiffResponse response =
        new SubmitSnapshotDiffResponse(1000L, JobStatus.CANCELLED, "previously cancelled");

    String message = response.getResponse();
    assertTrue(message.contains("Previous snapshot diff attempt found"));
    assertTrue(message.contains("CANCELLED"));
    assertTrue(message.contains("previously cancelled"));
    assertTrue(message.contains("Submitting a new job"));
    assertTrue(message.contains("--get-report"));
  }
}
