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

import java.util.Collections;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.SubStatus;
import org.junit.jupiter.api.Test;

class TestSnapshotDiffResponse {

  @Test
  void testReportOnlyNotFoundMessage() {
    SnapshotDiffResponse response = new SnapshotDiffResponse(createReport(),
        JobStatus.NOT_FOUND, 1000L, true);

    String message = response.toString();
    assertTrue(message.contains("No snapshot diff job found"));
    assertTrue(message.contains("--get-report"));
  }

  @Test
  void testReportOnlyRejectedMessage() {
    SnapshotDiffResponse response = new SnapshotDiffResponse(createReport(),
        JobStatus.REJECTED, 1000L, true);

    String message = response.toString();
    assertTrue(message.contains("REJECTED"));
    assertTrue(message.contains("resubmit the job without using the --get-report option"));
  }

  @Test
  void testReportOnlyFailedMessageIncludesReason() {
    SnapshotDiffResponse response = new SnapshotDiffResponse(createReport(),
        JobStatus.FAILED, 1000L, "some failure", true);

    String message = response.toString();
    assertTrue(message.contains("FAILED"));
    assertTrue(message.contains("some failure"));
    assertTrue(message.contains("resubmit the job without using the --get-report option"));
  }

  @Test
  void testReportOnlyInProgressIncludesSubStatusAndProgress() {
    SnapshotDiffResponse response = new SnapshotDiffResponse(createReport(),
        JobStatus.IN_PROGRESS, 1000L, true);
    response.setSubStatus(SubStatus.OBJECT_ID_MAP_GEN_OBS);
    response.setProgressPercent(55.5);

    String message = response.toString();
    assertTrue(message.contains("IN_PROGRESS"));
    assertTrue(message.contains("OBJECT_ID_MAP_GEN_OBS"));
    assertTrue(message.contains("Keys Processed Estimated Percentage"));
    assertTrue(message.contains("55.5"));
  }

  private SnapshotDiffReportOzone createReport() {
    return new SnapshotDiffReportOzone("snapshotRoot", "vol", "bucket", "fromSnap",
        "toSnap", Collections.emptyList(), null);
  }
}
