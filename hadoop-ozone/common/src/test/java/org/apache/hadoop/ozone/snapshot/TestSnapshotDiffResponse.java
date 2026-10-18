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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.SubStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

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

  @ParameterizedTest
  @EnumSource(value = SubStatus.class, names = {"OBJECT_ID_MAP_GEN_OBS", "OBJECT_ID_MAP_GEN_FSO",
      "OBJECT_ID_MAP_GEN_FSO_FILE", "OBJECT_ID_MAP_GEN_FSO_DIR"})
  void testInProgressWithMapGenSubStatusIncludesProgress(SubStatus subStatus) {
    SnapshotDiffResponse response = new SnapshotDiffResponse(createReport(),
        JobStatus.IN_PROGRESS, 1000L, true);
    response.setSubStatus(subStatus);
    response.setProgressPercent(55.5);

    String message = response.toString();
    assertTrue(message.contains("IN_PROGRESS"));
    assertTrue(message.contains(subStatus.name()));
    assertTrue(message.contains("Keys Processed Estimated Percentage"));
    assertTrue(message.contains("55.5"));
  }

  @ParameterizedTest
  @EnumSource(value = SubStatus.class,
      names = {"OBJECT_ID_MAP_GEN_OBS", "OBJECT_ID_MAP_GEN_FSO", "OBJECT_ID_MAP_GEN_FSO_FILE",
          "OBJECT_ID_MAP_GEN_FSO_DIR"},
      mode = Mode.EXCLUDE)
  void testInProgressWithNonMapGenSubStatusRendersSubStatusButNotProgress(SubStatus subStatus) {
    SnapshotDiffResponse response = new SnapshotDiffResponse(createReport(), JobStatus.IN_PROGRESS, 1000L, true);
    response.setSubStatus(subStatus);
    response.setProgressPercent(55.5);

    String message = response.toString();
    assertTrue(message.contains("IN_PROGRESS"));
    assertTrue(message.contains(subStatus.name()));
    assertFalse(message.contains("Keys Processed Estimated Percentage"));
    assertFalse(message.contains("55.5"));
  }

  @Test
  void testInProgressWithNullSubStatusOmitsSubStatusAndProgressLines() {
    SnapshotDiffResponse response = new SnapshotDiffResponse(createReport(), JobStatus.IN_PROGRESS, 1000L, true);
    String message = response.toString();
    assertTrue(message.contains("IN_PROGRESS"));
    assertFalse(message.contains("SubStatus"));
    assertFalse(message.contains("Keys Processed Estimated Percentage"));
  }

  private SnapshotDiffReportOzone createReport() {
    return new SnapshotDiffReportOzone("snapshotRoot", "vol", "bucket", "fromSnap",
        "toSnap", Collections.emptyList(), null);
  }
}
