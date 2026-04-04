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

import org.apache.commons.lang3.StringUtils;

/**
 * POJO for Submit Snapshot Diff Response.
 */
public class SubmitSnapshotDiffResponse {
  private final String response;

  public SubmitSnapshotDiffResponse(long waitTimeInMs, SnapshotDiffResponse.JobStatus prevStatus, String prevReason) {
    StringBuilder msgBuilder = new StringBuilder();
    if (prevStatus != null) {
      msgBuilder.append("Previous snapshot diff attempt found. Status: ")
          .append(prevStatus);
      if (StringUtils.isNotEmpty(prevReason)) {
        msgBuilder.append(", reason: ").append(prevReason);
      }
      msgBuilder.append(".\n");
    }
    if (prevStatus == SnapshotDiffResponse.JobStatus.DONE || prevStatus == SnapshotDiffResponse.JobStatus.IN_PROGRESS) {
      msgBuilder.append("Please get the report using --get-report option");
      if (prevStatus == SnapshotDiffResponse.JobStatus.IN_PROGRESS) {
        msgBuilder.append(" in ").append(waitTimeInMs).append(" ms");
      }
      msgBuilder.append(".\n");
    } else {
      msgBuilder.append("Submitting a new job. Please retry after ")
          .append(waitTimeInMs)
          .append(" ms using --get-report option to get the report.\n");
    }

    this.response = msgBuilder.toString();
  }

  public SubmitSnapshotDiffResponse(String response) {
    this.response = response;
  }

  public String getResponse() {
    return response;
  }

  @Override
  public String toString() {
    return response;
  }
}
