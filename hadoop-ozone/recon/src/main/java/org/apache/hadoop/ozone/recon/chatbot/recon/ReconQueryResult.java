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

package org.apache.hadoop.ozone.recon.chatbot.recon;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * JSON payload and execution metadata returned by {@link ReconQueryExecutor}.
 *
 * <p>Fields:
 * <ul>
 *   <li>{@link #responseBody} — the raw JSON from the Recon endpoint</li>
 *   <li>{@link #recordsProcessed} — estimated number of records in the response</li>
 *   <li>{@link #truncated} — true when records returned equals the enforced cap
 *       ({@link ReconQueryExecutor#MAX_RECORDS_PER_CALL}), indicating more data likely exists</li>
 *   <li>{@link #maxRecords} — the effective cap that was enforced on this call</li>
 * </ul>
 */
public class ReconQueryResult {
  private final JsonNode responseBody;
  private final int recordsProcessed;
  private final boolean truncated;
  private final int maxRecords;

  public ReconQueryResult(JsonNode responseBody,
                          int recordsProcessed,
                          boolean truncated,
                          int maxRecords) {
    this.responseBody = responseBody;
    this.recordsProcessed = recordsProcessed;
    this.truncated = truncated;
    this.maxRecords = maxRecords;
  }

  public JsonNode getResponseBody() {
    return responseBody;
  }

  public int getRecordsProcessed() {
    return recordsProcessed;
  }

  public boolean isTruncated() {
    return truncated;
  }

  public int getMaxRecords() {
    return maxRecords;
  }
}
