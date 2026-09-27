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
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.chatbot.agent.ChatbotUtils;

/**
 * Single chokepoint that executes authorized Recon data queries on behalf of the chatbot.
 *
 * <p>Every chatbot-initiated Recon API call passes through {@link #execute}, which:
 * <ol>
 *   <li>Strips {@code prevKey} — the chatbot never paginates.</li>
 *   <li>Clamps {@code limit} to at most {@link #MAX_RECORDS_PER_CALL} — this is not a
 *       search engine and returning unbounded data would exhaust memory and LLM context.</li>
 *   <li>Delegates to {@link ReconEndpointRouter} for the actual in-process call.</li>
 *   <li>Unwraps and returns the JSON response along with record-count metadata.</li>
 * </ol>
 *
 * <p>Endpoint-level safety checks (e.g. requiring a bucket-scoped {@code startPrefix} for
 * {@code listKeys}) are the responsibility of {@code ChatbotAgent.validateToolCall},
 * which runs before this class is ever invoked.
 */
@Singleton
public class ReconQueryExecutor {

  /**
   * Hard cap on the number of records returned per chatbot API call.
   * Keeping this at 1000 prevents memory exhaustion on large clusters and ensures
   * the response fits comfortably within the LLM's context window for summarization.
   */
  public static final int MAX_RECORDS_PER_CALL = 1000;

  private final ReconEndpointRouter router;

  @Inject
  public ReconQueryExecutor(ReconEndpointRouter router) {
    this.router = router;
  }

  /**
   * Executes a single authorized Recon query and returns the result with metadata.
   *
   * @param toolName   registered tool name to dispatch (e.g. {@code api_v1_datanodes})
   * @param parameters query parameters from the LLM tool call (defensively copied; never mutated)
   * @return the JSON response body, estimated record count, truncation flag, and enforced cap
   * @throws IOException if the underlying endpoint call fails
   */
  public ReconQueryResult execute(String toolName, Map<String, String> parameters)
      throws IOException {
    Map<String, String> params = new HashMap<>(
        parameters == null ? Collections.emptyMap() : parameters);
    // The chatbot never auto-paginates; always strip any cursor the LLM may have included.
    params.remove(ReconConstants.RECON_QUERY_PREVKEY);

    // Clamp the limit to MAX_RECORDS_PER_CALL. The router receives this pre-validated value.
    int requested = ChatbotUtils.parsePositiveInt(
        params.get(ReconConstants.RECON_QUERY_LIMIT), MAX_RECORDS_PER_CALL);
    int effective = Math.min(requested, MAX_RECORDS_PER_CALL);
    params.put(ReconConstants.RECON_QUERY_LIMIT, String.valueOf(effective));

    Response response = router.route(toolName, params);
    JsonNode jsonNode = ReconResponseUnwrapper.unwrap(response);

    int records = ChatbotUtils.estimateRecordCount(jsonNode);
    // Truncation is detected when the returned count equals the enforced cap.
    // This is a heuristic: it produces a false positive when the data happens to contain
    // exactly `effective` records, but it is safe to over-report (tells user to narrow scope).
    boolean truncated = records >= effective;

    return new ReconQueryResult(jsonNode, records, truncated, effective);
  }
}
