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

package org.apache.hadoop.ozone.recon.chatbot.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfigKeys;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes tool calls by making HTTP requests to Recon API endpoints.
 */
@Singleton
public class ToolExecutor {

  private static final Logger LOG =
      LoggerFactory.getLogger(ToolExecutor.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // We define the specific String suffixes for APIs we want to explicitly watch out for
  private static final String LIST_KEYS_ENDPOINT_SUFFIX = "/keys/listKeys";

  private final String reconBaseUrl;
  private final int connectTimeoutMs;
  private final int readTimeoutMs;

  @Inject
  public ToolExecutor(OzoneConfiguration configuration) {
    // Resolve the Recon HTTP address from ozone-site.xml (ozone.recon.http-address).
    // The configured value is typically "0.0.0.0:9888" (bind address), so we always
    // substitute 0.0.0.0 with 127.0.0.1 so the loopback call actually reaches this process.
    String rawAddress = configuration.get(
        ReconConfigKeys.OZONE_RECON_HTTP_ADDRESS_KEY,
        ReconConfigKeys.OZONE_RECON_HTTP_ADDRESS_DEFAULT);
    this.reconBaseUrl = "http://" + rawAddress.replace("0.0.0.0", "127.0.0.1");

    this.connectTimeoutMs = configuration.getInt(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_CONNECT_TIMEOUT_MS,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_CONNECT_TIMEOUT_MS_DEFAULT);
    this.readTimeoutMs = configuration.getInt(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_READ_TIMEOUT_MS,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_READ_TIMEOUT_MS_DEFAULT);

    // Execution-policy limits (max pages, page size) are owned by ChatbotAgent and
    // passed into executeToolCallWithPolicy(...) per request; we do not re-read them here.
    LOG.info("ToolExecutor initialized with Recon URL: {}, connectTimeoutMs={}, readTimeoutMs={}",
        reconBaseUrl, connectTimeoutMs, readTimeoutMs);
  }

  /**
   * What this does: It receives the request from the ChatbotAgent, cleans up the URL, and decides if it needs the
   * complex paging system (for listKeys) or just a simple, single network hit (for everything else).
   */
  public ToolExecutionOutcome executeToolCallWithPolicy(
      String endpoint,
      String method,
      Map<String, String> parameters,
      int maxPages,
      int pageSize) throws IOException {

    // First, make a safe copy of the parameters (like `limit=10`) so we can edit it without breaking anything
    Map<String, String> safeParams = parameters == null ? new HashMap<>() : new HashMap<>(parameters);

    // Normalize string. E.g., Change "clusterState" to "/api/v1/clusterState"
    String fullEndpoint = ChatbotUtils.normalizeEndpoint(endpoint);

    // If the LLM asked to list keys, redirect to our special paging loop logic!
    if (fullEndpoint.endsWith(LIST_KEYS_ENDPOINT_SUFFIX) && "GET".equalsIgnoreCase(method)) {
      return executeListKeysWithPaging(fullEndpoint, method, safeParams, maxPages, pageSize);
    }

    // For EVERY OTHER endpoint, just run a single, normal HTTP request
    JsonNode response = executeSingleCall(fullEndpoint, method, safeParams);

    // Count how many records we got back and return our structured DTO tracker
    int records = ChatbotUtils.estimateRecordCount(response);
    return new ToolExecutionOutcome(response, records, 1, false, null,
        createLimitsMap(maxPages, pageSize));
  }

  /**
   * The listKeys Pager - It uses a while() loop to continuously execute API calls, stitching all the
   * individual pages into one massive JSON array until it runs out of data or hits a hard security constraint limit.
   */
  private ToolExecutionOutcome executeListKeysWithPaging(
      String endpoint, String method, Map<String, String> parameters,
      int maxPages, int pageSize)
      throws IOException {

    // Safety Check: Did the LLM provide a bucket path to search in?
    String startPrefix = parameters.get("startPrefix");
    if (StringUtils.isBlank(startPrefix) || "/".equals(startPrefix.trim())) {
      throw new IllegalArgumentException(
          "listKeys requires 'startPrefix' at bucket level or deeper (for example /volume/bucket).");
    }

    // Figure out limits... Either use what the LLM specifically requested, or our system defaults.
    int requestedLimit = ChatbotUtils.parsePositiveInt(parameters.get("limit"), pageSize);
    int effectivePageSize = Math.max(1, Math.min(pageSize, requestedLimit));
    int safeMaxPages = Math.max(1, maxPages);

    ObjectNode merged = null;                               // This will hold the final, massive JSON object
    ArrayNode aggregatedKeys = MAPPER.createArrayNode();    // This will hold all the individual rows we find
    String nextCursor = parameters.get("prevKey");          // The "ID" of the last record so we know where to pick up
    int recordsProcessed = 0;                               // Counter for rows
    int pagesFetched = 0;                                   // Counter for pages

    // THE ENGINE LOOP: Keep pulling pages until data runs out or we hit the max page cap.
    // Total records are naturally bounded by safeMaxPages * effectivePageSize.
    while (pagesFetched < safeMaxPages) {
      Map<String, String> pageParams = new HashMap<>(parameters);
      pageParams.put("limit", String.valueOf(effectivePageSize));

      // If we have a cursor from a previous page, inject it so Recon gives us the NEXT page
      if (nextCursor != null && !nextCursor.isEmpty()) {
        pageParams.put("prevKey", nextCursor);
      } else {
        pageParams.remove("prevKey");
      }

      // FIRE THE API CALL FOR A SINGLE PAGE!
      JsonNode pageResponse = executeSingleCall(endpoint, method, pageParams);
      pagesFetched++;

      // If this is the first page, copy all the root JSON data (like total counts) into our master `merged` object
      if (merged == null && pageResponse != null && pageResponse.isObject()) {
        merged = ((ObjectNode) pageResponse).deepCopy();
      }

      // Loop over the list of keys (the rows) that Recon just gave us
      JsonNode keys = pageResponse == null ? null : pageResponse.get("keys");
      int pageCount = 0;
      if (keys != null && keys.isArray()) {
        for (JsonNode key : keys) {
          aggregatedKeys.add(key);
          recordsProcessed++;
          pageCount++;
        }
      }

      // Find the ID of the last row on this page so we can pass it into the loop for the next page
      String lastKey = ChatbotUtils.extractStringField(pageResponse, "lastKey");
      if (lastKey == null || lastKey.isEmpty() || pageCount == 0) {
        nextCursor = null;
        break;
      }
      nextCursor = lastKey;
    }

    // If we stopped because of the page cap but a cursor still remains, more data exists upstream.
    boolean truncated = nextCursor != null && !nextCursor.isEmpty();

    // Now that the loop is finished, reconstruct the final JSON block
    if (merged == null) {
      merged = MAPPER.createObjectNode();
    }
    merged.set("keys", aggregatedKeys);
    if (nextCursor != null) {
      merged.put("lastKey", nextCursor);
    }
    // Inject our metadata so ChatbotAgent can see what happened
    merged.put("truncated", truncated);
    merged.put("recordsProcessed", recordsProcessed);
    merged.put("pagesFetched", pagesFetched);

    // Package the results and send them back up to the ChatbotAgent
    return new ToolExecutionOutcome(merged, recordsProcessed, pagesFetched, truncated, nextCursor,
        createLimitsMap(safeMaxPages, effectivePageSize));
  }

  /**
   * The Actual HTTP Execution.
   *
   * <p>NOTE — Kerberos / SPNEGO: When {@code ozone.recon.http.auth.type=kerberos} is active,
   * every request to /api/v1/* is intercepted by ReconAuthFilter and requires a valid
   * SPNEGO Negotiate token. Plain {@link HttpURLConnection} carries no ticket and will
   * receive a 401 Unauthorized response. The long-term fix is to replace these loopback
   * HTTP calls with direct in-process invocations of the Recon service beans (injected via
   * Guice), which avoids the network hop and the auth requirement entirely. Until then, this
   * code works correctly for non-Kerberos deployments (the common Docker Compose use case).
   * TODO: Replace loopback HTTP with direct in-process service calls .</p>
   */
  JsonNode executeSingleCall(String endpoint, String method,
                                     Map<String, String> parameters)
      throws IOException {
    String url = buildUrl(endpoint, parameters);
    LOG.debug("Executing tool call: {} {}", method, url);

    HttpURLConnection conn = null;
    try {
      // Connect to the Recon URL
      conn = (HttpURLConnection) new URL(url).openConnection();
      conn.setRequestMethod(
          "GET".equalsIgnoreCase(method) ? "GET" : "POST");
      conn.setConnectTimeout(connectTimeoutMs);
      conn.setReadTimeout(readTimeoutMs);

      // Tell Recon we expect to receive JSON data format
      conn.setRequestProperty("Accept", "application/json");
      conn.setRequestProperty("Content-Type", "application/json");

      // Execute request.
      int statusCode = conn.getResponseCode();
      if (statusCode != 200) {
        // If the server threw a 500 error or a 404, capture the failure text and throw an exception
        String errorBody = ChatbotUtils.readErrorStream(conn);
        String errorMsg = String.format(
            "API request failed with status %d: %s",
            statusCode, errorBody);
        LOG.error(errorMsg);
        throw new IOException(errorMsg);
      }

      // Request succeeded! Read the raw byte data and convert it into a string
      String body = ChatbotUtils.readInputStream(conn);
      return ChatbotUtils.parseJsonSafely(body);
    } finally {
      // Always disconnect to free up memory on the server
      if (conn != null) {
        conn.disconnect();
      }
    }
  }

  /**
   * Transforms the LLM's parameters into a raw URL.
   * Handles both Path parameters (e.g. {path}) and Query parameters (e.g. ?limit=10).
   */
  private String buildUrl(String endpoint, Map<String, String> parameters) {
    String resolvedPath = endpoint;
    StringBuilder queryBuilder = new StringBuilder();
    boolean firstQueryParam = !endpoint.contains("?");

    for (Map.Entry<String, String> entry : parameters.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue() == null ? "" : entry.getValue();
      String placeholder = "{" + key + "}";

      // 1. Is it a Path Parameter? (e.g. replacing {path} with "vol1/bucket2")
      // If the provided endpoint string contains the placeholder block, we replace it 
      // directly inline and do NOT add it to the URL query string.
      if (resolvedPath.contains(placeholder)) {
        resolvedPath = resolvedPath.replace(placeholder, value);
      } else {
        // If the placeholder block wasn't found, we assume this is a URL filter (like ?limit=10)
        // and append it safely encoded to the end of the URL.
        queryBuilder.append(firstQueryParam ? '?' : '&');
        try {
          queryBuilder.append(key).append('=').append(URLEncoder.encode(value, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
          throw new RuntimeException("UTF-8 not supported", e);
        }
        firstQueryParam = false;
      }
    }

    // Combine the base URL, the resolved path, and the query string
    return reconBaseUrl + resolvedPath + queryBuilder.toString();
  }

  private Map<String, Object> createLimitsMap(int maxPages, int pageSize) {
    Map<String, Object> limits = new HashMap<>();
    limits.put("maxPagesPerAnswer", maxPages);
    limits.put("pageSize", pageSize);
    return limits;
  }

  /**
   * Structured tool execution result used by the policy-aware agent flow.
   */
  public static class ToolExecutionOutcome {
    private final Object responseBody;
    private final int recordsProcessed;
    private final int pagesFetched;
    private final boolean truncated;
    private final String nextCursor;
    private final Map<String, Object> limitsApplied;

    public ToolExecutionOutcome(Object responseBody,
                                int recordsProcessed,
                                int pagesFetched,
                                boolean truncated,
                                String nextCursor,
                                Map<String, Object> limitsApplied) {
      this.responseBody = responseBody;
      this.recordsProcessed = recordsProcessed;
      this.pagesFetched = pagesFetched;
      this.truncated = truncated;
      this.nextCursor = nextCursor;
      this.limitsApplied = limitsApplied;
    }

    public Object getResponseBody() {
      return responseBody;
    }

    public int getRecordsProcessed() {
      return recordsProcessed;
    }

    public int getPagesFetched() {
      return pagesFetched;
    }

    public boolean isTruncated() {
      return truncated;
    }

    public String getNextCursor() {
      return nextCursor;
    }

    public Map<String, Object> getLimitsApplied() {
      return limitsApplied;
    }
  }
}
