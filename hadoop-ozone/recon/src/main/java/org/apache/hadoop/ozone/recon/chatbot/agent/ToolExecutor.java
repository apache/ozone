/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Executes tool calls by making HTTP requests to Recon API endpoints.
 */
@Singleton
public class ToolExecutor {

  private static final Logger LOG =
      LoggerFactory.getLogger(ToolExecutor.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String LIST_KEYS_ENDPOINT_SUFFIX = "/keys/listKeys";
  private static final String NAMESPACE_DU_SUFFIX = "/namespace/du";
  private static final String NAMESPACE_USAGE_SUFFIX = "/namespace/usage";
  private static final String TASKS_SUFFIX = "/tasks";
  private static final String TASKS_STATUS_SUFFIX = "/tasks/status";
  private static final String TASK_STATUS_SUFFIX = "/task/status";

  private final String reconBaseUrl;
  private final HttpClient httpClient;
  private final int defaultMaxRecords;
  private final int defaultMaxPages;
  private final int defaultPageSize;

  @Inject
  public ToolExecutor(OzoneConfiguration configuration) {
    // Get Recon base URL from configuration
    // Default to localhost for local development
    this.reconBaseUrl = "http://localhost:9888";

    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .build();
    this.defaultMaxRecords = configuration.getInt(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_MAX_RECORDS,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_MAX_RECORDS_DEFAULT);
    this.defaultMaxPages = configuration.getInt(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_MAX_PAGES,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_MAX_PAGES_DEFAULT);
    this.defaultPageSize = configuration.getInt(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_PAGE_SIZE,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_PAGE_SIZE_DEFAULT);

    LOG.info("ToolExecutor initialized with Recon URL: {}, maxRecords={}, " +
            "maxPages={}, pageSize={}",
        reconBaseUrl, defaultMaxRecords, defaultMaxPages, defaultPageSize);
  }

  /**
   * Executes a tool call with bounded paging policy and returns execution
   * coverage metadata along with the response payload.
   */
  public ToolExecutionOutcome executeToolCallWithPolicy(
      String endpoint, String method, Map<String, String> parameters,
      int maxRecords, int maxPages, int pageSize)
      throws IOException, InterruptedException {

    Map<String, String> safeParams =
        parameters == null ? new HashMap<>() : new HashMap<>(parameters);
    String fullEndpoint = normalizeEndpoint(endpoint);

    if (fullEndpoint.endsWith(LIST_KEYS_ENDPOINT_SUFFIX) &&
        "GET".equalsIgnoreCase(method)) {
      return executeListKeysWithPaging(fullEndpoint, method, safeParams,
          maxRecords, maxPages, pageSize);
    }

    JsonNode response = executeSingleCall(fullEndpoint, method, safeParams);
    int records = estimateRecordCount(response);
    return new ToolExecutionOutcome(response, records, 1, false,
        null, createLimitsMap(maxRecords, maxPages, pageSize));
  }

  private ToolExecutionOutcome executeListKeysWithPaging(
      String endpoint, String method, Map<String, String> parameters,
      int maxRecords, int maxPages, int pageSize)
      throws IOException, InterruptedException {

    String startPrefix = parameters.get("startPrefix");
    if (startPrefix == null || startPrefix.trim().isEmpty() ||
        "/".equals(startPrefix.trim())) {
      throw new IllegalArgumentException("listKeys requires 'startPrefix' at " +
          "bucket level or deeper (for example /volume/bucket).");
    }

    int requestedLimit = parsePositiveInt(parameters.get("limit"), pageSize);
    int effectivePageSize = Math.max(1, Math.min(pageSize, requestedLimit));
    int safeMaxRecords = Math.max(1, maxRecords);
    int safeMaxPages = Math.max(1, maxPages);

    ObjectNode merged = null;
    ArrayNode aggregatedKeys = MAPPER.createArrayNode();
    String nextCursor = parameters.get("prevKey");
    int recordsProcessed = 0;
    int pagesFetched = 0;
    boolean truncated = false;

    while (pagesFetched < safeMaxPages && recordsProcessed < safeMaxRecords) {
      Map<String, String> pageParams = new HashMap<>(parameters);
      int remaining = safeMaxRecords - recordsProcessed;
      int pageLimit = Math.max(1, Math.min(effectivePageSize, remaining));
      pageParams.put("limit", String.valueOf(pageLimit));
      if (nextCursor != null && !nextCursor.isEmpty()) {
        pageParams.put("prevKey", nextCursor);
      } else {
        pageParams.remove("prevKey");
      }

      JsonNode pageResponse = executeSingleCall(endpoint, method, pageParams);
      pagesFetched++;

      if (merged == null && pageResponse != null && pageResponse.isObject()) {
        merged = ((ObjectNode) pageResponse).deepCopy();
      }

      JsonNode keys = pageResponse == null ? null : pageResponse.get("keys");
      int pageCount = 0;
      if (keys != null && keys.isArray()) {
        for (JsonNode key : keys) {
          if (recordsProcessed >= safeMaxRecords) {
            truncated = true;
            break;
          }
          aggregatedKeys.add(key);
          recordsProcessed++;
          pageCount++;
        }
      }

      String lastKey = extractStringField(pageResponse, "lastKey");
      if (lastKey == null || lastKey.isEmpty() || pageCount == 0) {
        nextCursor = null;
        break;
      }
      nextCursor = lastKey;

      if (recordsProcessed >= safeMaxRecords || pagesFetched >= safeMaxPages) {
        truncated = true;
      }
    }

    if (merged == null) {
      merged = MAPPER.createObjectNode();
    }
    merged.set("keys", aggregatedKeys);
    if (nextCursor != null) {
      merged.put("lastKey", nextCursor);
    }
    merged.put("truncated", truncated);
    merged.put("recordsProcessed", recordsProcessed);
    merged.put("pagesFetched", pagesFetched);

    return new ToolExecutionOutcome(merged, recordsProcessed, pagesFetched,
        truncated, nextCursor, createLimitsMap(safeMaxRecords,
        safeMaxPages, effectivePageSize));
  }

  private JsonNode executeSingleCall(String endpoint, String method,
                                     Map<String, String> parameters)
      throws IOException, InterruptedException {
    String resolvedEndpoint = replacePathParameters(endpoint, parameters);
    String url = buildUrl(resolvedEndpoint, parameters);
    LOG.debug("Executing tool call: {} {}", method, url);

    HttpRequest request = buildRequest(url, method);
    HttpResponse<String> response = httpClient.send(
        request, HttpResponse.BodyHandlers.ofString());
    ensureSuccess(response);
    return parseJsonSafely(response.body());
  }

  private String normalizeEndpoint(String endpoint) {
    if (endpoint == null || endpoint.trim().isEmpty()) {
      throw new IllegalArgumentException("Tool endpoint cannot be empty");
    }
    String fullEndpoint = endpoint;
    if (!fullEndpoint.startsWith("/api/v1/")) {
      fullEndpoint = "/api/v1" +
          (endpoint.startsWith("/") ? endpoint : "/" + endpoint);
    }
    if (fullEndpoint.endsWith(NAMESPACE_DU_SUFFIX)) {
      String mapped = fullEndpoint.substring(
          0, fullEndpoint.length() - NAMESPACE_DU_SUFFIX.length()) +
          NAMESPACE_USAGE_SUFFIX;
      LOG.info("Mapped deprecated endpoint {} to {}", fullEndpoint, mapped);
      fullEndpoint = mapped;
    }
    if (fullEndpoint.endsWith(TASKS_STATUS_SUFFIX) ||
        fullEndpoint.endsWith(TASKS_SUFFIX)) {
      String mapped;
      if (fullEndpoint.endsWith(TASKS_STATUS_SUFFIX)) {
        mapped = fullEndpoint.substring(
            0, fullEndpoint.length() - TASKS_STATUS_SUFFIX.length()) +
            TASK_STATUS_SUFFIX;
      } else {
        mapped = fullEndpoint.substring(
            0, fullEndpoint.length() - TASKS_SUFFIX.length()) +
            TASK_STATUS_SUFFIX;
      }
      LOG.info("Mapped deprecated endpoint {} to {}", fullEndpoint, mapped);
      fullEndpoint = mapped;
    }
    return fullEndpoint;
  }

  private String replacePathParameters(String endpoint,
                                       Map<String, String> parameters) {
    String resolved = endpoint;
    for (Map.Entry<String, String> entry : parameters.entrySet()) {
      String placeholder = "{" + entry.getKey() + "}";
      if (resolved.contains(placeholder)) {
        resolved = resolved.replace(placeholder, entry.getValue());
      }
    }
    return resolved;
  }

  private String buildUrl(String endpoint, Map<String, String> parameters) {
    StringBuilder urlBuilder = new StringBuilder(reconBaseUrl + endpoint);
    boolean firstParam = !endpoint.contains("?");
    for (Map.Entry<String, String> entry : parameters.entrySet()) {
      if (!endpoint.contains("{" + entry.getKey() + "}")) {
        urlBuilder.append(firstParam ? "?" : "&");
        String value = entry.getValue() == null ? "" : entry.getValue();
        urlBuilder.append(entry.getKey()).append("=")
            .append(URLEncoder.encode(value, StandardCharsets.UTF_8));
        firstParam = false;
      }
    }
    return urlBuilder.toString();
  }

  private HttpRequest buildRequest(String url, String method) {
    HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(30))
        .header("Accept", "application/json")
        .header("Content-Type", "application/json");

    if ("GET".equalsIgnoreCase(method)) {
      requestBuilder.GET();
    } else if ("POST".equalsIgnoreCase(method)) {
      requestBuilder.POST(HttpRequest.BodyPublishers.noBody());
    } else {
      throw new IllegalArgumentException("Unsupported HTTP method: " + method);
    }
    return requestBuilder.build();
  }

  private void ensureSuccess(HttpResponse<String> response) throws IOException {
    if (response.statusCode() != 200) {
      String errorMsg = String.format(
          "API request failed with status %d: %s",
          response.statusCode(), response.body());
      LOG.error(errorMsg);
      throw new IOException(errorMsg);
    }
  }

  private JsonNode parseJsonSafely(String body) throws IOException {
    if (body == null || body.trim().isEmpty()) {
      return MAPPER.createObjectNode();
    }
    return MAPPER.readTree(body);
  }

  private int estimateRecordCount(JsonNode response) {
    if (response == null) {
      return 0;
    }
    if (response.isArray()) {
      return response.size();
    }
    JsonNode keys = response.get("keys");
    if (keys != null && keys.isArray()) {
      return keys.size();
    }
    JsonNode data = response.get("data");
    if (data != null && data.isArray()) {
      return data.size();
    }
    return 0;
  }

  private int parsePositiveInt(String value, int defaultValue) {
    if (value == null || value.trim().isEmpty()) {
      return defaultValue;
    }
    try {
      int parsed = Integer.parseInt(value.trim());
      return parsed > 0 ? parsed : defaultValue;
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private String extractStringField(JsonNode node, String field) {
    if (node == null || field == null || field.isEmpty()) {
      return null;
    }
    JsonNode fieldNode = node.get(field);
    if (fieldNode == null || fieldNode.isNull()) {
      return null;
    }
    return fieldNode.asText("");
  }

  private Map<String, Object> createLimitsMap(int maxRecords, int maxPages,
                                              int pageSize) {
    Map<String, Object> limits = new HashMap<>();
    limits.put("maxRecordsPerAnswer", maxRecords);
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

    public ToolExecutionOutcome(Object responseBody, int recordsProcessed,
                                int pagesFetched, boolean truncated,
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
