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
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for the Chatbot Agent.
 *
 * <p>Contains pure functions for string manipulation, JSON parsing, security validation,
 * and I/O operations used by {@link ChatbotAgent} and {@link ToolExecutor}.</p>
 */
public final class ChatbotUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ChatbotUtils.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String API_V1_ROOT = "/api/v1";

  private ChatbotUtils() {
    // Prevent instantiation
  }

  // =========================================================================
  // Path & Security Utilities
  // =========================================================================

  public static String normalizeEndpoint(String endpoint) {
    if (StringUtils.isBlank(endpoint)) {
      return "";
    }
    String fullEndpoint = endpoint;
    if (!fullEndpoint.startsWith("/api/v1/")) {
      fullEndpoint = "/api/v1" + (endpoint.startsWith("/") ? endpoint : "/" + endpoint);
    }
    return fullEndpoint;
  }

  /**
   * Resolves {@code .} and {@code ..} in the path and ensures it stays under {@link #API_V1_ROOT}.
   * Returns an empty string when the path is invalid, contains a scheme ({@code ://}),
   * or escapes the Recon API root after normalization.
   */
  public static String canonicalizeEndpointPath(String endpointPath) {
    if (StringUtils.isBlank(endpointPath)) {
      return "";
    }
    if (endpointPath.indexOf("://") >= 0) {
      return "";
    }
    String pathOnly = endpointPath;
    int queryIdx = pathOnly.indexOf('?');
    if (queryIdx >= 0) {
      pathOnly = pathOnly.substring(0, queryIdx);
    }
    try {
      URI uri = new URI(null, null, pathOnly, null, null);
      String normalized = uri.normalize().getPath();
      if (normalized == null || normalized.isEmpty()) {
        return "";
      }
      if (!normalized.equals(API_V1_ROOT) && !normalized.startsWith(API_V1_ROOT + "/")) {
        return "";
      }
      return normalized;
    } catch (URISyntaxException e) {
      return "";
    }
  }

  /**
   * True when {@code path} is exactly {@code prefix} or a sub-path ({@code prefix + "/..."}).
   */
  public static boolean matchesAllowedPrefix(String path, String prefix) {
    return path.equals(prefix) || path.startsWith(prefix + "/");
  }

  /**
   * {@code listKeys} requires {@code startPrefix} scoped to at least volume/bucket level.
   */
  public static boolean isBucketScopedListKeysPrefix(String startPrefix) {
    if (startPrefix == null) {
      return false;
    }
    String trimmed = startPrefix.trim();
    if (trimmed.isEmpty() || "/".equals(trimmed)) {
      return false;
    }
    if (!trimmed.startsWith("/") || trimmed.contains("..")) {
      return false;
    }
    int segments = 0;
    for (String part : trimmed.split("/")) {
      if (!part.isEmpty()) {
        segments++;
      }
    }
    return segments >= 2;
  }

  // =========================================================================
  // JSON & Text Utilities
  // =========================================================================

  /**
   * <p>LLMs sometimes wrap their JSON response in prose text (e.g. "Here is the result: {...}")
   * despite being instructed to return JSON only. A simple greedy regex like {@code \{.*\}}
   * fails for nested objects because it can match from the first {@code {} to the last {@code }}
   * in the entire string, returning multiple concatenated objects or truncating nested ones.
   *
   * <p>This method uses brace-counting with string-awareness to reliably extract the first
   * outermost JSON object regardless of surrounding text, nesting depth, or number of
   * objects in the response:
   *
   * @param text the raw LLM response string, which may contain prose before/after JSON
   * @return the first complete JSON object string, or {@code null} if none is found
   */
  public static String extractFirstJsonObject(String text) {
    if (text == null) {
      return null;
    }
    int depth = 0;
    int start = -1;
    boolean inString = false;
    boolean escape = false;
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (escape) {
        escape = false;
        continue;
      }
      if (c == '\\' && inString) {
        escape = true;
        continue;
      }
      if (c == '"') {
        inString = !inString;
        continue;
      }
      if (inString) {
        continue;
      }
      if (c == '{') {
        if (depth == 0) {
          start = i;
        }
        depth++;
      } else if (c == '}') {
        depth--;
        if (depth == 0 && start != -1) {
          return text.substring(start, i + 1);
        }
      }
    }
    return null;
  }

  public static int parsePositiveInt(String value, int defaultValue) {
    if (StringUtils.isBlank(value)) {
      return defaultValue;
    }
    try {
      int parsed = Integer.parseInt(value.trim());
      return parsed > 0 ? parsed : defaultValue;
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  public static String extractStringField(JsonNode node, String field) {
    if (node == null || field == null || field.isEmpty()) {
      return null;
    }
    JsonNode fieldNode = node.get(field);
    if (fieldNode == null || fieldNode.isNull()) {
      return null;
    }
    return fieldNode.asText("");
  }

  public static int estimateRecordCount(JsonNode response) {
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

  public static JsonNode parseJsonSafely(String body) throws IOException {
    if (StringUtils.isBlank(body)) {
      return MAPPER.createObjectNode();
    }
    return MAPPER.readTree(body);
  }

  // =========================================================================
  // I/O & Resource Loading Utilities
  // =========================================================================

  public static String loadResourceFromClasspath(String resourcePath) {
    try (InputStream is = ChatbotUtils.class.getClassLoader().getResourceAsStream(resourcePath)) {
      if (is == null) {
        return "";
      }
      ByteArrayOutputStream result = new ByteArrayOutputStream();
      byte[] buffer = new byte[8192];
      int length;
      while ((length = is.read(buffer)) != -1) {
        result.write(buffer, 0, length);
      }
      return result.toString(StandardCharsets.UTF_8.name());
    } catch (IOException e) {
      LOG.error("Failed to load resource: {}", resourcePath, e);
      return "";
    }
  }

  public static String readInputStream(HttpURLConnection conn) throws IOException {
    StringBuilder sb = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"))) {
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
    }
    return sb.toString();
  }

  public static String readErrorStream(HttpURLConnection conn) {
    try {
      if (conn.getErrorStream() != null) {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream(), "UTF-8"))) {
          String line;
          while ((line = br.readLine()) != null) {
            sb.append(line);
          }
        }
        return sb.toString();
      }
    } catch (IOException e) {
      LOG.debug("Failed to read error stream", e);
    }
    return "";
  }
}
