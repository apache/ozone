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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for the Chatbot Agent.
 *
 * <p>Contains pure functions for string manipulation, JSON parsing, security validation,
 * and I/O operations used by {@link ChatbotAgent} and
 * {@link org.apache.hadoop.ozone.recon.chatbot.recon.ReconQueryExecutor}.</p>
 */
public final class ChatbotUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ChatbotUtils.class);

  private ChatbotUtils() {
    // Prevent instantiation
  }

  // =========================================================================
  // Path & Security Utilities
  // =========================================================================

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

  public static int parsePositiveInt(String value, int defaultValue) {
    if (StringUtils.isBlank(value)) {
      return defaultValue;
    }
    try {
      int parsed = Integer.parseInt(value.trim());
      if (parsed <= 0) {
        throw new IllegalArgumentException("limit must be a positive integer");
      }
      return parsed;
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  public static int estimateRecordCount(JsonNode response) {
    if (response == null) {
      return 0;
    }
    return countRecordArrays(response);
  }

  /**
   * Counts how many list-style records appear in a Recon JSON response.
   *
   * <p>Walks the whole tree and adds up every array whose items are objects
   * (e.g. containers, keys, datanodes). Field names do not matter — only
   * the shape "array of objects". Plain number/string arrays (like size bins)
   * are skipped.
   *
   * <p>Used only to guess whether a response hit the 1000-record cap. The count
   * does not need to be exact; slightly high is fine and only makes us warn
   * about a partial sample sooner.
   */
  private static int countRecordArrays(JsonNode node) {
    int count = 0;
    if (node.isArray()) {
      boolean holdsObjects = false;
      for (JsonNode element : node) {
        holdsObjects |= element.isObject();
        count += countRecordArrays(element);
      }
      if (holdsObjects) {
        count += node.size();
      }
    } else if (node.isObject()) {
      for (JsonNode child : node) {
        count += countRecordArrays(child);
      }
    }
    return count;
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
}
