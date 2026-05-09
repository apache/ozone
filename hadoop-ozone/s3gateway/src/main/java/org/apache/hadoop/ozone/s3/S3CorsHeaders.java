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

package org.apache.hadoop.ozone.s3;

import static org.apache.hadoop.ozone.s3.util.S3Consts.ACCESS_CONTROL_ALLOW_HEADERS;
import static org.apache.hadoop.ozone.s3.util.S3Consts.ACCESS_CONTROL_ALLOW_METHODS;
import static org.apache.hadoop.ozone.s3.util.S3Consts.ACCESS_CONTROL_ALLOW_ORIGIN;
import static org.apache.hadoop.ozone.s3.util.S3Consts.ACCESS_CONTROL_EXPOSE_HEADERS;
import static org.apache.hadoop.ozone.s3.util.S3Consts.ACCESS_CONTROL_MAX_AGE;

import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.helpers.CorsConfiguration;
import org.apache.hadoop.ozone.om.helpers.CorsRule;

/**
 * Evaluates S3 CORS rules and writes CORS response headers.
 */
public final class S3CorsHeaders {

  private S3CorsHeaders() {
  }

  public static Optional<CorsRule> findMatchingRule(
      CorsConfiguration configuration, String origin, String method,
      String requestedHeaders) {
    if (configuration == null || StringUtils.isBlank(origin)
        || StringUtils.isBlank(method)) {
      return Optional.empty();
    }
    return configuration.getRules().stream()
        .filter(rule -> matchesAny(origin, rule.getAllowedOrigins(), true))
        .filter(rule -> rule.getAllowedMethods().stream()
            .anyMatch(allowed -> allowed.equalsIgnoreCase(method)))
        .filter(rule -> requestedHeadersMatch(requestedHeaders, rule))
        .findFirst();
  }

  public static void applyHeaders(MultivaluedMap<String, Object> headers,
      CorsRule rule, String origin, String requestedHeaders,
      boolean preflight) {
    headers.putSingle(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    if (preflight) {
      headers.putSingle(ACCESS_CONTROL_ALLOW_METHODS,
          String.join(", ", rule.getAllowedMethods()));
      String allowedHeaders = allowedHeadersForResponse(requestedHeaders, rule);
      if (!allowedHeaders.isEmpty()) {
        headers.putSingle(ACCESS_CONTROL_ALLOW_HEADERS, allowedHeaders);
      }
      if (rule.getMaxAgeSeconds() != null) {
        headers.putSingle(ACCESS_CONTROL_MAX_AGE,
            String.valueOf(rule.getMaxAgeSeconds()));
      }
    }
    if (!rule.getExposeHeaders().isEmpty()) {
      headers.putSingle(ACCESS_CONTROL_EXPOSE_HEADERS,
          String.join(", ", rule.getExposeHeaders()));
    }
  }

  public static Response.ResponseBuilder applyHeaders(
      Response.ResponseBuilder builder, CorsRule rule, String origin,
      String requestedHeaders, boolean preflight) {
    builder.header(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    if (preflight) {
      builder.header(ACCESS_CONTROL_ALLOW_METHODS,
          String.join(", ", rule.getAllowedMethods()));
      String allowedHeaders = allowedHeadersForResponse(requestedHeaders, rule);
      if (!allowedHeaders.isEmpty()) {
        builder.header(ACCESS_CONTROL_ALLOW_HEADERS, allowedHeaders);
      }
      if (rule.getMaxAgeSeconds() != null) {
        builder.header(ACCESS_CONTROL_MAX_AGE,
            String.valueOf(rule.getMaxAgeSeconds()));
      }
    }
    if (!rule.getExposeHeaders().isEmpty()) {
      builder.header(ACCESS_CONTROL_EXPOSE_HEADERS,
          String.join(", ", rule.getExposeHeaders()));
    }
    return builder;
  }

  private static boolean requestedHeadersMatch(
      String requestedHeaders, CorsRule rule) {
    if (StringUtils.isBlank(requestedHeaders)) {
      return true;
    }
    return Arrays.stream(requestedHeaders.split(","))
        .map(String::trim)
        .filter(StringUtils::isNotEmpty)
        .allMatch(header -> matchesAny(header, rule.getAllowedHeaders(), false));
  }

  private static String allowedHeadersForResponse(
      String requestedHeaders, CorsRule rule) {
    if (StringUtils.isBlank(requestedHeaders)) {
      return "";
    }
    return Arrays.stream(requestedHeaders.split(","))
        .map(String::trim)
        .filter(StringUtils::isNotEmpty)
        .filter(header -> matchesAny(header, rule.getAllowedHeaders(), false))
        .collect(Collectors.joining(", "));
  }

  private static boolean matchesAny(String value, Iterable<String> patterns,
      boolean caseSensitive) {
    for (String pattern : patterns) {
      if (matches(value, pattern, caseSensitive)) {
        return true;
      }
    }
    return false;
  }

  private static boolean matches(String value, String pattern,
      boolean caseSensitive) {
    if (pattern == null) {
      return false;
    }
    String candidate = caseSensitive ? value : value.toLowerCase(Locale.ROOT);
    String normalizedPattern = caseSensitive ? pattern
        : pattern.toLowerCase(Locale.ROOT);
    if ("*".equals(normalizedPattern)) {
      return true;
    }
    int wildcard = normalizedPattern.indexOf('*');
    if (wildcard < 0) {
      return candidate.equals(normalizedPattern);
    }
    String prefix = normalizedPattern.substring(0, wildcard);
    String suffix = normalizedPattern.substring(wildcard + 1);
    return candidate.startsWith(prefix) && candidate.endsWith(suffix);
  }
}
