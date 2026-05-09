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
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.hadoop.ozone.om.helpers.CorsConfiguration;
import org.apache.hadoop.ozone.om.helpers.CorsRule;
import org.junit.jupiter.api.Test;

/**
 * Tests S3 CORS rule matching and response header generation.
 */
public class TestCorsHeaders {

  @Test
  public void firstMatchingRuleMustMatchOriginMethodAndHeaders() {
    CorsConfiguration configuration = CorsConfiguration.newBuilder()
        .addRule(rule("post-rule", "https://example.com",
            Collections.singletonList("POST"),
            Collections.singletonList("*")))
        .addRule(rule("read-rule", "https://example.com",
            Arrays.asList("GET", "HEAD"),
            Arrays.asList("Authorization", "x-amz-*")))
        .build();

    Optional<CorsRule> rule = S3CorsHeaders.findMatchingRule(
        configuration, "https://example.com", "GET",
        "Authorization, X-Amz-Date");

    assertThat(rule).isPresent();
    assertThat(rule.get().getId()).isEqualTo("read-rule");
    assertThat(S3CorsHeaders.findMatchingRule(configuration,
        "https://example.com", "GET", "Content-Type")).isEmpty();
  }

  @Test
  public void applyPreflightHeaders() {
    CorsRule rule = rule("read-rule", "https://example.com",
        Arrays.asList("GET", "HEAD"), Collections.singletonList("*"))
        .toBuilder()
        .setExposeHeaders(Collections.singletonList("ETag"))
        .setMaxAgeSeconds(3000)
        .build();
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();

    S3CorsHeaders.applyHeaders(headers, rule, "https://example.com",
        "Authorization", true);

    assertThat(headers.getFirst(ACCESS_CONTROL_ALLOW_ORIGIN))
        .isEqualTo("https://example.com");
    assertThat(headers.getFirst(ACCESS_CONTROL_ALLOW_METHODS))
        .isEqualTo("GET, HEAD");
    assertThat(headers.getFirst(ACCESS_CONTROL_ALLOW_HEADERS))
        .isEqualTo("Authorization");
    assertThat(headers.getFirst(ACCESS_CONTROL_MAX_AGE)).isEqualTo("3000");
    assertThat(headers.getFirst(ACCESS_CONTROL_EXPOSE_HEADERS))
        .isEqualTo("ETag");
  }

  private static CorsRule rule(String id, String origin,
      java.util.List<String> methods, java.util.List<String> headers) {
    return CorsRule.newBuilder()
        .setId(id)
        .setAllowedOrigins(Collections.singletonList(origin))
        .setAllowedMethods(methods)
        .setAllowedHeaders(headers)
        .build();
  }
}
