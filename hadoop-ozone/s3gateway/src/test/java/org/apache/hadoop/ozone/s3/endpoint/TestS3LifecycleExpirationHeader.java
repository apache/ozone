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

package org.apache.hadoop.ozone.s3.endpoint;

import static org.apache.hadoop.ozone.s3.endpoint.S3LifecycleConfiguration.getExpirationHeader;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.ozone.client.OzoneLifecycleConfiguration;
import org.apache.hadoop.ozone.client.OzoneLifecycleConfiguration.LifecycleAndOperator;
import org.apache.hadoop.ozone.client.OzoneLifecycleConfiguration.OzoneLCExpiration;
import org.apache.hadoop.ozone.client.OzoneLifecycleConfiguration.OzoneLCFilter;
import org.apache.hadoop.ozone.client.OzoneLifecycleConfiguration.OzoneLCRule;
import org.apache.hadoop.ozone.om.helpers.OmLCFilter.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for building the x-amz-expiration header from a bucket lifecycle configuration.
 */
class TestS3LifecycleExpirationHeader {
  private static final long CREATION_TIME = Instant.parse("2026-11-25T10:30:00Z").toEpochMilli();
  private static final Map<String, String> NO_TAGS = Collections.emptyMap();

  @Test
  void daysRuleRoundsUpToNextMidnightUtc() {
    OzoneLifecycleConfiguration lcc = config(rule("r1", "logs/", "Enabled", days(1), null));
    assertEquals("expiry-date=\"Fri, 27 Nov 2026 00:00:00 GMT\", rule-id=\"r1\"",
        getExpirationHeader(lcc, "logs/a", NO_TAGS, CREATION_TIME));
  }

  @Test
  void daysRuleAtMidnightIsNotRounded() {
    OzoneLifecycleConfiguration lcc = config(rule("r1", "", "Enabled", days(3), null));
    long midnight = Instant.parse("2026-11-25T00:00:00Z").toEpochMilli();
    assertEquals("expiry-date=\"Sat, 28 Nov 2026 00:00:00 GMT\", rule-id=\"r1\"",
        getExpirationHeader(lcc, "a", NO_TAGS, midnight));
  }

  @Test
  void dateRule() {
    OzoneLifecycleConfiguration lcc = config(rule("r1", "", "Enabled",
        new OzoneLCExpiration(null, "2027-01-05T00:00:00Z"), null));
    assertEquals("expiry-date=\"Tue, 05 Jan 2027 00:00:00 GMT\", rule-id=\"r1\"",
        getExpirationHeader(lcc, "a", NO_TAGS, CREATION_TIME));
  }

  @Test
  void noMatchingRule() {
    OzoneLifecycleConfiguration lcc = config(
        rule("prefix", "logs/", "Enabled", days(1), null),
        rule("disabled", "", "Disabled", days(1), null),
        rule("no-expiration", "", "Enabled", null, null));
    assertNull(getExpirationHeader(lcc, "data/a", NO_TAGS, CREATION_TIME));
  }

  @Test
  void filterTagAndAndOperator() {
    OzoneLifecycleConfiguration tagRule = config(rule("tag", null, "Enabled", days(1),
        new OzoneLCFilter(null, new Tag("k", "v"), null)));
    assertNull(getExpirationHeader(tagRule, "a", NO_TAGS, CREATION_TIME));
    assertEquals("expiry-date=\"Fri, 27 Nov 2026 00:00:00 GMT\", rule-id=\"tag\"",
        getExpirationHeader(tagRule, "a", ImmutableMap.of("k", "v"), CREATION_TIME));

    OzoneLifecycleConfiguration andRule = config(rule("and", null, "Enabled", days(1),
        new OzoneLCFilter(null, null, new LifecycleAndOperator(ImmutableMap.of("k", "v"), "logs/"))));
    assertNull(getExpirationHeader(andRule, "data/a", ImmutableMap.of("k", "v"), CREATION_TIME));
    assertNull(getExpirationHeader(andRule, "logs/a", ImmutableMap.of("k", "x"), CREATION_TIME));
    assertEquals("expiry-date=\"Fri, 27 Nov 2026 00:00:00 GMT\", rule-id=\"and\"",
        getExpirationHeader(andRule, "logs/a", ImmutableMap.of("k", "v"), CREATION_TIME));

    OzoneLifecycleConfiguration emptyFilter = config(rule("all", null, "Enabled", days(1),
        new OzoneLCFilter(null, null, null)));
    assertEquals("expiry-date=\"Fri, 27 Nov 2026 00:00:00 GMT\", rule-id=\"all\"",
        getExpirationHeader(emptyFilter, "a", NO_TAGS, CREATION_TIME));
  }

  @Test
  void earliestMatchingRuleWins() {
    OzoneLifecycleConfiguration lcc = config(
        rule("late", "", "Enabled", days(10), null),
        rule("early", "logs/", "Enabled", days(2), null));
    assertEquals("expiry-date=\"Sat, 28 Nov 2026 00:00:00 GMT\", rule-id=\"early\"",
        getExpirationHeader(lcc, "logs/a", NO_TAGS, CREATION_TIME));
  }

  private static OzoneLCExpiration days(int days) {
    return new OzoneLCExpiration(days, null);
  }

  private static OzoneLCRule rule(String id, String prefix, String status, OzoneLCExpiration expiration,
      OzoneLCFilter filter) {
    return new OzoneLCRule(id, prefix, status, expiration, null, filter);
  }

  private static OzoneLifecycleConfiguration config(OzoneLCRule... rules) {
    return new OzoneLifecycleConfiguration("s3v", "bucket", 0, Arrays.asList(rules));
  }
}
