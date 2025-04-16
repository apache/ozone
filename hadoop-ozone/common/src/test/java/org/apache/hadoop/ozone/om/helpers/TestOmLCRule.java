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

package org.apache.hadoop.ozone.om.helpers;


import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.assertOMException;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getFutureDateString;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getOmLCAndOperator;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getOmLCFilter;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

/**
 * Test OmLCRule.
 */
class TestOmLCRule {

  @Test
  public void testCreateValidOmLCRule() {
    OmLCExpiration exp = new OmLCExpiration.Builder()
        .setDays(30)
        .build();

    OmLCRule r1 = new OmLCRule.Builder()
        .setId("remove Spark logs after 30 days")
        .setEnabled(true)
        .setPrefix("/spark/logs")
        .setAction(exp)
        .build();
    assertDoesNotThrow(r1::valid);

    OmLCRule r2 = new OmLCRule.Builder()
        .setEnabled(true)
        .setPrefix("")
        .setAction(exp)
        .build();
    assertDoesNotThrow(r2::valid);

    // Empty id should generate a 48 (default) bit one.
    OmLCRule r3 = new OmLCRule.Builder()
        .setEnabled(true)
        .setAction(exp)
        .build();

    assertDoesNotThrow(r3::valid);
    assertEquals(OmLCRule.LC_ID_LENGTH, r3.getId().length(),
        "Expected a " + OmLCRule.LC_ID_LENGTH + " length generated ID");
  }

  @Test
  public void testCreateInValidOmLCRule() {
    OmLCExpiration exp = new OmLCExpiration.Builder()
        .setDays(30)
        .build();

    char[] id = new char[OmLCRule.LC_ID_MAX_LENGTH + 1];
    Arrays.fill(id, 'a');

    OmLCRule r1 = new OmLCRule.Builder()
        .setId(new String(id))
        .setAction(exp)
        .build();
    assertOMException(r1::valid, INVALID_REQUEST, "ID length should not exceed allowed limit of 255");

    OmLCRule r2 = new OmLCRule.Builder()
        .setId("remove Spark logs after 30 days")
        .setEnabled(true)
        .setPrefix("/spark/logs")
        .setAction(null)
        .build();
    assertOMException(r2::valid, INVALID_REQUEST,
        "At least one action needs to be specified in a rule");

    OmLCRule r3 = new OmLCRule.Builder()
        .setId("remove Spark logs after 30 days")
        .setEnabled(true)
        .setPrefix("/spark/logs")
        .setAction(new OmLCExpiration.Builder()
            .setDays(30)
            .setDate(getFutureDateString(100))
            .build())
        .build();
    assertOMException(r3::valid, INVALID_REQUEST,
        "Either 'days' or 'date' should be specified, but not both or neither.");

    OmLCFilter invalidFilter = getOmLCFilter("prefix", Pair.of("key", "value"), null);
    OmLCRule r5 = new OmLCRule.Builder()
        .setId("invalid-filter-rule")
        .setEnabled(true)
        .setFilter(invalidFilter)
        .setAction(new OmLCExpiration.Builder().setDays(30).build())
        .build();
    assertOMException(r5::valid, INVALID_REQUEST,
        "Only one of 'Prefix', 'Tag', or 'AndOperator' should be specified");
  }

  @Test
  public void testMultipleActionsInRule() {
    OmLCExpiration expiration1 = new OmLCExpiration.Builder()
        .setDays(30)
        .build();

    OmLCExpiration expiration2 = new OmLCExpiration.Builder()
        .setDays(60)
        .build();

    List<OmLCAction> actions = new ArrayList<>();
    actions.add(expiration1);
    actions.add(expiration2);

    OmLCRule.Builder builder = new OmLCRule.Builder();
    builder.setId("test-rule");

    // Using reflection to bypass the builder's single-action limitation
    OmLCRule rule = builder.build();
    rule.setActions(actions);

    assertOMException(rule::valid, INVALID_REQUEST, "A rule can have at most one Expiration action");
  }

  @Test
  public void testRuleWithAndOperatorFilter() {
    Map<String, String> tags = ImmutableMap.of("app", "hadoop", "env", "test");
    OmLifecycleRuleAndOperator andOperator = getOmLCAndOperator("/logs/", tags);
    OmLCFilter filter = getOmLCFilter(null, null, andOperator);

    OmLCRule rule = new OmLCRule.Builder()
        .setId("and-operator-rule")
        .setEnabled(true)
        .setFilter(filter)
        .setAction(new OmLCExpiration.Builder().setDays(30).build())
        .build();

    assertDoesNotThrow(rule::valid);
    assertTrue(rule.isPrefixEnable());
    assertTrue(rule.isTagEnable());
  }

  @Test
  public void testRuleWithTagFilter() {
    OmLCFilter filter = getOmLCFilter(null, Pair.of("app", "hadoop"), null);

    OmLCRule rule = new OmLCRule.Builder()
        .setId("tag-filter-rule")
        .setEnabled(true)
        .setFilter(filter)
        .setAction(new OmLCExpiration.Builder().setDays(30).build())
        .build();

    assertDoesNotThrow(rule::valid);
    assertFalse(rule.isPrefixEnable());
    assertTrue(rule.isTagEnable());
  }

  @Test
  public void testDuplicateRuleIDs() {
    List<OmLCRule> rules = new ArrayList<>();

    rules.add(new OmLCRule.Builder()
        .setId("duplicate-id")
        .setAction(new OmLCExpiration.Builder().setDays(30).build())
        .build());

    rules.add(new OmLCRule.Builder()
        .setId("duplicate-id") // Same ID
        .setAction(new OmLCExpiration.Builder().setDays(60).build())
        .build());

    OmLifecycleConfiguration config = new OmLifecycleConfiguration.Builder()
        .setVolume("volume")
        .setBucket("bucket")
        .setOwner("owner")
        .setRules(rules)
        .build();

    assertOMException(config::valid, INVALID_REQUEST, "Duplicate rule IDs found");
  }
}
