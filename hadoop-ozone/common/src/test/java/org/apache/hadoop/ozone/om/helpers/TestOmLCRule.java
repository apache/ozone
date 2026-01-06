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
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getOmLCAndOperatorBuilder;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getOmLCFilterBuilder;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getOmLCRuleBuilder;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleRule;
import org.junit.jupiter.api.Test;

/**
 * Test OmLCRule.
 */
class TestOmLCRule {

  @Test
  public void testCreateValidOmLCRule() throws OMException {
    long currentTime = System.currentTimeMillis();
    OmLCExpiration exp = new OmLCExpiration.Builder()
        .setDays(30)
        .build();

    OmLCRule.Builder r1 = new OmLCRule.Builder()
        .setId("remove Spark logs after 30 days")
        .setEnabled(true)
        .setPrefix("/spark/logs")
        .setAction(exp);
    assertDoesNotThrow(() -> r1.build().valid(BucketLayout.DEFAULT, currentTime));

    OmLCRule.Builder r2 = new OmLCRule.Builder()
        .setEnabled(true)
        .setPrefix("")
        .setAction(exp);
    OmLCRule omLCRule = assertDoesNotThrow(r2::build);
    assertDoesNotThrow(() -> omLCRule.valid(BucketLayout.DEFAULT, currentTime));

    // Empty id should generate a 48 (default) bit one.
    assertEquals(OmLCRule.LC_ID_LENGTH, omLCRule.getId().length(),
        "Expected a " + OmLCRule.LC_ID_LENGTH + " length generated ID");
  }

  @Test
  public void testCreateInValidOmLCRule() throws OMException {
    long currentTime = System.currentTimeMillis();
    OmLCExpiration exp = new OmLCExpiration.Builder()
        .setDays(30)
        .build();

    char[] id = new char[OmLCRule.LC_ID_MAX_LENGTH + 1];
    Arrays.fill(id, 'a');

    OmLCRule.Builder r1 = new OmLCRule.Builder()
        .setId(new String(id))
        .setAction(exp);
    assertOMException(() -> r1.build().valid(BucketLayout.DEFAULT, currentTime), INVALID_REQUEST,
        "ID length should not exceed allowed limit of 255");

    OmLCRule.Builder r2 = new OmLCRule.Builder()
        .setId("remove Spark logs after 30 days")
        .setEnabled(true)
        .setPrefix("/spark/logs")
        .setAction(null);
    assertOMException(() -> r2.build().valid(BucketLayout.DEFAULT, currentTime), INVALID_REQUEST,
        "At least one action needs to be specified in a rule");

    OmLCRule.Builder r3 = new OmLCRule.Builder()
        .setEnabled(true)
        .setAction(exp);

    assertOMException(() -> r3.build().valid(BucketLayout.DEFAULT, currentTime), INVALID_REQUEST,
        "Filter and Prefix cannot both be null.");
  }

  @Test
  public void testMultipleActionsInRule() throws OMException {
    long currentTime = System.currentTimeMillis();
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

    OmLCRule.Builder rule = builder.setActions(actions);

    assertOMException(() -> rule.build().valid(BucketLayout.DEFAULT, currentTime), INVALID_REQUEST,
        "A rule can have at most one Expiration action");
  }

  @Test
  public void testRuleWithAndOperatorFilter() throws OMException {
    long currentTime = System.currentTimeMillis();
    Map<String, String> tags = ImmutableMap.of("app", "hadoop", "env", "test");
    OmLifecycleRuleAndOperator andOperator = getOmLCAndOperatorBuilder("/logs/", tags).build();
    OmLCFilter filter = getOmLCFilterBuilder(null, null, andOperator).build();

    OmLCRule.Builder builder = new OmLCRule.Builder()
        .setId("and-operator-rule")
        .setEnabled(true)
        .setFilter(filter)
        .setAction(new OmLCExpiration.Builder().setDays(30).build());

    OmLCRule rule = assertDoesNotThrow(builder::build);
    assertDoesNotThrow(() -> rule.valid(BucketLayout.DEFAULT, currentTime));
    assertTrue(rule.isPrefixEnable());
    assertTrue(rule.isTagEnable());
  }

  @Test
  public void testRuleWithTagFilter() throws OMException {
    long currentTime = System.currentTimeMillis();
    OmLCFilter filter = getOmLCFilterBuilder(null, Pair.of("app", "hadoop"), null).build();

    OmLCRule.Builder builder = new OmLCRule.Builder()
        .setId("tag-filter-rule")
        .setEnabled(true)
        .setFilter(filter)
        .setAction(new OmLCExpiration.Builder().setDays(30).build());

    OmLCRule rule = assertDoesNotThrow(builder::build);
    assertDoesNotThrow(() -> rule.valid(BucketLayout.DEFAULT, currentTime));
    assertFalse(rule.isPrefixEnable());
    assertTrue(rule.isTagEnable());
  }

  @Test
  public void testDuplicateRuleIDs() throws OMException {
    List<OmLCRule> rules = new ArrayList<>();

    rules.add(new OmLCRule.Builder()
        .setId("duplicate-id")
            .setPrefix("")
        .setAction(new OmLCExpiration.Builder().setDays(30).build())
        .build());

    rules.add(new OmLCRule.Builder()
        .setId("duplicate-id") // Same ID
        .setPrefix("")
        .setAction(new OmLCExpiration.Builder().setDays(60).build())
        .build());

    OmLifecycleConfiguration.Builder config = new OmLifecycleConfiguration.Builder()
        .setVolume("volume")
        .setBucket("bucket")
        .setRules(rules);

    assertOMException(() -> config.buildAndValid(), INVALID_REQUEST, "Duplicate rule IDs found");
  }

  @Test
  public void testTrashPrefixValidation() throws OMException {
    long currentTime = System.currentTimeMillis();
    OmLCExpiration exp = new OmLCExpiration.Builder()
        .setDays(30)
        .build();

    // Case 1: Prefix is .Trash
    OmLCRule.Builder r1 = new OmLCRule.Builder()
        .setId("trash-rule-1")
        .setEnabled(true)
        .setPrefix(FileSystem.TRASH_PREFIX)
        .setAction(exp);
    assertOMException(() -> r1.build().valid(BucketLayout.DEFAULT, currentTime), INVALID_REQUEST,
        "Lifecycle rule prefix cannot be trash root");

    // Case 4: Prefix is .Trash/subdir
    OmLCRule.Builder r2 = new OmLCRule.Builder()
        .setId("trash-rule-2")
        .setEnabled(true)
        .setPrefix(FileSystem.TRASH_PREFIX + "/user")
        .setAction(exp);
    assertOMException(() -> r2.build().valid(BucketLayout.DEFAULT, currentTime), INVALID_REQUEST,
        "Lifecycle rule prefix cannot be trash root");
  }

  @Test
  public void testTagValidation() throws OMException {
    long currentTime = System.currentTimeMillis();
    OmLCExpiration exp = new OmLCExpiration.Builder()
        .setDays(30)
        .build();

    // Case 1: Tag key too long
    String longKey = RandomStringUtils.randomAlphanumeric(129);
    OmLCFilter filterKeyTooLong = getOmLCFilterBuilder(null, Pair.of(longKey, "value"), null).build();
    OmLCRule.Builder r1 = new OmLCRule.Builder()
        .setId("long-tag-key")
        .setEnabled(true)
        .setFilter(filterKeyTooLong)
        .setAction(exp);
    assertOMException(() -> r1.build().valid(BucketLayout.DEFAULT, currentTime), INVALID_REQUEST,
        "A Tag's Key must be a length between 1 and 128");

    // Case 2: Tag value too long
    String longValue = RandomStringUtils.randomAlphanumeric(257);
    OmLCFilter filterValueTooLong = getOmLCFilterBuilder(null, Pair.of("key", longValue), null).build();
    OmLCRule.Builder r2 = new OmLCRule.Builder()
        .setId("long-tag-value")
        .setEnabled(true)
        .setFilter(filterValueTooLong)
        .setAction(exp);
    assertOMException(() -> r2.build().valid(BucketLayout.DEFAULT, currentTime), INVALID_REQUEST,
        "A Tag's Value must be a length between 0 and 256");
  }

  @Test
  public void testPrefixLengthValidation() throws OMException {
    long currentTime = System.currentTimeMillis();
    OmLCExpiration exp = new OmLCExpiration.Builder()
        .setDays(30)
        .build();

    // Case 1: Prefix too long
    String longPrefix = RandomStringUtils.randomAlphanumeric(1025);
    OmLCRule.Builder r1 = new OmLCRule.Builder()
        .setId("long-prefix")
        .setEnabled(true)
        .setPrefix(longPrefix)
        .setAction(exp);
    assertOMException(() -> r1.build().valid(BucketLayout.DEFAULT, currentTime), INVALID_REQUEST,
        "The maximum size of a prefix is 1024");

    // Case 2: Filter Prefix too long
    OmLCFilter filterPrefixTooLong = getOmLCFilterBuilder(longPrefix, null, null).build();
    OmLCRule.Builder r2 = new OmLCRule.Builder()
        .setId("filter-long-prefix")
        .setEnabled(true)
        .setFilter(filterPrefixTooLong)
        .setAction(exp);
    assertOMException(() -> r2.build().valid(BucketLayout.DEFAULT, currentTime), INVALID_REQUEST,
        "The maximum size of a prefix is 1024");
  }

  @Test
  public void testProtobufConversion() throws OMException {
    // Only Filter
    // Object to proto
    OmLCFilter filter1 = getOmLCFilterBuilder("prefix", null, null).build();
    OmLCRule rule1 = getOmLCRuleBuilder("test-rule", null, true, 1, filter1).build();
    LifecycleRule proto = rule1.getProtobuf();

    // Proto to Object
    OmLCRule ruleFromProto1 = OmLCRule.getFromProtobuf(proto, BucketLayout.DEFAULT);
    assertEquals("test-rule", ruleFromProto1.getId());
    assertEquals("prefix", ruleFromProto1.getEffectivePrefix());
    assertTrue(ruleFromProto1.isEnabled());
    assertNotNull(ruleFromProto1.getExpiration());
    assertEquals(1, ruleFromProto1.getExpiration().getDays());
    assertNotNull(ruleFromProto1.getFilter());
    assertEquals("prefix", ruleFromProto1.getFilter().getPrefix());

    // Only Prefix
    // Object to proto
    OmLCRule rule2 = getOmLCRuleBuilder("test-rule", "/logs/", false, 30, null).build();
    LifecycleRule proto2 = rule2.getProtobuf();

    // Proto to Object
    OmLCRule ruleFromProto2 = OmLCRule.getFromProtobuf(proto2, BucketLayout.DEFAULT);
    assertEquals("test-rule", ruleFromProto2.getId());
    assertFalse(ruleFromProto2.isEnabled());
    assertEquals("/logs/", ruleFromProto2.getEffectivePrefix());
    assertNotNull(ruleFromProto2.getExpiration());
    assertEquals(30, ruleFromProto2.getExpiration().getDays());
    assertNull(ruleFromProto2.getFilter());

    // Prefix is ""
    // Object to proto
    OmLCRule rule3 = getOmLCRuleBuilder("test-rule", "", true, 30, null).build();
    LifecycleRule proto3 = rule3.getProtobuf();

    // Proto to Object
    OmLCRule ruleFromProto3 = OmLCRule.getFromProtobuf(proto3, BucketLayout.DEFAULT);
    assertEquals("test-rule", ruleFromProto3.getId());
    assertTrue(ruleFromProto3.isEnabled());
    assertEquals("", ruleFromProto3.getEffectivePrefix());
    assertNotNull(ruleFromProto3.getExpiration());
    assertEquals(30, ruleFromProto3.getExpiration().getDays());
    assertNull(ruleFromProto3.getFilter());
  }
}
