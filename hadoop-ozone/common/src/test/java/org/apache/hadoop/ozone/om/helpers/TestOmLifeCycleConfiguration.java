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
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getOmLCAndOperatorBuilder;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getOmLCFilterBuilder;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getOmLCRuleBuilder;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getOmLifecycleConfiguration;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.collect.ImmutableMap;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Test lifecycle configuration related entities.
 */
public class TestOmLifeCycleConfiguration {

  @Test
  public void testCreateValidLCConfiguration() throws OMException {
    OmLifecycleConfiguration lcc = new OmLifecycleConfiguration.Builder()
        .setVolume("s3v")
        .setBucket("spark")
        .setRules(Collections.singletonList(new OmLCRule.Builder()
            .setId("spark logs")
                .setPrefix("")
            .setAction(new OmLCExpiration.Builder()
                .setDays(30)
                .build())
            .build()))
        .build();

    assertDoesNotThrow(lcc::valid);
  }

  @Test
  public void testCreateInValidLCConfiguration() throws OMException {
    OmLCRule rule = new OmLCRule.Builder()
        .setId("spark logs")
        .setPrefix("")
        .setAction(new OmLCExpiration.Builder().setDays(30).build())
        .build();

    List<OmLCRule> rules = Collections.singletonList(rule);

    OmLifecycleConfiguration.Builder lcc0 = getOmLifecycleConfiguration(null, "bucket", rules);
    assertOMException(lcc0::build, INVALID_REQUEST, "Volume cannot be blank");

    OmLifecycleConfiguration.Builder lcc1 = getOmLifecycleConfiguration("volume", null, rules);
    assertOMException(lcc1::build, INVALID_REQUEST, "Bucket cannot be blank");

    OmLifecycleConfiguration.Builder lcc3 = getOmLifecycleConfiguration(
        "volume", "bucket", Collections.emptyList());
    assertOMException(lcc3::build, INVALID_REQUEST,
        "At least one rules needs to be specified in a lifecycle configuration");

    List<OmLCRule> rules4 = new ArrayList<>(
        OmLifecycleConfiguration.LC_MAX_RULES + 1);
    for (int i = 0; i < OmLifecycleConfiguration.LC_MAX_RULES + 1; i++) {
      OmLCRule r = new OmLCRule.Builder()
          .setId(Integer.toString(i))
          .setAction(new OmLCExpiration.Builder().setDays(30).build())
          .setPrefix("")
          .build();
      rules4.add(r);
    }
    OmLifecycleConfiguration.Builder lcc4 = getOmLifecycleConfiguration("volume", "bucket", rules4);
    assertOMException(lcc4::build, INVALID_REQUEST,
        "The number of lifecycle rules must not exceed the allowed limit of");
  }

  @Test
  public void testToBuilder() throws OMException {
    String volume = "test-volume";
    String bucket = "test-bucket";
    long creationTime = System.currentTimeMillis();
    long objectID = 123456L;
    long updateID = 78910L;

    OmLCRule rule1 = new OmLCRule.Builder()
        .setId("test-rule1")
        .setAction(new OmLCExpiration.Builder().setDays(30).build())
        .setPrefix("")
        .build();

    OmLCRule rule2 = new OmLCRule.Builder()
        .setId("test-rule2")
        .setPrefix("")
        .setAction(new OmLCExpiration.Builder().setDays(60).build())
        .build();

    OmLifecycleConfiguration originalConfig = new OmLifecycleConfiguration.Builder()
        .setVolume(volume)
        .setBucket(bucket)
        .setCreationTime(creationTime)
        .addRule(rule1)
        .addRule(rule2)
        .setObjectID(objectID)
        .setUpdateID(updateID)
        .build();

    OmLifecycleConfiguration.Builder builder = originalConfig.toBuilder();
    OmLifecycleConfiguration rebuiltConfig = builder.build();

    assertEquals(volume, rebuiltConfig.getVolume());
    assertEquals(bucket, rebuiltConfig.getBucket());
    assertEquals(creationTime, rebuiltConfig.getCreationTime());
    assertEquals(2, rebuiltConfig.getRules().size());
    assertEquals(rule1.getId(), rebuiltConfig.getRules().get(0).getId());
    assertEquals(rule2.getId(), rebuiltConfig.getRules().get(1).getId());
    assertEquals(objectID, rebuiltConfig.getObjectID());
    assertEquals(updateID, rebuiltConfig.getUpdateID());
  }

  @Test
  public void testComplexLifecycleConfiguration() throws OMException {
    List<OmLCRule> rules = new ArrayList<>();

    // Rule 1: Simple expiration by days with prefix
    rules.add(new OmLCRule.Builder()
        .setId("rule1")
        .setEnabled(true)
        .setPrefix("/logs/")
        .setAction(new OmLCExpiration.Builder().setDays(30).build())
        .build());

    // Rule 2: Expiration by date with tag filter
    rules.add(new OmLCRule.Builder()
        .setId("rule2")
        .setEnabled(true)
        .setFilter(getOmLCFilterBuilder(null, Pair.of("temporary", "true"), null).build())
        .setAction(new OmLCExpiration.Builder()
            .setDate(getFutureDateString(100))
            .build())
        .build());

    // Rule 3: Expiration with complex AND filter
    rules.add(new OmLCRule.Builder()
        .setId("rule3")
        .setEnabled(true)
        .setFilter(getOmLCFilterBuilder(null, null,
            getOmLCAndOperatorBuilder("/backups/",
                ImmutableMap.of("tier", "archive", "retention", "short"))
                .build())
            .build())
        .setAction(new OmLCExpiration.Builder().setDays(365).build())
        .build());

    OmLifecycleConfiguration config = new OmLifecycleConfiguration.Builder()
        .setVolume("test-volume")
        .setBucket("test-bucket")
        .setRules(rules)
        .build();

    assertDoesNotThrow(config::valid);
    assertEquals(3, config.getRules().size());
  }

  @Test
  public void testDisabledRule() throws OMException {
    OmLCRule rule = new OmLCRule.Builder()
        .setId("disabled-rule")
        .setEnabled(false) // Explicitly disabled
        .setPrefix("/temp/")
        .setAction(new OmLCExpiration.Builder().setDays(7).build())
        .build();

    assertFalse(rule.isEnabled());
    assertDoesNotThrow(() -> rule.valid(BucketLayout.DEFAULT, System.currentTimeMillis()));
  }

  @Test
  public void testProtobufConversion() throws OMException {
    // Object to proto
    OmLCRule rule = getOmLCRuleBuilder("test-rule", "/logs/", true,  30, null).build();
    List<OmLCRule> rules = Collections.singletonList(rule);
    OmLifecycleConfiguration.Builder builder = getOmLifecycleConfiguration("test-volume", "test-bucket", rules);
    OmLifecycleConfiguration config = builder.setCreationTime(System.currentTimeMillis())
        .setRules(rules)
        .setObjectID(123456L)
        .setUpdateID(78910L)
        .build();
    LifecycleConfiguration proto = config.getProtobuf();

    // Proto to Object
    OmLifecycleConfiguration configFromProto =
        OmLifecycleConfiguration.getFromProtobuf(proto);
    assertEquals("test-volume", configFromProto.getVolume());
    assertEquals("test-bucket", configFromProto.getBucket());
    assertEquals(config.getCreationTime(), configFromProto.getCreationTime());
    assertEquals(config.getObjectID(), configFromProto.getObjectID());
    assertEquals(config.getUpdateID(), configFromProto.getUpdateID());
    assertNotNull(configFromProto.getRules());
    assertEquals(1, configFromProto.getRules().size());
    OmLCRule ruleFromProto = configFromProto.getRules().get(0);
    assertEquals(config.getRules().get(0).getId(), ruleFromProto.getId());
    assertEquals(config.getRules().get(0).getEffectivePrefix(), ruleFromProto.getEffectivePrefix());
    assertEquals(30, ruleFromProto.getExpiration().getDays());
  }

  @Test
  public void testOMStartupWithPastExpirationDate() throws OMException {
    // Simulate a lifecycle configuration with expiration date in the past
    // This scenario can happen when OM restarts after some time has passed
    // since the lifecycle configuration was created.
    
    // Create a rule with expiration date that is in the past (simulating old config)
    String pastDate = getFutureDateString(-1); // A date clearly in the past (1 day ago)
    OmLCExpiration pastExpiration = new OmLCExpiration.Builder()
        .setDate(pastDate)
        .build();
    
    OmLCRule ruleWithPastDate = new OmLCRule.Builder()
        .setId("test-rule-past-date")
        .setPrefix("/old-logs/")
        .setEnabled(true)
        .addAction(pastExpiration)
        .build();
    
    OmLifecycleConfiguration config = new OmLifecycleConfiguration.Builder()
        .setVolume("test-volume")
        .setBucket("test-bucket")
        .setBucketLayout(BucketLayout.DEFAULT)
        // An Expiration was created two days ago and expired 1 day ago, should be valid
        .setCreationTime(ZonedDateTime.now(ZoneOffset.UTC).minusDays(2).toInstant().toEpochMilli())
        .addRule(ruleWithPastDate)
        .setObjectID(123456L)
        .setUpdateID(78910L)
        .build();
    
    LifecycleConfiguration proto = config.getProtobuf();
    OmLifecycleConfiguration configFromProto = assertDoesNotThrow(() ->
        OmLifecycleConfiguration.getFromProtobuf(proto));
    assertDoesNotThrow(configFromProto::valid);
  }

}
