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
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.VALID_OM_LC_AND_OPERATOR;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.assertOMException;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getFutureDateString;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getOmLCAndOperator;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getOmLCFilter;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getOmLCRule;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getOmLifecycleConfiguration;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

/**
 * Test lifecycle configuration related entities.
 */
public class TestOmLifeCycleConfiguration {

  @Test
  public void testCreateValidLCConfiguration() {
    OmLifecycleConfiguration lcc = new OmLifecycleConfiguration.Builder()
        .setVolume("s3v")
        .setBucket("spark")
        .setOwner("ozone")
        .setRules(Collections.singletonList(new OmLCRule.Builder()
            .setId("spark logs")
            .setAction(new OmLCExpiration.Builder()
                .setDays(30)
                .build())
            .build()))
        .build();

    assertDoesNotThrow(lcc::valid);
  }

  @Test
  public void testCreateInValidLCConfiguration() {
    OmLCRule rule = new OmLCRule.Builder()
        .setId("spark logs")
        .setAction(new OmLCExpiration.Builder().setDays(30).build())
        .build();

    List<OmLCRule> rules = Collections.singletonList(rule);

    OmLifecycleConfiguration lcc0 = getOmLifecycleConfiguration(null, "bucket", "owner", rules);
    assertOMException(lcc0::valid, INVALID_REQUEST, "Volume cannot be blank");

    OmLifecycleConfiguration lcc1 = getOmLifecycleConfiguration("volume", null, "owner", rules);
    assertOMException(lcc1::valid, INVALID_REQUEST, "Bucket cannot be blank");

    OmLifecycleConfiguration lcc2 = getOmLifecycleConfiguration("volume", "bucket", null, rules);
    assertOMException(lcc2::valid, INVALID_REQUEST, "Owner cannot be blank");

    OmLifecycleConfiguration lcc3 = getOmLifecycleConfiguration(
        "volume", "bucket", "owner", Collections.emptyList());
    assertOMException(lcc3::valid, INVALID_REQUEST,
        "At least one rules needs to be specified in a lifecycle configuration");

    List<OmLCRule> rules4 = new ArrayList<>(
        OmLifecycleConfiguration.LC_MAX_RULES + 1);
    for (int i = 0; i < OmLifecycleConfiguration.LC_MAX_RULES + 1; i++) {
      OmLCRule r = new OmLCRule.Builder()
          .setId(Integer.toString(i))
          .setAction(new OmLCExpiration.Builder().setDays(30).build())
          .build();
      rules4.add(r);
    }
    OmLifecycleConfiguration lcc4 = getOmLifecycleConfiguration("volume", "bucket", "owner", rules4);
    assertOMException(lcc4::valid, INVALID_REQUEST,
        "The number of lifecycle rules must not exceed the allowed limit of");

    List<OmLCRule> rules5 = rules4.subList(0, rules4.size() - 2);
    // last rule is invalid.
    rules5.add(new OmLCRule.Builder().build());
  }


  @Test
  public void testInValidFilterInLCConfiguration() {
    OmLCFilter invalidLCFilter = getOmLCFilter("prefix", Pair.of("key", "value"), VALID_OM_LC_AND_OPERATOR);
    OmLCRule rule1 = getOmLCRule("id", null, true, 1, invalidLCFilter);
    OmLifecycleConfiguration lcc0 =
        getOmLifecycleConfiguration("volume", "bucket", "owner", Collections.singletonList(rule1));
    assertOMException(lcc0::valid, INVALID_REQUEST,
        "Only one of 'Prefix', 'Tag', or 'AndOperator' should be specified");
  }

  @Test
  public void testInValidAndOperatorInLCConfiguration() {
    OmLifecycleRuleAndOperator invalidAndOperator = getOmLCAndOperator("prefix", null);
    OmLCFilter invalidLCFilter = getOmLCFilter(null, null, invalidAndOperator);
    OmLCRule rule1 = getOmLCRule("id", null, true, 1, invalidLCFilter);
    OmLifecycleConfiguration lcc0 =
        getOmLifecycleConfiguration("volume", "bucket", "owner", Collections.singletonList(rule1));
    assertOMException(lcc0::valid, INVALID_REQUEST,
        "'Prefix' alone is not allowed");
  }

  @Test
  public void testToBuilder() {
    String volume = "test-volume";
    String bucket = "test-bucket";
    String owner = "test-owner";
    long creationTime = System.currentTimeMillis();
    long objectID = 123456L;
    long updateID = 78910L;

    OmLCRule rule1 = new OmLCRule.Builder()
        .setId("test-rule1")
        .setAction(new OmLCExpiration.Builder().setDays(30).build())
        .build();

    OmLCRule rule2 = new OmLCRule.Builder()
        .setId("test-rule2")
        .setAction(new OmLCExpiration.Builder().setDays(60).build())
        .build();

    OmLifecycleConfiguration originalConfig = new OmLifecycleConfiguration.Builder()
        .setVolume(volume)
        .setBucket(bucket)
        .setOwner(owner)
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
    assertEquals(owner, rebuiltConfig.getOwner());
    assertEquals(creationTime, rebuiltConfig.getCreationTime());
    assertEquals(2, rebuiltConfig.getRules().size());
    assertEquals(rule1.getId(), rebuiltConfig.getRules().get(0).getId());
    assertEquals(rule2.getId(), rebuiltConfig.getRules().get(1).getId());
    assertEquals(objectID, rebuiltConfig.getObjectID());
    assertEquals(updateID, rebuiltConfig.getUpdateID());
  }

  @Test
  public void testComplexLifecycleConfiguration() {
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
        .setFilter(getOmLCFilter(null, Pair.of("temporary", "true"), null))
        .setAction(new OmLCExpiration.Builder()
            .setDate(getFutureDateString(100))
            .build())
        .build());

    // Rule 3: Expiration with complex AND filter
    rules.add(new OmLCRule.Builder()
        .setId("rule3")
        .setEnabled(true)
        .setFilter(getOmLCFilter(null, null,
            getOmLCAndOperator("/backups/",
                ImmutableMap.of("tier", "archive", "retention", "short"))))
        .setAction(new OmLCExpiration.Builder().setDays(365).build())
        .build());

    OmLifecycleConfiguration config = new OmLifecycleConfiguration.Builder()
        .setVolume("test-volume")
        .setBucket("test-bucket")
        .setOwner("test-owner")
        .setRules(rules)
        .build();

    assertDoesNotThrow(config::valid);
    assertEquals(3, config.getRules().size());
  }

  @Test
  public void testDisabledRule() {
    OmLCRule rule = new OmLCRule.Builder()
        .setId("disabled-rule")
        .setEnabled(false) // Explicitly disabled
        .setPrefix("/temp/")
        .setAction(new OmLCExpiration.Builder().setDays(7).build())
        .build();

    assertFalse(rule.isEnabled());
    assertDoesNotThrow(rule::valid);
  }

}
