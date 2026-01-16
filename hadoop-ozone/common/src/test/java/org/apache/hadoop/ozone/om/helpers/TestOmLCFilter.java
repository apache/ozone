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
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.VALID_OM_LC_FILTER;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.assertOMException;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getOmLCAndOperatorBuilder;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getOmLCFilterBuilder;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getOmLCRuleBuilder;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Collections;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleFilter;
import org.junit.jupiter.api.Test;

/**
 * Test OmLCExpiration.
 */
class TestOmLCFilter {

  @Test
  public void testInValidOmLCRulePrefixFilterCoExist() throws OMException {
    long currentTime = System.currentTimeMillis();
    OmLCRule.Builder rule1 = getOmLCRuleBuilder("id", "prefix", true, 1, VALID_OM_LC_FILTER);
    assertOMException(() -> rule1.build().valid(BucketLayout.DEFAULT, currentTime), INVALID_REQUEST,
        "Filter and Prefix cannot be used together");

    OmLCRule.Builder rule2 = getOmLCRuleBuilder("id", "", true, 1, VALID_OM_LC_FILTER);
    assertOMException(() -> rule2.build().valid(BucketLayout.DEFAULT, currentTime), INVALID_REQUEST,
        "Filter and Prefix cannot be used together");
  }

  @Test
  public void testValidFilter() throws OMException {
    OmLCFilter lcFilter1 = getOmLCFilterBuilder("prefix", null, null).build();
    assertDoesNotThrow(() -> lcFilter1.valid(BucketLayout.DEFAULT));

    OmLCFilter lcFilter2 = getOmLCFilterBuilder(null, Pair.of("key", "value"), null).build();
    assertDoesNotThrow(() -> lcFilter2.valid(BucketLayout.DEFAULT));

    OmLCFilter lcFilter3 = getOmLCFilterBuilder(null, null, VALID_OM_LC_AND_OPERATOR).build();
    assertDoesNotThrow(() -> lcFilter3.valid(BucketLayout.DEFAULT));

    OmLCFilter lcFilter4 = getOmLCFilterBuilder(null, null, null).build();
    assertDoesNotThrow(() -> lcFilter4.valid(BucketLayout.DEFAULT));

    OmLCFilter lcFilter5 = getOmLCFilterBuilder("", null, null).build();
    assertDoesNotThrow(() -> lcFilter5.valid(BucketLayout.DEFAULT));
  }

  @Test
  public void testInValidFilter() {
    OmLCFilter.Builder lcFilter1 = getOmLCFilterBuilder("prefix", Pair.of("key", "value"), VALID_OM_LC_AND_OPERATOR);
    assertOMException(() -> lcFilter1.build().valid(BucketLayout.DEFAULT), INVALID_REQUEST,
        "Only one of 'Prefix', 'Tag', or 'AndOperator' should be specified");

    OmLCFilter.Builder lcFilter2 = getOmLCFilterBuilder("prefix", Pair.of("key", "value"), null);
    assertOMException(() -> lcFilter2.build().valid(BucketLayout.DEFAULT), INVALID_REQUEST,
        "Only one of 'Prefix', 'Tag', or 'AndOperator' should be specified");

    OmLCFilter.Builder lcFilter3 = getOmLCFilterBuilder("prefix", null, VALID_OM_LC_AND_OPERATOR);
    assertOMException(() -> lcFilter3.build().valid(BucketLayout.DEFAULT), INVALID_REQUEST,
        "Only one of 'Prefix', 'Tag', or 'AndOperator' should be specified");

    OmLCFilter.Builder lcFilter4 = getOmLCFilterBuilder(null, Pair.of("key", "value"), VALID_OM_LC_AND_OPERATOR);
    assertOMException(() -> lcFilter4.build().valid(BucketLayout.DEFAULT), INVALID_REQUEST,
        "Only one of 'Prefix', 'Tag', or 'AndOperator' should be specified");

  }

  @Test
  public void testFilterValidation() {
    // 1. Prefix is Trash path
    OmLCFilter.Builder trashPrefixFilter = getOmLCFilterBuilder(FileSystem.TRASH_PREFIX, null, null);
    assertOMException(() -> trashPrefixFilter.build().valid(BucketLayout.DEFAULT), INVALID_REQUEST,
        "Lifecycle rule prefix cannot be trash root");

    // 2. Prefix too long
    String longPrefix = RandomStringUtils.randomAlphanumeric(1025);
    OmLCFilter.Builder longPrefixFilter = getOmLCFilterBuilder(longPrefix, null, null);
    assertOMException(() -> longPrefixFilter.build().valid(BucketLayout.DEFAULT), INVALID_REQUEST,
        "The maximum size of a prefix is 1024");

    // 3. Tag key too long
    String longKey = RandomStringUtils.randomAlphanumeric(129);
    OmLCFilter.Builder longKeyFilter = getOmLCFilterBuilder(null, Pair.of(longKey, "value"), null);
    assertOMException(() -> longKeyFilter.build().valid(BucketLayout.DEFAULT), INVALID_REQUEST,
        "A Tag's Key must be a length between 1 and 128");

    // 4. Tag value too long
    String longValue = RandomStringUtils.randomAlphanumeric(257);
    OmLCFilter.Builder longValueFilter = getOmLCFilterBuilder(null, Pair.of("key", longValue), null);
    assertOMException(() -> longValueFilter.build().valid(BucketLayout.DEFAULT), INVALID_REQUEST,
        "A Tag's Value must be a length between 0 and 256");
  }

  @Test
  public void testProtobufConversion() throws OMException {
    // Only prefix
    OmLCFilter filter1 = getOmLCFilterBuilder("prefix", null, null).build();
    LifecycleFilter proto1 = filter1.getProtobuf();
    OmLCFilter filterFromProto1 = OmLCFilter.getFromProtobuf(proto1, BucketLayout.DEFAULT);
    assertEquals("prefix", filterFromProto1.getPrefix());
    assertNull(filterFromProto1.getTag());
    assertNull(filterFromProto1.getAndOperator());

    // Only tag
    OmLCFilter filter2 = getOmLCFilterBuilder(null, Pair.of("key", "value"), null).build();
    LifecycleFilter proto2 = filter2.getProtobuf();
    OmLCFilter filterFromProto2 = OmLCFilter.getFromProtobuf(proto2, BucketLayout.DEFAULT);
    assertNull(filterFromProto2.getPrefix());
    assertNotNull(filterFromProto2.getTag());
    assertEquals("key", filterFromProto2.getTag().getKey());
    assertEquals("value", filterFromProto2.getTag().getValue());

    // Only andOperator
    OmLifecycleRuleAndOperator andOp = getOmLCAndOperatorBuilder(
        "prefix", Collections.singletonMap("tag1", "value1")).build();
    OmLCFilter filter3 = getOmLCFilterBuilder(null, null, andOp).build();
    LifecycleFilter proto3 = filter3.getProtobuf();
    OmLCFilter filterFromProto3 = OmLCFilter.getFromProtobuf(proto3, BucketLayout.DEFAULT);
    assertNull(filterFromProto3.getPrefix());
    assertNull(filterFromProto3.getTag());
    assertNotNull(filterFromProto3.getAndOperator());

    // Only prefix and prefix is ""
    OmLCFilter filter4 = getOmLCFilterBuilder("", null, null).build();
    LifecycleFilter proto4 = filter4.getProtobuf();
    OmLCFilter filterFromProto4 = OmLCFilter.getFromProtobuf(proto4, BucketLayout.DEFAULT);
    assertEquals("", filterFromProto4.getPrefix());
    assertNull(filterFromProto4.getTag());
    assertNull(filterFromProto4.getAndOperator());
  }

}
