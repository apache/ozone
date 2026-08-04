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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleRuleAndOperator;
import org.junit.jupiter.api.Test;

/**
 * Test OmLifecycleRuleAndOperator.
 */
class TestOmLifecycleRuleAndOperator {

  @Test
  public void testValidAndOperator() throws OMException {
    OmLifecycleRuleAndOperator andOperator1 =
        getOmLCAndOperatorBuilder("prefix", Collections.singletonMap("tag1", "value1")).build();
    assertDoesNotThrow(() -> andOperator1.valid(BucketLayout.DEFAULT));

    OmLifecycleRuleAndOperator andOperator2 =
        getOmLCAndOperatorBuilder("", Collections.singletonMap("tag1", "value1")).build();
    assertDoesNotThrow(() -> andOperator2.valid(BucketLayout.DEFAULT));

    OmLifecycleRuleAndOperator andOperator3 = getOmLCAndOperatorBuilder(
        "prefix", ImmutableMap.of("tag1", "value1", "tag2", "value2")).build();
    assertDoesNotThrow(() -> andOperator3.valid(BucketLayout.DEFAULT));

    OmLifecycleRuleAndOperator andOperator4 = getOmLCAndOperatorBuilder(
        null, ImmutableMap.of("tag1", "value1", "tag2", "value2")).build();
    assertDoesNotThrow(() -> andOperator4.valid(BucketLayout.DEFAULT));
  }

  @Test
  public void testInValidAndOperator() {
    OmLifecycleRuleAndOperator.Builder andOperator1 = getOmLCAndOperatorBuilder("prefix", null);
    assertOMException(() -> andOperator1.build().valid(BucketLayout.DEFAULT), INVALID_REQUEST,
        "'Prefix' alone is not allowed");

    OmLifecycleRuleAndOperator.Builder andOperator2 =
        getOmLCAndOperatorBuilder(null, Collections.singletonMap("tag1", "value1"));
    assertOMException(() -> andOperator2.build().valid(BucketLayout.DEFAULT), INVALID_REQUEST,
        "If 'Tags' are specified without 'Prefix', there should be more than one tag");

    OmLifecycleRuleAndOperator.Builder andOperator3 = getOmLCAndOperatorBuilder(null, null);
    assertOMException(() -> andOperator3.build().valid(BucketLayout.DEFAULT), INVALID_REQUEST,
        "Either 'Tags' or 'Prefix' must be specified.");
  }

  @Test
  public void testValidation() {
    // 1. Prefix is Trash path
    OmLifecycleRuleAndOperator.Builder trashPrefixAndOp = getOmLCAndOperatorBuilder(
        FileSystem.TRASH_PREFIX, Collections.singletonMap("tag1", "value1"));
    assertOMException(() -> trashPrefixAndOp.build().valid(BucketLayout.DEFAULT), INVALID_REQUEST,
        "Lifecycle rule prefix cannot be trash root");

    // 2. Prefix too long
    String longPrefix = RandomStringUtils.randomAlphanumeric(1025);
    OmLifecycleRuleAndOperator.Builder longPrefixAndOp = getOmLCAndOperatorBuilder(
        longPrefix, Collections.singletonMap("tag1", "value1"));
    assertOMException(() -> longPrefixAndOp.build().valid(BucketLayout.DEFAULT), INVALID_REQUEST,
        "The maximum size of a prefix is 1024");

    // 3. Tag key too long
    String longKey = RandomStringUtils.randomAlphanumeric(129);
    OmLifecycleRuleAndOperator.Builder longKeyAndOp = getOmLCAndOperatorBuilder(
        "prefix", Collections.singletonMap(longKey, "value"));
    assertOMException(() -> longKeyAndOp.build().valid(BucketLayout.DEFAULT), INVALID_REQUEST,
        "A Tag's Key must be a length between 1 and 128");

    // 4. Tag value too long
    String longValue = RandomStringUtils.randomAlphanumeric(257);
    OmLifecycleRuleAndOperator.Builder longValueAndOp = getOmLCAndOperatorBuilder(
        "prefix", Collections.singletonMap("key", longValue));
    assertOMException(() -> longValueAndOp.build().valid(BucketLayout.DEFAULT), INVALID_REQUEST,
        "A Tag's Value must be a length between 0 and 256");
  }

  @Test
  public void testProtobufConversion() throws OMException {
    // Prefix and tags
    Map<String, String> tags = ImmutableMap.of("tag1", "value1", "tag2", "");
    OmLifecycleRuleAndOperator andOp = getOmLCAndOperatorBuilder("prefix", tags).build();
    LifecycleRuleAndOperator proto = andOp.getProtobuf();
    OmLifecycleRuleAndOperator andOpFromProto =
        OmLifecycleRuleAndOperator.getFromProtobuf(proto, BucketLayout.DEFAULT);
    assertEquals("prefix", andOpFromProto.getPrefix());
    assertEquals(2, andOpFromProto.getTags().size());
    assertTrue(andOpFromProto.getTags().containsKey("tag1"));
    assertEquals("value1", andOpFromProto.getTags().get("tag1"));
    assertTrue(andOpFromProto.getTags().containsKey("tag2"));
    assertEquals("", andOpFromProto.getTags().get("tag2"));

    // Multiple tags
    OmLifecycleRuleAndOperator andOp2 = getOmLCAndOperatorBuilder(null, tags).build();
    LifecycleRuleAndOperator proto2 = andOp2.getProtobuf();
    OmLifecycleRuleAndOperator andOpFromProto2 =
        OmLifecycleRuleAndOperator.getFromProtobuf(proto2, BucketLayout.DEFAULT);
    assertNull(andOpFromProto2.getPrefix());
    assertEquals(2, andOpFromProto2.getTags().size());

    // Prefix is ""
    OmLifecycleRuleAndOperator andOp3 = getOmLCAndOperatorBuilder("", tags).build();
    LifecycleRuleAndOperator proto3 = andOp3.getProtobuf();
    OmLifecycleRuleAndOperator andOpFromProto3 =
        OmLifecycleRuleAndOperator.getFromProtobuf(proto3, BucketLayout.DEFAULT);
    assertEquals("", andOpFromProto3.getPrefix());
    assertEquals(2, andOpFromProto2.getTags().size());
  }

}
