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
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getOmLCAndOperator;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleRuleAndOperator;
import org.junit.jupiter.api.Test;

/**
 * Test OmLifecycleRuleAndOperator.
 */
class TestOmLifecycleRuleAndOperator {

  @Test
  public void testValidAndOperator() {
    OmLifecycleRuleAndOperator andOperator1 = getOmLCAndOperator("prefix", Collections.singletonMap("tag1", "value1"));
    assertDoesNotThrow(andOperator1::valid);

    OmLifecycleRuleAndOperator andOperator2 =
        getOmLCAndOperator("prefix", ImmutableMap.of("tag1", "value1", "tag2", "value2"));
    assertDoesNotThrow(andOperator2::valid);

    OmLifecycleRuleAndOperator andOperator3 = getOmLCAndOperator(
        null, ImmutableMap.of("tag1", "value1", "tag2", "value2"));
    assertDoesNotThrow(andOperator3::valid);
  }

  @Test
  public void testInValidAndOperator() {
    OmLifecycleRuleAndOperator andOperator1 = getOmLCAndOperator("prefix", null);
    assertOMException(andOperator1::valid, INVALID_REQUEST, "'Prefix' alone is not allowed");

    OmLifecycleRuleAndOperator andOperator2 = getOmLCAndOperator(null, Collections.singletonMap("tag1", "value1"));
    assertOMException(andOperator2::valid, INVALID_REQUEST,
        "If 'Tags' are specified without 'Prefix', there should be more than one tag");

    OmLifecycleRuleAndOperator andOperator3 = getOmLCAndOperator(null, null);
    assertOMException(andOperator3::valid, INVALID_REQUEST, "Either 'Tags' or 'Prefix' must be specified.");
  }

  @Test
  public void testProtobufConversion() {
    // Prefix and tags
    Map<String, String> tags = ImmutableMap.of("tag1", "value1", "tag2", "value2");
    OmLifecycleRuleAndOperator andOp = getOmLCAndOperator("prefix", tags);
    LifecycleRuleAndOperator proto = andOp.getProtobuf();
    OmLifecycleRuleAndOperator andOpFromProto =
        OmLifecycleRuleAndOperator.getFromProtobuf(proto);
    assertEquals("prefix", andOpFromProto.getPrefix());
    assertEquals(2, andOpFromProto.getTags().size());
    assertTrue(andOpFromProto.getTags().containsKey("tag1"));
    assertEquals("value1", andOpFromProto.getTags().get("tag1"));
    assertTrue(andOpFromProto.getTags().containsKey("tag2"));
    assertEquals("value2", andOpFromProto.getTags().get("tag2"));

    // Multiple tags
    OmLifecycleRuleAndOperator andOp2 = getOmLCAndOperator(null, tags);
    LifecycleRuleAndOperator proto2 = andOp2.getProtobuf();
    OmLifecycleRuleAndOperator andOpFromProto2 =
        OmLifecycleRuleAndOperator.getFromProtobuf(proto2);
    assertNull(andOpFromProto2.getPrefix());
    assertEquals(2, andOpFromProto2.getTags().size());
  }

}
