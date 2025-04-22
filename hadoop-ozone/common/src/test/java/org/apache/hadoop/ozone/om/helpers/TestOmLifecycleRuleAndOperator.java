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

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.jupiter.api.Test;

/**
 * Test OmLifecycleRuleAndOperator.
 */
class TestOmLifecycleRuleAndOperator {

  @Test
  public void testValidAndOperator() throws OMException {
    OmLifecycleRuleAndOperator andOperator1 =
        getOmLCAndOperatorBuilder("prefix", Collections.singletonMap("tag1", "value1")).build();
    assertDoesNotThrow(andOperator1::valid);

    OmLifecycleRuleAndOperator andOperator2 =
        getOmLCAndOperatorBuilder("", Collections.singletonMap("tag1", "value1")).build();
    assertDoesNotThrow(andOperator2::valid);

    OmLifecycleRuleAndOperator andOperator3 = getOmLCAndOperatorBuilder(
        "prefix", ImmutableMap.of("tag1", "value1", "tag2", "value2")).build();
    assertDoesNotThrow(andOperator3::valid);

    OmLifecycleRuleAndOperator andOperator4 = getOmLCAndOperatorBuilder(
        null, ImmutableMap.of("tag1", "value1", "tag2", "value2")).build();
    assertDoesNotThrow(andOperator4::valid);


  }

  @Test
  public void testInValidAndOperator() {
    OmLifecycleRuleAndOperator.Builder andOperator1 = getOmLCAndOperatorBuilder("prefix", null);
    assertOMException(andOperator1::build, INVALID_REQUEST, "'Prefix' alone is not allowed");

    OmLifecycleRuleAndOperator.Builder andOperator2 =
        getOmLCAndOperatorBuilder(null, Collections.singletonMap("tag1", "value1"));
    assertOMException(andOperator2::build, INVALID_REQUEST,
        "If 'Tags' are specified without 'Prefix', there should be more than one tag");

    OmLifecycleRuleAndOperator.Builder andOperator3 = getOmLCAndOperatorBuilder(null, null);
    assertOMException(andOperator3::build, INVALID_REQUEST, "Either 'Tags' or 'Prefix' must be specified.");
  }

}
