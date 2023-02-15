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
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getOmLCFilter;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getOmLCRule;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

/**
 * Test OmLCExpiration.
 */
class TestOmLCFilter {

  @Test
  public void testInValidOmLCRulePrefixFilterCoExist() {
    OmLCRule rule1 = getOmLCRule("id", "prefix", true, 1, VALID_OM_LC_FILTER);
    assertOMException(rule1::valid, INVALID_REQUEST, "Filter and Prefix cannot be used together");

    OmLCRule rule2 = getOmLCRule("id", "", true, 1, VALID_OM_LC_FILTER);
    assertOMException(rule2::valid, INVALID_REQUEST, "Filter and Prefix cannot be used together");
  }

  @Test
  public void testValidFilter() {
    OmLCFilter lcFilter1 = getOmLCFilter("prefix", null, null);
    assertDoesNotThrow(lcFilter1::valid);

    OmLCFilter lcFilter2 = getOmLCFilter(null, Pair.of("key", "value"), null);
    assertDoesNotThrow(lcFilter2::valid);

    OmLCFilter lcFilter3 = getOmLCFilter(null, null, VALID_OM_LC_AND_OPERATOR);
    assertDoesNotThrow(lcFilter3::valid);

    OmLCFilter lcFilter4 = getOmLCFilter(null, null, null);
    assertDoesNotThrow(lcFilter4::valid);

    OmLCFilter lcFilter5 = getOmLCFilter("", null, null);
    assertDoesNotThrow(lcFilter5::valid);
  }

  @Test
  public void testInValidFilter() {
    OmLCFilter lcFilter1 = getOmLCFilter("prefix", Pair.of("key", "value"), VALID_OM_LC_AND_OPERATOR);
    assertOMException(lcFilter1::valid, INVALID_REQUEST,
        "Only one of 'Prefix', 'Tag', or 'AndOperator' should be specified");

    OmLCFilter lcFilter2 = getOmLCFilter("prefix", Pair.of("key", "value"), null);
    assertOMException(lcFilter2::valid, INVALID_REQUEST,
        "Only one of 'Prefix', 'Tag', or 'AndOperator' should be specified");

    OmLCFilter lcFilter3 = getOmLCFilter("prefix", null, VALID_OM_LC_AND_OPERATOR);
    assertOMException(lcFilter3::valid, INVALID_REQUEST,
        "Only one of 'Prefix', 'Tag', or 'AndOperator' should be specified");

    OmLCFilter lcFilter4 = getOmLCFilter(null, Pair.of("key", "value"), VALID_OM_LC_AND_OPERATOR);
    assertOMException(lcFilter4::valid, INVALID_REQUEST,
        "Only one of 'Prefix', 'Tag', or 'AndOperator' should be specified");

  }

}
