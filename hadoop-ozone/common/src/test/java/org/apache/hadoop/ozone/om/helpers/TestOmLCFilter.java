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
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getOmLCFilterBuilder;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getOmLCRuleBuilder;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.jupiter.api.Test;

/**
 * Test OmLCExpiration.
 */
class TestOmLCFilter {

  @Test
  public void testInValidOmLCRulePrefixFilterCoExist() throws OMException {
    OmLCRule.Builder rule1 = getOmLCRuleBuilder("id", "prefix", true, 1, VALID_OM_LC_FILTER);
    assertOMException(rule1::build, INVALID_REQUEST, "Filter and Prefix cannot be used together");

    OmLCRule.Builder rule2 = getOmLCRuleBuilder("id", "", true, 1, VALID_OM_LC_FILTER);
    assertOMException(rule2::build, INVALID_REQUEST, "Filter and Prefix cannot be used together");
  }

  @Test
  public void testValidFilter() throws OMException {
    OmLCFilter lcFilter1 = getOmLCFilterBuilder("prefix", null, null).build();
    assertDoesNotThrow(lcFilter1::valid);

    OmLCFilter lcFilter2 = getOmLCFilterBuilder(null, Pair.of("key", "value"), null).build();
    assertDoesNotThrow(lcFilter2::valid);

    OmLCFilter lcFilter3 = getOmLCFilterBuilder(null, null, VALID_OM_LC_AND_OPERATOR).build();
    assertDoesNotThrow(lcFilter3::valid);

    OmLCFilter lcFilter4 = getOmLCFilterBuilder(null, null, null).build();
    assertDoesNotThrow(lcFilter4::valid);

    OmLCFilter lcFilter5 = getOmLCFilterBuilder("", null, null).build();
    assertDoesNotThrow(lcFilter5::valid);
  }

  @Test
  public void testInValidFilter() {
    OmLCFilter.Builder lcFilter1 = getOmLCFilterBuilder("prefix", Pair.of("key", "value"), VALID_OM_LC_AND_OPERATOR);
    assertOMException(lcFilter1::build, INVALID_REQUEST,
        "Only one of 'Prefix', 'Tag', or 'AndOperator' should be specified");

    OmLCFilter.Builder lcFilter2 = getOmLCFilterBuilder("prefix", Pair.of("key", "value"), null);
    assertOMException(lcFilter2::build, INVALID_REQUEST,
        "Only one of 'Prefix', 'Tag', or 'AndOperator' should be specified");

    OmLCFilter.Builder lcFilter3 = getOmLCFilterBuilder("prefix", null, VALID_OM_LC_AND_OPERATOR);
    assertOMException(lcFilter3::build, INVALID_REQUEST,
        "Only one of 'Prefix', 'Tag', or 'AndOperator' should be specified");

    OmLCFilter.Builder lcFilter4 = getOmLCFilterBuilder(null, Pair.of("key", "value"), VALID_OM_LC_AND_OPERATOR);
    assertOMException(lcFilter4::build, INVALID_REQUEST,
        "Only one of 'Prefix', 'Tag', or 'AndOperator' should be specified");

  }

}
