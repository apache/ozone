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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.jupiter.api.function.Executable;

/**
 * Util Class for OM lifecycle.
 */
public final class OMLCUtils {

  public static final OmLCFilter VALID_OM_LC_FILTER = getOmLCFilter("prefix", null, null);
  public static final OmLifecycleRuleAndOperator VALID_OM_LC_AND_OPERATOR =
      getOmLCAndOperator("prefix", Collections.singletonMap("tag1", "value1"));

  public static void assertOMException(Executable action, OMException.ResultCodes expectedResultCode,
      String expectedMessageContent) {
    OMException e = assertThrows(OMException.class, action);
    assertEquals(expectedResultCode, e.getResult());
    assertTrue(e.getMessage().contains(expectedMessageContent),
        "Expected: " + expectedMessageContent + "\n Actual: " + e.getMessage());
  }

  public static String getFutureDateString(long daysInFuture) {
    return ZonedDateTime.now(ZoneOffset.UTC)
        .plusDays(daysInFuture)
        .withHour(0)
        .withMinute(0)
        .withSecond(0)
        .withNano(0)
        .format(DateTimeFormatter.ISO_DATE_TIME);
  }

  public static OmLifecycleConfiguration getOmLifecycleConfiguration(
      String volume, String bucket, String owner, List<OmLCRule> rules) {
    return new OmLifecycleConfiguration.Builder()
        .setVolume(volume)
        .setBucket(bucket)
        .setOwner(owner)
        .setRules(rules)
        .build();
  }

  public static OmLCRule getOmLCRule(String id, String prefix, boolean enabled, int expirationDays, OmLCFilter filter) {
    OmLCRule.Builder rBuilder = new OmLCRule.Builder()
        .setEnabled(enabled)
        .setId(id)
        .setPrefix(prefix)
        .setFilter(filter);

    if (expirationDays > 0) {
      rBuilder.setAction(new OmLCExpiration.Builder()
          .setDays(expirationDays).build());
    }

    return rBuilder.build();
  }

  public static OmLCFilter getOmLCFilter(String filterPrefix, Pair<String, String> filterTag,
      OmLifecycleRuleAndOperator andOperator) {
    OmLCFilter.Builder lcfBuilder = new OmLCFilter.Builder()
        .setPrefix(filterPrefix)
        .setAndOperator(andOperator);
    if (filterTag != null) {
      lcfBuilder.setTag(filterTag.getKey(), filterTag.getValue());
    }
    return lcfBuilder.build();
  }

  public static OmLifecycleRuleAndOperator getOmLCAndOperator(String prefix, Map<String, String> tags) {
    return new OmLifecycleRuleAndOperator.Builder()
        .setPrefix(prefix)
        .setTags(tags)
        .build();
  }

  private OMLCUtils() {
    throw new UnsupportedOperationException();
  }
}
