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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleAction;
import org.junit.jupiter.api.Test;

/**
 * Tests the lifecycle action that reclaims a key's noncurrent versions.
 */
class TestOmLCNoncurrentVersionExpiration {

  private static OmLCNoncurrentVersionExpiration of(Integer days,
      Integer versions) {
    OmLCNoncurrentVersionExpiration.Builder builder =
        new OmLCNoncurrentVersionExpiration.Builder();
    if (days != null) {
      builder.setNoncurrentDays(days);
    }
    if (versions != null) {
      builder.setNewerNoncurrentVersions(versions);
    }
    return builder.build();
  }

  @Test
  void eitherLimitOnItsOwnIsValid() {
    assertDoesNotThrow(() -> of(30, null).valid(0L));
    assertDoesNotThrow(() -> of(null, 5).valid(0L));
    assertDoesNotThrow(() -> of(30, 5).valid(0L));
  }

  /** Unlike Expiration's Days and Date, the two limits are not exclusive. */
  @Test
  void aRuleThatLimitsNothingIsRejected() {
    assertOMException(() -> of(null, null).valid(0L), INVALID_REQUEST,
        "must specify 'NoncurrentDays', 'NewerNoncurrentVersions', or both");
  }

  @Test
  void noncurrentDaysMustBePositive() {
    assertOMException(() -> of(0, null).valid(0L), INVALID_REQUEST,
        "'NoncurrentDays' must be a positive integer");
  }

  @Test
  void newerNoncurrentVersionsIsBoundedLikeS3() {
    assertDoesNotThrow(() -> of(null, 1).valid(0L));
    assertDoesNotThrow(() -> of(null,
        OmLCNoncurrentVersionExpiration.MAX_NEWER_NONCURRENT_VERSIONS)
        .valid(0L));

    assertOMException(() -> of(null, 0).valid(0L), INVALID_REQUEST,
        "'NewerNoncurrentVersions' must be between 1 and 100");
    assertOMException(() -> of(null,
        OmLCNoncurrentVersionExpiration.MAX_NEWER_NONCURRENT_VERSIONS + 1)
        .valid(0L), INVALID_REQUEST,
        "'NewerNoncurrentVersions' must be between 1 and 100");
  }

  /**
   * A rule that only caps how many versions are kept says nothing about age,
   * so no version is ever expired by time under it.
   */
  @Test
  void aCountOnlyRuleNeverExpiresByAge() throws Exception {
    OmLCNoncurrentVersionExpiration rule = of(null, 5);
    rule.valid(0L);

    assertFalse(rule.isExpired(0L));
  }

  @Test
  void aVersionExpiresOnceItHasBeenNoncurrentLongEnough() throws Exception {
    OmLCNoncurrentVersionExpiration rule = of(1, null);
    rule.valid(0L);

    final long twoDaysAgo =
        System.currentTimeMillis() - java.util.concurrent.TimeUnit.DAYS
            .toMillis(2);
    assertTrue(rule.isExpired(twoDaysAgo));
    assertFalse(rule.isExpired(System.currentTimeMillis()));
  }

  /**
   * The age is settled when the rule is built. Deriving it in valid() instead
   * would make an unvalidated rule read as zero days, which expires every
   * version the moment it becomes noncurrent.
   */
  @Test
  void ageIsHonouredWithoutValidationHavingRun() {
    OmLCNoncurrentVersionExpiration rule = of(1, null);

    final long anHourAgo =
        System.currentTimeMillis()
            - java.util.concurrent.TimeUnit.HOURS.toMillis(1);
    assertFalse(rule.isExpired(anHourAgo));
  }

  @Test
  void theActionSurvivesAProtobufRoundTrip() {
    LifecycleAction proto = of(30, 5).getProtobuf();
    OmLCNoncurrentVersionExpiration back =
        OmLCNoncurrentVersionExpiration.getFromProtobuf(
            proto.getNoncurrentVersionExpiration());

    assertEquals(30, back.getNoncurrentDays());
    assertEquals(5, back.getNewerNoncurrentVersions());

    OmLCNoncurrentVersionExpiration daysOnly =
        OmLCNoncurrentVersionExpiration.getFromProtobuf(
            of(30, null).getProtobuf().getNoncurrentVersionExpiration());
    assertEquals(30, daysOnly.getNoncurrentDays());
    assertNull(daysOnly.getNewerNoncurrentVersions());
  }
}
