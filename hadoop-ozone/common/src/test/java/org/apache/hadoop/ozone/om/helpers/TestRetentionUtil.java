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

import java.time.Instant;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RetentionMode;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RetentionTimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Tests for retention validation and expiration calculations.
 */
class TestRetentionUtil {
  @Test
  void validatesEventHoldDurationBounds() {
    for (RetentionTimeUnit unit : RetentionTimeUnit.values()) {
      long maximum = unit == RetentionTimeUnit.DAYS ? 36_500 : 100;
      RetentionDuration valid = rule(unit, maximum);
      RetentionDuration invalid = rule(unit, maximum + 1);
      RetentionUtil.validateBucketRetention(new BucketRetention(RetentionMode.GOVERNANCE, null, valid));
      RetentionUtil.validateBucketRetention(new BucketRetention(RetentionMode.GOVERNANCE, invalid, null));
      RetentionUtil.validateObjectRetention(new ObjectRetention(RetentionMode.GOVERNANCE, 0,
          new EventHold(true, valid)), 100);
      for (RetentionDuration invalidHold : new RetentionDuration[]{invalid, rule(unit, 0), rule(unit, -1)}) {
        assertThrows(IllegalArgumentException.class, () -> RetentionUtil.validateBucketRetention(
            new BucketRetention(RetentionMode.GOVERNANCE, null, invalidHold)));
        assertThrows(IllegalArgumentException.class, () -> RetentionUtil.validateObjectRetention(
            new ObjectRetention(RetentionMode.GOVERNANCE, 0, new EventHold(true, invalidHold)), 100));
      }
    }
  }

  @Test
  void calculatesDaysAndCalendarYearsInUtc() {
    long start = Instant.parse("2024-02-29T12:34:56.789Z").toEpochMilli();
    assertEquals(Instant.parse("2024-03-01T12:34:56.789Z").toEpochMilli(),
        RetentionUtil.calculateRetainUntilDate(rule(RetentionTimeUnit.DAYS, 1), start));
    assertEquals(Instant.parse("2025-02-28T12:34:56.789Z").toEpochMilli(),
        RetentionUtil.calculateRetainUntilDate(rule(RetentionTimeUnit.YEARS, 1), start));
  }

  @Test
  void validatesBucketRetentionDuration() {
    RetentionUtil.validateBucketRetention(new BucketRetention(RetentionMode.GOVERNANCE, null, null));
    RetentionUtil.validateBucketRetention(
        new BucketRetention(RetentionMode.GOVERNANCE, rule(RetentionTimeUnit.DAYS, 1), null));
    assertThrows(IllegalArgumentException.class, () -> RetentionUtil.validateBucketRetention(
        new BucketRetention(RetentionMode.GOVERNANCE, rule(RetentionTimeUnit.DAYS, 0), null)));
    assertThrows(IllegalArgumentException.class, () -> RetentionUtil.validateBucketRetention(
        new BucketRetention(RetentionMode.GOVERNANCE, rule(RetentionTimeUnit.DAYS, -1), null)));
  }

  @Test
  void rejectsInvalidCalculationInputsAndOverflow() {
    assertThrows(IllegalArgumentException.class, () -> RetentionUtil.calculateRetainUntilDate(null, 0));
    assertThrows(IllegalArgumentException.class,
        () -> RetentionUtil.calculateRetainUntilDate(rule(RetentionTimeUnit.DAYS, 0), 0));
    assertThrows(IllegalArgumentException.class,
        () -> RetentionUtil.calculateRetainUntilDate(rule(RetentionTimeUnit.DAYS, 1), -1));
    assertThrows(IllegalArgumentException.class,
        () -> RetentionUtil.calculateRetainUntilDate(rule(RetentionTimeUnit.DAYS, Long.MAX_VALUE), 0));
    assertThrows(IllegalArgumentException.class,
        () -> RetentionUtil.calculateRetainUntilDate(rule(RetentionTimeUnit.DAYS, 1), Long.MAX_VALUE));
    assertThrows(IllegalArgumentException.class,
        () -> RetentionUtil.calculateRetainUntilDate(rule(RetentionTimeUnit.YEARS, Long.MAX_VALUE), 0));
  }

  @Test
  void validatesExpirationBoundary() {
    RetentionUtil.validateObjectRetention(new ObjectRetention(RetentionMode.GOVERNANCE, 0, null), 100);
    RetentionUtil.validateObjectRetention(new ObjectRetention(RetentionMode.GOVERNANCE, 101, null), 100);
    for (long date : new long[]{100, 99, -1}) {
      assertThrows(IllegalArgumentException.class, () -> RetentionUtil.validateObjectRetention(
          new ObjectRetention(RetentionMode.GOVERNANCE, date, null), 100));
    }
  }

  private static RetentionDuration rule(RetentionTimeUnit unit, long duration) {
    return new RetentionDuration(unit, duration);
  }
}
