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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.EventHold;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RetentionConfig;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RetentionMode;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Rule;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Tests for retention configuration conversion and expiration calculations.
 */
class TestRetention {
  @Test
  void preservesOptionalRules() {
    Rule rule = rule(TimeUnit.DAYS, 30);
    for (RetentionConfig proto : new RetentionConfig[]{RetentionConfig.getDefaultInstance(),
        RetentionConfig.newBuilder().setRule(rule).build(),
        RetentionConfig.newBuilder().setEventHold(EventHold.newBuilder().setEnabled(true).setRule(rule)).build(),
        RetentionConfig.newBuilder().setRule(rule)
            .setEventHold(EventHold.newBuilder().setEnabled(false).setRule(rule)).build()}) {
      Retention retention = Retention.fromProto(proto);
      assertEquals(proto, retention.toProto());
      assertEquals(retention, Retention.fromProto(retention.toProto()));
      assertEquals(retention.hashCode(), Retention.fromProto(retention.toProto()).hashCode());
    }
    assertFalse(new Retention(null, null).toProto().hasRule());
  }

  @Test
  void calculatesDaysAndCalendarYearsInUtc() {
    long start = Instant.parse("2024-02-29T12:34:56.789Z").toEpochMilli();
    assertEquals(Instant.parse("2024-03-01T12:34:56.789Z").toEpochMilli(),
        RetentionUtil.calculateRetainUntilDate(rule(TimeUnit.DAYS, 1), start));
    assertEquals(Instant.parse("2025-02-28T12:34:56.789Z").toEpochMilli(),
        RetentionUtil.calculateRetainUntilDate(rule(TimeUnit.YEARS, 1), start));
  }

  @Test
  void rejectsInvalidRulesAndOverflow() {
    assertThrows(IllegalArgumentException.class, () -> RetentionUtil.validateRule(null));
    assertThrows(IllegalArgumentException.class, () -> RetentionUtil.validateRule(Rule.getDefaultInstance()));
    assertThrows(IllegalArgumentException.class, () -> RetentionUtil.validateRule(rule(TimeUnit.DAYS, 0)));
    assertThrows(IllegalArgumentException.class, () -> RetentionUtil.validateRule(rule(TimeUnit.DAYS, -1)));
    assertThrows(IllegalArgumentException.class,
        () -> RetentionUtil.calculateRetainUntilDate(rule(TimeUnit.DAYS, 1), -1));
    assertThrows(IllegalArgumentException.class,
        () -> RetentionUtil.calculateRetainUntilDate(rule(TimeUnit.DAYS, Long.MAX_VALUE), 0));
    assertThrows(IllegalArgumentException.class,
        () -> RetentionUtil.calculateRetainUntilDate(rule(TimeUnit.DAYS, 1), Long.MAX_VALUE));
    assertThrows(IllegalArgumentException.class,
        () -> RetentionUtil.calculateRetainUntilDate(rule(TimeUnit.YEARS, Long.MAX_VALUE), 0));
  }

  @Test
  void validatesExpirationBoundary() {
    RetentionUtil.validateRetainUntilDate(101, 100);
    assertThrows(IllegalArgumentException.class, () -> RetentionUtil.validateRetainUntilDate(100, 100));
    assertThrows(IllegalArgumentException.class, () -> RetentionUtil.validateRetainUntilDate(99, 100));
    assertThrows(IllegalArgumentException.class, () -> RetentionUtil.validateRetainUntilDate(-1, 100));
  }

  private static Rule rule(TimeUnit unit, long duration) {
    return Rule.newBuilder().setRetentionMode(RetentionMode.GOVERNANCE).setTimeUnit(unit).setDuration(duration).build();
  }
}
