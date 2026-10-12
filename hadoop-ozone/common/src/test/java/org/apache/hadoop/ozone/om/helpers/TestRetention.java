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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import java.time.DateTimeException;
import java.time.Instant;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.EventHold;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfoProtoLight;
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
  void usesNestedKeyRetentionAndSeparateBucketMode() {
    Descriptor key = KeyInfo.getDescriptor();
    FieldDescriptor retention = key.findFieldByName("retention");
    assertNotNull(retention);
    assertEquals(23, retention.getNumber());
    assertEquals("Retention", retention.getMessageType().getName());
    assertEquals(24, key.findFieldByName("legalHold").getNumber());
    assertEquals(key.findFieldByName("retention").getNumber(),
        KeyInfoProtoLight.getDescriptor().findFieldByName("retention").getNumber());
    assertNotNull(RetentionConfig.getDescriptor().findFieldByName("retentionMode"));
    assertEquals(2, RetentionConfig.getDescriptor().findFieldByName("rule").getNumber());
    assertEquals(3, RetentionConfig.getDescriptor().findFieldByName("eventHold").getNumber());
    assertEquals(1, Rule.getDescriptor().findFieldByName("timeUnit").getNumber());
    assertEquals(2, Rule.getDescriptor().findFieldByName("duration").getNumber());
  }

  @Test
  void preservesOptionalRules() {
    Rule rule = rule(TimeUnit.DAYS, 30);
    for (RetentionConfig proto : new RetentionConfig[]{
        RetentionConfig.newBuilder().setRetentionMode(RetentionMode.GOVERNANCE).build(),
        RetentionConfig.newBuilder().setRetentionMode(RetentionMode.GOVERNANCE).setRule(rule).build(),
        RetentionConfig.newBuilder().setRetentionMode(RetentionMode.COMPLIANCE)
            .setEventHold(EventHold.newBuilder().setEnabled(true).setRule(rule)).build(),
        RetentionConfig.newBuilder().setRetentionMode(RetentionMode.GOVERNANCE).setRule(rule)
            .setEventHold(EventHold.newBuilder().setEnabled(false).setRule(rule)).build()}) {
      Retention retention = Retention.fromProto(proto);
      assertEquals(proto, retention.toProto());
      assertEquals(retention, Retention.fromProto(retention.toProto()));
      assertEquals(retention.hashCode(), Retention.fromProto(retention.toProto()).hashCode());
    }
    assertFalse(new Retention(RetentionMode.GOVERNANCE, null, null).toProto().hasRule());
  }

  @Test
  void preservesObjectRetentionAndUtcDates() {
    long date = Instant.parse("2027-01-15T08:00:00.123Z").toEpochMilli();
    EventHold hold = EventHold.newBuilder().setEnabled(true).setRule(rule(TimeUnit.YEARS, 1)).build();
    for (ObjectRetention retention : new ObjectRetention[]{
        new ObjectRetention(RetentionMode.GOVERNANCE, date, null),
        new ObjectRetention(RetentionMode.COMPLIANCE, null, hold),
        new ObjectRetention(RetentionMode.COMPLIANCE, date, hold)}) {
      assertEquals(retention, ObjectRetention.fromProto(retention.toProto()));
      assertEquals(retention.hashCode(), ObjectRetention.fromProto(retention.toProto()).hashCode());
      assertEquals(retention.getRetentionDate() != null, retention.toProto().hasRetentionDate());
    }
    ObjectRetention retention = new ObjectRetention(RetentionMode.GOVERNANCE, date, null);
    assertEquals("2027-01-15T08:00:00.123Z", retention.toProto().getRetentionDate());
    assertEquals(retention, ObjectRetention.fromProto(retention.toProto().toBuilder()
        .setRetentionDate("2027-01-15T09:00:00.123+01:00").build()));
    assertThrows(DateTimeException.class, () -> ObjectRetention.fromProto(retention.toProto().toBuilder()
        .setRetentionDate("invalid").build()));
    assertThrows(NullPointerException.class, () -> new ObjectRetention(null, date, null));
    assertThrows(NullPointerException.class, () -> new Retention(null, null, null));
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
    return Rule.newBuilder().setTimeUnit(unit).setDuration(duration).build();
  }
}
