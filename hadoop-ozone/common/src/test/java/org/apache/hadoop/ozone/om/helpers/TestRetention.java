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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import java.io.IOException;
import java.time.Instant;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfoProtoLight;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RetentionMode;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RetentionTimeUnit;
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
    assertEquals(24, retention.getNumber());
    assertEquals("KeyRetention", retention.getMessageType().getName());
    assertEquals(FieldDescriptor.Type.UINT64, retention.getMessageType().findFieldByName("retentionDate").getType());
    assertEquals(25, key.findFieldByName("legalHold").getNumber());
    assertEquals(key.findFieldByName("retention").getNumber(),
        KeyInfoProtoLight.getDescriptor().findFieldByName("retention").getNumber());
    assertNotNull(OzoneManagerProtocolProtos.BucketRetention.getDescriptor().findFieldByName("retentionMode"));
    assertEquals(2, OzoneManagerProtocolProtos.BucketRetention.getDescriptor().findFieldByName("duration").getNumber());
    assertEquals(3, OzoneManagerProtocolProtos.BucketRetention.getDescriptor().findFieldByName("defaultEventHold")
        .getNumber());
    assertEquals(1, OzoneManagerProtocolProtos.RetentionDuration.getDescriptor()
        .findFieldByName("timeUnit").getNumber());
    assertEquals(2, OzoneManagerProtocolProtos.RetentionDuration.getDescriptor()
        .findFieldByName("duration").getNumber());
  }

  @Test
  void preservesOptionalRules() {
    RetentionDuration rule = rule(RetentionTimeUnit.DAYS, 30);
    for (OzoneManagerProtocolProtos.BucketRetention proto : new OzoneManagerProtocolProtos.BucketRetention[]{
        OzoneManagerProtocolProtos.BucketRetention.newBuilder().setRetentionMode(RetentionMode.GOVERNANCE).build(),
        OzoneManagerProtocolProtos.BucketRetention.newBuilder().setRetentionMode(RetentionMode.GOVERNANCE)
            .setDuration(rule.toProto()).build(),
        OzoneManagerProtocolProtos.BucketRetention.newBuilder().setRetentionMode(RetentionMode.COMPLIANCE)
            .setDefaultEventHold(rule.toProto()).build(),
        OzoneManagerProtocolProtos.BucketRetention.newBuilder().setRetentionMode(RetentionMode.GOVERNANCE)
            .setDuration(rule.toProto())
            .setDefaultEventHold(rule.toProto()).build()}) {
      BucketRetention retention = BucketRetention.fromProto(proto);
      assertEquals(proto, retention.toProto());
      assertEquals(retention, BucketRetention.fromProto(retention.toProto()));
      assertEquals(retention.hashCode(), BucketRetention.fromProto(retention.toProto()).hashCode());
    }
    assertFalse(new BucketRetention(RetentionMode.GOVERNANCE, null, null).toProto().hasDuration());
  }

  @Test
  void defaultsKeyEventHoldToEnabled() throws IOException {
    OzoneManagerProtocolProtos.EventHold hold = OzoneManagerProtocolProtos.EventHold.newBuilder()
        .setDuration(rule(RetentionTimeUnit.DAYS, 30).toProto()).build();
    OzoneManagerProtocolProtos.EventHold decoded = OzoneManagerProtocolProtos.EventHold.parseFrom(hold.toByteArray());
    assertFalse(decoded.hasEnabled());
    assertTrue(decoded.getEnabled());
    ObjectRetention retention = new ObjectRetention(RetentionMode.GOVERNANCE, 0, EventHold.fromProto(decoded));
    assertTrue(ObjectRetention.fromProto(retention.toProto()).getEventHold().getEnabled());
    assertEquals(decoded, retention.toProto().getEventHold());
    assertFalse(retention.toProto().getEventHold().hasEnabled());
    EventHold released = new EventHold(false, rule(RetentionTimeUnit.DAYS, 30));
    assertFalse(ObjectRetention.fromProto(new ObjectRetention(RetentionMode.GOVERNANCE, 0, released).toProto())
        .getEventHold().getEnabled());
    assertTrue(OzoneManagerProtocolProtos.EventHold.getDescriptor().findFieldByName("enabled").isOptional());
    assertEquals("RetentionDuration", OzoneManagerProtocolProtos.BucketRetention.getDescriptor()
        .findFieldByName("defaultEventHold").getMessageType().getName());
    assertNull(OzoneManagerProtocolProtos.BucketRetention.getDescriptor().findFieldByName("eventHold"));
  }

  @Test
  void roundTripsEventHoldEnabled() {
    RetentionDuration duration = rule(RetentionTimeUnit.DAYS, 30);
    for (boolean enabled : new boolean[]{false, true}) {
      EventHold hold = new EventHold(enabled, duration);
      OzoneManagerProtocolProtos.EventHold proto = hold.toProto();
      assertEquals(!enabled, proto.hasEnabled());
      assertEquals(enabled, proto.getEnabled());
      assertEquals(hold, EventHold.fromProto(proto));
      assertEquals(hold.hashCode(), EventHold.fromProto(proto).hashCode());
      assertEquals(proto, EventHold.fromProto(proto).toProto());
    }
    assertEquals(new EventHold(true, duration), EventHold.fromProto(
        new EventHold(true, duration).toProto().toBuilder().setEnabled(true).build()));
    assertNotEquals(new EventHold(true, duration), new EventHold(false, duration));
    assertNotEquals(new EventHold(true, duration), new EventHold(true, rule(RetentionTimeUnit.DAYS, 31)));
    assertThrows(IllegalArgumentException.class,
        () -> EventHold.fromProto(OzoneManagerProtocolProtos.EventHold.getDefaultInstance()));
  }

  @Test
  void preservesObjectRetentionAndUtcDates() {
    long date = Instant.parse("2027-01-15T08:00:00.123Z").toEpochMilli();
    EventHold hold = new EventHold(true, rule(RetentionTimeUnit.YEARS, 1));
    for (ObjectRetention retention : new ObjectRetention[]{
        new ObjectRetention(RetentionMode.GOVERNANCE, date, null),
        new ObjectRetention(RetentionMode.COMPLIANCE, 0, hold),
        new ObjectRetention(RetentionMode.COMPLIANCE, date, hold)}) {
      assertEquals(retention, ObjectRetention.fromProto(retention.toProto()));
      assertEquals(retention.hashCode(), ObjectRetention.fromProto(retention.toProto()).hashCode());
      assertEquals(retention.getRetentionDate() > 0, retention.toProto().hasRetentionDate());
    }
    ObjectRetention retention = new ObjectRetention(RetentionMode.GOVERNANCE, date, null);
    assertEquals(date, retention.toProto().getRetentionDate());
    ObjectRetention unset = ObjectRetention.fromProto(retention.toProto().toBuilder().setRetentionDate(0).build());
    assertEquals(0, unset.getRetentionDate());
    assertFalse(unset.toProto().hasRetentionDate());
    assertThrows(NullPointerException.class, () -> new ObjectRetention(null, date, null));
    assertThrows(NullPointerException.class, () -> new BucketRetention(null, null, null));
  }

  @Test
  void comparesEachBucketRetentionField() {
    RetentionDuration days = rule(RetentionTimeUnit.DAYS, 30);
    RetentionDuration hold = days;
    BucketRetention retention = new BucketRetention(RetentionMode.GOVERNANCE, days, hold);
    assertEquals(retention, new BucketRetention(RetentionMode.GOVERNANCE, days, hold));
    assertNotEquals(retention, new BucketRetention(RetentionMode.COMPLIANCE, days, hold));
    assertNotEquals(retention, new BucketRetention(RetentionMode.GOVERNANCE, rule(RetentionTimeUnit.DAYS, 31), hold));
    assertNotEquals(retention, new BucketRetention(RetentionMode.GOVERNANCE, days, null));
  }

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
      EventHold hold = new EventHold(true, invalid);
      BucketRetention retention = new BucketRetention(RetentionMode.GOVERNANCE, null, invalid);
      assertEquals(retention, BucketRetention.fromProto(retention.toProto()));
      ObjectRetention objectRetention = new ObjectRetention(RetentionMode.GOVERNANCE, 0, hold);
      assertEquals(objectRetention, ObjectRetention.fromProto(objectRetention.toProto()));
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
  void rejectsInvalidRulesAndOverflow() {
    assertThrows(IllegalArgumentException.class, () -> RetentionUtil.calculateRetainUntilDate(null, 0));
    assertThrows(IllegalArgumentException.class,
        () -> RetentionDuration.fromProto(OzoneManagerProtocolProtos.RetentionDuration.getDefaultInstance()));
    assertThrows(IllegalArgumentException.class, () -> RetentionUtil.validateBucketRetention(
        new BucketRetention(RetentionMode.GOVERNANCE, rule(RetentionTimeUnit.DAYS, 0), null)));
    assertThrows(IllegalArgumentException.class, () -> RetentionUtil.validateBucketRetention(
        new BucketRetention(RetentionMode.GOVERNANCE, rule(RetentionTimeUnit.DAYS, -1), null)));
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
