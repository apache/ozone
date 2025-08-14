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
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.getFutureDateString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleAction;
import org.junit.jupiter.api.Test;

/**
 * Test OmLCExpiration.
 */
class TestOmLCExpiration {

  @Test
  public void testCreateValidOmLCExpiration() {
    OmLCExpiration.Builder exp1 = new OmLCExpiration.Builder()
        .setDays(30);
    assertDoesNotThrow(() -> exp1.build().valid());

    OmLCExpiration.Builder exp2 = new OmLCExpiration.Builder()
        .setDate("2099-10-10T00:00:00Z");
    assertDoesNotThrow(() -> exp2.build().valid());

    OmLCExpiration.Builder exp3 = new OmLCExpiration.Builder()
        .setDays(1);
    assertDoesNotThrow(() -> exp3.build().valid());

    OmLCExpiration.Builder exp4 = new OmLCExpiration.Builder()
        .setDate("2099-12-31T00:00:00Z");
    assertDoesNotThrow(() -> exp4.build().valid());

    OmLCExpiration.Builder exp5 = new OmLCExpiration.Builder()
        .setDate("2099-02-15T00:00:00.000Z");
    assertDoesNotThrow(() -> exp5.build().valid());

    OmLCExpiration.Builder exp6 = new OmLCExpiration.Builder()
        .setDate("2042-04-02T00:00:00Z");
    assertDoesNotThrow(() -> exp6.build().valid());

    OmLCExpiration.Builder exp7 = new OmLCExpiration.Builder()
        .setDate("2042-04-02T00:00:00+00:00");
    assertDoesNotThrow(() -> exp7.build().valid());

    OmLCExpiration.Builder exp8 = new OmLCExpiration.Builder()
        .setDate("2099-12-31T00:00:00+00:00");
    assertDoesNotThrow(() -> exp8.build().valid());

    OmLCExpiration.Builder exp9 = new OmLCExpiration.Builder()
        .setDate("2099-12-31T23:00:00-01:00");
    assertDoesNotThrow(() -> exp9.build().valid());

    OmLCExpiration.Builder exp10 = new OmLCExpiration.Builder()
        .setDate("2100-01-01T01:00:00+01:00");
    assertDoesNotThrow(() -> exp10.build().valid());

    OmLCExpiration.Builder exp11 = new OmLCExpiration.Builder()
        .setDate("2099-12-31T12:00:00-12:00");
    assertDoesNotThrow(() -> exp11.build().valid());

    OmLCExpiration.Builder exp12 = new OmLCExpiration.Builder()
        .setDate("2100-01-01T12:00:00+12:00");
    assertDoesNotThrow(() -> exp12.build().valid());
  }

  @Test
  public void testCreateInValidOmLCExpiration() {
    OmLCExpiration.Builder exp1 = new OmLCExpiration.Builder()
        .setDays(30)
        .setDate(getFutureDateString(100));
    assertOMException(() -> exp1.build().valid(), INVALID_REQUEST,
        "Either 'days' or 'date' should be specified, but not both or neither.");

    OmLCExpiration.Builder exp2 = new OmLCExpiration.Builder()
        .setDays(-1);
    assertOMException(() -> exp2.build().valid(), INVALID_REQUEST,
        "'Days' for Expiration action must be a positive integer");

    OmLCExpiration.Builder exp3 = new OmLCExpiration.Builder()
        .setDate(null);
    assertOMException(() -> exp3.build().valid(), INVALID_REQUEST,
        "Either 'days' or 'date' should be specified, but not both or neither.");

    OmLCExpiration.Builder exp4 = new OmLCExpiration.Builder()
        .setDate("");
    assertOMException(() -> exp4.build().valid(), INVALID_REQUEST,
        "Either 'days' or 'date' should be specified, but not both or neither.");

    OmLCExpiration.Builder exp5 = new OmLCExpiration.Builder();
    assertOMException(() -> exp5.build().valid(), INVALID_REQUEST,
        "Either 'days' or 'date' should be specified, but not both or neither.");

    OmLCExpiration.Builder exp6 = new OmLCExpiration.Builder()
        .setDate("10-10-2099");
    assertOMException(() -> exp6.build().valid(), INVALID_REQUEST,
        "'Date' must be in ISO 8601 format");

    OmLCExpiration.Builder exp7 = new OmLCExpiration.Builder()
        .setDate("2099-12-31T00:00:00");
    assertOMException(() -> exp7.build().valid(), INVALID_REQUEST,
        "'Date' must be in ISO 8601 format");

    // Testing for date in the past
    OmLCExpiration.Builder exp8 = new OmLCExpiration.Builder()
        .setDate(getFutureDateString(-1))
        .setCreateDate(ZonedDateTime.now(ZoneOffset.UTC));
    assertOMException(() -> exp8.build().valid(), INVALID_REQUEST,
        "'Date' must be in the future");

    OmLCExpiration.Builder exp9 = new OmLCExpiration.Builder()
        .setDays(0);
    assertOMException(() -> exp9.build().valid(), INVALID_REQUEST,
        "'Days' for Expiration action must be a positive integer");

    // 1 minute ago
    OmLCExpiration.Builder exp10 = new OmLCExpiration.Builder()
        .setDate(getFutureDateString(0, 0, -1))
        .setCreateDate(ZonedDateTime.now(ZoneOffset.UTC));
    assertOMException(() -> exp10.build().valid(), INVALID_REQUEST,
        "'Date' must be in the future");
  }

  @Test
  public void testDateMustBeAtMidnightUTC() {
    // Acceptable date - midnight UTC
    OmLCExpiration.Builder validExp = new OmLCExpiration.Builder()
        .setDate("2099-10-10T00:00:00Z");
    assertDoesNotThrow(() -> validExp.build().valid());

    // Non-midnight UTC dates should be rejected
    OmLCExpiration.Builder exp1 = new OmLCExpiration.Builder()
        .setDate("2099-10-10T10:00:00Z");
    assertOMException(() -> exp1.build().valid(), INVALID_REQUEST, "'Date' must represent midnight UTC");

    OmLCExpiration.Builder exp2 = new OmLCExpiration.Builder()
        .setDate("2099-10-10T00:30:00Z");
    assertOMException(() -> exp2.build().valid(), INVALID_REQUEST, "'Date' must represent midnight UTC");

    OmLCExpiration.Builder exp3 = new OmLCExpiration.Builder()
        .setDate("2099-10-10T00:00:30Z");
    assertOMException(() -> exp3.build().valid(), INVALID_REQUEST, "'Date' must represent midnight UTC");

    OmLCExpiration.Builder exp4 = new OmLCExpiration.Builder()
        .setDate("2099-10-10T00:00:00.123Z");
    assertOMException(() -> exp4.build().valid(), INVALID_REQUEST, "'Date' must represent midnight UTC");

    // Non-UTC timezone should be rejected
    OmLCExpiration.Builder exp5 = new OmLCExpiration.Builder()
        .setDate("2099-10-10T00:00:00+01:00");
    assertOMException(() -> exp5.build().valid(), INVALID_REQUEST, "'Date' must represent midnight UTC");
  }

  @Test
  public void testProtobufConversion() throws OMException {
    // Only Days
    OmLCExpiration expDays = new OmLCExpiration.Builder()
        .setDays(30)
        .build();
    LifecycleAction protoFromDays = expDays.getProtobuf();
    OmLCExpiration expFromProto = OmLCExpiration.getFromProtobuf(
        protoFromDays.getExpiration());
    assertEquals(30, expFromProto.getDays());
    assertNull(expFromProto.getDate());

    // Only Date
    String dateStr = "2099-10-10T00:00:00Z";
    OmLCExpiration expDate = new OmLCExpiration.Builder()
        .setDate(dateStr)
        .build();
    LifecycleAction protoFromDate = expDate.getProtobuf();
    OmLCExpiration expFromProto2 = OmLCExpiration.getFromProtobuf(
        protoFromDate.getExpiration());
    assertNull(expFromProto2.getDays());
    assertEquals(dateStr, expFromProto2.getDate());
  }
}
