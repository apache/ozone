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

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleAction;
import org.junit.jupiter.api.Test;

/**
 * Test OmLCExpiration.
 */
class TestOmLCExpiration {

  @Test
  public void testCreateValidOmLCExpiration() {
    OmLCExpiration exp1 = new OmLCExpiration.Builder()
        .setDays(30)
        .build();
    assertDoesNotThrow(exp1::valid);

    OmLCExpiration exp2 = new OmLCExpiration.Builder()
        .setDate("2099-10-10T00:00:00Z")
        .build();
    assertDoesNotThrow(exp2::valid);

    OmLCExpiration exp3 = new OmLCExpiration.Builder()
        .setDays(1)
        .build();
    assertDoesNotThrow(exp3::valid);

    OmLCExpiration exp4 = new OmLCExpiration.Builder()
        .setDate("2099-12-31T00:00:00Z")
        .build();
    assertDoesNotThrow(exp4::valid);

    OmLCExpiration exp5 = new OmLCExpiration.Builder()
        .setDate("2099-02-15T00:00:00.000Z")
        .build();
    assertDoesNotThrow(exp5::valid);
  }

  @Test
  public void testCreateInValidOmLCExpiration() {
    OmLCExpiration exp1 = new OmLCExpiration.Builder()
        .setDays(30)
        .setDate(getFutureDateString(100))
        .build();
    assertOMException(exp1::valid, INVALID_REQUEST,
        "Either 'days' or 'date' should be specified, but not both or neither.");

    OmLCExpiration exp2 = new OmLCExpiration.Builder()
        .setDays(-1)
        .build();
    assertOMException(exp2::valid, INVALID_REQUEST,
        "'Days' for Expiration action must be a positive integer");

    OmLCExpiration exp3 = new OmLCExpiration.Builder()
        .setDate(null)
        .build();
    assertOMException(exp3::valid, INVALID_REQUEST,
        "Either 'days' or 'date' should be specified, but not both or neither.");

    OmLCExpiration exp4 = new OmLCExpiration.Builder()
        .setDate("")
        .build();
    assertOMException(exp4::valid, INVALID_REQUEST,
        "Either 'days' or 'date' should be specified, but not both or neither.");

    OmLCExpiration exp5 = new OmLCExpiration.Builder()
        .build();
    assertOMException(exp5::valid, INVALID_REQUEST,
        "Either 'days' or 'date' should be specified, but not both or neither.");

    OmLCExpiration exp6 = new OmLCExpiration.Builder()
        .setDate("10-10-2025")
        .build();
    assertOMException(exp6::valid, INVALID_REQUEST,
        "'Date' must be in ISO 8601 format");

    // Testing for date in the past
    OmLCExpiration exp7 = new OmLCExpiration.Builder()
        .setDate(getFutureDateString(-1))
        .build();
    assertOMException(exp7::valid, INVALID_REQUEST,
        "'Date' must be in the future");

    OmLCExpiration exp8 = new OmLCExpiration.Builder()
        .setDays(0)
        .build();
    assertOMException(exp8::valid, INVALID_REQUEST,
        "Either 'days' or 'date' should be specified, but not both or neither.");
  }

  @Test
  public void testDateMustBeAtMidnightUTC() {
    // Acceptable date - midnight UTC
    OmLCExpiration validExp = new OmLCExpiration.Builder()
        .setDate("2099-10-10T00:00:00Z")
        .build();
    assertDoesNotThrow(validExp::valid);

    // Non-midnight UTC dates should be rejected
    OmLCExpiration exp1 = new OmLCExpiration.Builder()
        .setDate("2099-10-10T10:00:00Z")
        .build();
    assertOMException(exp1::valid, INVALID_REQUEST, "'Date' must be at midnight GMT");

    OmLCExpiration exp2 = new OmLCExpiration.Builder()
        .setDate("2099-10-10T00:30:00Z")
        .build();
    assertOMException(exp2::valid, INVALID_REQUEST, "'Date' must be at midnight GMT");

    OmLCExpiration exp3 = new OmLCExpiration.Builder()
        .setDate("2099-10-10T00:00:30Z")
        .build();
    assertOMException(exp3::valid, INVALID_REQUEST, "'Date' must be at midnight GMT");

    OmLCExpiration exp4 = new OmLCExpiration.Builder()
        .setDate("2099-10-10T00:00:00.123Z")
        .build();
    assertOMException(exp4::valid, INVALID_REQUEST, "'Date' must be at midnight GMT");

    // Non-UTC timezone should be rejected
    OmLCExpiration exp5 = new OmLCExpiration.Builder()
        .setDate("2099-10-10T00:00:00+01:00")
        .build();
    assertOMException(exp5::valid, INVALID_REQUEST, "'Date' must be at midnight GMT");

    OmLCExpiration exp8 = new OmLCExpiration.Builder()
        .setDate("2099-10-10T00:00:00")
        .build();
    assertOMException(exp8::valid, INVALID_REQUEST, "ISO 8601 format");
  }

  @Test
  public void testProtobufConversion() {
    // Only Days
    OmLCExpiration expDays = new OmLCExpiration.Builder()
        .setDays(30)
        .build();
    LifecycleAction protoFromDays = expDays.getProtobuf();
    OmLCExpiration expFromProto = OmLCExpiration.getFromProtobuf(
        protoFromDays.getExpiration());
    assertEquals(30, expFromProto.getDays());
    assertEquals("", expFromProto.getDate());

    // Only Date
    String dateStr = "2099-10-10T00:00:00Z";
    OmLCExpiration expDate = new OmLCExpiration.Builder()
        .setDate(dateStr)
        .build();
    LifecycleAction protoFromDate = expDate.getProtobuf();
    OmLCExpiration expFromProto2 = OmLCExpiration.getFromProtobuf(
        protoFromDate.getExpiration());
    assertEquals(0, expFromProto2.getDays());
    assertEquals(dateStr, expFromProto2.getDate());
  }
}
