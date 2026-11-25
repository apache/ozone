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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetCallerIdentityResponse;
import org.junit.jupiter.api.Test;

/**
 * Test CallerIdentityInfo.
 */
public class TestCallerIdentityInfo {

  private static final String ACCOUNT = "123456789012";
  private static final String ARN = "arn:aws:iam::123456789012:user/om";
  private static final String USER_ID = "om/polarisclient@root.comops.site";

  @Test
  public void testConstructor() {
    final CallerIdentityInfo identity = new CallerIdentityInfo(ACCOUNT, ARN, USER_ID);

    assertEquals(ACCOUNT, identity.getAccount());
    assertEquals(ARN, identity.getArn());
    assertEquals(USER_ID, identity.getUserId());
  }

  @Test
  public void testProtobufConversion() {
    final CallerIdentityInfo identity = new CallerIdentityInfo(ACCOUNT, ARN, USER_ID);

    final GetCallerIdentityResponse proto = identity.getProtobuf();

    assertNotNull(proto);
    assertEquals(ACCOUNT, proto.getAccount());
    assertEquals(ARN, proto.getArn());
    assertEquals(USER_ID, proto.getUserId());
  }

  @Test
  public void testFromProtobuf() {
    final GetCallerIdentityResponse proto = GetCallerIdentityResponse.newBuilder()
        .setAccount(ACCOUNT)
        .setArn(ARN)
        .setUserId(USER_ID)
        .build();

    final CallerIdentityInfo identity = CallerIdentityInfo.fromProtobuf(proto);

    assertEquals(ACCOUNT, identity.getAccount());
    assertEquals(ARN, identity.getArn());
    assertEquals(USER_ID, identity.getUserId());
  }

  @Test
  public void testProtobufRoundTrip() {
    final CallerIdentityInfo original = new CallerIdentityInfo(ACCOUNT, ARN, USER_ID);

    final CallerIdentityInfo recovered = CallerIdentityInfo.fromProtobuf(original.getProtobuf());

    assertEquals(original, recovered);
  }

  @Test
  public void testEqualsAndHashCodeWithIdenticalObjects() {
    final CallerIdentityInfo identity1 = new CallerIdentityInfo(ACCOUNT, ARN, USER_ID);
    final CallerIdentityInfo identity2 = new CallerIdentityInfo(ACCOUNT, ARN, USER_ID);

    assertEquals(identity1, identity2);
    assertEquals(identity1.hashCode(), identity2.hashCode());
  }

  @Test
  public void testNotEqualsWithDifferentArn() {
    final CallerIdentityInfo identity1 = new CallerIdentityInfo(ACCOUNT, ARN, USER_ID);
    final CallerIdentityInfo identity2 = new CallerIdentityInfo(
        ACCOUNT, "arn:aws:iam::123456789012:user/other", USER_ID);

    assertNotEquals(identity1, identity2);
    assertNotEquals(identity1.hashCode(), identity2.hashCode());
  }

  @Test
  public void testToString() {
    final CallerIdentityInfo identity = new CallerIdentityInfo(ACCOUNT, ARN, USER_ID);

    assertEquals(
        "CallerIdentityInfo{account='123456789012', arn='" + ARN + "', userId='" + USER_ID + "'}", identity.toString());
  }
}
