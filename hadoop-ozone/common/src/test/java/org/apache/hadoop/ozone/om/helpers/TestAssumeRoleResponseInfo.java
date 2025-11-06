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

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssumeRoleResponse;
import org.junit.jupiter.api.Test;

/**
 * Test AssumeRoleResponseInfo.
 */
public class TestAssumeRoleResponseInfo {

  private static final String ACCESS_KEY_ID = "ASIA7O1AJD8VV4KCEAX5";
  private static final String SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
  private static final String SESSION_TOKEN = "jgIDCAMaI2lkYnJva2VyL2lkYnJva2VyY2xpZW50QEVYQU1QTEUuQ09" +
      "NOLDJ8bClM2IjaWRicm9rZXIvaWRicm9rZXJjbGllbnRARVhBTVBMRS5DT02CASRjMGM5YTk2NS00YTU1LTRmMjQtYTUxMi0" +
      "3MGQ3M2JiMzg0ZDSKARRBU0lBN08xQUpEOFdWNEtDRUFYNZIBKGFybjphd3M6aWFtOjoxMjM0NTY3ODkwMTI6cm9sZS9mbS1" +
      "kd3JvbGWaAXAvdmNOSjNqRW5zc3UyWklKYWxJbGRXZSswV1VmYkRvSmwxdXV1eDBPVExQalVzZ0VqOVE5T0FZVUZTd2JtUGo" +
      "zZHNhaXpjMytacEJiVXJDNWRSV1FOTE4xcWJsVkhSdEZiZFBPTXp4NU5YY1pXdz09ogHQAVt7InJvbGVOYW1lIjoiZm0tZHd" +
      "yb2xlIiwiZ3JhbnRzIjpbeyJvYmplY3RzIjpbImtleTogL3Mzdi9idWNrZXQxLyoiXSwicGVybWlzc2lvbnMiOlsicmVhZCJ" +
      "dfSx7Im9iamVjdHMiOlsidm9sdW1lOiAvczN2Il0sInBlcm1pc3Npb25zIjpbInJlYWQiXX0seyJvYmplY3RzIjpbImJ1Y2t" +
      "ldDogL3Mzdi9idWNrZXQxIl0sInBlcm1pc3Npb25zIjpbInJlYWQiXX1dfV0gCil_LhVjpP4hfMez4L5wNZDeqEubSeBfEow" +
      "VoRnSQ-wIU1RTVG9rZW4DU1RT";
  private static final long EXPIRATION_EPOCH_SECONDS = 1577836800L;
  private static final String ASSUMED_ROLE_ID = "arn:aws:iam::123456789012:role/MyRole";

  @Test
  public void testConstructor() {
    final AssumeRoleResponseInfo response = new AssumeRoleResponseInfo(
        ACCESS_KEY_ID,
        SECRET_ACCESS_KEY,
        SESSION_TOKEN,
        EXPIRATION_EPOCH_SECONDS,
        ASSUMED_ROLE_ID
    );

    assertEquals(ACCESS_KEY_ID, response.getAccessKeyId());
    assertEquals(SECRET_ACCESS_KEY, response.getSecretAccessKey());
    assertEquals(SESSION_TOKEN, response.getSessionToken());
    assertEquals(EXPIRATION_EPOCH_SECONDS, response.getExpirationEpochSeconds());
    assertEquals(ASSUMED_ROLE_ID, response.getAssumedRoleId());
  }

  @Test
  public void testProtobufConversion() {
    final AssumeRoleResponseInfo response = new AssumeRoleResponseInfo(
        ACCESS_KEY_ID,
        SECRET_ACCESS_KEY,
        SESSION_TOKEN,
        EXPIRATION_EPOCH_SECONDS,
        ASSUMED_ROLE_ID
    );

    final AssumeRoleResponse proto = response.getProtobuf();

    assertNotNull(proto);
    assertEquals(ACCESS_KEY_ID, proto.getAccessKeyId());
    assertEquals(SECRET_ACCESS_KEY, proto.getSecretAccessKey());
    assertEquals(SESSION_TOKEN, proto.getSessionToken());
    assertEquals(EXPIRATION_EPOCH_SECONDS, proto.getExpirationEpochSeconds());
    assertEquals(ASSUMED_ROLE_ID, proto.getAssumedRoleId());
  }

  @Test
  public void testFromProtobuf() {
    final AssumeRoleResponse proto = AssumeRoleResponse.newBuilder()
        .setAccessKeyId(ACCESS_KEY_ID)
        .setSecretAccessKey(SECRET_ACCESS_KEY)
        .setSessionToken(SESSION_TOKEN)
        .setExpirationEpochSeconds(EXPIRATION_EPOCH_SECONDS)
        .setAssumedRoleId(ASSUMED_ROLE_ID)
        .build();

    final AssumeRoleResponseInfo response = AssumeRoleResponseInfo.fromProtobuf(proto);

    assertEquals(ACCESS_KEY_ID, response.getAccessKeyId());
    assertEquals(SECRET_ACCESS_KEY, response.getSecretAccessKey());
    assertEquals(SESSION_TOKEN, response.getSessionToken());
    assertEquals(EXPIRATION_EPOCH_SECONDS, response.getExpirationEpochSeconds());
    assertEquals(ASSUMED_ROLE_ID, response.getAssumedRoleId());
  }

  @Test
  public void testProtobufRoundTrip() {
    final AssumeRoleResponseInfo originalResponse = new AssumeRoleResponseInfo(
        ACCESS_KEY_ID,
        SECRET_ACCESS_KEY,
        SESSION_TOKEN,
        EXPIRATION_EPOCH_SECONDS,
        ASSUMED_ROLE_ID
    );

    final AssumeRoleResponse proto = originalResponse.getProtobuf();
    final AssumeRoleResponseInfo recoveredResponse = AssumeRoleResponseInfo.fromProtobuf(proto);

    assertEquals(originalResponse, recoveredResponse);
  }

  @Test
  public void testEqualsAndHashCodeWithIdenticalObjects() {
    final AssumeRoleResponseInfo response1 = new AssumeRoleResponseInfo(
        ACCESS_KEY_ID,
        SECRET_ACCESS_KEY,
        SESSION_TOKEN,
        EXPIRATION_EPOCH_SECONDS,
        ASSUMED_ROLE_ID
    );

    final AssumeRoleResponseInfo response2 = new AssumeRoleResponseInfo(
        ACCESS_KEY_ID,
        SECRET_ACCESS_KEY,
        SESSION_TOKEN,
        EXPIRATION_EPOCH_SECONDS,
        ASSUMED_ROLE_ID
    );

    assertEquals(response1, response2);
    assertEquals(response1.hashCode(), response2.hashCode());
  }

  @Test
  public void testNotEqualsAndHashCodeWithDifferentAccessKeyId() {
    final AssumeRoleResponseInfo response1 = new AssumeRoleResponseInfo(
        ACCESS_KEY_ID,
        SECRET_ACCESS_KEY,
        SESSION_TOKEN,
        EXPIRATION_EPOCH_SECONDS,
        ASSUMED_ROLE_ID
    );

    final AssumeRoleResponseInfo response2 = new AssumeRoleResponseInfo(
        "DIFFERENT_KEY_ID",
        SECRET_ACCESS_KEY,
        SESSION_TOKEN,
        EXPIRATION_EPOCH_SECONDS,
        ASSUMED_ROLE_ID
    );

    assertNotEquals(response1, response2);
    assertNotEquals(response1.hashCode(), response2.hashCode());
  }

  @Test
  public void testNotEqualsAndHashCodeWithDifferentSecretAccessKey() {
    final AssumeRoleResponseInfo response1 = new AssumeRoleResponseInfo(
        ACCESS_KEY_ID,
        SECRET_ACCESS_KEY,
        SESSION_TOKEN,
        EXPIRATION_EPOCH_SECONDS,
        ASSUMED_ROLE_ID
    );

    final AssumeRoleResponseInfo response2 = new AssumeRoleResponseInfo(
        ACCESS_KEY_ID,
        "DIFFERENT_SECRET_KEY",
        SESSION_TOKEN,
        EXPIRATION_EPOCH_SECONDS,
        ASSUMED_ROLE_ID
    );

    assertNotEquals(response1, response2);
    assertNotEquals(response1.hashCode(), response2.hashCode());
  }

  @Test
  public void testNotEqualsAndHashCodeWithDifferentSessionToken() {
    final AssumeRoleResponseInfo response1 = new AssumeRoleResponseInfo(
        ACCESS_KEY_ID,
        SECRET_ACCESS_KEY,
        SESSION_TOKEN,
        EXPIRATION_EPOCH_SECONDS,
        ASSUMED_ROLE_ID
    );

    final AssumeRoleResponseInfo response2 = new AssumeRoleResponseInfo(
        ACCESS_KEY_ID,
        SECRET_ACCESS_KEY,
        "DIFFERENT_TOKEN",
        EXPIRATION_EPOCH_SECONDS,
        ASSUMED_ROLE_ID
    );

    assertNotEquals(response1, response2);
    assertNotEquals(response1.hashCode(), response2.hashCode());
  }

  @Test
  public void testNotEqualsAndHashCodeWithDifferentExpirationEpochSeconds() {
    final AssumeRoleResponseInfo response1 = new AssumeRoleResponseInfo(
        ACCESS_KEY_ID,
        SECRET_ACCESS_KEY,
        SESSION_TOKEN,
        EXPIRATION_EPOCH_SECONDS,
        ASSUMED_ROLE_ID
    );

    final AssumeRoleResponseInfo response2 = new AssumeRoleResponseInfo(
        ACCESS_KEY_ID,
        SECRET_ACCESS_KEY,
        SESSION_TOKEN,
        9999999999L,
        ASSUMED_ROLE_ID
    );

    assertNotEquals(response1, response2);
    assertNotEquals(response1.hashCode(), response2.hashCode());
  }

  @Test
  public void testNotEqualsAndHashCodeWithDifferentAssumedRoleId() {
    final AssumeRoleResponseInfo response1 = new AssumeRoleResponseInfo(
        ACCESS_KEY_ID,
        SECRET_ACCESS_KEY,
        SESSION_TOKEN,
        EXPIRATION_EPOCH_SECONDS,
        ASSUMED_ROLE_ID
    );

    final AssumeRoleResponseInfo response2 = new AssumeRoleResponseInfo(
        ACCESS_KEY_ID,
        SECRET_ACCESS_KEY,
        SESSION_TOKEN,
        EXPIRATION_EPOCH_SECONDS,
        "DIFFERENT_ROLE_ID"
    );

    assertNotEquals(response1, response2);
    assertNotEquals(response1.hashCode(), response2.hashCode());
  }

  @Test
  public void testNotEqualsWithNull() {
    final AssumeRoleResponseInfo response = new AssumeRoleResponseInfo(
        ACCESS_KEY_ID,
        SECRET_ACCESS_KEY,
        SESSION_TOKEN,
        EXPIRATION_EPOCH_SECONDS,
        ASSUMED_ROLE_ID
    );

    assertNotEquals(null, response);
  }

  @Test
  public void testToString() {
    final AssumeRoleResponseInfo response = new AssumeRoleResponseInfo(
        ACCESS_KEY_ID,
        SECRET_ACCESS_KEY,
        SESSION_TOKEN,
        EXPIRATION_EPOCH_SECONDS,
        ASSUMED_ROLE_ID
    );

    final String toString = response.toString();
    final String expectedString = "AssumeRoleResponseInfo{" +
        "accessKeyId='" + ACCESS_KEY_ID + '\'' +
        ", secretAccessKey='" + SECRET_ACCESS_KEY + '\'' +
        ", sessionToken='" + SESSION_TOKEN + '\'' +
        ", expirationEpochSeconds=" + EXPIRATION_EPOCH_SECONDS +
        ", assumedRoleId='" + ASSUMED_ROLE_ID + '\'' +
        '}';

    assertNotNull(toString);
    assertEquals(expectedString, toString);
  }
}

