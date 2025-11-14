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

package org.apache.hadoop.ozone.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.UUID;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for STSTokenIdentifier.
 */
public class TestSTSTokenIdentifier {

  @Test
  public void testKindAndService() {
    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(
        "tempAccessKeyId", "originalAccessKeyId", "roleArn",
        Instant.now().plusSeconds(3600), "secretAccessKey", "sessionPolicy");

    assertEquals("STSToken", stsTokenIdentifier.getKind().toString());
    assertEquals("STS", stsTokenIdentifier.getService());
  }

  @Test
  public void testProtoBufRoundTrip() throws IOException {
    final Instant expiry = Instant.now().plusSeconds(7200);
    final STSTokenIdentifier originalTokenIdentifier = new STSTokenIdentifier(
        "tempAccess", "origAccess", "arn:aws:iam::123456789012:role/RoleY",
        expiry, "secretKey", "sessionPolicy");
    final UUID secretKeyId = UUID.randomUUID();
    originalTokenIdentifier.setSecretKeyId(secretKeyId);

    final OMTokenProto proto = originalTokenIdentifier.toProtoBuf();
    assertThat(proto.getType()).isEqualTo(OMTokenProto.Type.S3_STS_TOKEN);
    assertThat(proto.getOwner()).isEqualTo("tempAccess");
    assertThat(proto.getMaxDate()).isEqualTo(expiry.toEpochMilli());
    assertThat(proto.getOriginalAccessKeyId()).isEqualTo("origAccess");
    assertThat(proto.getRoleArn()).isEqualTo("arn:aws:iam::123456789012:role/RoleY");
    assertThat(proto.getSecretAccessKey()).isEqualTo("secretKey");
    assertThat(proto.getSessionPolicy()).isEqualTo("sessionPolicy");
    assertThat(proto.getSecretKeyId()).isEqualTo(secretKeyId.toString());

    final STSTokenIdentifier parsedTokenIdentifier = new STSTokenIdentifier(
        "other", "other", "other", Instant.now(), "other",
        "other");
    parsedTokenIdentifier.fromProtoBuf(proto);

    assertThat(parsedTokenIdentifier.getOwnerId()).isEqualTo("tempAccess");
    assertThat(parsedTokenIdentifier.getExpiry()).isEqualTo(expiry);
    assertThat(parsedTokenIdentifier.getOriginalAccessKeyId()).isEqualTo("origAccess");
    assertThat(parsedTokenIdentifier.getRoleArn()).isEqualTo("arn:aws:iam::123456789012:role/RoleY");
    assertThat(parsedTokenIdentifier.getSecretAccessKey()).isEqualTo("secretKey");
    assertThat(parsedTokenIdentifier.getSecretKeyId()).isEqualTo(secretKeyId);
    assertThat(parsedTokenIdentifier.getSessionPolicy()).isEqualTo("sessionPolicy");
    assertThat(parsedTokenIdentifier).isEqualTo(originalTokenIdentifier);
    assertThat(parsedTokenIdentifier.hashCode()).isEqualTo(originalTokenIdentifier.hashCode());
  }

  @Test
  public void testFromProtoBufInvalidSecretKeyId() {
    final OMTokenProto invalid = OMTokenProto.newBuilder()
        .setType(OMTokenProto.Type.S3_STS_TOKEN)
        .setOwner("tempAccessKeyId")
        .setMaxDate(Instant.now().toEpochMilli())
        .setSecretKeyId("not-a-uuid")
        .build();

    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(
        "tempAccessKeyId", "originalAccessKeyId", "roleArn", Instant.now(),
        "secretAccessKey", "sessionPolicy");

    final IOException ex = assertThrows(IOException.class, () -> stsTokenIdentifier.fromProtoBuf(invalid));
    assertThat(ex.getMessage()).isEqualTo("Invalid secretKeyId format in STS token: not-a-uuid");
  }

  @Test
  public void testProtobufRoundTripWithNullSessionPolicy() throws IOException {
    final Instant expiry = Instant.now().plusSeconds(7200);
    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(
        "tempAccess", "origAccess", "arn:aws:iam::123456789012:role/RoleX",
        expiry, "secretKey", null);

    final OMTokenProto proto = stsTokenIdentifier.toProtoBuf();
    assertThat(proto.getSessionPolicy()).isEmpty();

    final STSTokenIdentifier parsedTokenIdentifier = new STSTokenIdentifier(
        "other", "other", "other", Instant.now(), "other",
        "other");
    parsedTokenIdentifier.fromProtoBuf(proto);

    assertThat(parsedTokenIdentifier.getSessionPolicy()).isEmpty();
  }

  @Test
  public void testProtobufRoundTripWithEmptySessionPolicy() throws IOException {
    final Instant expiry = Instant.now().plusSeconds(4000);
    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(
        "tempAccess", "origAccess", "arn:aws:iam::123456789012:role/RoleZ",
        expiry, "secretKey", "");

    final OMTokenProto proto = stsTokenIdentifier.toProtoBuf();
    assertThat(proto.getSessionPolicy()).isEmpty();

    final STSTokenIdentifier parsedTokenIdentifier = new STSTokenIdentifier(
        "other", "other", "other", Instant.now(), "other",
        "other");
    parsedTokenIdentifier.fromProtoBuf(proto);

    assertThat(parsedTokenIdentifier.getSessionPolicy()).isEmpty();
  }

  @Test
  public void testFromProtoBufInvalidTokenType() {
    final OMTokenProto invalidType = OMTokenProto.newBuilder()
        .setType(OMTokenProto.Type.DELEGATION_TOKEN)  // Wrong type
        .setOwner("tempAccessKeyId")
        .setMaxDate(Instant.now().toEpochMilli())
        .build();

    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(
        "tempAccessKeyId", "origAccessKeyId", "roleArn", Instant.now(),
        "secretAccessKey", "sessionPolicy");

    final IllegalArgumentException ex = assertThrows(
        IllegalArgumentException.class, () -> stsTokenIdentifier.fromProtoBuf(invalidType));
    assertThat(ex.getMessage()).isEqualTo("Invalid token type for STSTokenIdentifier: DELEGATION_TOKEN");
  }

  @Test
  public void testWriteToAndReadFromByteArray() throws Exception {
    final STSTokenIdentifier originalTokenIdentifier = new STSTokenIdentifier(
        "tempAccessKeyId", "originalAccessKeyId", "roleArn",
        Instant.now().plusSeconds(1000), "secretAccessKey", "sessionPolicy");
    originalTokenIdentifier.setSecretKeyId(UUID.randomUUID());

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(baos)) {
      originalTokenIdentifier.write(out);
    }

    final byte[] bytes = baos.toByteArray();
    final STSTokenIdentifier parsedTokenIdentifier = new STSTokenIdentifier(
        "other", "other", "other", Instant.now(), "other",
        "other");
    parsedTokenIdentifier.readFromByteArray(bytes);

    assertThat(parsedTokenIdentifier).isEqualTo(originalTokenIdentifier);
  }

  @Test
  public void testWriteToAndReadFromByteArrayWithDifferentSecretKeyIds() throws Exception {
    final UUID uuid1 = UUID.randomUUID();
    UUID uuid2 = UUID.randomUUID();
    if (uuid2.equals(uuid1)) {
      uuid2 = UUID.randomUUID();
    }

    final Instant expiry = Instant.now().plusSeconds(1500);
    final STSTokenIdentifier originalTokenIdentifier = new STSTokenIdentifier(
        "tempAccessKeyId", "originalAccessKeyId", "roleArn", expiry,
        "secretAccessKey", "sessionPolicy");
    originalTokenIdentifier.setSecretKeyId(uuid1);

    final ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(baos1)) {
      originalTokenIdentifier.write(out);
    }

    final STSTokenIdentifier anotherTokenIdentifier = new STSTokenIdentifier(
        "tempAccessKeyId", "originalAccessKeyId", "roleArn", expiry,
        "secretAccessKey", "sessionPolicy");
    anotherTokenIdentifier.setSecretKeyId(uuid2);

    final ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(baos2)) {
      anotherTokenIdentifier.write(out);
    }

    // The byte arrays should be different due to different secret key IDs
    assertThat(baos1.toByteArray()).isNotEqualTo(baos2.toByteArray());
  }

  @Test
  public void testWriteToAndReadFromByteArrayWithSameSecretKeyIds() throws Exception {
    final UUID uuid = UUID.randomUUID();
    final Instant expiry = Instant.now().plusSeconds(1700);

    final STSTokenIdentifier originalTokenIdentifier = new STSTokenIdentifier(
        "tempAccessKeyId", "originalAccessKeyId", "roleArn", expiry,
        "secretAccessKey", "sessionPolicy");
    originalTokenIdentifier.setSecretKeyId(uuid);

    final ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(baos1)) {
      originalTokenIdentifier.write(out);
    }

    final STSTokenIdentifier anotherTokenIdentifier = new STSTokenIdentifier(
        "tempAccessKeyId", "originalAccessKeyId", "roleArn", expiry,
        "secretAccessKey", "sessionPolicy");
    anotherTokenIdentifier.setSecretKeyId(uuid);

    final ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(baos2)) {
      anotherTokenIdentifier.write(out);
    }

    // The byte arrays should be the same since they have the same contents
    assertThat(baos1.toByteArray()).isEqualTo(baos2.toByteArray());
  }

  @Test
  public void testGettersReturnCorrectValues() {
    final Instant expiry = Instant.now().plusSeconds(3600);
    final String tempAccessKeyId = "ASIATEMP123456";
    final String originalAccessKeyId = "AKIAORIGINAL123";
    final String roleArn = "arn:aws:iam::123456789012:role/MyRole";
    final String secretAccessKey = "mySecretKey";
    final String sessionPolicy = "myPolicy";

    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(
        tempAccessKeyId, originalAccessKeyId, roleArn, expiry, secretAccessKey, sessionPolicy);

    assertThat(stsTokenIdentifier.getOwnerId()).isEqualTo(tempAccessKeyId);
    assertThat(stsTokenIdentifier.getTempAccessKeyId()).isEqualTo(tempAccessKeyId);
    assertThat(stsTokenIdentifier.getOriginalAccessKeyId()).isEqualTo(originalAccessKeyId);
    assertThat(stsTokenIdentifier.getRoleArn()).isEqualTo(roleArn);
    assertThat(stsTokenIdentifier.getExpiry()).isEqualTo(expiry);
    assertThat(stsTokenIdentifier.getSecretAccessKey()).isEqualTo(secretAccessKey);
    assertThat(stsTokenIdentifier.getSessionPolicy()).isEqualTo(sessionPolicy);
  }

  @Test
  public void testEqualsAndHashCode() {
    final Instant expiry = Instant.now().plusSeconds(3600);
    final UUID uuid = UUID.randomUUID();

    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(
        "tempAccessKeyId", "originalAccessKeyId", "roleArn", expiry,
        "secretAccessKey", "sessionPolicy");
    stsTokenIdentifier.setSecretKeyId(uuid);

    final STSTokenIdentifier stsTokenIdentifier2 = new STSTokenIdentifier(
        "tempAccessKeyId", "originalAccessKeyId", "roleArn", expiry,
        "secretAccessKey", "sessionPolicy");
    stsTokenIdentifier2.setSecretKeyId(uuid);

    assertThat(stsTokenIdentifier).isEqualTo(stsTokenIdentifier2);
    assertThat(stsTokenIdentifier.hashCode()).isEqualTo(stsTokenIdentifier2.hashCode());
  }

  @Test
  public void testNotEqualsWhenTempAccessKeyIdDiffers() {
    final Instant expiry = Instant.now().plusSeconds(3600);

    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(
        "tempAccessKeyId1", "originalAccessKeyId", "roleArn",
        expiry, "secretAccessKey", "sessionPolicy");

    final STSTokenIdentifier stsTokenIdentifier2 = new STSTokenIdentifier(
        "tempAccessKeyId2", "originalAccessKeyId", "roleArn", expiry,
        "secretAccessKey", "sessionPolicy");

    assertThat(stsTokenIdentifier).isNotEqualTo(stsTokenIdentifier2);
  }

  @Test
  public void testNotEqualsWhenOriginalAccessKeyIdDiffers() {
    final Instant expiry = Instant.now().plusSeconds(3600);

    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(
        "tempAccessKeyId", "originalAccessKeyId1", "roleArn", expiry,
        "secretAccessKey", "sessionPolicy");

    final STSTokenIdentifier stsTokenIdentifier2 = new STSTokenIdentifier(
        "tempAccessKeyId", "originalAccessKeyId2", "roleArn", expiry,
        "secretAccessKey", "sessionPolicy");

    assertThat(stsTokenIdentifier).isNotEqualTo(stsTokenIdentifier2);
  }

  @Test
  public void testNotEqualsWhenRoleArnDiffers() {
    final Instant expiry = Instant.now().plusSeconds(3600);

    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(
        "tempAccessKeyId", "originalAccessKeyId", "roleArn1", expiry,
        "secretAccessKey", "sessionPolicy");

    final STSTokenIdentifier stsTokenIdentifier2 = new STSTokenIdentifier(
        "tempAccessKeyId", "originalAccessKeyId", "roleArn2", expiry,
        "secretAccessKey", "sessionPolicy");

    assertThat(stsTokenIdentifier).isNotEqualTo(stsTokenIdentifier2);
  }

  @Test
  public void testNotEqualsWhenExpirationDiffers() {
    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(
        "tempAccessKeyId", "originalAccessKeyId", "roleArn",
        Instant.now().plusSeconds(3600), "secretAccessKey", "sessionPolicy");

    final STSTokenIdentifier stsTokenIdentifier2 = new STSTokenIdentifier(
        "tempAccessKeyId", "originalAccessKeyId", "roleArn",
        Instant.now().plusSeconds(7600), "secretAccessKey", "sessionPolicy");

    assertThat(stsTokenIdentifier).isNotEqualTo(stsTokenIdentifier2);
  }

  @Test
  public void testNotEqualsWhenSecretAccessKeyDiffers() {
    final Instant expiry = Instant.now().plusSeconds(3600);

    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(
        "tempAccessKeyId", "originalAccessKeyId", "roleArn", expiry,
        "secretAccessKey1", "sessionPolicy");

    final STSTokenIdentifier stsTokenIdentifier2 = new STSTokenIdentifier(
        "tempAccessKeyId", "originalAccessKeyId", "roleArn", expiry,
        "secretAccessKey2", "sessionPolicy");

    assertThat(stsTokenIdentifier).isNotEqualTo(stsTokenIdentifier2);
  }

  @Test
  public void testNotEqualsWhenSessionPolicyDiffers() {
    final Instant expiry = Instant.now().plusSeconds(3600);

    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(
        "tempAccessKeyId", "originalAccessKeyId", "roleArn", expiry,
        "secretAccessKey", "sessionPolicy1");

    final STSTokenIdentifier stsTokenIdentifier2 = new STSTokenIdentifier(
        "tempAccessKeyId", "originalAccessKeyId", "roleArn", expiry,
        "secretAccessKey", "sessionPolicy2");

    assertThat(stsTokenIdentifier).isNotEqualTo(stsTokenIdentifier2);
  }

  @Test
  public void testToString() {
    final Instant expiry = Instant.now().plusSeconds(3600);
    final UUID uuid = UUID.randomUUID();

    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(
        "tempAccessKeyId", "originalAccessKeyId", "roleArn", expiry,
        "secretAccessKey", "sessionPolicy");
    stsTokenIdentifier.setSecretKeyId(uuid);

    final String stsTokenIdentifierStr = stsTokenIdentifier.toString();
    final String expectedString = "STSTokenIdentifier{" + "tempAccessKeyId='tempAccessKeyId'" +
        ", originalAccessKeyId='originalAccessKeyId'" + ", roleArn='roleArn'" + ", expiry='" + expiry +
        "', secretKeyId='" + uuid + "', sessionPolicy='sessionPolicy'" + '}';

    assertEquals(expectedString, stsTokenIdentifierStr);
  }

  @Test
  public void testNotEqualsWithNull() {
    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(
        "tempAccessKeyId", "originalAccessKeyId", "roleArn", Instant.now(),
        "secretAccessKey", "sessionPolicy");

    assertThat(stsTokenIdentifier).isNotEqualTo(null);
  }
}


