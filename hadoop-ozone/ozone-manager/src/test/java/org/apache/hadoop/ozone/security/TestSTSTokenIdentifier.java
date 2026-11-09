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
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for STSTokenIdentifier.
 */
public class TestSTSTokenIdentifier {

  private static final byte[] SECRET_KEY_BYTES = new byte[5];
  private static final ManagedSecretKey MANAGED_SECRET_KEY;
  private static final Instant CREATION_TIME = Instant.ofEpochMilli(1_700_000_000_000L);

  static {
    ThreadLocalRandom.current().nextBytes(SECRET_KEY_BYTES);
    MANAGED_SECRET_KEY = createManagedSecretKey(SECRET_KEY_BYTES);
  }

  @Test
  public void testKindAndService() {
    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(Instant.now().plusSeconds(3600))
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .build());

    assertEquals("STSToken", stsTokenIdentifier.getKind().toString());
    assertEquals("STS", stsTokenIdentifier.getService());
  }

  @Test
  public void testProtoBufRoundTrip() throws IOException {
    // STSTokenIdentifier persists expiry with millisecond precision (via toEpochMilli),
    // so use a millisecond-precision Instant to avoid nanos-only differences across
    // platforms/JDKs during round-trips.
    final Instant expiry = Instant.now().plusSeconds(7200).truncatedTo(ChronoUnit.MILLIS);
    final STSTokenIdentifier originalTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccess")
        .setOriginalAccessKeyId("origAccess")
        .setRoleArn("arn:aws:iam::123456789012:role/RoleY")
        .setExpiry(expiry)
        .setSecretAccessKey("secretKey")
        .setSessionPolicy("sessionPolicy")
        .setManagedSecretKey(MANAGED_SECRET_KEY)
        .setAssumedRoleId("AROATEST123456789:testsess")
        .setAssumedRoleUserArn("arn:aws:sts::123456789012:assumed-role/RoleY/testsess")
        .build());
    final UUID secretKeyId = MANAGED_SECRET_KEY.getId();

    final OMTokenProto proto = originalTokenIdentifier.toProtoBuf();
    assertThat(proto.getType()).isEqualTo(OMTokenProto.Type.S3_STS_TOKEN);
    assertThat(proto.getOwner()).isEqualTo("tempAccess");
    assertThat(proto.getMaxDate()).isEqualTo(expiry.toEpochMilli());
    assertThat(proto.getIssueDate()).isEqualTo(CREATION_TIME.toEpochMilli());
    assertThat(proto.getOriginalAccessKeyId()).isEqualTo("origAccess");
    assertThat(proto.getRoleArn()).isEqualTo("arn:aws:iam::123456789012:role/RoleY");
    assertThat(proto.getSecretAccessKey()).isNotEqualTo("secretKey");   // must be encrypted
    assertThat(proto.getSessionPolicy()).isEqualTo("sessionPolicy");
    assertThat(proto.getAssumedRoleId()).isEqualTo("AROATEST123456789:testsess");
    assertThat(proto.getAssumedRoleUserArn())
        .isEqualTo("arn:aws:sts::123456789012:assumed-role/RoleY/testsess");
    assertThat(proto.getSecretKeyId()).isEqualTo(secretKeyId.toString());

    final STSTokenIdentifier parsedTokenIdentifier = new STSTokenIdentifier();
    parsedTokenIdentifier.setManagedSecretKey(MANAGED_SECRET_KEY);
    parsedTokenIdentifier.fromProtoBuf(proto);

    assertThat(parsedTokenIdentifier.getOwnerId()).isEqualTo("tempAccess");
    assertThat(parsedTokenIdentifier.getExpiry()).isEqualTo(expiry);
    assertThat(parsedTokenIdentifier.getCreationTime()).isEqualTo(CREATION_TIME);
    assertThat(parsedTokenIdentifier.getOriginalAccessKeyId()).isEqualTo("origAccess");
    assertThat(parsedTokenIdentifier.getRoleArn()).isEqualTo("arn:aws:iam::123456789012:role/RoleY");
    assertThat(parsedTokenIdentifier.getSecretAccessKey()).isEqualTo("secretKey");
    assertThat(parsedTokenIdentifier.getSecretKeyId()).isEqualTo(secretKeyId);
    assertThat(parsedTokenIdentifier.getSessionPolicy()).isEqualTo("sessionPolicy");
    assertThat(parsedTokenIdentifier.getAssumedRoleId()).isEqualTo("AROATEST123456789:testsess");
    assertThat(parsedTokenIdentifier.getAssumedRoleUserArn())
        .isEqualTo("arn:aws:sts::123456789012:assumed-role/RoleY/testsess");
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

    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(Instant.now())
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .build());

    final IOException ex = assertThrows(IOException.class, () -> stsTokenIdentifier.fromProtoBuf(invalid));
    assertThat(ex.getMessage()).isEqualTo("Invalid secretKeyId format in STS token: not-a-uuid");
  }

  @Test
  public void testProtobufRoundTripWithNullSessionPolicy() throws IOException {
    final Instant expiry = Instant.now().plusSeconds(7200);
    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccess")
        .setOriginalAccessKeyId("origAccess")
        .setRoleArn("arn:aws:iam::123456789012:role/RoleX")
        .setExpiry(expiry)
        .setSecretAccessKey("secretKey")
        .setManagedSecretKey(MANAGED_SECRET_KEY)
        .build());

    final OMTokenProto proto = stsTokenIdentifier.toProtoBuf();
    assertThat(proto.getSessionPolicy()).isEmpty();

    final STSTokenIdentifier parsedTokenIdentifier = new STSTokenIdentifier();
    parsedTokenIdentifier.setManagedSecretKey(MANAGED_SECRET_KEY);
    parsedTokenIdentifier.fromProtoBuf(proto);

    assertThat(parsedTokenIdentifier.getSessionPolicy()).isEmpty();
  }

  @Test
  public void testProtobufRoundTripWithEmptySessionPolicy() throws IOException {
    final Instant expiry = Instant.now().plusSeconds(4000);
    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccess")
        .setOriginalAccessKeyId("origAccess")
        .setRoleArn("arn:aws:iam::123456789012:role/RoleZ")
        .setExpiry(expiry)
        .setSecretAccessKey("secretKey")
        .setSessionPolicy("")
        .setManagedSecretKey(MANAGED_SECRET_KEY)
        .build());

    final OMTokenProto proto = stsTokenIdentifier.toProtoBuf();
    assertThat(proto.getSessionPolicy()).isEmpty();

    final STSTokenIdentifier parsedTokenIdentifier = new STSTokenIdentifier();
    parsedTokenIdentifier.setManagedSecretKey(MANAGED_SECRET_KEY);
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

    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("origAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(Instant.now())
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .build());

    final IllegalArgumentException ex = assertThrows(
        IllegalArgumentException.class, () -> stsTokenIdentifier.fromProtoBuf(invalidType));
    assertThat(ex.getMessage()).isEqualTo("Invalid token type for STSTokenIdentifier: DELEGATION_TOKEN");
  }

  @Test
  public void testWriteToAndReadFromByteArray() throws Exception {
    // Use millisecond-precision Instant so that the value survives the
    // toEpochMilli() / Instant.ofEpochMilli() round-trip without losing precision
    // compared to the original object, which is compared using equals().
    final Instant expiry =
        Instant.now().plusSeconds(1000).truncatedTo(ChronoUnit.MILLIS);
    final STSTokenIdentifier originalTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(expiry)
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .setManagedSecretKey(MANAGED_SECRET_KEY)
        .setAssumedRoleId("AROATEST123456789:testsess")
        .setAssumedRoleUserArn("arn:aws:sts::123456789012:assumed-role/test-role/testsess")
        .build());

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(baos)) {
      originalTokenIdentifier.write(out);
    }

    final byte[] bytes = baos.toByteArray();
    final STSTokenIdentifier parsedTokenIdentifier = new STSTokenIdentifier();
    parsedTokenIdentifier.setManagedSecretKey(MANAGED_SECRET_KEY);
    parsedTokenIdentifier.readFromByteArray(bytes);

    assertThat(parsedTokenIdentifier).isEqualTo(originalTokenIdentifier);
  }

  @Test
  public void testWriteToAndReadFromByteArrayWithDifferentSecretKeys() throws Exception {
    final Instant expiry = Instant.now().plusSeconds(1500);
    final STSTokenIdentifier originalTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(expiry)
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .setManagedSecretKey(MANAGED_SECRET_KEY)
        .build());

    final ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(baos1)) {
      originalTokenIdentifier.write(out);
    }

    byte[] rawBytes = new byte[5];
    ManagedSecretKey managedSecretKey2 = createManagedSecretKey(rawBytes);
    final STSTokenIdentifier anotherTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(expiry)
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .setManagedSecretKey(managedSecretKey2)
        .build());

    final ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(baos2)) {
      anotherTokenIdentifier.write(out);
    }

    // The byte arrays will not be the same because the encrypted secretAccessKey cipher for each will differ.
    // However, the STSTokenIdentifier derived from each byte array should also not be the same.
    assertThat(baos1.toByteArray()).isNotEqualTo(baos2.toByteArray());
    final byte[] byteArr1 = baos1.toByteArray();
    final byte[] byteArr2 = baos2.toByteArray();
    assertThat(byteArr1).isNotEqualTo(byteArr2);
    final STSTokenIdentifier tokenFromByteArr1 = new STSTokenIdentifier();
    tokenFromByteArr1.setManagedSecretKey(MANAGED_SECRET_KEY);
    tokenFromByteArr1.readFromByteArray(byteArr1);
    final STSTokenIdentifier tokenFromByteArr2 = new STSTokenIdentifier();
    tokenFromByteArr2.setManagedSecretKey(managedSecretKey2);
    tokenFromByteArr2.readFromByteArray(byteArr2);
    assertThat(tokenFromByteArr1).isNotEqualTo(tokenFromByteArr2);
  }

  @Test
  public void testWriteToAndReadFromByteArrayWithSameSecretKeyIds() throws Exception {
    final Instant expiry = Instant.now().plusSeconds(1700);

    final STSTokenIdentifier originalTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(expiry)
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .setManagedSecretKey(MANAGED_SECRET_KEY)
        .build());

    final ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(baos1)) {
      originalTokenIdentifier.write(out);
    }

    final STSTokenIdentifier anotherTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(expiry)
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .setManagedSecretKey(MANAGED_SECRET_KEY)
        .build());

    final ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(baos2)) {
      anotherTokenIdentifier.write(out);
    }

    // The byte arrays should not be the same because the encrypted secretAccessKey cipher for each will differ.
    // However, the STSTokenIdentifier derived from each byte array should be the same.
    final byte[] byteArr1 = baos1.toByteArray();
    final byte[] byteArr2 = baos2.toByteArray();
    assertThat(byteArr1).isNotEqualTo(byteArr2);
    final STSTokenIdentifier tokenFromByteArr1 = new STSTokenIdentifier();
    tokenFromByteArr1.setManagedSecretKey(MANAGED_SECRET_KEY);
    tokenFromByteArr1.readFromByteArray(byteArr1);
    final STSTokenIdentifier tokenFromByteArr2 = new STSTokenIdentifier();
    tokenFromByteArr2.setManagedSecretKey(MANAGED_SECRET_KEY);
    tokenFromByteArr2.readFromByteArray(byteArr2);
    assertThat(tokenFromByteArr1).isEqualTo(tokenFromByteArr2);
  }

  @Test
  public void testGettersReturnCorrectValues() {
    final Instant expiry = Instant.now().plusSeconds(3600);
    final String tempAccessKeyId = "ASIATEMP123456";
    final String originalAccessKeyId = "AKIAORIGINAL123";
    final String roleArn = "arn:aws:iam::123456789012:role/MyRole";
    final String secretAccessKey = "mySecretKey";
    final String sessionPolicy = "myPolicy";

    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId(tempAccessKeyId)
        .setOriginalAccessKeyId(originalAccessKeyId)
        .setRoleArn(roleArn)
        .setExpiry(expiry)
        .setSecretAccessKey(secretAccessKey)
        .setSessionPolicy(sessionPolicy)
        .build());

    assertThat(stsTokenIdentifier.getOwnerId()).isEqualTo(tempAccessKeyId);
    assertThat(stsTokenIdentifier.getTempAccessKeyId()).isEqualTo(tempAccessKeyId);
    assertThat(stsTokenIdentifier.getOriginalAccessKeyId()).isEqualTo(originalAccessKeyId);
    assertThat(stsTokenIdentifier.getRoleArn()).isEqualTo(roleArn);
    assertThat(stsTokenIdentifier.getCreationTime()).isEqualTo(CREATION_TIME);
    assertThat(stsTokenIdentifier.getExpiry()).isEqualTo(expiry);
    assertThat(stsTokenIdentifier.getSecretAccessKey()).isEqualTo(secretAccessKey);
    assertThat(stsTokenIdentifier.getSessionPolicy()).isEqualTo(sessionPolicy);
  }

  @Test
  public void testEqualsAndHashCode() {
    final Instant expiry = Instant.now().plusSeconds(3600);
    final UUID uuid = UUID.randomUUID();

    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(expiry)
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .build());
    stsTokenIdentifier.setSecretKeyId(uuid);

    final STSTokenIdentifier stsTokenIdentifier2 = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(expiry)
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .build());
    stsTokenIdentifier2.setSecretKeyId(uuid);

    assertThat(stsTokenIdentifier).isEqualTo(stsTokenIdentifier2);
    assertThat(stsTokenIdentifier.hashCode()).isEqualTo(stsTokenIdentifier2.hashCode());
  }

  @Test
  public void testNotEqualsWhenTempAccessKeyIdDiffers() {
    final Instant expiry = Instant.now().plusSeconds(3600);

    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId1")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(expiry)
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .build());

    final STSTokenIdentifier stsTokenIdentifier2 = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId2")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(expiry)
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .build());

    assertThat(stsTokenIdentifier).isNotEqualTo(stsTokenIdentifier2);
  }

  @Test
  public void testNotEqualsWhenOriginalAccessKeyIdDiffers() {
    final Instant expiry = Instant.now().plusSeconds(3600);

    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId1")
        .setRoleArn("roleArn")
        .setExpiry(expiry)
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .build());

    final STSTokenIdentifier stsTokenIdentifier2 = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId2")
        .setRoleArn("roleArn")
        .setExpiry(expiry)
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .build());

    assertThat(stsTokenIdentifier).isNotEqualTo(stsTokenIdentifier2);
  }

  @Test
  public void testNotEqualsWhenRoleArnDiffers() {
    final Instant expiry = Instant.now().plusSeconds(3600);

    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn1")
        .setExpiry(expiry)
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .build());

    final STSTokenIdentifier stsTokenIdentifier2 = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn2")
        .setExpiry(expiry)
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .build());

    assertThat(stsTokenIdentifier).isNotEqualTo(stsTokenIdentifier2);
  }

  @Test
  public void testNotEqualsWhenExpirationDiffers() {
    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(Instant.now().plusSeconds(3600))
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .build());

    final STSTokenIdentifier stsTokenIdentifier2 = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(Instant.now().plusSeconds(7600))
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .build());

    assertThat(stsTokenIdentifier).isNotEqualTo(stsTokenIdentifier2);
  }

  @Test
  public void testNotEqualsWhenSecretAccessKeyDiffers() {
    final Instant expiry = Instant.now().plusSeconds(3600);

    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(expiry)
        .setSecretAccessKey("secretAccessKey1")
        .setSessionPolicy("sessionPolicy")
        .build());

    final STSTokenIdentifier stsTokenIdentifier2 = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(expiry)
        .setSecretAccessKey("secretAccessKey2")
        .setSessionPolicy("sessionPolicy")
        .build());

    assertThat(stsTokenIdentifier).isNotEqualTo(stsTokenIdentifier2);
  }

  @Test
  public void testNotEqualsWhenSessionPolicyDiffers() {
    final Instant expiry = Instant.now().plusSeconds(3600);

    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(expiry)
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy1")
        .build());

    final STSTokenIdentifier stsTokenIdentifier2 = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(expiry)
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy2")
        .build());

    assertThat(stsTokenIdentifier).isNotEqualTo(stsTokenIdentifier2);
  }

  @Test
  public void testToString() {
    final Instant expiry = Instant.now().plusSeconds(3600);
    final UUID uuid = UUID.randomUUID();

    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(expiry)
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .setAssumedRoleId("AROATEST123456789:testsess")
        .setAssumedRoleUserArn("arn:aws:sts::123456789012:assumed-role/test-role/testsess")
        .build());
    stsTokenIdentifier.setSecretKeyId(uuid);

    final String stsTokenIdentifierStr = stsTokenIdentifier.toString();
    final String expectedString = "STSTokenIdentifier{" + "tempAccessKeyId='tempAccessKeyId'" +
        ", originalAccessKeyId='originalAccessKeyId'" + ", roleArn='roleArn'" +
        ", assumedRoleId='AROATEST123456789:testsess'" +
        ", assumedRoleUserArn='arn:aws:sts::123456789012:assumed-role/test-role/testsess'" +
        ", creationTime='" + CREATION_TIME + "', expiry='" + expiry +
        "', secretKeyId='" + uuid + "', sessionPolicy='sessionPolicy'" + '}';

    assertEquals(expectedString, stsTokenIdentifierStr);
  }

  @Test
  public void testNotEqualsWithNull() {
    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(Instant.now())
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .build());

    assertThat(stsTokenIdentifier).isNotEqualTo(null);
  }

  @Test
  public void testEqualsWithDifferentManagedSecretKeys() {
    final Instant expiry = Instant.now().plusSeconds(3600).truncatedTo(ChronoUnit.MILLIS);
    final UUID uuid = UUID.randomUUID();

    // Create first identifier with the default key
    final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(expiry)
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .build());
    stsTokenIdentifier.setSecretKeyId(uuid);

    // Create second identifier with a different ManagedSecretKey but otherwise same parameters
    byte[] differentKeyBytes = new byte[5];
    ThreadLocalRandom.current().nextBytes(differentKeyBytes);

    final STSTokenIdentifier stsTokenIdentifier2 = new STSTokenIdentifier(paramsBuilder()
        .setTempAccessKeyId("tempAccessKeyId")
        .setOriginalAccessKeyId("originalAccessKeyId")
        .setRoleArn("roleArn")
        .setExpiry(expiry)
        .setSecretAccessKey("secretAccessKey")
        .setSessionPolicy("sessionPolicy")
        .setManagedSecretKey(createManagedSecretKey(differentKeyBytes))
        .build());
    stsTokenIdentifier2.setSecretKeyId(uuid);

    // They should still be equal because managedSecretKey is transient/ignored for identity
    assertThat(stsTokenIdentifier).isEqualTo(stsTokenIdentifier2);
    assertThat(stsTokenIdentifier.hashCode()).isEqualTo(stsTokenIdentifier2.hashCode());
  }

  private static ManagedSecretKey createManagedSecretKey(byte[] keyBytes) {
    return new ManagedSecretKey(
        UUID.randomUUID(),
        CREATION_TIME,
        CREATION_TIME.plus(Duration.ofDays(1)),
        new SecretKeySpec(keyBytes, "HmacSHA256"));
  }

  private static STSTokenIdentifier.Params.Builder paramsBuilder() {
    return STSTokenIdentifier.Params.newBuilder()
        .setCreationTime(CREATION_TIME)
        .setManagedSecretKey(MANAGED_SECRET_KEY);
  }
}
