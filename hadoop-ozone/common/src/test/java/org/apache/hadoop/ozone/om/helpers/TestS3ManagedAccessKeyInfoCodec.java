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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3ManagedAccessKeyInfoProto;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link S3ManagedAccessKeyInfo}.
 */
public class TestS3ManagedAccessKeyInfoCodec {

  @Test
  public void testCodecRoundTripCoversAllFields() throws Exception {
    S3ManagedAccessKeyInfo original = createInfo(true);

    Codec<S3ManagedAccessKeyInfo> codec = S3ManagedAccessKeyInfo.getCodec();
    S3ManagedAccessKeyInfo decoded =
        codec.fromPersistedFormat(codec.toPersistedFormat(original));

    assertEquals(original, decoded);
    assertEquals(original.hashCode(), decoded.hashCode());
    assertEquals(Arrays.asList("dev", "ops", "audit"), decoded.getGroups());
    assertSame(original, codec.copyObject(original));
  }

  @Test
  public void testProtobufConversionCoversAllFields() {
    S3ManagedAccessKeyInfo original = createInfo(true);

    S3ManagedAccessKeyInfoProto proto = original.getProtobuf();

    assertEquals("access-key-1", proto.getAccessKeyId());
    assertEquals(ByteString.copyFromUtf8("ciphertext"), proto.getEncryptedSecretKey());
    assertEquals("secret-key-id-1", proto.getSecretKeyId());
    assertEquals("alice", proto.getEffectiveUser());
    assertEquals(Arrays.asList("dev", "ops", "audit"), proto.getGroupsList());
    assertEquals("local build key", proto.getDescription());
    assertEquals(123456789L, proto.getCreatedAt());
    assertEquals(123456999L, proto.getExpiresAt());
    assertTrue(proto.getDisabled());
    assertEquals("om-admin", proto.getCreatedBy());
    assertEquals("{\"Statement\":[{\"Action\":\"s3:GetObject\"}]}",
        proto.getPolicyDocument());
    assertEquals(original, S3ManagedAccessKeyInfo.fromProtobuf(proto));
  }

  @Test
  public void testDisabledFalseAndAbsentProtoDefault() {
    S3ManagedAccessKeyInfo enabled = createInfo(false);

    assertFalse(enabled.isDisabled());
    assertFalse(S3ManagedAccessKeyInfo.fromProtobuf(
        enabled.getProtobuf()).isDisabled());

    S3ManagedAccessKeyInfo fromAbsent =
        S3ManagedAccessKeyInfo.fromProtobuf(
            S3ManagedAccessKeyInfoProto.newBuilder().build());
    assertFalse(fromAbsent.isDisabled());
    assertFalse(fromAbsent.getProtobuf().getDisabled());
  }

  @Test
  public void testGroupsAreImmutableAndOrdered() {
    List<String> groups = new ArrayList<>(Arrays.asList("first", "second"));
    S3ManagedAccessKeyInfo info = S3ManagedAccessKeyInfo.newBuilder()
        .setGroups(groups)
        .build();
    groups.add("third");

    assertEquals(Arrays.asList("first", "second"), info.getGroups());
    assertThrows(UnsupportedOperationException.class,
        () -> info.getGroups().add("third"));
  }

  @Test
  public void testToBuilderDoesNotMutateOriginal() {
    S3ManagedAccessKeyInfo original = createInfo(false);

    S3ManagedAccessKeyInfo modified = original.toBuilder()
        .setDescription("rotated key")
        .addGroup("security")
        .setDisabled(true)
        .build();

    assertFalse(original.isDisabled());
    assertEquals("local build key", original.getDescription());
    assertEquals(Arrays.asList("dev", "ops", "audit"), original.getGroups());
    assertTrue(modified.isDisabled());
    assertEquals("rotated key", modified.getDescription());
    assertEquals(Arrays.asList("dev", "ops", "audit", "security"),
        modified.getGroups());
  }

  @Test
  public void testToStringRedactsSensitiveFields() {
    S3ManagedAccessKeyInfo info = createInfo(true);

    String text = info.toString();

    assertThat(text).contains("accessKeyId='access-key-1'");
    assertThat(text).contains("encryptedSecretKey=<redacted>");
    assertThat(text).contains("policyDocument=<redacted>");
    assertThat(text).doesNotContain("ciphertext");
    assertThat(text).doesNotContain("{\"Statement\"");
  }

  private static S3ManagedAccessKeyInfo createInfo(boolean disabled) {
    return S3ManagedAccessKeyInfo.newBuilder()
        .setAccessKeyId("access-key-1")
        .setEncryptedSecretKey(ByteString.copyFromUtf8("ciphertext"))
        .setSecretKeyId("secret-key-id-1")
        .setEffectiveUser("alice")
        .setGroups(Arrays.asList("dev", "ops", "audit"))
        .setDescription("local build key")
        .setCreatedAt(123456789L)
        .setExpiresAt(123456999L)
        .setDisabled(disabled)
        .setCreatedBy("om-admin")
        .setPolicyDocument("{\"Statement\":[{\"Action\":\"s3:GetObject\"}]}")
        .build();
  }
}
