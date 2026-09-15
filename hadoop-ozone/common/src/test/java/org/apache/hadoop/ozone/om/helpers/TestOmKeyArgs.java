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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test for {@link OmKeyArgs}.
 */
class TestOmKeyArgs {

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void toBuilderPreservesHeadOp(boolean headOp) {
    OmKeyArgs subject = new OmKeyArgs.Builder()
        .setHeadOp(headOp)
        .build();

    assertEquals(headOp, subject.isHeadOp());
    assertEquals(headOp, subject.toBuilder().build().isHeadOp());
  }

  @Test
  void validateAddressedVersionRejectsNamingAVersionTwice() {
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName("vol1")
        .setBucketName("bucket1")
        .setKeyName("key1")
        .setVersionId(42L)
        .setNullVersion(true)
        .build();

    OMException ex = assertThrows(OMException.class,
        () -> OmKeyArgs.validateAddressedVersion(keyArgs));
    assertEquals(OMException.ResultCodes.INVALID_REQUEST, ex.getResult());
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void validateAddressedVersionAcceptsEitherOnItsOwn(boolean byId)
      throws Exception {
    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName("vol1")
        .setBucketName("bucket1")
        .setKeyName("key1");
    if (byId) {
      keyArgs.setVersionId(42L);
    } else {
      keyArgs.setNullVersion(true);
    }

    OmKeyArgs.validateAddressedVersion(keyArgs.build());
  }

  @Test
  void validateAddressedVersionAcceptsAddressingNoVersion() throws Exception {
    OmKeyArgs.validateAddressedVersion(KeyArgs.newBuilder()
        .setVolumeName("vol1")
        .setBucketName("bucket1")
        .setKeyName("key1")
        .build());
  }

  @Test
  void toProtobufPreservesVersionId() {
    OmKeyArgs subject = new OmKeyArgs.Builder()
        .setVolumeName("vol1")
        .setBucketName("bucket1")
        .setKeyName("key1")
        .setVersionId(42L)
        .build();

    assertTrue(subject.toProtobuf().hasVersionId());
    assertEquals(42L, subject.toProtobuf().getVersionId());
    assertFalse(subject.toProtobuf().getNullVersion());
  }

  @Test
  void toProtobufPreservesNullVersion() {
    OmKeyArgs subject = new OmKeyArgs.Builder()
        .setVolumeName("vol1")
        .setBucketName("bucket1")
        .setKeyName("key1")
        .setNullVersion(true)
        .build();

    assertTrue(subject.toProtobuf().getNullVersion());
    assertFalse(subject.toProtobuf().hasVersionId());
  }

  @Test
  void toProtobufLeavesTheVersionUnsetWhenNoneIsAddressed() {
    OmKeyArgs subject = new OmKeyArgs.Builder()
        .setVolumeName("vol1")
        .setBucketName("bucket1")
        .setKeyName("key1")
        .build();

    assertFalse(subject.toProtobuf().hasVersionId());
    assertFalse(subject.toProtobuf().getNullVersion());
  }

}
