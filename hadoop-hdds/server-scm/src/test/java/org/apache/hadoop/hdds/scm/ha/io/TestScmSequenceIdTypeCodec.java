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

package org.apache.hadoop.hdds.scm.ha.io;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.Map;
import org.apache.hadoop.hdds.scm.ha.SequenceIdType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.junit.jupiter.api.Test;

/**
 * Testing serialization of SequenceIdType objects for SCM HA Ratis payloads.
 */
public class TestScmSequenceIdTypeCodec {

  private final LegacyStringSequenceIdCodecForTesting legacyCodec
      = new LegacyStringSequenceIdCodecForTesting();
  private final ScmSequenceIdTypeCodec newCodec
      = new ScmSequenceIdTypeCodec();

  @Test
  public void testCodecIsRegistered() throws Exception {
    assertTrue(ScmCodecFactory.getInstance().getCodec(SequenceIdType.class)
            instanceof ScmSequenceIdTypeCodec,
        "Codec was not registered with ScmCodecFactory!");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testPersistingExplicitBytes() throws Exception {

    Map<SequenceIdType, byte[]> testCases = new EnumMap<>(SequenceIdType.class);

    testCases.put(SequenceIdType.LOCAL_ID,
        new byte[] {'L', 'O', 'C', 'A', 'L', '_', 'I', 'D'});

    testCases.put(SequenceIdType.DEL_TXN_ID,
        new byte[] {'D', 'E', 'L', '_', 'T', 'X', 'N', '_', 'I', 'D'});

    testCases.put(SequenceIdType.CONTAINER_ID,
        new byte[] {'C', 'O', 'N', 'T', 'A', 'I', 'N', 'E', 'R', '_', 'I', 'D'});

    testCases.put(SequenceIdType.CERTIFICATE_ID,
        new byte[] {'C', 'E', 'R', 'T', 'I', 'F', 'I', 'C', 'A', 'T', 'E', '_', 'I', 'D'});

    testCases.put(SequenceIdType.ROOT_CERTIFICATE_ID,
        new byte[] {'R', 'O', 'O', 'T', '_', 'C', 'E', 'R', 'T', 'I', 'F', 'I', 'C', 'A', 'T', 'E', '_', 'I', 'D'});

    for (Map.Entry<SequenceIdType, byte[]> entry : testCases.entrySet()) {
      checkPersisting(entry.getKey(), entry.getValue());
    }
  }

  @Test
  public void testConvertAndReadBackAllConstants() throws Exception {
    for (SequenceIdType type : SequenceIdType.values()) {
      assertSequenceIdType(type);
    }
  }

  @Test
  public void testDeserializeInvalidStringThrowsException() {
    String invalidData = "NON_EXISTENT_ID";
    ByteString invalidByteString = UnsafeByteOperations.unsafeWrap(
        invalidData.getBytes(StandardCharsets.UTF_8));

    InvalidProtocolBufferException exception = assertThrows(
        InvalidProtocolBufferException.class,
        () -> newCodec.deserialize(invalidByteString)
    );

    assertTrue(exception.getMessage().contains("Unknown SequenceIdType: " + invalidData));
  }

  private void assertSequenceIdType(SequenceIdType type) throws Exception {
    final ByteString expectedBytes = legacyCodec.serialize(type);
    final ByteString computedBytes = newCodec.serialize(type);

    // Ensure the new enum codec exactly matches the legacy string codec on the wire
    assertArrayEquals(expectedBytes.toByteArray(), computedBytes.toByteArray());

    // Ensure both codecs can read each other's data perfectly
    assertEquals(type, legacyCodec.deserialize(expectedBytes));
    assertEquals(type, newCodec.deserialize(expectedBytes));
    assertEquals(type, legacyCodec.deserialize(computedBytes));
    assertEquals(type, newCodec.deserialize(computedBytes));
  }

  private void checkPersisting(SequenceIdType type, byte[] expected) throws Exception {
    // 1. Verify legacy string behavior matches the expected raw byte array
    final ByteString encodedByLegacy = legacyCodec.serialize(type);
    assertArrayEquals(expected, encodedByLegacy.toByteArray());

    // 2. Verify new enum behavior also matches the exact same byte array
    final ByteString encodedByNew = newCodec.serialize(type);
    assertArrayEquals(expected, encodedByNew.toByteArray());
  }
}
