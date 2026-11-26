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

package org.apache.hadoop.hdds.scm.ha;

import static org.apache.hadoop.hdds.scm.ha.SCMRatisProtocolCompatibilityTestUtil.proto2Request;
import static org.apache.hadoop.hdds.scm.ha.SCMRatisProtocolCompatibilityTestUtil.randomProto2SCMRatisResponseProto;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol;
import org.apache.hadoop.hdds.protocol.proto.testing.Proto2SCMRatisProtocolForTesting;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.junit.jupiter.api.Test;

/**
 * Tests proto2 to proto3 compatibility for SCMRatisProtocol.
 */
public class TestSCMRatisProtocolCompatibility {
  static final Random RANDOM = new Random();
  static final Class<?>[] TYPES = {String.class, Integer.class, byte[].class};

  static <T> ByteString randomValueProto3(Class<T> clazz) {
    if (clazz == String.class) {
      final int length = RANDOM.nextInt(3);
      final StringBuilder builder = new StringBuilder(length);
      for (int i = 0; i < length; i++) {
        builder.append(RANDOM.nextInt(10));
      }
      final String string = builder.toString();
      assertEquals(length, string.length());
      return ByteString.copyFromUtf8(string);
    } else if (clazz == Integer.class) {
      final ByteBuffer buffer = ByteBuffer.allocate(4);
      buffer.putInt(RANDOM.nextInt());
      return ByteString.copyFrom(buffer.array());
    } else if (clazz == byte[].class) {
      final byte[] bytes = new byte[RANDOM.nextInt(3)];
      RANDOM.nextBytes(bytes);
      return ByteString.copyFrom(bytes);
    }
    throw new IllegalArgumentException("Unrecognized class " + clazz);
  }

  static SCMRatisProtocol.MethodArgument randomProto3MethodArgument() {
    final Class<?> type = TYPES[RANDOM.nextInt(TYPES.length)];
    return SCMRatisProtocol.MethodArgument.newBuilder()
            .setType(type.getName())
            .setValue(randomValueProto3(type))
            .build();
  }

  static SCMRatisProtocol.SCMRatisResponseProto randomProto3SCMRatisResponseProto() {
    final Class<?> type = TYPES[RANDOM.nextInt(TYPES.length)];
    return SCMRatisProtocol.SCMRatisResponseProto.newBuilder()
            .setType(type.getName())
            .setValue(randomValueProto3(type))
            .build();
  }

  static SCMRatisProtocol.SCMRatisRequestProto proto3Request(
          String name, SCMRatisProtocol.RequestType type, int numArgs) {
    final SCMRatisProtocol.Method.Builder b = SCMRatisProtocol.Method.newBuilder()
            .setName(name);
    for (int i = 0; i < numArgs; i++) {
      b.addArgs(randomProto3MethodArgument());
    }

    return SCMRatisProtocol.SCMRatisRequestProto.newBuilder()
            .setType(type)
            .setMethod(b)
            .build();
  }

  /**
   * Verifies that messages encoded with the legacy proto2 schema
   * can be parsed by the current proto3 schema.
   */
  @Test
  public void testProto2RequestCanBeParsedByProto3() throws Exception {
    for (Proto2SCMRatisProtocolForTesting.RequestType type : Proto2SCMRatisProtocolForTesting.RequestType.values()) {
      for (int numArgs = 0; numArgs < 3; numArgs++) {
        final String name = type + "_" + numArgs;
        runTestProto2RequestCanBeParsedByProto3(name, type, numArgs);
      }
    }
  }

  static void runTestProto2RequestCanBeParsedByProto3(
      String name, Proto2SCMRatisProtocolForTesting.RequestType type, int numArgs) throws Exception {
    // Build request using proto2 (test-only schema)
    final Proto2SCMRatisProtocolForTesting.SCMRatisRequestProto proto2 = proto2Request(name, type, numArgs);
    assertEquals(type, proto2.getType());
    assertEquals(name, proto2.getMethod().getName());
    assertEquals(numArgs, proto2.getMethod().getArgsCount());

    // Parse using proto3
    final SCMRatisProtocol.SCMRatisRequestProto proto3 = SCMRatisProtocol.SCMRatisRequestProto.parseFrom(
        proto2.toByteArray());

    // Presence + value checks (proto3 optional fields)
    assertTrue(proto3.hasType(), "proto3 should see type presence from proto2 bytes");
    assertTrue(proto3.hasMethod(), "proto3 should see method presence from proto2 bytes");
    assertEquals(SCMRatisProtocol.RequestType.valueOf(type.name()), proto3.getType());
    assertEquals(name, proto3.getMethod().getName());
    assertEquals(numArgs, proto3.getMethod().getArgsCount());
    for (int i = 0; i < numArgs; i++) {
      assertMethodArgument(proto2.getMethod().getArgs(i), proto3.getMethod().getArgs(i));
    }

    assertEquals(proto2.toString(), proto3.toString());
    assertShortDebugString(proto2, proto3);
    assertEquals(proto2, Proto2SCMRatisProtocolForTesting.SCMRatisRequestProto.parseFrom(proto3.toByteArray()));
  }

  private static void assertByteStringEquals(com.google.protobuf.ByteString proto2, ByteString proto3) {
    assertEquals(UnsafeByteOperations.unsafeWrap(proto2.asReadOnlyByteBuffer()), proto3);
  }

  static void assertMethodArgument(Proto2SCMRatisProtocolForTesting.MethodArgument proto2,
      SCMRatisProtocol.MethodArgument proto3) {
    assertEquals(proto2.getType(), proto3.getType());
    assertByteStringEquals(proto2.getValue(), proto3.getValue());
  }

  static void assertSCMRatisResponseProto(Proto2SCMRatisProtocolForTesting.SCMRatisResponseProto proto2,
      SCMRatisProtocol.SCMRatisResponseProto proto3) {
    assertEquals(proto2.getType(), proto3.getType());
    assertByteStringEquals(proto2.getValue(), proto3.getValue());
  }

  /**
   * Verifies that responses encoded with the legacy proto2 schema
   * can be parsed by the current proto3 schema.
   */
  @Test
  public void testProto2ResponseCanBeParsedByProto3() throws Exception {
    for (int i = 0; i < 10; i++) {
      runTestProto2ResponseCanBeParsedByProto3();
    }
  }

  static void runTestProto2ResponseCanBeParsedByProto3() throws Exception {
    // Build response using proto2
    final Proto2SCMRatisProtocolForTesting.SCMRatisResponseProto proto2 = randomProto2SCMRatisResponseProto();

    // Parse using proto3 (production schema)
    final SCMRatisProtocol.SCMRatisResponseProto proto3 = SCMRatisProtocol.SCMRatisResponseProto.parseFrom(
        proto2.toByteArray());

    assertTrue(proto3.hasType(), "proto3 should see type presence from proto2 bytes");
    assertTrue(proto3.hasValue(), "proto3 should see value presence from proto2 bytes");
    assertSCMRatisResponseProto(proto2, proto3);

    assertEquals(proto2.toString(), proto3.toString());
    assertShortDebugString(proto2, proto3);
    assertEquals(proto2, Proto2SCMRatisProtocolForTesting.SCMRatisResponseProto.parseFrom(proto3.toByteArray()));
  }

  @Test
  public void testRequestType() {
    for (Proto2SCMRatisProtocolForTesting.RequestType proto2 : Proto2SCMRatisProtocolForTesting.RequestType.values()) {
      final SCMRatisProtocol.RequestType proto3 = SCMRatisProtocol.RequestType.valueOf(proto2.name());
      assertEquals(proto3.getNumber(), proto2.getNumber());
      assertEquals(proto3.toString(), proto2.toString());
    }
  }

  /**
   * Verifies that messages encoded with the current proto3 schema
   * can be parsed by the legacy proto2 schema.
   */
  @Test
  public void testProto3RequestCanBeParsedByProto2() throws Exception {
    for (SCMRatisProtocol.RequestType type : SCMRatisProtocol.RequestType.values()) {
      // Skip default (0) and UNRECOGNIZED values: proto3 may omit default enums
      // on the wire, and UNRECOGNIZED cannot be serialized; proto2 requires `type`.
      if (type == SCMRatisProtocol.RequestType.UNRECOGNIZED
              || type == SCMRatisProtocol.RequestType.REQUEST_TYPE_UNSPECIFIED) {
        continue;
      }
      for (int numArgs = 0; numArgs < 3; numArgs++) {
        final String name = type + "_" + numArgs;
        runTestProto3RequestCanBeParsedByProto2(name, type, numArgs);
      }
    }
  }

  static void runTestProto3RequestCanBeParsedByProto2(
          String name, SCMRatisProtocol.RequestType type, int numArgs) throws Exception {

    final SCMRatisProtocol.SCMRatisRequestProto proto3 = proto3Request(name, type, numArgs);
    assertEquals(type, proto3.getType());
    assertEquals(name, proto3.getMethod().getName());
    assertEquals(numArgs, proto3.getMethod().getArgsCount());
    // Parse using proto2 (legacy test schema)
    final Proto2SCMRatisProtocolForTesting.SCMRatisRequestProto proto2 =
            Proto2SCMRatisProtocolForTesting.SCMRatisRequestProto.parseFrom(proto3.toByteArray());

    assertEquals(Proto2SCMRatisProtocolForTesting.RequestType.valueOf(type.name()), proto2.getType());
    assertTrue(proto2.hasMethod());
    assertEquals(name, proto2.getMethod().getName());
    assertEquals(numArgs, proto2.getMethod().getArgsCount());

    for (int i = 0; i < numArgs; i++) {
      assertEquals(proto3.getMethod().getArgs(i).getType(), proto2.getMethod().getArgs(i).getType());
      assertByteStringEquals(proto2.getMethod().getArgs(i).getValue(),
          proto3.getMethod().getArgs(i).getValue());
    }

    assertEquals(proto2.toString(), proto3.toString());
    assertShortDebugString(proto2, proto3);
    assertEquals(proto3, SCMRatisProtocol.SCMRatisRequestProto.parseFrom(proto2.toByteArray()));
  }

  @Test
  public void testProto3ResponseCanBeParsedByProto2() throws Exception {
    for (int i = 0; i < 10; i++) {
      runTestProto3ResponseCanBeParsedByProto2();
    }
  }

  static void runTestProto3ResponseCanBeParsedByProto2() throws Exception {
    final SCMRatisProtocol.SCMRatisResponseProto proto3 = randomProto3SCMRatisResponseProto();

    final Proto2SCMRatisProtocolForTesting.SCMRatisResponseProto proto2 =
            Proto2SCMRatisProtocolForTesting.SCMRatisResponseProto.parseFrom(proto3.toByteArray());

    assertEquals(proto3.getType(), proto2.getType());
    assertByteStringEquals(proto2.getValue(), proto3.getValue());

    assertEquals(proto2.toString(), proto3.toString());
    assertShortDebugString(proto2, proto3);
    assertEquals(proto3, SCMRatisProtocol.SCMRatisResponseProto.parseFrom(proto2.toByteArray()));
  }

  private static void assertShortDebugString(com.google.protobuf.MessageOrBuilder proto2,
      org.apache.ratis.thirdparty.com.google.protobuf.MessageOrBuilder proto3) {
    assertEquals(com.google.protobuf.TextFormat.shortDebugString(proto2),
        org.apache.ratis.thirdparty.com.google.protobuf.TextFormat.shortDebugString(proto3));
  }
}
