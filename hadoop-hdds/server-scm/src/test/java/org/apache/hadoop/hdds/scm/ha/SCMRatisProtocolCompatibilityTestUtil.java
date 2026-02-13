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

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdds.protocol.proto.testing.Proto2SCMRatisProtocolForTesting;

import java.nio.ByteBuffer;

import static org.apache.hadoop.hdds.scm.ha.TestSCMRatisProtocolCompatibility.RANDOM;
import static org.apache.hadoop.hdds.scm.ha.TestSCMRatisProtocolCompatibility.TYPES;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests proto2 to proto3 compatibility for SCMRatisProtocol.
 */
public class SCMRatisProtocolCompatibilityTestUtil {
  static <T> ByteString randomValueProto2(Class<T> clazz) {
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

  static Proto2SCMRatisProtocolForTesting.MethodArgument randomProto2MethodArgument() {
    final Class<?> type = TYPES[RANDOM.nextInt(TYPES.length)];
    return Proto2SCMRatisProtocolForTesting.MethodArgument.newBuilder()
        .setType(type.getName())
        .setValue(randomValueProto2(type))
        .build();
  }

  static Proto2SCMRatisProtocolForTesting.SCMRatisResponseProto randomProto2SCMRatisResponseProto() {
    final Class<?> type = TYPES[RANDOM.nextInt(TYPES.length)];
    return Proto2SCMRatisProtocolForTesting.SCMRatisResponseProto.newBuilder()
        .setType(type.getName())
        .setValue(randomValueProto2(type))
        .build();
  }

  static Proto2SCMRatisProtocolForTesting.SCMRatisRequestProto proto2Request(
      String name, Proto2SCMRatisProtocolForTesting.RequestType type, int numArgs) {
    // Build request using proto2 (test-only schema)
    final Proto2SCMRatisProtocolForTesting.Method.Builder b =
        Proto2SCMRatisProtocolForTesting.Method.newBuilder()
            .setName(name);
    for (int i = 0; i < numArgs; i++) {
      b.addArgs(randomProto2MethodArgument());
    }

    return Proto2SCMRatisProtocolForTesting.SCMRatisRequestProto.newBuilder()
        .setType(type)
        .setMethod(b)
        .build();
  }
}
