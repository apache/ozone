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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol;
import org.apache.hadoop.hdds.protocol.proto.testing.Proto2SCMRatisProtocolForTesting;
import org.junit.jupiter.api.Test;

/**
 * Tests proto2 to proto3 compatibility for SCMRatisProtocol.
 */
public class TestSCMRatisProtocolCompatibility {

  @Test
  public void testProto2RequestCanBeParsedByProto3() throws Exception {
    // Build request using proto2 (test-only schema)
    Proto2SCMRatisProtocolForTesting.MethodArgument arg =
        Proto2SCMRatisProtocolForTesting.MethodArgument.newBuilder()
            .setType("java.lang.String")
            .setValue(ByteString.copyFromUtf8("v"))
            .build();

    Proto2SCMRatisProtocolForTesting.Method method =
        Proto2SCMRatisProtocolForTesting.Method.newBuilder()
            .setName("testOp")
            .addArgs(arg)
            .build();

    Proto2SCMRatisProtocolForTesting.SCMRatisRequestProto proto2 =
        Proto2SCMRatisProtocolForTesting.SCMRatisRequestProto.newBuilder()
            .setType(Proto2SCMRatisProtocolForTesting.RequestType.PIPELINE)
            .setMethod(method)
            .build();

    byte[] bytes = proto2.toByteArray();

    // Parse using proto3
    SCMRatisProtocol.SCMRatisRequestProto proto3 =
        SCMRatisProtocol.SCMRatisRequestProto.parseFrom(bytes);

    // Presence + value checks (proto3 optional fields)
    assertTrue(proto3.hasType(), "proto3 should see type presence from proto2 bytes");
    assertTrue(proto3.hasMethod(), "proto3 should see method presence from proto2 bytes");
    assertEquals(SCMRatisProtocol.RequestType.PIPELINE, proto3.getType());
    assertEquals("testOp", proto3.getMethod().getName());
    assertEquals(1, proto3.getMethod().getArgsCount());
    assertEquals("java.lang.String", proto3.getMethod().getArgs(0).getType());
    assertEquals(ByteString.copyFromUtf8("v"), proto3.getMethod().getArgs(0).getValue());
  }

  @Test
  public void testProto2ResponseCanBeParsedByProto3() throws Exception {
    // Build response using proto2
    Proto2SCMRatisProtocolForTesting.SCMRatisResponseProto proto2 =
        Proto2SCMRatisProtocolForTesting.SCMRatisResponseProto.newBuilder()
            .setType("java.lang.String")
            .setValue(ByteString.copyFromUtf8("ok"))
            .build();

    byte[] bytes = proto2.toByteArray();

    // Parse using proto3 (production schema)
    SCMRatisProtocol.SCMRatisResponseProto proto3 =
        SCMRatisProtocol.SCMRatisResponseProto.parseFrom(bytes);

    assertTrue(proto3.hasType(), "proto3 should see type presence from proto2 bytes");
    assertTrue(proto3.hasValue(), "proto3 should see value presence from proto2 bytes");
    assertEquals("java.lang.String", proto3.getType());
    assertEquals(ByteString.copyFromUtf8("ok"), proto3.getValue());
  }
}
