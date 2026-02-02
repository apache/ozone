package org.apache.hadoop.hdds.scm.ha;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol;
import org.apache.hadoop.hdds.protocol.proto.testing.Proto2SCMRatisProtocolForTesting;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    // Parse using proto3 (production schema)
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
    // Build response using proto2 (note: response uses field numbers 2 and 3)
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
