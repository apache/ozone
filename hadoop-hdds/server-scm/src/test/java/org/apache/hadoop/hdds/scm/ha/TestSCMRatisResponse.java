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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for SCMRatisResponse.
 */
public class TestSCMRatisResponse {
  private RaftGroupMemberId raftId;

  @BeforeEach
  public void init() {
    raftId = RaftGroupMemberId.valueOf(
        RaftPeerId.valueOf("peer"), RaftGroupId.randomId());
  }

  @Test
  public void testEncodeAndDecodeSuccess() throws Exception {
    RaftClientReply reply = RaftClientReply.newBuilder()
        .setClientId(ClientId.randomId())
        .setServerId(raftId)
        .setGroupId(RaftGroupId.emptyGroupId())
        .setCallId(1L)
        .setSuccess(true)
        .setMessage(Message.EMPTY)
        .setException(null)
        .setLogIndex(1L)
        .build();
    SCMRatisResponse response = SCMRatisResponse.decode(reply);
    assertTrue(response.isSuccess());
    assertEquals(Message.EMPTY, SCMRatisResponse.encode(response.getResult()));
  }

  @Test
  public void testDecodeOperationFailureWithException() throws Exception {
    RaftClientReply reply = RaftClientReply.newBuilder()
        .setClientId(ClientId.randomId())
        .setServerId(raftId)
        .setGroupId(RaftGroupId.emptyGroupId())
        .setCallId(1L)
        .setSuccess(false)
        .setMessage(Message.EMPTY)
        .setException(new LeaderNotReadyException(raftId))
        .setLogIndex(1L)
        .build();
    SCMRatisResponse response = SCMRatisResponse.decode(reply);
    assertFalse(response.isSuccess());
    assertInstanceOf(RaftException.class, response.getException());
    assertNull(response.getResult());
  }

  @Test
  public void testEncodeFailureWithNonProto() {
    // Non proto input
    Message message = Message.valueOf("test");
    // Should fail with exception.
    assertThrows(InvalidProtocolBufferException.class,
        () -> SCMRatisResponse.encode(message));
  }

  @Test
  public void testResponseDecodeMissingTypeShouldFail() throws Exception {
    // Build response proto missing type
    SCMRatisProtocol.SCMRatisResponseProto proto =
        SCMRatisProtocol.SCMRatisResponseProto.newBuilder()
            // no type
            .setValue(ByteString.copyFromUtf8("v"))
            .build();

    RaftClientReply reply = mock(RaftClientReply.class);
    when(reply.isSuccess()).thenReturn(true);
    when(reply.getMessage()).thenReturn(Message.valueOf(
        org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations
            .unsafeWrap(proto.toByteString().asReadOnlyByteBuffer())));

    InvalidProtocolBufferException ex = assertThrows(
        InvalidProtocolBufferException.class,
        () -> SCMRatisResponse.decode(reply));

    assertTrue(ex.getMessage().contains("Missing response type"));
  }

  @Test
  public void testResponseDecodeMissingValueShouldFail() throws Exception {
    // Build response proto missing value
    SCMRatisProtocol.SCMRatisResponseProto proto =
        SCMRatisProtocol.SCMRatisResponseProto.newBuilder()
            .setType("java.lang.String")
            // no value
            .build();

    RaftClientReply reply = mock(RaftClientReply.class);
    when(reply.isSuccess()).thenReturn(true);
    when(reply.getMessage()).thenReturn(Message.valueOf(
        org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations
            .unsafeWrap(proto.toByteString().asReadOnlyByteBuffer())));

    InvalidProtocolBufferException ex = assertThrows(
        InvalidProtocolBufferException.class,
        () -> SCMRatisResponse.decode(reply));

    assertTrue(ex.getMessage().contains("Missing response value"));
  }
}
