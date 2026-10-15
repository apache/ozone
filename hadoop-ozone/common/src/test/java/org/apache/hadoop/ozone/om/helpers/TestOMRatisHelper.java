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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link OMRatisHelper}.
 */
class TestOMRatisHelper {

  private static OMResponse createKeyResponse() {
    return OMResponse.newBuilder()
        .setCmdType(Type.CreateKey)
        .setStatus(Status.OK)
        .setSuccess(true)
        .setCreateKeyResponse(CreateKeyResponse.newBuilder()
            .setKeyInfo(KeyInfo.newBuilder()
                .setVolumeName("vol")
                .setBucketName("buck")
                .setKeyName("key")
                .setDataSize(4096)
                .setType(ReplicationType.RATIS)
                .setFactor(ReplicationFactor.THREE)
                .setCreationTime(1)
                .setModificationTime(2)
                .build())
            .setID(42)
            .setOpenVersion(1)
            .build())
        .build();
  }

  /**
   * The reply {@link Message} is kept by the Ratis retry cache for the whole retry-cache timeout,
   * so it must hold the serialized bytes only, not the parsed {@link OMResponse} tree.
   * Serializing eagerly makes {@link Message#getContent()} return the very same instance every time.
   */
  @Test
  void convertResponseToMessageSerializesEagerly() {
    final OMResponse response = createKeyResponse();
    final Message message = OMRatisHelper.convertResponseToMessage(response);

    final ByteString content = message.getContent();
    assertSame(content, message.getContent());
    assertEquals(UnsafeByteOperations.unsafeWrap(response.toByteString().asReadOnlyByteBuffer()), content);
  }

  /**
   * A retried request answered from the retry cache must get back exactly the original response.
   */
  @Test
  void convertResponseToMessageRoundTrips() throws Exception {
    final OMResponse response = createKeyResponse();
    final Message message = OMRatisHelper.convertResponseToMessage(response);

    assertEquals(response, OMRatisHelper.convertByteStringToOMResponse(message.getContent()));
  }

  /**
   * The leader stamps its node id on the reply it returns to the client; the result must be the
   * cached response with that one field set.
   */
  @Test
  void getOMResponseFromRaftClientReplySetsLeaderId() throws Exception {
    final OMResponse response = createKeyResponse();
    final RaftClientReply reply = mock(RaftClientReply.class);
    when(reply.getMessage()).thenReturn(OMRatisHelper.convertResponseToMessage(response));

    assertEquals(response, OMRatisHelper.getOMResponseFromRaftClientReply(reply, null));
    assertEquals(OMResponse.newBuilder(response).setLeaderOMNodeId("om1").build(),
        OMRatisHelper.getOMResponseFromRaftClientReply(reply, RaftPeerId.valueOf("om1")));
  }
}
