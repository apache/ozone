/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for SCMRatisResponse.
 */
public class TestSCMRatisResponse {
  private RaftGroupMemberId raftId;

  @Before
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
    Assert.assertTrue(response.isSuccess());
    Assert.assertEquals(Message.EMPTY,
        SCMRatisResponse.encode(response.getResult()));
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
    Assert.assertFalse(response.isSuccess());
    Assert.assertTrue(response.getException() instanceof RaftException);
    Assert.assertNull(response.getResult());
  }

  @Test(expected =  InvalidProtocolBufferException.class)
  public void testEncodeFailureWithNonProto() throws Exception {
    // Non proto input
    Message message = Message.valueOf("test");
    // Should fail with exception.
    SCMRatisResponse.encode(message);
  }
}
