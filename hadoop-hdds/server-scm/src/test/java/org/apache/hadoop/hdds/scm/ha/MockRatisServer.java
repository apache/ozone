/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.ha;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.StateMachineException;
import org.apache.ratis.server.RaftServer;

/**
 * Mock RatisServer implementation for testing.
 */
public class MockRatisServer implements SCMRatisServer{

  @Override
  public void start() throws IOException {
    // no op
  }

  @Override
  public void registerStateMachineHandler(
      RequestType handlerType, Object handler) {
    // no op
  }

  @Override
  public SCMRatisResponse submitRequest(SCMRatisRequest request)
      throws IOException, ExecutionException, InterruptedException {
    RaftGroupMemberId groupMemberId =
        RaftGroupMemberId.valueOf(
            RaftPeerId.valueOf("mock"), RaftGroupId.emptyGroupId());
    // create a response with StateMachineException.
    return SCMRatisResponse.decode(new RaftClientReply(
        ClientId.randomId(), groupMemberId, 1L, false,
        Message.EMPTY,
        new StateMachineException(groupMemberId, new Throwable()),
        1L, null));
  }

  @Override
  public void stop() throws IOException {
    // no op
  }

  @Override
  public RaftServer getServer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public RaftGroupId getRaftGroupId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<RaftPeer> getRaftPeers() {
    throw new UnsupportedOperationException();
  }
}
