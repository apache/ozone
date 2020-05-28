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
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.StateMachineException;

/**
 * Mock SCMHAManager implementation for testing.
 */
public final class MockSCMHAManager implements SCMHAManager {

  private final SCMRatisServer ratisServer;

  public static SCMHAManager getInstance() {
    return new MockSCMHAManager();
  }

  /**
   * Creates MockSCMHAManager instance.
   */
  private MockSCMHAManager() {
    this.ratisServer = new MockRatisServer();
  }

  @Override
  public void start() throws IOException {
    ratisServer.start();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isLeader() {
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SCMRatisServer getRatisServer() {
    return ratisServer;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void shutdown() throws IOException {
    ratisServer.stop();
  }

  private static class MockRatisServer implements SCMRatisServer {

    private Map<RequestType, Object> handlers =
        new EnumMap<>(RequestType.class);

    @Override
    public void start() {
    }

    @Override
    public void registerStateMachineHandler(final RequestType handlerType,
                                            final Object handler) {
      handlers.put(handlerType, handler);
    }

    @Override
    public SCMRatisResponse submitRequest(final SCMRatisRequest request)
        throws IOException {
      final RaftGroupMemberId raftId = RaftGroupMemberId.valueOf(
          RaftPeerId.valueOf("peer"), RaftGroupId.randomId());
      RaftClientReply reply;
      try {
        final Message result = process(request);
        return SCMRatisResponse.decode(new RaftClientReply(ClientId.randomId(),
            raftId, 1L, true, result, null, 1L, null));
      } catch (Exception ex) {
        return SCMRatisResponse.decode(new RaftClientReply(ClientId.randomId(),
            raftId, 1L, false, null,
            new StateMachineException(raftId, ex), 1L, null));
      }
    }

    private Message process(final SCMRatisRequest request)
        throws Exception {
      try {
        final Object handler = handlers.get(request.getType());

        if (handler == null) {
          throw new IOException("No handler found for request type " +
              request.getType());
        }

        final List<Class<?>> argumentTypes = new ArrayList<>();
        for(Object args : request.getArguments()) {
          argumentTypes.add(args.getClass());
        }
        final Object result = handler.getClass().getMethod(
            request.getOperation(), argumentTypes.toArray(new Class<?>[0]))
            .invoke(handler, request.getArguments());

        return SCMRatisResponse.encode(result);
      } catch (NoSuchMethodException | SecurityException ex) {
        throw new InvalidProtocolBufferException(ex.getMessage());
      } catch (InvocationTargetException e) {
        final Exception targetEx = (Exception) e.getTargetException();
        throw targetEx != null ? targetEx : e;
      }
    }

    @Override
    public void stop() {
    }
  }

}