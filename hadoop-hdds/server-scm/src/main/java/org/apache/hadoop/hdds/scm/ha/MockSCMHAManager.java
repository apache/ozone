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
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.protocol.TermIndex;

// TODO: Move this class to test package after fixing Recon
/**
 * Mock SCMHAManager implementation for testing.
 */
public final class MockSCMHAManager implements SCMHAManager {

  private final SCMRatisServer ratisServer;
  private boolean isLeader;
  private DBTransactionBuffer transactionBuffer;

  public static SCMHAManager getInstance(boolean isLeader) {
    return new MockSCMHAManager(isLeader);
  }

  public static SCMHAManager getInstance(boolean isLeader,
      DBTransactionBuffer buffer) {
    return new MockSCMHAManager(isLeader, buffer);
  }

  /**
   * Creates MockSCMHAManager instance.
   */
  private MockSCMHAManager(boolean isLeader) {
    this(isLeader, new MockSCMHADBTransactionBuffer());
  }

  private MockSCMHAManager(boolean isLeader, DBTransactionBuffer buffer) {
    this.ratisServer = new MockRatisServer();
    this.isLeader = isLeader;
    this.transactionBuffer = buffer;
  }

  @Override
  public void start() throws IOException {
    ratisServer.start();
  }

  /**
   * Informs MockRatisServe to behaviour as a leader SCM or a follower SCM.
   */
  boolean isLeader() {
    return isLeader;
  }

  public void setIsLeader(boolean isLeader) {
    this.isLeader = isLeader;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SCMRatisServer getRatisServer() {
    return ratisServer;
  }

  @Override
  public DBTransactionBuffer getDBTransactionBuffer() {
    return transactionBuffer;
  }

  @Override
  public SCMHADBTransactionBuffer asSCMHADBTransactionBuffer() {
    return null;
  }

  @Override
  public SCMSnapshotProvider getSCMSnapshotProvider() {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void shutdown() throws IOException {
    ratisServer.stop();
  }

  @Override
  public boolean addSCM(AddSCMRequest request) throws IOException {
    return false;
  }

  @Override
  public DBCheckpoint downloadCheckpointFromLeader(String leaderId) {
    return null;
  }

  @Override
  public TermIndex verifyCheckpointFromLeader(String leaderId,
                                              DBCheckpoint checkpoint) {
    return null;
  }

  @Override
  public TermIndex installCheckpoint(DBCheckpoint dbCheckpoint) {
    return null;
  }

  private class MockRatisServer implements SCMRatisServer {

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
      if (isLeader()) {
        try {
          final Message result = process(request);
          reply = RaftClientReply.newBuilder().setClientId(ClientId.randomId())
              .setServerId(raftId).setGroupId(RaftGroupId.emptyGroupId())
              .setCallId(1L).setSuccess(true).setMessage(result)
              .setException(null).setLogIndex(1L).build();
        } catch (Exception ex) {
          reply = RaftClientReply.newBuilder().setClientId(ClientId.randomId())
              .setServerId(raftId).setGroupId(RaftGroupId.emptyGroupId())
              .setCallId(1L).setSuccess(false).setMessage(Message.EMPTY)
              .setException(new StateMachineException(raftId, ex))
              .setLogIndex(1L).build();
        }
      } else {
        reply = RaftClientReply.newBuilder().setClientId(ClientId.randomId())
            .setServerId(raftId).setGroupId(RaftGroupId.emptyGroupId())
            .setCallId(1L).setSuccess(false).setMessage(Message.EMPTY)
            .setException(triggerNotLeaderException()).setLogIndex(1L).build();
      }
      return SCMRatisResponse.decode(reply);
    }

    private Message process(final SCMRatisRequest request) throws Exception {
      try {
        final Object handler = handlers.get(request.getType());

        if (handler == null) {
          throw new IOException(
              "No handler found for request type " + request.getType());
        }

        final List<Class<?>> argumentTypes = new ArrayList<>();
        for (Object args : request.getArguments()) {
          argumentTypes.add(args.getClass());
        }
        final Object result = handler.getClass()
            .getMethod(request.getOperation(),
                argumentTypes.toArray(new Class<?>[0]))
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

    @Override
    public RaftServer.Division getDivision() {
      return null;
    }

    @Override
    public List<String> getRatisRoles() {
      return Arrays
          .asList("180.3.14.5:9894", "180.3.14.21:9894", "180.3.14.145:9894");
    }

    @Override
    public NotLeaderException triggerNotLeaderException() {
      return new NotLeaderException(RaftGroupMemberId
          .valueOf(RaftPeerId.valueOf("peer"), RaftGroupId.randomId()), null,
          new ArrayList<>());
    }

    @Override
    public SCMStateMachine getSCMStateMachine() {
      return null;
    }

    @Override
    public boolean addSCM(AddSCMRequest request) throws IOException {
      return false;
    }

    @Override
    public GrpcTlsConfig getGrpcTlsConfig() {
      return null;
    }
  }
}