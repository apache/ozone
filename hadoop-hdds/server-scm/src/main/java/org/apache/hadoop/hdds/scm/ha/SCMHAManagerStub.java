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

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.RemoveSCMRequest;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;

/**
 * SCMHAManagerStub implementation for Recon and testing. It uses
 * RatisServerStub for HA implementation of components in Recon.
 */
public final class SCMHAManagerStub implements SCMHAManager {

  private final SCMRatisServer ratisServer;
  private boolean isLeader;
  private final DBTransactionBuffer transactionBuffer;

  public static SCMHAManager getInstance(boolean isLeader) {
    return new SCMHAManagerStub(isLeader);
  }

  public static SCMHAManager getInstance(boolean isLeader,
      DBTransactionBuffer buffer) {
    return new SCMHAManagerStub(isLeader, buffer);
  }

  public static SCMHAManager getInstance(boolean isLeader, DBStore dbStore) {
    return new SCMHAManagerStub(isLeader,
        new SCMHADBTransactionBufferStub(dbStore));
  }

  /**
   * Creates SCMHAManagerStub instance.
   */
  private SCMHAManagerStub(boolean isLeader) {
    this(isLeader, new SCMHADBTransactionBufferStub());
  }

  private SCMHAManagerStub(boolean isLeader, DBTransactionBuffer buffer) {
    this.ratisServer = new RatisServerStub();
    this.isLeader = isLeader;
    this.transactionBuffer = buffer;
  }

  @Override
  public void start() throws IOException {
    ratisServer.start();
  }

  @Override
  public void stop() throws IOException {
    ratisServer.stop();
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(transactionBuffer::close);
  }

  /**
   * Informs RatisServerStub to behaviour as a leader SCM or a follower SCM.
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
    Preconditions
        .checkArgument(transactionBuffer instanceof SCMHADBTransactionBuffer);
    return (SCMHADBTransactionBuffer) transactionBuffer;
  }

  @Override
  public SCMSnapshotProvider getSCMSnapshotProvider() {
    return null;
  }

  @Override
  public boolean addSCM(AddSCMRequest request) throws IOException {
    return false;
  }

  @Override
  public boolean removeSCM(RemoveSCMRequest request) {
    return false;
  }

  @Override
  public DBCheckpoint downloadCheckpointFromLeader(String leaderId) {
    return null;
  }

  @Override
  public List<ManagedSecretKey> getSecretKeysFromLeader(String leaderID) {
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

  private class RatisServerStub implements SCMRatisServer {

    private Map<RequestType, Object> handlers =
        new EnumMap<>(RequestType.class);

    private RaftPeerId leaderId = RaftPeerId.valueOf(UUID.randomUUID().toString());

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

    @Override
    public boolean triggerSnapshot() throws IOException {
      throw new IOException("submitSnapshotRequest is called.");
    }

    private Message process(final SCMRatisRequest request) throws Exception {
      try {
        final Object handler = handlers.get(request.getType());

        if (handler == null) {
          throw new IOException(
              "No handler found for request type " + request.getType());
        }

        final Object result = handler.getClass()
            .getMethod(request.getOperation(),
                request.getParameterTypes())
            .invoke(handler, request.getArguments());

        return SCMRatisResponse.encode(result);
      } catch (NoSuchMethodException | SecurityException ex) {
        throw new InvalidProtocolBufferException(ex.getMessage());
      } catch (InvocationTargetException e) {
        final Throwable target = e.getTargetException();
        throw target instanceof Exception ? (Exception) target : e;
      }
    }

    @Override
    public void stop() {
    }

    @Override
    public boolean isStopped() {
      return false;
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
    public boolean addSCM(AddSCMRequest request) {
      return false;
    }

    @Override
    public boolean removeSCM(RemoveSCMRequest request) {
      return false;
    }

    @Override
    public GrpcTlsConfig getGrpcTlsConfig() {
      return null;
    }

    @Override
    public RaftPeerId getLeaderId() {
      return leaderId;
    }
  }
}
