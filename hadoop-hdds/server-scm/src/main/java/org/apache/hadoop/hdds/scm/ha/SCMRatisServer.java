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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocolProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.net.NetUtils;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;



public class SCMRatisServer {

  private final Configuration conf;
  private final InetSocketAddress address;
  private final RaftServer server;
  private final RaftGroupId raftGroupId;
  private final RaftGroup raftGroup;
  private final RaftPeerId raftPeerId;
  private final SCMStateMachine scmStateMachine;
  private final ClientId clientId = ClientId.randomId();
  private final AtomicLong callId = new AtomicLong();


  SCMRatisServer(final Configuration conf) throws IOException {
    this.conf = conf;
    final String scmServiceId = "SCM-HA-Service";
    final String scmNodeId = "localhost";
    this.raftPeerId = RaftPeerId.getRaftPeerId(scmNodeId);
    this.address = NetUtils.createSocketAddr("0.0.0.0:9865");
    final RaftPeer localRaftPeer = new RaftPeer(raftPeerId, address);
    final List<RaftPeer> raftPeers = new ArrayList<>();
    raftPeers.add(localRaftPeer);
    final RaftProperties serverProperties = newRaftProperties(conf);
    this.raftGroupId = RaftGroupId.valueOf(UUID.nameUUIDFromBytes(scmServiceId.getBytes(StandardCharsets.UTF_8)));
    this.raftGroup = RaftGroup.valueOf(raftGroupId, raftPeers);
    this.scmStateMachine = new SCMStateMachine();
    this.server = RaftServer.newBuilder()
        .setServerId(this.raftPeerId)
        .setGroup(this.raftGroup)
        .setProperties(serverProperties)
        .setStateMachine(scmStateMachine)
        .build();
  }



  private RaftProperties newRaftProperties(Configuration conf) {
    final RaftProperties properties = new RaftProperties();

    RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
    GrpcConfigKeys.Server.setPort(properties, address.getPort());
    String storageDir = conf.get(ScmConfigKeys.OZONE_SCM_DB_DIRS);
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(new File(storageDir, "ratis")));
    RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf("16k"));
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties, 1024);
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties, SizeInBytes.valueOf("32m"));
    RaftServerConfigKeys.Log.setPreallocatedSize(properties, SizeInBytes.valueOf("16k"));
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, false);
    RaftServerConfigKeys.Log.setPurgeGap(properties, 1000000);
    GrpcConfigKeys.setMessageSizeMax(properties, SizeInBytes.valueOf("32m"));
    TimeDuration serverRequestTimeout = TimeDuration.valueOf(3000, TimeUnit.MILLISECONDS);
    RaftServerConfigKeys.Rpc.setRequestTimeout(properties, serverRequestTimeout);
    TimeDuration retryCacheTimeout = TimeDuration.valueOf(600000, TimeUnit.MILLISECONDS);
    RaftServerConfigKeys.RetryCache.setExpiryTime(properties, retryCacheTimeout);
    TimeDuration serverMinTimeout = TimeDuration.valueOf(1, TimeUnit.SECONDS);
    long serverMaxTimeoutDuration = serverMinTimeout.toLong(TimeUnit.MILLISECONDS) + 200;
    final TimeDuration serverMaxTimeout = TimeDuration.valueOf(serverMaxTimeoutDuration, TimeUnit.SECONDS);
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties, serverMinTimeout);
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties, serverMaxTimeout);
    RaftServerConfigKeys.Log.setSegmentCacheNumMax(properties, 2);
    TimeDuration leaderElectionMinTimeout = TimeDuration.valueOf(1, TimeUnit.SECONDS);
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties, leaderElectionMinTimeout);
    long leaderElectionMaxTimeout = leaderElectionMinTimeout.toLong(TimeUnit.MILLISECONDS) + 200;
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(leaderElectionMaxTimeout, TimeUnit.MILLISECONDS));
    TimeDuration nodeFailureTimeout = TimeDuration.valueOf(120, TimeUnit.SECONDS);
    RaftServerConfigKeys.Notification.setNoLeaderTimeout(properties, nodeFailureTimeout);
    RaftServerConfigKeys.Rpc.setSlownessTimeout(properties, nodeFailureTimeout);
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, 400000);

    return properties;
  }

  void start() throws IOException {
    server.start();
  }

  public void registerStateMachineHandler(final SCMRatisProtocolProtos.RequestType handlerType,
                                          final Object handler) {
    scmStateMachine.registerHandler(handlerType, handler);
  }

  SCMRatisResponse submitRequest(SCMRatisRequest request)
      throws IOException, ExecutionException, InterruptedException {
    final RaftClientRequest raftClientRequest = new RaftClientRequest(
        clientId, server.getId(), raftGroupId, nextCallId(), request.encode(),
        RaftClientRequest.writeRequestType(), null);
    final RaftClientReply raftClientReply = server.submitClientRequestAsync(raftClientRequest).get();
    return SCMRatisResponse.decode(raftClientReply);
  }

  private long nextCallId() {
    return callId.getAndIncrement() & Long.MAX_VALUE;
  }

  void stop() throws IOException {
      server.close();
  }

}
