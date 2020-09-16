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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO.
 */
public class SCMRatisServerImpl implements SCMRatisServer {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMRatisServerImpl.class);

  private final InetSocketAddress address;
  private final RaftServer server;
  private final RaftGroupId raftGroupId;
  private final RaftGroup raftGroup;
  private final RaftPeerId raftPeerId;
  private final SCMStateMachine scmStateMachine;
  private final ClientId clientId = ClientId.randomId();
  private final AtomicLong callId = new AtomicLong();


  // TODO: Refactor and remove ConfigurationSource and use only
  //  SCMHAConfiguration.
  SCMRatisServerImpl(final SCMHAConfiguration haConf,
                     final ConfigurationSource conf)
      throws IOException {
    // If the SCM group starts from OZONE_SCM_NAMES, its raft peers
    // should locate on different nodes, and use the same port to
    // communicate with each other.
    //
    // Assume ozone.scm.names is "ip0,ip1,ip2", scm with ip0 identifies
    // its RaftPeerId as scm0, scm with ip1 identifies its RaftPeerId
    // as scm1, scm with ip2 identifies its RaftPeerId as scm2. After
    // startup, they will communicate with each other via
    // ozone.scm.ha.ratis.bind.port, and form a raft group with
    // groupID "SCM-HA-Service".
    List<String> scmHosts =
        Arrays.stream(conf.getTrimmedStrings(ScmConfigKeys.OZONE_SCM_NAMES))
            .map(scmName -> HddsUtils.getHostName(scmName).get())
            .collect(Collectors.toList());

    this.address = haConf.getRatisBindAddress();
    InetAddress localHost = InetAddress.getLocalHost();

    int selfIndex = -1;
    RaftPeerId selfPeerId = null;
    final List<RaftPeer> raftPeers = new ArrayList<>();

    for (int i = 0; i < scmHosts.size(); ++i) {
      String scmNodeId = "scm" + i;
      RaftPeerId scmPeerId = RaftPeerId.getRaftPeerId(scmNodeId);

      String scmHost = scmHosts.get(i);
      if (InetAddress.getByName(scmHost).equals(localHost)) {
        selfIndex = i;
        selfPeerId = scmPeerId;
      }

      raftPeers.add(new RaftPeer(
          scmPeerId, scmHost + ":" + address.getPort()));
    }

    if (selfIndex == -1) {
      String errorMessage = "localhost " +  localHost
          + " does not exist in ozone.scm.names "
          + conf.get(ScmConfigKeys.OZONE_SCM_NAMES);
      throw new IOException(errorMessage);
    }

    LOG.info("SCMRatisServer started, " +
        "localHost: {}, OZONE_SCM_NAMES: {}, selfIndex: {}",
        localHost, conf.get(ScmConfigKeys.OZONE_SCM_NAMES), selfIndex);

    this.raftPeerId = selfPeerId;

    final String scmServiceId = "SCM-HA-Service";
    this.raftGroupId = RaftGroupId.valueOf(
        UUID.nameUUIDFromBytes(scmServiceId.getBytes(StandardCharsets.UTF_8)));
    this.raftGroup = RaftGroup.valueOf(raftGroupId, raftPeers);

    final RaftProperties serverProperties = RatisUtil
        .newRaftProperties(haConf, conf);
    this.scmStateMachine = new SCMStateMachine();
    this.server = RaftServer.newBuilder()
        .setServerId(raftPeerId)
        .setGroup(raftGroup)
        .setProperties(serverProperties)
        .setStateMachine(scmStateMachine)
        .build();
  }

  @Override
  public void start() throws IOException {
    server.start();
  }

  @Override
  public void registerStateMachineHandler(final RequestType handlerType,
                                          final Object handler) {
    scmStateMachine.registerHandler(handlerType, handler);
  }

  @Override
  public SCMRatisResponse submitRequest(SCMRatisRequest request)
      throws IOException, ExecutionException, InterruptedException {
    final RaftClientRequest raftClientRequest = new RaftClientRequest(
        clientId, server.getId(), raftGroupId, nextCallId(), request.encode(),
        RaftClientRequest.writeRequestType(), null);
    final RaftClientReply raftClientReply =
        server.submitClientRequestAsync(raftClientRequest).get();
    return SCMRatisResponse.decode(raftClientReply);
  }

  private long nextCallId() {
    return callId.getAndIncrement() & Long.MAX_VALUE;
  }

  @Override
  public void stop() throws IOException {
    server.close();
  }

  @Override
  public RaftServer getServer() {
    return server;
  }

  @Override
  public RaftGroupId getRaftGroupId() {
    return raftGroupId;
  }

  @Override
  public List<RaftPeer> getRaftPeers() {
    return Collections.singletonList(new RaftPeer(raftPeerId));
  }
}
