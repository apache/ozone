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
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.server.RaftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO.
 */
public class SCMRatisServerImpl implements SCMRatisServer {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMRatisServerImpl.class);

  private final RaftServer.Division division;
  private final InetSocketAddress address;
  private final ClientId clientId = ClientId.randomId();
  private final AtomicLong callId = new AtomicLong();

  // TODO: Refactor and remove ConfigurationSource and use only
  //  SCMHAConfiguration.
  SCMRatisServerImpl(final SCMHAConfiguration haConf,
                     final ConfigurationSource conf)
      throws IOException {
    this.address = haConf.getRatisBindAddress();

    SCMHAGroupBuilder haGrpBuilder = new SCMHAGroupBuilder(haConf, conf);

    final RaftProperties serverProperties = RatisUtil
        .newRaftProperties(haConf, conf);

    RaftServer server = RaftServer.newBuilder()
        .setServerId(haGrpBuilder.getPeerId())
        .setGroup(haGrpBuilder.getRaftGroup())
        .setProperties(serverProperties)
        .setStateMachine(new SCMStateMachine())
        .build();

    this.division = server.getDivision(haGrpBuilder.getRaftGroupId());
  }

  @Override
  public void start() throws IOException {
    division.getRaftServer().start();
  }

  @Override
  public void registerStateMachineHandler(final RequestType handlerType,
                                          final Object handler) {
    ((SCMStateMachine) division.getStateMachine())
        .registerHandler(handlerType, handler);
  }

  @Override
  public SCMRatisResponse submitRequest(SCMRatisRequest request)
      throws IOException, ExecutionException, InterruptedException {
    final RaftClientRequest raftClientRequest =
        new RaftClientRequest(
            clientId,
            division.getId(),
            division.getGroup().getGroupId(),
            nextCallId(),
            request.encode(),
            RaftClientRequest.writeRequestType(),
            null);
    final RaftClientReply raftClientReply =
        division.getRaftServer()
            .submitClientRequestAsync(raftClientRequest)
            .get();
    return SCMRatisResponse.decode(raftClientReply);
  }

  private long nextCallId() {
    return callId.getAndIncrement() & Long.MAX_VALUE;
  }

  @Override
  public void stop() throws IOException {
    division.getRaftServer().close();
  }

  @Override
  public RaftServer.Division getDivision() {
    return division;
  }

  @Override
  public List<String> getRatisRoles() {
    return division.getGroup().getPeers().stream()
        .map(peer -> peer.getAddress() == null ? "" : peer.getAddress())
        .collect(Collectors.toList());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NotLeaderException triggerNotLeaderException() {
    return new NotLeaderException(
        division.getMemberId(), null, division.getGroup().getPeers());
  }

  /**
   * If the SCM group starts from {@link ScmConfigKeys#OZONE_SCM_NAMES},
   * its raft peers should locate on different nodes, and use the same port
   * to communicate with each other.
   *
   * Each of the raft peer figures out its {@link RaftPeerId} by computing
   * its position in {@link ScmConfigKeys#OZONE_SCM_NAMES}.
   *
   * Assume {@link ScmConfigKeys#OZONE_SCM_NAMES} is "ip0,ip1,ip2",
   * scm with ip0 identifies its {@link RaftPeerId} as scm0,
   * scm with ip1 identifies its {@link RaftPeerId} as scm1,
   * scm with ip2 identifies its {@link RaftPeerId} as scm2.
   *
   * After startup, they will form a {@link RaftGroup} with groupID
   * "SCM-HA-Service", and communicate with each other via
   * ozone.scm.ha.ratis.bind.port.
   */
  private static class SCMHAGroupBuilder {
    private final static String SCM_SERVICE_ID = "SCM-HA-Service";

    private final RaftGroupId raftGroupId;
    private final RaftGroup raftGroup;
    private RaftPeerId selfPeerId;

    /**
     * @return raft group
     */
    public RaftGroup getRaftGroup() {
      return raftGroup;
    }

    /**
     * @return raft group id
     */
    public RaftGroupId getRaftGroupId() {
      return raftGroupId;
    }

    /**
     * @return raft peer id
     */
    public RaftPeerId getPeerId() {
      return selfPeerId;
    }

    SCMHAGroupBuilder(final SCMHAConfiguration haConf,
                      final ConfigurationSource conf) throws IOException {
      // fetch port
      int port = haConf.getRatisBindAddress().getPort();

      // fetch localhost
      InetAddress localHost = InetAddress.getLocalHost();

      // fetch hosts from ozone.scm.names
      List<String> hosts = parseHosts(conf);

      final List<RaftPeer> raftPeers = new ArrayList<>();
      for (int i = 0; i < hosts.size(); ++i) {
        String nodeId = "scm" + i;
        RaftPeerId peerId = RaftPeerId.getRaftPeerId(nodeId);

        String host = hosts.get(i);
        if (InetAddress.getByName(host).equals(localHost)) {
          selfPeerId = peerId;
        }

        raftPeers.add(RaftPeer.newBuilder()
            .setId(peerId)
            .setAddress(host + ":" + port)
            .build());
      }

      if (selfPeerId == null) {
        String errorMessage = "localhost " +  localHost
            + " does not exist in ozone.scm.names "
            + conf.get(ScmConfigKeys.OZONE_SCM_NAMES);
        throw new IOException(errorMessage);
      }

      LOG.info("Build a RaftGroup for SCMHA, " +
              "localHost: {}, OZONE_SCM_NAMES: {}, selfPeerId: {}",
          localHost, conf.get(ScmConfigKeys.OZONE_SCM_NAMES), selfPeerId);

      raftGroupId = RaftGroupId.valueOf(UUID.nameUUIDFromBytes(
          SCM_SERVICE_ID.getBytes(StandardCharsets.UTF_8)));

      raftGroup = RaftGroup.valueOf(raftGroupId, raftPeers);
    }

    private List<String> parseHosts(final ConfigurationSource conf)
        throws UnknownHostException {
      // fetch hosts from ozone.scm.names
      List<String> hosts =
          Arrays.stream(conf.getTrimmedStrings(ScmConfigKeys.OZONE_SCM_NAMES))
              .map(scmName -> HddsUtils.getHostName(scmName).get())
              .collect(Collectors.toList());

      // if this is not a conf for a multi-server raft cluster,
      // it means we are in integration test, and need to augment
      // the conf to help build a single-server raft cluster.
      if (hosts.size() == 0) {
        // ozone.scm.names is not set
        hosts.add(InetAddress.getLocalHost().getHostName());
      } else if (hosts.size() == 1) {
        // ozone.scm.names is set, yet the conf may not be usable.
        hosts.set(0, InetAddress.getLocalHost().getHostName());
      }

      LOG.info("fetch hosts {} from ozone.scm.names {}.",
          hosts, conf.get(ScmConfigKeys.OZONE_SCM_NAMES));
      return hosts;
    }
  }
}
