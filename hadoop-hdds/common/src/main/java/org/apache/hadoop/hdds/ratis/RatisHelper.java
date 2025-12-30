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

package org.apache.hadoop.hdds.ratis;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ratis.util.Preconditions.assertTrue;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import javax.net.ssl.TrustManager;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.ratis.retrypolicy.RetryPolicyCreator;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.RoutingTable;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.JvmPauseMonitor;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ratis helper methods.
 */
public final class RatisHelper {

  private static final Logger LOG = LoggerFactory.getLogger(RatisHelper.class);

  private static final OzoneConfiguration CONF = new OzoneConfiguration();

  // Prefix for Ratis Server GRPC and Ratis client conf.
  public static final String HDDS_DATANODE_RATIS_PREFIX_KEY = "hdds.ratis";

  /* TODO: use a dummy id for all groups for the moment.
   *       It should be changed to a unique id for each group.
   */
  private static final RaftGroupId DUMMY_GROUP_ID =
      RaftGroupId.valueOf(ByteString.copyFromUtf8("AOzoneRatisGroup"));

  private static final RaftGroup EMPTY_GROUP = RaftGroup.valueOf(DUMMY_GROUP_ID,
      Collections.emptyList());

  // Used for OM/SCM HA transfer leadership
  @VisibleForTesting
  public static final int NEUTRAL_PRIORITY = 0;
  private static final int HIGHER_PRIORITY = 1;

  private RatisHelper() {
  }

  public static JvmPauseMonitor newJvmPauseMonitor(String name) {
    return JvmPauseMonitor.newBuilder().setName(name).build();
  }

  private static String toRaftPeerIdString(DatanodeDetails id) {
    return id.getUuidString();
  }

  public static UUID toDatanodeId(String peerIdString) {
    return UUID.fromString(peerIdString);
  }

  public static UUID toDatanodeId(RaftPeerId peerId) {
    return toDatanodeId(peerId.toString());
  }

  public static UUID toDatanodeId(RaftProtos.RaftPeerProto peerId) {
    return toDatanodeId(RaftPeerId.valueOf(peerId.getId()));
  }

  private static String toRaftPeerAddress(DatanodeDetails id, Port.Name port) {
    if (datanodeUseHostName()) {
      final String address =
              id.getHostName() + ":" + id.getPort(port).getValue();
      LOG.debug("Datanode is using hostname for raft peer address: {}",
              address);
      return address;
    } else {
      final String address =
              id.getIpAddress() + ":" + id.getPort(port).getValue();
      LOG.debug("Datanode is using IP for raft peer address: {}", address);
      return address;
    }
  }

  public static RaftPeerId toRaftPeerId(DatanodeDetails id) {
    return RaftPeerId.valueOf(toRaftPeerIdString(id));
  }

  public static RaftPeer toRaftPeer(DatanodeDetails id) {
    return raftPeerBuilderFor(id).build();
  }

  public static RaftPeer toRaftPeer(DatanodeDetails id, int priority) {
    return raftPeerBuilderFor(id)
        .setPriority(priority)
        .build();
  }

  private static RaftPeer.Builder raftPeerBuilderFor(DatanodeDetails dn) {
    return RaftPeer.newBuilder()
        .setId(toRaftPeerId(dn))
        .setAddress(toRaftPeerAddress(dn, Port.Name.RATIS_SERVER))
        .setAdminAddress(toRaftPeerAddress(dn, Port.Name.RATIS_ADMIN))
        .setClientAddress(toRaftPeerAddress(dn, Port.Name.RATIS))
        .setDataStreamAddress(
            toRaftPeerAddress(dn, Port.Name.RATIS_DATASTREAM));
  }

  private static List<RaftPeer> toRaftPeers(Pipeline pipeline) {
    return toRaftPeers(pipeline.getNodes());
  }

  private static <E extends DatanodeDetails> List<RaftPeer> toRaftPeers(
      List<E> datanodes) {
    return datanodes.stream().map(RatisHelper::toRaftPeer)
        .collect(Collectors.toList());
  }

  private static RaftGroup emptyRaftGroup() {
    return EMPTY_GROUP;
  }

  private static RaftGroup newRaftGroup(Collection<RaftPeer> peers) {
    return peers.isEmpty() ? emptyRaftGroup()
        : RaftGroup.valueOf(DUMMY_GROUP_ID, peers);
  }

  public static RaftGroup newRaftGroup(RaftGroupId groupId,
      List<DatanodeDetails> peers, List<Integer> priorityList) {
    assert peers.size() == priorityList.size();

    final List<RaftPeer> newPeers = new ArrayList<>();
    for (int i = 0; i < peers.size(); i++) {
      RaftPeer peer = RatisHelper.toRaftPeer(peers.get(i), priorityList.get(i));
      newPeers.add(peer);
    }
    return peers.isEmpty() ? RaftGroup.valueOf(groupId, Collections.emptyList())
        : RaftGroup.valueOf(groupId, newPeers);
  }

  public static RaftGroup newRaftGroup(RaftGroupId groupId,
      Collection<DatanodeDetails> peers) {
    final List<RaftPeer> newPeers = peers.stream()
        .map(RatisHelper::toRaftPeer)
        .collect(Collectors.toList());
    return peers.isEmpty() ? RaftGroup.valueOf(groupId, Collections.emptyList())
        : RaftGroup.valueOf(groupId, newPeers);
  }

  public static RaftGroup newRaftGroup(Pipeline pipeline) {
    return RaftGroup.valueOf(RaftGroupId.valueOf(pipeline.getId().getId()),
        toRaftPeers(pipeline));
  }

  /**
   * Create a Raft client used primarily for Ozone client communications with
   * the Ratis pipeline.
   * @param rpcType rpc type
   * @param pipeline pipeline
   * @param retryPolicy retry policy
   * @param tlsConfig tls config
   * @param ozoneConfiguration configuration
   * @return Raft client
   * @throws IOException IOException
   */
  public static RaftClient newRaftClient(RpcType rpcType, Pipeline pipeline,
      RetryPolicy retryPolicy, GrpcTlsConfig tlsConfig,
      ConfigurationSource ozoneConfiguration) throws IOException {
    return newRaftClient(rpcType,
        toRaftPeerId(pipeline.getLeaderNode()),
        toRaftPeer(pipeline.getClosestNode()),
        newRaftGroup(RaftGroupId.valueOf(pipeline.getId().getId()),
            pipeline.getNodes()), retryPolicy, tlsConfig, ozoneConfiguration);
  }

  private static RpcType getRpcType(ConfigurationSource conf) {
    return SupportedRpcType.valueOfIgnoreCase(conf.get(
        ScmConfigKeys.HDDS_CONTAINER_RATIS_RPC_TYPE_KEY,
        ScmConfigKeys.HDDS_CONTAINER_RATIS_RPC_TYPE_DEFAULT));
  }

  public static BiFunction<RaftPeer, GrpcTlsConfig, RaftClient> newRaftClient(
      ConfigurationSource conf) {
    return (leader, tlsConfig) -> newRaftClient(getRpcType(conf), leader,
        RatisHelper.createRetryPolicy(conf), tlsConfig, conf);
  }

  public static BiFunction<RaftPeer, GrpcTlsConfig, RaftClient> newRaftClientNoRetry(
      ConfigurationSource conf) {
    return (leader, tlsConfig) -> newRaftClient(getRpcType(conf), leader,
        RetryPolicies.noRetry(), tlsConfig, conf);
  }

  public static RaftClient newRaftClient(RpcType rpcType, RaftPeer leader,
      RetryPolicy retryPolicy, GrpcTlsConfig tlsConfig,
      ConfigurationSource configuration) {
    return newRaftClient(rpcType, leader.getId(), leader,
        newRaftGroup(Collections.singletonList(leader)), retryPolicy,
        tlsConfig, configuration);
  }

  public static RaftClient newRaftClient(RpcType rpcType, RaftPeer leader,
      RetryPolicy retryPolicy,
      ConfigurationSource ozoneConfiguration) {
    return newRaftClient(rpcType, leader.getId(), leader,
        newRaftGroup(Collections.singletonList(leader)), retryPolicy, null,
        ozoneConfiguration);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private static RaftClient newRaftClient(RpcType rpcType, RaftPeerId leader,
      RaftPeer primary, RaftGroup group, RetryPolicy retryPolicy,
      GrpcTlsConfig tlsConfig, ConfigurationSource ozoneConfiguration) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("newRaftClient: {}, leader={}, group={}",
          rpcType, leader, group);
    }
    final RaftProperties properties = newRaftProperties(rpcType);
    enableNettyStreaming(properties);

    // Set the ratis client headers which are matching with regex.
    createRaftClientProperties(ozoneConfiguration, properties);

    return RaftClient.newBuilder()
        .setRaftGroup(group)
        .setLeaderId(leader)
        .setPrimaryDataStreamServer(primary)
        .setProperties(properties)
        .setParameters(setClientTlsConf(rpcType, tlsConfig))
        .setRetryPolicy(retryPolicy)
        .build();
  }

  public static Parameters setClientTlsConf(RpcType rpcType,
      GrpcTlsConfig tlsConfig) {
    // TODO: GRPC TLS only for now, netty/hadoop RPC TLS support later.
    if (tlsConfig != null && rpcType == SupportedRpcType.GRPC) {
      Parameters parameters = new Parameters();
      setAdminTlsConf(parameters, tlsConfig);
      setClientTlsConf(parameters, tlsConfig);
      return parameters;
    }
    return null;
  }

  private static void setAdminTlsConf(Parameters parameters,
      GrpcTlsConfig tlsConfig) {
    if (tlsConfig != null) {
      GrpcConfigKeys.Admin.setTlsConf(parameters, tlsConfig);
    }
  }

  private static void setClientTlsConf(Parameters parameters,
      GrpcTlsConfig tlsConfig) {
    if (tlsConfig != null) {
      GrpcConfigKeys.Client.setTlsConf(parameters, tlsConfig);
      NettyConfigKeys.DataStream.Client.setTlsConf(parameters, tlsConfig);
    }
  }

  public static Parameters setServerTlsConf(
      GrpcTlsConfig serverConf, GrpcTlsConfig clientConf) {
    final Parameters parameters = new Parameters();
    if (serverConf != null) {
      GrpcConfigKeys.Server.setTlsConf(parameters, serverConf);
      GrpcConfigKeys.TLS.setConf(parameters, serverConf);
      setAdminTlsConf(parameters, serverConf);

      NettyConfigKeys.DataStream.Server.setTlsConf(parameters, serverConf);
    }
    setClientTlsConf(parameters, clientConf);
    return parameters;
  }

  public static Parameters setServerTlsConf(GrpcTlsConfig tlsConf) {
    return setServerTlsConf(tlsConf, tlsConf);
  }

  public static RaftProperties newRaftProperties(RpcType rpcType) {
    final RaftProperties properties = new RaftProperties();
    setRpcType(properties, rpcType);
    return properties;
  }

  public static RaftProperties setRpcType(RaftProperties properties,
      RpcType rpcType) {
    RaftConfigKeys.Rpc.setType(properties, rpcType);
    return properties;
  }

  public static RaftProperties enableNettyStreaming(RaftProperties properties) {
    RaftConfigKeys.DataStream.setType(properties,
        SupportedDataStreamType.NETTY);
    return properties;
  }

  /**
   * Set all client properties matching with regex
   * {@link RatisHelper#HDDS_DATANODE_RATIS_PREFIX_KEY} in
   * ozone configuration object and configure it to RaftProperties.
   */
  public static void createRaftClientProperties(ConfigurationSource ozoneConf,
      RaftProperties raftProperties) {

    // As for client we do not require server and grpc server/tls. exclude them.
    Map<String, String> ratisClientConf =
        getDatanodeRatisPrefixProps(ozoneConf);
    ratisClientConf.forEach((key, val) -> {
      if (isClientConfig(key) || isGrpcClientConfig(key)
              || isNettyStreamConfig(key) || isDataStreamConfig(key)) {
        raftProperties.set(key, val);
      }
    });
  }

  private static boolean isClientConfig(String key) {
    return key.startsWith(RaftClientConfigKeys.PREFIX);
  }

  private static boolean isDataStreamConfig(String key) {
    return key.startsWith(RaftConfigKeys.DataStream.PREFIX);
  }

  private static boolean isGrpcClientConfig(String key) {
    return key.startsWith(GrpcConfigKeys.PREFIX) &&
        !key.startsWith(GrpcConfigKeys.TLS.PREFIX) &&
        !key.startsWith(GrpcConfigKeys.Admin.PREFIX) &&
        !key.startsWith(GrpcConfigKeys.Server.PREFIX);
  }

  private static boolean isNettyStreamConfig(String key) {
    return key.startsWith(NettyConfigKeys.DataStream.PREFIX);
  }

  private static boolean isStreamClientConfig(String key) {
    return key.startsWith(RaftClientConfigKeys.DataStream.PREFIX);
  }

  /**
   * Set all server properties matching with prefix
   * {@link RatisHelper#HDDS_DATANODE_RATIS_PREFIX_KEY} in
   * ozone configuration object and configure it to RaftProperties.
   */
  public static void createRaftServerProperties(ConfigurationSource ozoneConf,
       RaftProperties raftProperties) {

    Map<String, String> ratisServerConf =
        getDatanodeRatisPrefixProps(ozoneConf);
    ratisServerConf.forEach((key, val) -> {
      // Exclude ratis client configuration.
      if (isNettyStreamConfig(key) || isStreamClientConfig(key) ||
          !isClientConfig(key)) {
        raftProperties.set(key, val);
      }
    });
  }

  private static Map<String, String> getDatanodeRatisPrefixProps(
      ConfigurationSource configuration) {
    return configuration.getPropsMatchPrefixAndTrimPrefix(HDDS_DATANODE_RATIS_PREFIX_KEY + '.');
  }

  // For External gRPC client to server with gRPC TLS.
  // No mTLS for external client as SCM CA does not issued certificates for them
  public static GrpcTlsConfig createTlsClientConfig(SecurityConfig conf,
      TrustManager trustManager) {
    GrpcTlsConfig tlsConfig = null;
    if (conf.isSecurityEnabled() && conf.isGrpcTlsEnabled()) {
      tlsConfig = new GrpcTlsConfig(null, trustManager, false);
    }
    return tlsConfig;
  }

  public static RetryPolicy createRetryPolicy(ConfigurationSource conf) {
    try {
      RatisClientConfig scmClientConfig =
          conf.getObject(RatisClientConfig.class);
      Class<? extends RetryPolicyCreator> policyClass = getClass(
          scmClientConfig.getRetryPolicy(),
          RetryPolicyCreator.class);
      return policyClass.newInstance().create(conf);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static Long getMinReplicatedIndex(
      Collection<RaftProtos.CommitInfoProto> commitInfos) {
    return commitInfos.stream().map(RaftProtos.CommitInfoProto::getCommitIndex)
        .min(Long::compareTo).orElse(null);
  }

  private static boolean datanodeUseHostName() {
    return CONF.getBoolean(
            HddsConfigKeys.HDDS_DATANODE_USE_DN_HOSTNAME,
            HddsConfigKeys.HDDS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
  }

  private static <U> Class<? extends U> getClass(String name,
      Class<U> xface) {
    try {
      Class<?> theClass = Class.forName(name);
      if (!xface.isAssignableFrom(theClass)) {
        throw new RuntimeException(theClass + " not " + xface.getName());
      } else {
        return theClass.asSubclass(xface);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  public static RoutingTable getRoutingTable(Pipeline pipeline) {
    RaftPeerId primaryId = null;
    List<RaftPeerId> raftPeers = new ArrayList<>();

    for (DatanodeDetails dn : pipeline.getNodesInOrder()) {
      final RaftPeerId raftPeerId = RaftPeerId.valueOf(dn.getUuidString());
      try {
        if (dn == pipeline.getClosestNode()) {
          primaryId = raftPeerId;
        }
      } catch (IOException e) {
        LOG.error("Can not get primary node from the pipeline: {} with " +
            "exception: {}", pipeline, e.getLocalizedMessage());
        return null;
      }
      raftPeers.add(raftPeerId);
    }

    RoutingTable.Builder builder = RoutingTable.newBuilder();
    RaftPeerId previousId = primaryId;
    for (RaftPeerId peerId : raftPeers) {
      if (peerId.equals(primaryId)) {
        continue;
      }
      builder.addSuccessor(previousId, peerId);
      previousId = peerId;
    }

    return builder.build();
  }

  public static void debug(ByteBuffer buffer, String name, Logger log) {
    if (!log.isDebugEnabled()) {
      return;
    }
    buffer = buffer.duplicate();
    final StringBuilder builder = new StringBuilder();
    for (int i = 1; buffer.remaining() > 0; i++) {
      builder.append(buffer.get()).append(i % 20 == 0 ? "\n  " : ", ");
    }
    log.debug("{}: {}\n  {}", name, buffer, builder);
  }

  public static void debug(ByteBuf buf, String name, Logger log) {
    if (!log.isDebugEnabled()) {
      return;
    }
    buf = buf.duplicate();
    final StringBuilder builder = new StringBuilder();
    for (int i = 1; buf.readableBytes() > 0; i++) {
      builder.append(buf.readByte()).append(i % 20 == 0 ? "\n  " : ", ");
    }
    log.debug("{}: {}\n  {}", name, buf, builder);
  }

  static RaftPeer newRaftPeer(RaftPeer peer, RaftPeerId target) {
    final int priority = peer.getId().equals(target) ?
        HIGHER_PRIORITY : NEUTRAL_PRIORITY;
    return RaftPeer.newBuilder(peer).setPriority(priority).build();
  }

  /**
   * Use raft client to send admin request, transfer the leadership.
   * 1. Set priority and send setConfiguration request
   * 2. Trigger transferLeadership API.
   *
   * @param group         the Raft group
   * @param targetPeerId  the target expected leader
   */
  public static void transferRatisLeadership(ConfigurationSource conf,
      RaftGroup group, RaftPeerId targetPeerId, GrpcTlsConfig tlsConfig)
      throws IOException {
    if (group.getPeer(targetPeerId) == null) {
      throw new IOException("Target " + targetPeerId + " not found in group "
          + group.getPeers().stream().map(RaftPeer::getId)
              .collect(Collectors.toList()) + ".");
    }
    if (!group.getPeer(targetPeerId).getStartupRole().equals(RaftProtos.RaftPeerRole.FOLLOWER)) {
      throw new IOException("Target " + targetPeerId + " not in FOLLOWER role. "
          + group.getPeers().stream()
          .map(p -> p.getId() + ":" + p.getStartupRole())
          .collect(Collectors.toList()) + ".");
    }

    LOG.info("Start transferring leadership to {}", targetPeerId);
    try (RaftClient client = newRaftClient(SupportedRpcType.GRPC, null,
        null, group, createRetryPolicy(conf), tlsConfig, conf)) {
      final GroupInfoReply info = client.getGroupManagementApi(targetPeerId)
          .info(group.getGroupId());
      if (!info.isSuccess()) {
        throw new IOException("Failed to get info for " + group.getGroupId()
            + " from " + targetPeerId);
      }

      final RaftGroup remote = info.getGroup();
      if (!group.equals(remote)) {
        throw new IOException("Group mismatched: the given group " + group
          + " and the remote group from " + targetPeerId + " are not equal."
          + "\n Given: " + group
          + "\n Remote: " + remote);
      }

      RaftClientReply setConf = null;
      try {
        // Set priority
        final List<RaftPeer> followerWithNewPriorities = group.getPeers().stream()
            .filter(peer -> peer.getStartupRole().equals(RaftProtos.RaftPeerRole.FOLLOWER))
            .map(peer -> newRaftPeer(peer, targetPeerId))
            .collect(Collectors.toList());
        final List<RaftPeer> listenerWithNewPriorities = group.getPeers().stream()
            .filter(peer -> peer.getStartupRole().equals(RaftProtos.RaftPeerRole.LISTENER))
            .map(peer -> newRaftPeer(peer, targetPeerId))
            .collect(Collectors.toList());
        // Set new configuration
        setConf = client.admin().setConfiguration(followerWithNewPriorities, listenerWithNewPriorities);
        if (setConf.isSuccess()) {
          LOG.info("Successfully set priority: Follower: {}, Listener: {}", followerWithNewPriorities,
              listenerWithNewPriorities);
        } else {
          throw new IOException("Failed to set priority.",
              setConf.getException());
        }

        // Trigger the transferLeadership
        final RaftClientReply reply = client.admin()
            .transferLeadership(targetPeerId, 60_000);
        if (reply.isSuccess()) {
          LOG.info("Successfully transferred leadership to {}.", targetPeerId);
        } else {
          LOG.warn("Failed to transfer leadership to {}. Ratis reply: {}",
              targetPeerId, reply);
          throw new IOException(reply.getException());
        }
      } finally {
        // Reset peers regardless of the result of transfer leadership
        if (setConf != null && setConf.isSuccess()) {
          resetPriorities(remote, client);
        }
      }
    }
  }

  private static void resetPriorities(RaftGroup original, RaftClient client) {
    final List<RaftPeer> resetFollower = original.getPeers().stream()
        .filter(peer -> peer.getStartupRole().equals(RaftProtos.RaftPeerRole.FOLLOWER))
        .map(originalPeer -> RaftPeer.newBuilder(originalPeer)
            .setPriority(NEUTRAL_PRIORITY).build())
        .collect(Collectors.toList());
    final List<RaftPeer> resetListener = original.getPeers().stream()
        .filter(peer -> peer.getStartupRole().equals(RaftProtos.RaftPeerRole.LISTENER))
        .map(originalPeer -> RaftPeer.newBuilder(originalPeer)
            .setPriority(NEUTRAL_PRIORITY).build())
        .collect(Collectors.toList());
    LOG.info("Resetting Raft peers priorities to Follower: {}, Listener: {}", resetFollower, resetListener);
    try {
      RaftClientReply reply = client.admin().setConfiguration(resetFollower, resetListener);
      if (reply.isSuccess()) {
        LOG.info("Successfully reset priorities: {}", original);
      } else {
        LOG.warn("Failed to reset priorities: {}, reply: {}", original, reply);
      }
    } catch (IOException e) {
      LOG.error("Failed to reset priorities for " + original, e);
      // Not re-thrown in order to keep the main exception, if there is any.
    }
  }

  /**
   * Similar to {@link JavaUtils#attemptUntilTrue(BooleanSupplier, int, TimeDuration, String, Logger)},
   * but:
   * <li>takes max. {@link Duration} instead of number of attempts</li>
   * <li>accepts {@link Duration} instead of {@link TimeDuration} for sleep time</li>
   *
   * @return true if attempt was successful,
   * false if wait for condition to become true timed out or was interrupted
   */
  public static boolean attemptUntilTrue(BooleanSupplier condition, Duration pollInterval, Duration timeout) {
    try {
      final int attempts = calculateAttempts(pollInterval, timeout);
      final TimeDuration sleepTime = TimeDuration.valueOf(pollInterval.toMillis(), MILLISECONDS);
      JavaUtils.attemptUntilTrue(condition, attempts, sleepTime, null, null);
      return true;
    } catch (InterruptedException | IllegalStateException exception) {
      return false;
    }
  }

  public static int calculateAttempts(Duration pollInterval, Duration maxDuration) {
    final long max = maxDuration.toMillis();
    final long interval = pollInterval.toMillis();
    assertTrue(max >= interval, () -> "max: " + maxDuration + " < interval:" + pollInterval);
    return (int) (max / interval);
  }

  public static void setFirstElectionTimeoutDuration(
      ConfigurationSource conf, RaftProperties properties, String configKey) {
    long firstElectionTimeout = conf.getTimeDuration(configKey, -1, TimeUnit.MILLISECONDS);
    if (firstElectionTimeout > 0) {
      RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMin(
          properties,  TimeDuration.valueOf(firstElectionTimeout, TimeUnit.MILLISECONDS));
      RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMax(
          properties,  TimeDuration.valueOf(firstElectionTimeout + 200, TimeUnit.MILLISECONDS));
    }
  }

}
