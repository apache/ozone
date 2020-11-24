/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.ratis;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.ratis.retrypolicy.RetryPolicyCreator;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ratis helper methods.
 */
public final class RatisHelper {

  private static final Logger LOG = LoggerFactory.getLogger(RatisHelper.class);

  // Prefix for Ratis Server GRPC and Ratis client conf.
  public static final String HDDS_DATANODE_RATIS_PREFIX_KEY = "hdds.ratis";

  /* TODO: use a dummy id for all groups for the moment.
   *       It should be changed to a unique id for each group.
   */
  private static final RaftGroupId DUMMY_GROUP_ID =
      RaftGroupId.valueOf(ByteString.copyFromUtf8("AOzoneRatisGroup"));

  private static final RaftGroup EMPTY_GROUP = RaftGroup.valueOf(DUMMY_GROUP_ID,
      Collections.emptyList());

  private RatisHelper() {
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

  private static String toRaftPeerAddressString(DatanodeDetails id) {
    return id.getIpAddress() + ":" +
        id.getPort(DatanodeDetails.Port.Name.RATIS).getValue();
  }

  public static RaftPeerId toRaftPeerId(DatanodeDetails id) {
    return RaftPeerId.valueOf(toRaftPeerIdString(id));
  }

  public static RaftPeer toRaftPeer(DatanodeDetails id) {
    return RaftPeer.newBuilder()
        .setId(toRaftPeerId(id))
        .setAddress(toRaftPeerAddressString(id))
        .build();
  }

  public static RaftPeer toRaftPeer(DatanodeDetails id, int priority) {
    return RaftPeer.newBuilder()
        .setId(toRaftPeerId(id))
        .setAddress(toRaftPeerAddressString(id))
        .setPriority(priority)
        .build();
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
    return peers.isEmpty()? emptyRaftGroup()
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

  public static RaftClient newRaftClient(RpcType rpcType, Pipeline pipeline,
      RetryPolicy retryPolicy, GrpcTlsConfig tlsConfig,
      ConfigurationSource ozoneConfiguration) throws IOException {
    return newRaftClient(rpcType,
        toRaftPeerId(pipeline.getLeaderNode()),
        newRaftGroup(RaftGroupId.valueOf(pipeline.getId().getId()),
            pipeline.getNodes()), retryPolicy, tlsConfig, ozoneConfiguration);
  }

  private static RpcType getRpcType(ConfigurationSource conf) {
    return SupportedRpcType.valueOfIgnoreCase(conf.get(
        ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY,
        ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_DEFAULT));
  }

  public static RaftClient newRaftClient(RaftPeer leader,
      ConfigurationSource conf, GrpcTlsConfig tlsConfig) {
    return newRaftClient(getRpcType(conf), leader,
        RatisHelper.createRetryPolicy(conf), tlsConfig, conf);
  }

  public static RaftClient newRaftClient(RpcType rpcType, RaftPeer leader,
      RetryPolicy retryPolicy, GrpcTlsConfig tlsConfig,
      ConfigurationSource configuration) {
    return newRaftClient(rpcType, leader.getId(),
        newRaftGroup(Collections.singletonList(leader)), retryPolicy,
        tlsConfig, configuration);
  }

  public static RaftClient newRaftClient(RpcType rpcType, RaftPeer leader,
      RetryPolicy retryPolicy,
      ConfigurationSource ozoneConfiguration) {
    return newRaftClient(rpcType, leader.getId(),
        newRaftGroup(Collections.singletonList(leader)), retryPolicy, null,
        ozoneConfiguration);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private static RaftClient newRaftClient(RpcType rpcType, RaftPeerId leader,
      RaftGroup group, RetryPolicy retryPolicy,
      GrpcTlsConfig tlsConfig, ConfigurationSource ozoneConfiguration) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("newRaftClient: {}, leader={}, group={}",
          rpcType, leader, group);
    }
    final RaftProperties properties = new RaftProperties();

    RaftConfigKeys.Rpc.setType(properties, rpcType);

    // Set the ratis client headers which are matching with regex.
    createRaftClientProperties(ozoneConfiguration, properties);

    RaftClient.Builder builder =  RaftClient.newBuilder()
        .setRaftGroup(group)
        .setLeaderId(leader)
        .setProperties(properties)
        .setRetryPolicy(retryPolicy);

    // TODO: GRPC TLS only for now, netty/hadoop RPC TLS support later.
    if (tlsConfig != null && rpcType == SupportedRpcType.GRPC) {
      builder.setParameters(GrpcFactory.newRaftParameters(tlsConfig));
    }
    return builder.build();
  }

  /**
   * Set all client properties matching with regex
   * {@link RatisHelper#HDDS_DATANODE_RATIS_PREFIX_KEY} in
   * ozone configuration object and configure it to RaftProperties.
   * @param ozoneConf
   * @param raftProperties
   */
  public static void createRaftClientProperties(ConfigurationSource ozoneConf,
      RaftProperties raftProperties) {

    // As for client we do not require server and grpc server/tls. exclude them.
    Map<String, String> ratisClientConf =
        getDatanodeRatisPrefixProps(ozoneConf);
    ratisClientConf.forEach((key, val) -> {
      if (isClientConfig(key) || isGrpcClientConfig(key)) {
        raftProperties.set(key, val);
      }
    });
  }

  private static boolean isClientConfig(String key) {
    return key.startsWith(RaftClientConfigKeys.PREFIX);
  }

  private static boolean isGrpcClientConfig(String key) {
    return key.startsWith(GrpcConfigKeys.PREFIX) && !key
        .startsWith(GrpcConfigKeys.TLS.PREFIX) && !key
        .startsWith(GrpcConfigKeys.Server.PREFIX);
  }
  /**
   * Set all server properties matching with prefix
   * {@link RatisHelper#HDDS_DATANODE_RATIS_PREFIX_KEY} in
   * ozone configuration object and configure it to RaftProperties.
   * @param ozoneConf
   * @param raftProperties
   */
  public static void createRaftServerProperties(ConfigurationSource ozoneConf,
       RaftProperties raftProperties) {

    Map<String, String> ratisServerConf =
        getDatanodeRatisPrefixProps(ozoneConf);
    ratisServerConf.forEach((key, val) -> {
      // Exclude ratis client configuration.
      if (!isClientConfig(key)) {
        raftProperties.set(key, val);
      }
    });
  }


  private static Map<String, String> getDatanodeRatisPrefixProps(
      ConfigurationSource configuration) {
    return configuration.getPropsWithPrefix(
        StringUtils.appendIfNotPresent(HDDS_DATANODE_RATIS_PREFIX_KEY, '.'));
  }

  // For External gRPC client to server with gRPC TLS.
  // No mTLS for external client as SCM CA does not issued certificates for them
  public static GrpcTlsConfig createTlsClientConfig(SecurityConfig conf,
      X509Certificate caCert) {
    GrpcTlsConfig tlsConfig = null;
    if (conf.isSecurityEnabled() && conf.isGrpcTlsEnabled()) {
      tlsConfig = new GrpcTlsConfig(null, null,
          caCert, false);
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
}
