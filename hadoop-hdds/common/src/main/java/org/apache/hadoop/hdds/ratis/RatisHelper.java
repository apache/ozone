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
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;

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
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ratis helper methods.
 */
public interface RatisHelper {
  Logger LOG = LoggerFactory.getLogger(RatisHelper.class);

  // Ratis Client and Grpc header regex filters.
  String RATIS_CLIENT_HEADER_REGEX = "raft\\.client\\.([a-z\\.]+)";
  String RATIS_GRPC_CLIENT_HEADER_REGEX = "raft\\.grpc\\.(?!server|tls)" +
      "([a-z\\.]+)";

  // Ratis Server header regex filter.
  String RATIS_SERVER_HEADER_REGEX = "raft\\.server\\.([a-z\\.]+)";

  static String toRaftPeerIdString(DatanodeDetails id) {
    return id.getUuidString();
  }

  static UUID toDatanodeId(String peerIdString) {
    return UUID.fromString(peerIdString);
  }

  static UUID toDatanodeId(RaftPeerId peerId) {
    return toDatanodeId(peerId.toString());
  }

  static UUID toDatanodeId(RaftProtos.RaftPeerProto peerId) {
    return toDatanodeId(RaftPeerId.valueOf(peerId.getId()));
  }

  static String toRaftPeerAddressString(DatanodeDetails id) {
    return id.getIpAddress() + ":" +
        id.getPort(DatanodeDetails.Port.Name.RATIS).getValue();
  }

  static RaftPeerId toRaftPeerId(DatanodeDetails id) {
    return RaftPeerId.valueOf(toRaftPeerIdString(id));
  }

  static RaftPeer toRaftPeer(DatanodeDetails id) {
    return new RaftPeer(toRaftPeerId(id), toRaftPeerAddressString(id));
  }

  static List<RaftPeer> toRaftPeers(Pipeline pipeline) {
    return toRaftPeers(pipeline.getNodes());
  }

  static <E extends DatanodeDetails> List<RaftPeer> toRaftPeers(
      List<E> datanodes) {
    return datanodes.stream().map(RatisHelper::toRaftPeer)
        .collect(Collectors.toList());
  }

  /* TODO: use a dummy id for all groups for the moment.
   *       It should be changed to a unique id for each group.
   */
  RaftGroupId DUMMY_GROUP_ID =
      RaftGroupId.valueOf(ByteString.copyFromUtf8("AOzoneRatisGroup"));

  RaftGroup EMPTY_GROUP = RaftGroup.valueOf(DUMMY_GROUP_ID,
      Collections.emptyList());

  static RaftGroup emptyRaftGroup() {
    return EMPTY_GROUP;
  }

  static RaftGroup newRaftGroup(Collection<RaftPeer> peers) {
    return peers.isEmpty()? emptyRaftGroup()
        : RaftGroup.valueOf(DUMMY_GROUP_ID, peers);
  }

  static RaftGroup newRaftGroup(RaftGroupId groupId,
      Collection<DatanodeDetails> peers) {
    final List<RaftPeer> newPeers = peers.stream()
        .map(RatisHelper::toRaftPeer)
        .collect(Collectors.toList());
    return peers.isEmpty() ? RaftGroup.valueOf(groupId, Collections.emptyList())
        : RaftGroup.valueOf(groupId, newPeers);
  }

  static RaftGroup newRaftGroup(Pipeline pipeline) {
    return RaftGroup.valueOf(RaftGroupId.valueOf(pipeline.getId().getId()),
        toRaftPeers(pipeline));
  }

  static RaftClient newRaftClient(RpcType rpcType, Pipeline pipeline,
      RetryPolicy retryPolicy, int maxOutStandingRequest,
      GrpcTlsConfig tlsConfig, TimeDuration timeout,
      Configuration ozoneConfiguration) throws IOException {
    return newRaftClient(rpcType,
        toRaftPeerId(pipeline.getLeaderNode()),
        newRaftGroup(RaftGroupId.valueOf(pipeline.getId().getId()),
            pipeline.getNodes()), retryPolicy, maxOutStandingRequest, tlsConfig,
        timeout, ozoneConfiguration);
  }

  static TimeDuration getClientRequestTimeout(Configuration conf) {
    // Set the client requestTimeout
    final TimeUnit timeUnit =
        OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_DEFAULT
            .getUnit();
    final long duration = conf.getTimeDuration(
        OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_KEY,
        OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_DEFAULT
            .getDuration(), timeUnit);
    return TimeDuration.valueOf(duration, timeUnit);
  }

  static RpcType getRpcType(Configuration conf) {
    return SupportedRpcType.valueOfIgnoreCase(conf.get(
        ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY,
        ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_DEFAULT));
  }

  static RaftClient newRaftClient(RaftPeer leader, Configuration conf) {
    return newRaftClient(getRpcType(conf), leader, RetryPolicies.noRetry(),
        GrpcConfigKeys.OutputStream.OUTSTANDING_APPENDS_MAX_DEFAULT,
        getClientRequestTimeout(conf), conf);
  }

  static RaftClient newRaftClient(RpcType rpcType, RaftPeer leader,
      RetryPolicy retryPolicy, int maxOutstandingRequests,
      GrpcTlsConfig tlsConfig, TimeDuration clientRequestTimeout,
      Configuration configuration) {
    return newRaftClient(rpcType, leader.getId(),
        newRaftGroup(Collections.singletonList(leader)), retryPolicy,
        maxOutstandingRequests, tlsConfig, clientRequestTimeout, configuration);
  }

  static RaftClient newRaftClient(RpcType rpcType, RaftPeer leader,
      RetryPolicy retryPolicy, int maxOutstandingRequests,
      TimeDuration clientRequestTimeout,
      Configuration ozoneConfiguration) {
    return newRaftClient(rpcType, leader.getId(),
        newRaftGroup(Collections.singletonList(leader)), retryPolicy,
        maxOutstandingRequests, null, clientRequestTimeout,
        ozoneConfiguration);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  static RaftClient newRaftClient(RpcType rpcType, RaftPeerId leader,
      RaftGroup group, RetryPolicy retryPolicy, int maxOutStandingRequest,
      GrpcTlsConfig tlsConfig, TimeDuration clientRequestTimeout,
      Configuration ozoneConfiguration) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("newRaftClient: {}, leader={}, group={}",
          rpcType, leader, group);
    }
    final RaftProperties properties = new RaftProperties();

    // Set the ratis client headers which are matching with regex.
    createRaftClientProperties(ozoneConfiguration, properties);

    // Set the ratis grpc client headers which are matching with regex.
    createRaftGrpcProperties(ozoneConfiguration, properties);

    RaftConfigKeys.Rpc.setType(properties, rpcType);
    RaftClientConfigKeys.Rpc
        .setRequestTimeout(properties, clientRequestTimeout);

    GrpcConfigKeys.setMessageSizeMax(properties,
        SizeInBytes.valueOf(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE));

    // set async max outstanding requests.
    RaftClientConfigKeys.Async.setMaxOutstandingRequests(properties,
        maxOutStandingRequest);

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
   * Set all the properties matching with regex RATIS_CLIENT_HEADER_REGEX in
   * ozone configuration object and configure it to RaftProperties.
   * @param ozoneConf
   * @param raftProperties
   */
  static void createRaftClientProperties(Configuration ozoneConf,
      RaftProperties raftProperties) {
    Map<String, String> ratisClientConf =
        ozoneConf.getValByRegex(RATIS_CLIENT_HEADER_REGEX);
    ratisClientConf.forEach((key, val) -> raftProperties.set(key, val));
  }

  /**
   * Set all the properties matching with regex
   * {@link RatisHelper#RATIS_GRPC_CLIENT_HEADER_REGEX} in ozone
   * configuration object and configure it to RaftProperties.
   * @param ozoneConf
   * @param raftProperties
   */
  static void createRaftGrpcProperties(Configuration ozoneConf,
      RaftProperties raftProperties) {
    Map<String, String> ratisClientConf =
        ozoneConf.getValByRegex(RATIS_GRPC_CLIENT_HEADER_REGEX);
    ratisClientConf.forEach((key, val) -> raftProperties.set(key, val));
  }

  /**
   * Set all the properties matching with regex
   * {@link RatisHelper#RATIS_SERVER_HEADER_REGEX} in ozone configuration
   * object and configure it to RaftProperties.
   * @param ozoneConf
   * @param raftProperties
   */
  static void createRaftServerProperties(Configuration ozoneConf,
       RaftProperties raftProperties) {
    Map<String, String> ratisServerConf =
        ozoneConf.getValByRegex(RATIS_SERVER_HEADER_REGEX);
    ratisServerConf.forEach((key, val) -> raftProperties.set(key, val));
  }

  // For External gRPC client to server with gRPC TLS.
  // No mTLS for external client as SCM CA does not issued certificates for them
  static GrpcTlsConfig createTlsClientConfig(SecurityConfig conf,
      X509Certificate caCert) {
    GrpcTlsConfig tlsConfig = null;
    if (conf.isSecurityEnabled() && conf.isGrpcTlsEnabled()) {
      tlsConfig = new GrpcTlsConfig(null, null,
          caCert, false);
    }
    return tlsConfig;
  }

  // For Internal gRPC client from SCM to DN with gRPC TLS
  static GrpcTlsConfig createTlsClientConfigForSCM(SecurityConfig conf,
      CertificateServer certificateServer) throws IOException {
    if (conf.isSecurityEnabled() && conf.isGrpcTlsEnabled()) {
      try {
        X509Certificate caCert =
            CertificateCodec.getX509Certificate(
                certificateServer.getCACertificate());
        return new GrpcTlsConfig(null, null,
            caCert, false);
      } catch (CertificateException ex) {
        throw new SCMSecurityException("Fail to find SCM CA certificate.", ex);
      }
    }
    return null;
  }

  // For gRPC server running DN container service with gPRC TLS
  // No mTLS as the channel is shared for for external client, which
  // does not have SCM CA issued certificates.
  // In summary:
  // authenticate from server to client is via TLS.
  // authenticate from client to server is via block token (or container token).
  static GrpcTlsConfig createTlsServerConfigForDN(SecurityConfig conf,
      CertificateClient caClient)  {
    if (conf.isSecurityEnabled() && conf.isGrpcTlsEnabled()) {
      return new GrpcTlsConfig(
          caClient.getPrivateKey(), caClient.getCertificate(),
          null, false);
    }
    return null;
  }

  static RetryPolicy createRetryPolicy(Configuration conf) {
    int maxRetryCount =
        conf.getInt(OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_MAX_RETRIES_KEY,
            OzoneConfigKeys.
                DFS_RATIS_CLIENT_REQUEST_MAX_RETRIES_DEFAULT);
    long retryInterval = conf.getTimeDuration(OzoneConfigKeys.
        DFS_RATIS_CLIENT_REQUEST_RETRY_INTERVAL_KEY, OzoneConfigKeys.
        DFS_RATIS_CLIENT_REQUEST_RETRY_INTERVAL_DEFAULT
        .toIntExact(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
    TimeDuration sleepDuration =
        TimeDuration.valueOf(retryInterval, TimeUnit.MILLISECONDS);
    RetryPolicy retryPolicy = RetryPolicies
        .retryUpToMaximumCountWithFixedSleep(maxRetryCount, sleepDuration);
    return retryPolicy;
  }

  static Long getMinReplicatedIndex(
      Collection<RaftProtos.CommitInfoProto> commitInfos) {
    return commitInfos.stream().map(RaftProtos.CommitInfoProto::getCommitIndex)
        .min(Long::compareTo).orElse(null);
  }
}
