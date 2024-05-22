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

package org.apache.hadoop.ozone.admin.ratis;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.client.ClientTrustManager;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CACertificateProvider;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.proto.RaftProtos.FollowerInfoProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.rpc.SupportedRpcType;

import java.util.List;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.ratis.shell.cli.RaftUtils.buildRaftGroupIdFromStr;
import static org.apache.ratis.shell.cli.RaftUtils.buildRaftPeersFromStr;
import static org.apache.ratis.shell.cli.RaftUtils.retrieveGroupInfoByGroupId;
import static org.apache.ratis.shell.cli.RaftUtils.retrieveRemoteGroupId;

/**
 * The base class for all the ratis command.
 */
public class BaseRatisCommand {
  private RaftGroup raftGroup;
  private GroupInfoReply groupInfoReply;
  private OzoneConfiguration config;
  private String omServiceId;

  public void run(String peers, String groupid, String omServiceId) throws Exception {
    this.config = new OzoneConfiguration();
    this.omServiceId = omServiceId;
    List<RaftPeer> peerList = buildRaftPeersFromStr(peers);
    RaftGroupId raftGroupIdFromConfig = buildRaftGroupIdFromStr(groupid);
    raftGroup = RaftGroup.valueOf(raftGroupIdFromConfig, peerList);

    try (RaftClient client = buildRaftClient(raftGroup)) {
      RaftGroupId remoteGroupId = retrieveRemoteGroupId(raftGroupIdFromConfig, peerList, client, System.out);
      groupInfoReply = retrieveGroupInfoByGroupId(remoteGroupId, peerList, client, System.out);
      raftGroup = groupInfoReply.getGroup();
    }

  }

  protected RaftClient buildRaftClient(RaftGroup raftGroup) throws Exception {
    GrpcTlsConfig tlsConfig = null;
    if (config.getBoolean(OZONE_SECURITY_ENABLED_KEY, OZONE_SECURITY_ENABLED_DEFAULT) &&
        config.getBoolean(HDDS_GRPC_TLS_ENABLED, HDDS_GRPC_TLS_ENABLED_DEFAULT)) {
      tlsConfig = createGrpcTlsConf(omServiceId);
    }

    return RatisHelper.newRaftClient(
        SupportedRpcType.GRPC, null, raftGroup,
        RatisHelper.createRetryPolicy(config), tlsConfig, config);
  }

  public RaftGroup getRaftGroup() {
    return raftGroup;
  }

  public GroupInfoReply getGroupInfoReply() {
    return groupInfoReply;
  }

  /**
   * Get the leader id.
   *
   * @param roleInfo the role info
   * @return the leader id
   */
  protected RaftPeerProto getLeader(RoleInfoProto roleInfo) {
    if (roleInfo == null) {
      return null;
    }
    if (roleInfo.getRole() == RaftPeerRole.LEADER) {
      return roleInfo.getSelf();
    }
    FollowerInfoProto followerInfo = roleInfo.getFollowerInfo();
    if (followerInfo == null) {
      return null;
    }
    return followerInfo.getLeaderInfo().getId();
  }

  private GrpcTlsConfig createGrpcTlsConf(String omServiceId) throws Exception {
    OzoneClient certClient = OzoneClientFactory.getRpcClient(omServiceId,
        config);

    ServiceInfoEx serviceInfoEx = certClient
        .getObjectStore()
        .getClientProxy()
        .getOzoneManagerClient()
        .getServiceInfo();

    CACertificateProvider remoteCAProvider =
        serviceInfoEx::provideCACerts;
    ClientTrustManager trustManager = new ClientTrustManager(remoteCAProvider, serviceInfoEx);

    return RatisHelper.createTlsClientConfig(new SecurityConfig(config), trustManager);
  }
}
