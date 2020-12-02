/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ScmUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import sun.nio.ch.Net;

import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.hdds.HddsUtils.getPortNumberFromConfigKeys;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_INTERNAL_SERVICE_ID;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY;

/**
 * Construct SCM node details.
 */
public final class SCMNodeDetails {
  private String scmServiceId;
  private String scmNodeId;
  private InetSocketAddress rpcAddress;
  private int ratisPort;
  private String httpAddress;
  private String httpsAddress;
  private String blockProtocolServerAddress;
  private String clientProtocolServerAddress;
  private String datanodeProtocolServerAddress;
  private RaftGroup raftGroup;
  private RaftPeerId selfPeerId;

  public static final Logger LOG =
      LoggerFactory.getLogger(SCMNodeDetails.class);

  /**
   * Constructs SCMNodeDetails object.
   */
  private SCMNodeDetails(String serviceId, String nodeId,
                        InetSocketAddress rpcAddr, int ratisPort,
                        String httpAddress, String httpsAddress,
                        String blockProtocolServerAddress,
                        String clientProtocolServerAddress,
                        String datanodeProtocolServerAddress,
                        RaftGroup group,
                        RaftPeerId selfPeerId) {
    this.scmServiceId = serviceId;
    this.scmNodeId = nodeId;
    this.rpcAddress = rpcAddr;
    this.ratisPort = ratisPort;
    this.httpAddress = httpAddress;
    this.httpsAddress = httpsAddress;
    this.blockProtocolServerAddress = blockProtocolServerAddress;
    this.clientProtocolServerAddress = clientProtocolServerAddress;
    this.datanodeProtocolServerAddress = datanodeProtocolServerAddress;
    this.raftGroup = group;
    this.selfPeerId = selfPeerId;
  }

  @Override
  public String toString() {
    return "SCMNodeDetails["
        + "scmServiceId=" + scmServiceId +
        ", scmNodeId=" + scmNodeId +
        ", rpcAddress=" + rpcAddress +
        ", ratisPort=" + ratisPort +
        ", httpAddress=" + httpAddress +
        ", httpsAddress=" + httpsAddress +
        "]";
  }

  /**
   * Builder class for SCMNodeDetails.
   */
  public static class Builder {
    private String scmServiceId;
    private String scmNodeId;
    private InetSocketAddress rpcAddress;
    private int ratisPort;
    private String httpAddr;
    private String httpsAddr;
    private String blockProtocolServerAddress;
    private String clientProtocolServerAddress;
    private String datanodeProtocolServerAddress;
    private RaftGroup raftGroup;
    private RaftPeerId selfPeerId;

    public Builder setBlockProtocolServerAddress(String address) {
      this.blockProtocolServerAddress = address;
      return this;
    }

    public Builder setClientProtocolServerAddress(String address) {
      this.clientProtocolServerAddress = address;
      return this;
    }

    public Builder setDatanodeProtocolServerAddress(String address) {
      this.datanodeProtocolServerAddress = address;
      return this;
    }

    public Builder setRaftGroup(RaftGroup group) {
      this.raftGroup = group;
      return this;
    }

    public Builder setSelfPeerId(RaftPeerId selfPeerId) {
      this.selfPeerId = selfPeerId;
      return this;
    }

    public Builder setRpcAddress(InetSocketAddress rpcAddr) {
      this.rpcAddress = rpcAddr;
      return this;
    }

    public Builder setRatisPort(int port) {
      this.ratisPort = port;
      return this;
    }

    public Builder setSCMServiceId(String serviceId) {
      this.scmServiceId = serviceId;
      return this;
    }

    public Builder setSCMNodeId(String nodeId) {
      this.scmNodeId = nodeId;
      return this;
    }

    public Builder setHttpAddress(String httpAddress) {
      this.httpAddr = httpAddress;
      return this;
    }

    public Builder setHttpsAddress(String httpsAddress) {
      this.httpsAddr = httpsAddress;
      return this;
    }

    public SCMNodeDetails build() {
      return new SCMNodeDetails(scmServiceId, scmNodeId, rpcAddress,
          ratisPort, httpAddr, httpsAddr, blockProtocolServerAddress,
          clientProtocolServerAddress, datanodeProtocolServerAddress,
          raftGroup, selfPeerId);
    }
  }

  public RaftPeerId getSelfPeerId() {
    return selfPeerId;
  }

  public RaftGroupId getRaftGroupId() {
    return raftGroup.getGroupId();
  }

  public RaftGroup getRaftGroup() {
    return raftGroup;
  }

  public String getSCMServiceId() {
    return scmServiceId;
  }

  public String getSCMNodeId() {
    return scmNodeId;
  }

  public InetSocketAddress getRpcAddress() {
    return rpcAddress;
  }

  public InetAddress getAddress() {
    return rpcAddress.getAddress();
  }

  public int getRatisPort() {
    return ratisPort;
  }

  public String getRpcAddressString() {
    return NetUtils.getHostPortString(rpcAddress);
  }

  public InetSocketAddress getDatanodeProtocolServerAddress() {
    return NetUtils.createSocketAddr(datanodeProtocolServerAddress);
  }

  public InetSocketAddress getClientProtocolServerAddress() {
    return NetUtils.createSocketAddr(clientProtocolServerAddress);
  }

  public InetSocketAddress getBlockProtocolServerAddress() {
    return NetUtils.createSocketAddr(blockProtocolServerAddress);
  }

  public static SCMNodeDetails initStandAlone(
      OzoneConfiguration conf) throws IOException {
    String localSCMServiceId = conf.getTrimmed(OZONE_SCM_INTERNAL_SERVICE_ID);
    if (localSCMServiceId == null) {
      // There is no internal om service id is being set, fall back to ozone
      // .om.service.ids.
      LOG.info("{} is not defined, falling back to {} to find serviceID for "
              + "SCM if it is HA enabled cluster",
          OZONE_SCM_INTERNAL_SERVICE_ID, OZONE_SCM_SERVICE_IDS_KEY);
      localSCMServiceId = conf.getTrimmed(
          OZONE_SCM_SERVICE_IDS_KEY);
    } else {
      LOG.info("ServiceID for SCM is {}", localSCMServiceId);
    }
    String localSCMNodeId = SCMHAUtils.getLocalSCMNodeId(localSCMServiceId);
    int ratisPort = conf.getInt(
        ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY,
        ScmConfigKeys.OZONE_SCM_RATIS_PORT_DEFAULT);
    InetSocketAddress rpcAddress = new InetSocketAddress(
        InetAddress.getLocalHost(), 0);
    SCMNodeDetails scmNodeDetails = new SCMNodeDetails.Builder()
        .setRatisPort(ratisPort)
        .setRpcAddress(rpcAddress)
        .setSCMNodeId(localSCMNodeId)
        .setSCMServiceId(localSCMServiceId)
        .build();
    return scmNodeDetails;
  }

  public static SCMNodeDetails initHA(
          OzoneConfiguration conf) throws IOException {
    String localScmServiceId = conf.getTrimmed(ScmConfigKeys.OZONE_SCM_INTERNAL_SERVICE_ID);
    if (localScmServiceId == null) {
      throw new IOException("serviceId is not provided");
    }

    LOG.info("ServiceID for StorageContainerManager is {}", localScmServiceId);
    String scmNodeId = conf.get(ScmConfigKeys.OZONE_SCM_NODE_ID_KEY);
    if (scmNodeId == null) {
      String errorMessage = "SCM NODE ID is not provided by "
              + ScmConfigKeys.OZONE_SCM_NODE_ID_KEY;
      throw new IOException(errorMessage);
    }
    RaftPeerId selfPeerId = RaftPeerId.getRaftPeerId(scmNodeId);
    InetSocketAddress thisRatisAddr = null;
    Collection<String> scmNodeIds = ScmUtils.getSCMNodeIds(conf, localScmServiceId);
    final List<RaftPeer> raftPeers = new ArrayList<>();
    for (String nodeId : scmNodeIds) {
      RaftPeerId peerId = RaftPeerId.getRaftPeerId(nodeId);
      String ratisAddrKey = ScmUtils.addKeySuffixes(ScmConfigKeys.OZONE_SCM_RATIS_BIND_ADDRESS_KEY,
              localScmServiceId, nodeId);
      String ratisPortKey = ScmUtils.addKeySuffixes(ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY,
              localScmServiceId, nodeId);
      String ratisAddr = conf.get(ratisAddrKey);
      int ratisPort = conf.getInt(ratisPortKey, 0);
      if (nodeId.equals(scmNodeId)) {
        thisRatisAddr = new InetSocketAddress(ratisAddr, ratisPort);
      }
      raftPeers.add(new RaftPeer(peerId, ratisAddr + ":" + ratisPort));
    }

    LOG.info("Building a RaftGroup for Scm HA: {} ", raftPeers);

    RaftGroupId raftGroupId = RaftGroupId.valueOf(UUID.nameUUIDFromBytes(
            localScmServiceId.getBytes(StandardCharsets.UTF_8)));
    RaftGroup raftGroup = RaftGroup.valueOf(raftGroupId, raftPeers);

    SCMNodeDetails.Builder builder = new Builder();
    builder.setSCMNodeId(scmNodeId)
            .setSCMServiceId(localScmServiceId)
            .setRatisPort(thisRatisAddr.getPort())
            .setRaftGroup(raftGroup)
            .setSelfPeerId(selfPeerId)
            .setHttpAddress(loadHAHttpAddress(conf, localScmServiceId, scmNodeId))
            .setHttpsAddress(loadHAHttpsAddress(conf, localScmServiceId, scmNodeId))
            .setBlockProtocolServerAddress(loadHAScmBlockProtocolServerAddress(conf, localScmServiceId, scmNodeId))
            .setClientProtocolServerAddress(loadHAClientProtocolServerAddress(conf, localScmServiceId, scmNodeId))
            .setDatanodeProtocolServerAddress(loadHADatanodeProtocolServerAddress(conf, localScmServiceId, scmNodeId));

    return builder.build();
  }

  private static String loadHAScmBlockProtocolServerAddress(OzoneConfiguration config,
      String localScmServiceId, String nodeId) {
    String addressKey = ScmUtils.addKeySuffixes(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
        localScmServiceId, nodeId);
    return config.get(addressKey);
  }

  private static String loadHAClientProtocolServerAddress(OzoneConfiguration config,
      String localScmServiceId, String nodeId) {
    String addressKey = ScmUtils.addKeySuffixes(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY,
        localScmServiceId, nodeId);
    return config.get(addressKey);
  }

  private static String loadHADatanodeProtocolServerAddress(OzoneConfiguration config,
      String localScmServiceId, String nodeId) {
    String addressKey = ScmUtils.addKeySuffixes(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY,
        localScmServiceId, nodeId);
    return config.get(addressKey);
  }

  private static String loadHAHttpAddress(OzoneConfiguration config,
      String localScmServiceId, String nodeId) {
    String addressKey = ScmUtils.addKeySuffixes(ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY,
        localScmServiceId, nodeId);
    return config.get(addressKey);
  }

  private static String loadHAHttpsAddress(OzoneConfiguration config,
      String localScmServiceId, String nodeId) {
    String addressKey = ScmUtils.addKeySuffixes(ScmConfigKeys.OZONE_SCM_HTTPS_ADDRESS_KEY,
        localScmServiceId, nodeId);
    return config.get(addressKey);
  }
}
