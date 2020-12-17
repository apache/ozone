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
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalInt;

import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.hdds.HddsUtils.getPortNumberFromConfigKeys;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY;
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
  private InetSocketAddress blockProtocolServerAddress;
  private String blockProtocolServerAddressKey;
  private InetSocketAddress clientProtocolServerAddress;
  private String clientProtocolServerAddressKey;
  private InetSocketAddress datanodeProtocolServerAddress;
  private String datanodeAddressKey;
  private RaftGroup raftGroup;
  private RaftPeerId selfPeerId;

  public static final Logger LOG =
      LoggerFactory.getLogger(SCMNodeDetails.class);

  /**
   * Constructs SCMNodeDetails object.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private SCMNodeDetails(String serviceId, String nodeId,
      InetSocketAddress rpcAddr, int ratisPort, String httpAddress,
      String httpsAddress, InetSocketAddress blockProtocolServerAddress,
      InetSocketAddress clientProtocolServerAddress,
      InetSocketAddress datanodeProtocolServerAddress,
      RaftGroup group, RaftPeerId selfPeerId, String datanodeAddressKey,
      String blockProtocolServerAddressKey,
      String clientProtocolServerAddressAddressKey) {
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
    this.datanodeAddressKey = datanodeAddressKey;
    this.blockProtocolServerAddressKey = blockProtocolServerAddressKey;
    this.clientProtocolServerAddressKey = clientProtocolServerAddressAddressKey;
  }

  @Override
  public String toString() {
    // TODO: add new fields to toString
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
    private InetSocketAddress blockProtocolServerAddress;
    private String blockProtocolServerAddressKey;
    private InetSocketAddress clientProtocolServerAddress;
    private String clientProtocolServerAddressKey;
    private InetSocketAddress datanodeProtocolServerAddress;
    private String datanodeAddressKey;
    private RaftGroup raftGroup;
    private RaftPeerId selfPeerId;

    public Builder setDatanodeAddressKey(String addressKey) {
      this.datanodeAddressKey = addressKey;
      return this;
    }

    public Builder setBlockProtocolServerAddressKey(String addressKey) {
      this.blockProtocolServerAddressKey = addressKey;
      return this;
    }

    public Builder setBlockProtocolServerAddress(InetSocketAddress address) {
      this.blockProtocolServerAddress = address;
      return this;
    }

    public Builder setClientProtocolServerAddress(InetSocketAddress address) {
      this.clientProtocolServerAddress = address;
      return this;
    }

    public Builder setClientProtocolServerAddressKey(String addressKey) {
      this.clientProtocolServerAddressKey = addressKey;
      return this;
    }

    public Builder setDatanodeProtocolServerAddress(InetSocketAddress address) {
      this.datanodeProtocolServerAddress = address;
      return this;
    }

    public Builder setRaftGroup(RaftGroup group) {
      this.raftGroup = group;
      return this;
    }

    public Builder setSelfPeerId(RaftPeerId peerId) {
      this.selfPeerId = peerId;
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
          raftGroup, selfPeerId, datanodeAddressKey,
          blockProtocolServerAddressKey, clientProtocolServerAddressKey);
    }
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

  public InetSocketAddress getClientProtocolServerAddress() {
    return clientProtocolServerAddress;
  }

  public String getClientProtocolServerAddressKey() {
    return clientProtocolServerAddressKey;
  }

  public InetSocketAddress getBlockProtocolServerAddress() {
    return blockProtocolServerAddress;
  }

  public String getBlockProtocolServerAddressKey() {
    return blockProtocolServerAddressKey;
  }

  public InetSocketAddress getDatanodeProtocolServerAddress() {
    return datanodeProtocolServerAddress;
  }

  public String getDatanodeAddressKey() {
    return datanodeAddressKey;
  }

  public static SCMNodeDetails loadDefaultConfig(
      OzoneConfiguration conf) throws IOException {
    int ratisPort = conf.getInt(
        ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY,
        ScmConfigKeys.OZONE_SCM_RATIS_PORT_DEFAULT);
    InetSocketAddress rpcAddress = new InetSocketAddress(
        InetAddress.getLocalHost(), 0);
    SCMNodeDetails scmNodeDetails = new SCMNodeDetails.Builder()
        .setRatisPort(ratisPort)
        .setRpcAddress(rpcAddress)
        .setDatanodeProtocolServerAddress(
            HddsServerUtil.getScmDataNodeBindAddress(conf))
        .setDatanodeAddressKey(OZONE_SCM_DATANODE_ADDRESS_KEY)
        .setBlockProtocolServerAddress(
            HddsServerUtil.getScmBlockClientBindAddress(conf))
        .setBlockProtocolServerAddressKey(
            ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY)
        .setClientProtocolServerAddress(
            HddsServerUtil.getScmClientBindAddress(conf))
        .setClientProtocolServerAddressKey(
            ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY)
        .build();
    return scmNodeDetails;
  }

  public static SCMNodeDetails loadSCMHAConfig(
      OzoneConfiguration conf) throws IOException {
    SCMNodeDetails.Builder builder = new Builder();

    String localScmServiceId = null;
    String localScmNodeId = null;

    Collection<String> scmServiceIds;

    localScmServiceId = conf.getTrimmed(
        ScmConfigKeys.OZONE_SCM_INTERNAL_SERVICE_ID);

    LOG.info("ServiceID for StorageContainerManager is {}", localScmServiceId);

    if (localScmServiceId == null) {
      // There is no internal scm service id is being set, fall back to ozone
      // .scm.service.ids.
      LOG.info("{} is not defined, falling back to {} to find serviceID for "
              + "StorageContainerManager if it is HA enabled cluster",
          OZONE_SCM_INTERNAL_SERVICE_ID, OZONE_SCM_SERVICE_IDS_KEY);
      scmServiceIds = conf.getTrimmedStringCollection(
          OZONE_SCM_SERVICE_IDS_KEY);
    } else {
      LOG.info("ServiceID for StorageContainerManager is {}",
          localScmServiceId);
      builder.setSCMServiceId(localScmServiceId);
      scmServiceIds = Collections.singleton(localScmServiceId);
    }

    localScmNodeId = conf.get(ScmConfigKeys.OZONE_SCM_NODE_ID_KEY);
    boolean isScmHAConfigSet = false;
    for (String serviceId : scmServiceIds) {
      Collection<String> scmNodeIds = ScmUtils.getSCMNodeIds(conf, serviceId);

      if (scmNodeIds.size() == 0) {
        throw new IllegalArgumentException(
            String.format("Configuration does not have any value set for %s " +
            "for the service %s. List of OM Node ID's should be specified " +
            "for an OM service", ScmConfigKeys.OZONE_SCM_NODES_KEY, serviceId));
      }
      // TODO: load Ratis peers configuration
      isScmHAConfigSet = true;
    }

    if (!isScmHAConfigSet) {
      // If HA config is not set properly, fall back to default configuration
      return loadDefaultConfig(conf);
    }

    builder
        .setBlockProtocolServerAddress(getScmBlockProtocolServerAddress(
            conf, localScmServiceId, localScmNodeId))
        .setBlockProtocolServerAddressKey(
            ScmUtils.addKeySuffixes(
                ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
                localScmServiceId, localScmNodeId))
        .setClientProtocolServerAddress(getClientProtocolServerAddress(conf,
            localScmServiceId, localScmNodeId))
        .setClientProtocolServerAddressKey(
            ScmUtils.addKeySuffixes(
                ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY,
                localScmServiceId, localScmNodeId))
        .setDatanodeProtocolServerAddress(
            getScmDataNodeBindAddress(conf, localScmServiceId, localScmNodeId))
        .setDatanodeAddressKey(
            ScmUtils.addKeySuffixes(
                ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY,
                localScmServiceId, localScmNodeId));

    return builder.build();
  }

  private static InetSocketAddress getScmBlockProtocolServerAddress(
      OzoneConfiguration conf, String localScmServiceId, String nodeId) {
    String bindHostKey = ScmUtils.addKeySuffixes(
        ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_BIND_HOST_KEY,
        localScmServiceId, nodeId);
    final Optional<String> host = getHostNameFromConfigKeys(conf, bindHostKey);

    String addressKey = ScmUtils.addKeySuffixes(
        ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
        localScmServiceId, nodeId);
    final OptionalInt port = getPortNumberFromConfigKeys(conf, addressKey);

    return NetUtils.createSocketAddr(
        host.orElse(
            ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_BIND_HOST_DEFAULT) + ":" +
            port.orElse(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT));
  }

  private static InetSocketAddress getClientProtocolServerAddress(
      OzoneConfiguration conf, String localScmServiceId, String nodeId) {
    String bindHostKey = ScmUtils.addKeySuffixes(
        ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_KEY,
        localScmServiceId, nodeId);

    final String host = getHostNameFromConfigKeys(conf, bindHostKey)
        .orElse(ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_DEFAULT);

    String addressKey = ScmUtils.addKeySuffixes(
        ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY,
        localScmServiceId, nodeId);

    final int port = getPortNumberFromConfigKeys(conf, addressKey)
        .orElse(ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT);

    return NetUtils.createSocketAddr(host + ":" + port);
  }

  private static InetSocketAddress getScmDataNodeBindAddress(
      ConfigurationSource conf, String localScmServiceId, String nodeId) {
    String bindHostKey = ScmUtils.addKeySuffixes(
        ScmConfigKeys.OZONE_SCM_DATANODE_BIND_HOST_KEY,
        localScmServiceId, nodeId
    );
    final Optional<String> host = getHostNameFromConfigKeys(conf, bindHostKey);
    String addressKey = ScmUtils.addKeySuffixes(
        ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY,
        localScmServiceId, nodeId
    );
    final OptionalInt port = getPortNumberFromConfigKeys(conf, addressKey);

    return NetUtils.createSocketAddr(
        host.orElse(ScmConfigKeys.OZONE_SCM_DATANODE_BIND_HOST_DEFAULT) + ":" +
            port.orElse(ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT));
  }
}
