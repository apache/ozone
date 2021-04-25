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

import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.hdds.NodeDetails;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Construct SCM node details.
 */
public final class SCMNodeDetails extends NodeDetails {
  private InetSocketAddress blockProtocolServerAddress;
  private String blockProtocolServerAddressKey;
  private InetSocketAddress clientProtocolServerAddress;
  private String clientProtocolServerAddressKey;
  private InetSocketAddress datanodeProtocolServerAddress;
  private String datanodeAddressKey;
  private int grpcPort;
  public static final Logger LOG =
      LoggerFactory.getLogger(SCMNodeDetails.class);

  /**
   * Constructs SCMNodeDetails object.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private SCMNodeDetails(String serviceId, String nodeId,
      InetSocketAddress rpcAddr, int ratisPort, int grpcPort,
      String httpAddress, String httpsAddress,
      InetSocketAddress blockProtocolServerAddress,
      InetSocketAddress clientProtocolServerAddress,
      InetSocketAddress datanodeProtocolServerAddress, RaftGroup group,
      RaftPeerId selfPeerId, String datanodeAddressKey,
      String blockProtocolServerAddressKey,
      String clientProtocolServerAddressAddressKey) {
    super(serviceId, nodeId, rpcAddr, ratisPort,
        httpAddress, httpsAddress);
    this.grpcPort = grpcPort;
    this.blockProtocolServerAddress = blockProtocolServerAddress;
    this.clientProtocolServerAddress = clientProtocolServerAddress;
    this.datanodeProtocolServerAddress = datanodeProtocolServerAddress;
    this.datanodeAddressKey = datanodeAddressKey;
    this.blockProtocolServerAddressKey = blockProtocolServerAddressKey;
    this.clientProtocolServerAddressKey = clientProtocolServerAddressAddressKey;
  }

  @Override
  public String toString() {
    return "SCMNodeDetails["
        + "scmServiceId=" + getServiceId() +
        ", scmNodeId=" + getNodeId() +
        ", rpcAddress=" + getRpcAddressString() +
        ", ratisPort=" + getRatisPort() +
        ", httpAddress=" + getHttpAddress() +
        ", httpsAddress=" + getHttpsAddress() +
        ", blockProtocolServerAddress=" + getBlockProtocolServerAddress() +
        ", clientProtocolServerAddress=" + getClientProtocolServerAddress() +
        ", datanodeProtocolServerAddress=" + getDatanodeProtocolServerAddress()
        + "]";
  }

  /**
   * Builder class for SCMNodeDetails.
   */
  public static class Builder {
    private String scmServiceId;
    private String scmNodeId;
    private InetSocketAddress rpcAddress;
    private int ratisPort;
    private int grpcPort;
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

    public Builder setGrpcPort(int port) {
      this.grpcPort = port;
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
          ratisPort, grpcPort, httpAddr, httpsAddr, blockProtocolServerAddress,
          clientProtocolServerAddress, datanodeProtocolServerAddress,
          raftGroup, selfPeerId, datanodeAddressKey,
          blockProtocolServerAddressKey, clientProtocolServerAddressKey);
    }
  }

  public String getRpcAddressString() {
    return NetUtils.getHostPortString(getRpcAddress());
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

  public int getGrpcPort() {
    return grpcPort;
  }
}
