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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Construct SCM node details.
 */
public final class SCMNodeDetails extends NodeDetails {
  private final InetSocketAddress blockProtocolServerAddress;
  private final String blockProtocolServerAddressKey;
  private final InetSocketAddress clientProtocolServerAddress;
  private final String clientProtocolServerAddressKey;
  private final InetSocketAddress datanodeProtocolServerAddress;
  private final String datanodeAddressKey;
  private final int grpcPort;

  public static final Logger LOG =
      LoggerFactory.getLogger(SCMNodeDetails.class);

  private SCMNodeDetails(Builder b) {
    super(b.scmServiceId, b.scmNodeId, b.rpcAddress, b.ratisPort, b.httpAddr, b.httpsAddr);
    grpcPort = b.grpcPort;
    blockProtocolServerAddress = b.blockProtocolServerAddress;
    clientProtocolServerAddress = b.clientProtocolServerAddress;
    datanodeProtocolServerAddress = b.datanodeProtocolServerAddress;
    datanodeAddressKey = b.datanodeAddressKey;
    blockProtocolServerAddressKey = b.blockProtocolServerAddressKey;
    clientProtocolServerAddressKey = b.clientProtocolServerAddressKey;
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
      return new SCMNodeDetails(this);
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
