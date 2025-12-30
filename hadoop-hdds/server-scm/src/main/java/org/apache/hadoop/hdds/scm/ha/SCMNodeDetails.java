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

package org.apache.hadoop.hdds.scm.ha;

import java.net.InetSocketAddress;
import org.apache.hadoop.hdds.NodeDetails;
import org.apache.hadoop.net.NetUtils;

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

  @Override
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
