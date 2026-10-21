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

package org.apache.hadoop.hdds;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.hadoop.net.NetUtils;

/**
 * Basic information about nodes in an HA setup.
 */
public abstract class NodeDetails {
  private String serviceId;
  private final String nodeId;
  private String hostAddress;
  private int rpcPort;
  private int ratisPort;
  private String httpAddress;
  private String httpsAddress;

  private InetSocketAddress rpcAddress;

  /**
   * Constructs NodeDetails object.
   */
  public NodeDetails(String serviceId, String nodeId,
      InetSocketAddress rpcAddress, int ratisPort,
      String httpAddress, String httpsAddress) {
    this.serviceId = serviceId;
    this.nodeId = nodeId;
    this.rpcAddress = rpcAddress;
    if (rpcAddress != null) {
      // Keep the configured host. getHostName() would replace an IP literal with its reverse DNS name.
      this.hostAddress = rpcAddress.getHostString();
      this.rpcPort = rpcAddress.getPort();
    }
    this.ratisPort = ratisPort;
    this.httpAddress = httpAddress;
    this.httpsAddress = httpsAddress;
  }

  /**
   * Constructs NodeDetails object.
   */
  public NodeDetails(String serviceId, String nodeId, String hostAddr,
      int rpcPort, int ratisPort, String httpAddress, String httpsAddress) {
    this.serviceId = serviceId;
    this.nodeId = nodeId;
    this.hostAddress = hostAddr;
    this.rpcPort = rpcPort;
    this.ratisPort = ratisPort;
    this.httpAddress = httpAddress;
    this.httpsAddress = httpsAddress;
  }

  public String getServiceId() {
    return serviceId;
  }

  public String getNodeId() {
    return nodeId;
  }

  public String threadNamePrefix() {
    return HddsUtils.threadNamePrefix(nodeId);
  }

  public InetSocketAddress getRpcAddress() {
    if (rpcAddress == null) {
      rpcAddress = NetUtils.createSocketAddr(hostAddress, rpcPort);
    }
    return rpcAddress;
  }

  public boolean isHostUnresolved() {
    return getRpcAddress().isUnresolved();
  }

  public InetAddress getInetAddress() {
    return getRpcAddress().getAddress();
  }

  public String getHostName() {
    return getRpcAddress().getHostName();
  }

  public String getHostAddress() {
    return hostAddress;
  }

  public String getRatisHostPortStr() {
    return HddsUtils.getHostPortString(hostAddress, ratisPort);
  }

  /**
   * Checks whether a Ratis peer address refers to this node. Besides {@link #getRatisHostPortStr()}, it accepts any
   * host that resolves to this node's IP address with the Ratis port, such as a host name stored in the Ratis
   * configuration of an existing cluster.
   */
  public boolean isRatisAddress(String address) {
    if (getRatisHostPortStr().equals(address)) {
      return true;
    }
    final InetAddress inetAddress = getInetAddress();
    if (inetAddress == null) {
      return false;
    }
    try {
      final InetSocketAddress other = NetUtils.createSocketAddr(address);
      return other.getPort() == ratisPort && inetAddress.equals(other.getAddress());
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  public int getRatisPort() {
    return ratisPort;
  }

  public String getRpcAddressString() {
    final InetSocketAddress addr = getRpcAddress();
    return HddsUtils.getHostPortString(addr.getHostName(), addr.getPort());
  }

  public String getHttpAddress() {
    return httpAddress;
  }

  public String getHttpsAddress() {
    return httpsAddress;
  }
}
