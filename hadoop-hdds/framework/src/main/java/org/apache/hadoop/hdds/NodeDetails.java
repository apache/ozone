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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds;


import org.apache.hadoop.net.NetUtils;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Basic information about nodes in an HA setup.
 */
public abstract class NodeDetails {
  private String serviceId;
  private String nodeId;
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
      this.hostAddress = rpcAddress.getHostName();
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
    StringBuilder hostPort = new StringBuilder();
    hostPort.append(getHostName())
        .append(":")
        .append(ratisPort);
    return hostPort.toString();
  }

  public String getRatisAddressPortStr() {
    StringBuilder hostPort = new StringBuilder();
    hostPort.append(getInetAddress().getHostAddress())
        .append(":")
        .append(ratisPort);
    return hostPort.toString();
  }


  public int getRatisPort() {
    return ratisPort;
  }

  public String getRpcAddressString() {
    return NetUtils.getHostPortString(getRpcAddress());
  }

  public String getHttpAddress() {
    return httpAddress;
  }

  public String getHttpsAddress() {
    return httpsAddress;
  }
}
