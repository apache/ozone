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

package org.apache.hadoop.hdds.scm.proxy;

import java.net.InetSocketAddress;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to store SCM proxy info.
 */
public class SCMProxyInfo {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMProxyInfo.class);

  private final String serviceId;
  private final String nodeId;
  private final String rpcAddrStr;
  private final InetSocketAddress rpcAddr;
  /**
   * Original "host:port" config string, preserved so the failover
   * provider can re-resolve DNS on connection failure (Kubernetes pod
   * IP-change recovery). Null when the legacy constructor was used --
   * in that case, refresh-on-failure is disabled for this entry.
   */
  private final String hostAndPort;

  public SCMProxyInfo(String serviceID, String nodeID,
                      InetSocketAddress rpcAddress) {
    this(serviceID, nodeID, rpcAddress, null);
  }

  public SCMProxyInfo(String serviceID, String nodeID,
                      InetSocketAddress rpcAddress, String hostAndPort) {
    Objects.requireNonNull(rpcAddress, "rpcAddress == null");
    this.serviceId = serviceID;
    this.nodeId = nodeID;
    this.rpcAddrStr = rpcAddress.toString();
    this.rpcAddr = rpcAddress;
    this.hostAndPort = hostAndPort;
    if (rpcAddr.isUnresolved()) {
      LOG.warn("SCM address {} for serviceID {} remains unresolved " +
              "for node ID {} Check your ozone-site.xml file to ensure scm " +
              "addresses are configured properly.",
          rpcAddress, serviceId, nodeId);
    }
  }

  /** @return the original config-time host:port string, or null. */
  public String getHostAndPort() {
    return hostAndPort;
  }

  @Override
  public String toString() {
    return "nodeId=" + nodeId + ",nodeAddress=" + rpcAddrStr;
  }

  public InetSocketAddress getAddress() {
    return rpcAddr;
  }

  public String getServiceId() {
    return serviceId;
  }

  public String getNodeId() {
    return nodeId;
  }
}
