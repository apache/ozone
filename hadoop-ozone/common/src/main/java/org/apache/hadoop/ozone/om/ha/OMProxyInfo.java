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

package org.apache.hadoop.ozone.om.ha;

import java.net.InetSocketAddress;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.FailoverProxyProvider.ProxyInfo;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to store OM proxy information.
 */
public class OMProxyInfo<T> extends ProxyInfo<T> {
  private final String nodeId;
  private final String rpcAddrStr;
  private final InetSocketAddress rpcAddr;
  private final Text dtService;

  private static final Logger LOG =
      LoggerFactory.getLogger(OMProxyInfo.class);

  public OMProxyInfo(T proxy, String serviceID, String nodeID, String rpcAddress, String proxyInfo) {
    super(proxy, proxyInfo);
    this.nodeId = nodeID;
    this.rpcAddrStr = rpcAddress;
    this.rpcAddr = NetUtils.createSocketAddr(rpcAddrStr);
    if (rpcAddr.isUnresolved()) {
      LOG.warn("OzoneManager address {} for serviceID {} remains unresolved " +
              "for node ID {} Check your ozone-site.xml file to ensure ozone " +
              "manager addresses are configured properly.",
          rpcAddress, serviceID, nodeId);
      this.dtService = null;
    } else {
      // This issue will be a problem with docker/kubernetes world where one of
      // the container is killed, and that OM address will be unresolved.
      // For now skip the unresolved OM address setting it to the token
      // service field.
      this.dtService = SecurityUtil.buildTokenService(rpcAddr);
    }
  }

  public OMProxyInfo(String serviceID, String nodeID, String rpcAddress) {
    this(null, serviceID, nodeID, rpcAddress, "nodeId=" + nodeID + ",nodeAddress=" + rpcAddress);
  }

  public String getNodeId() {
    return nodeId;
  }

  public String getRpcAddrStr() {
    return rpcAddrStr;
  }

  public InetSocketAddress getRpcAddr() {
    return rpcAddr;
  }

  public InetSocketAddress getAddress() {
    return rpcAddr;
  }

  public Text getDelegationTokenService() {
    return dtService;
  }
}
