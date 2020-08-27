/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.proxy;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Class to store SCM proxy info.
 */
public class SCMProxyInfo {
  private String serviceId;
  private String nodeId;
  private String rpcAddrStr;
  private InetSocketAddress rpcAddr;

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMProxyInfo.class);

  public SCMProxyInfo(String serviceID, String nodeID,
                      InetSocketAddress rpcAddress) {
    Preconditions.checkNotNull(rpcAddress);
    this.serviceId = serviceID;
    this.nodeId = nodeID;
    this.rpcAddrStr = rpcAddress.toString();
    this.rpcAddr = rpcAddress;
    if (rpcAddr.isUnresolved()) {
      LOG.warn("SCM address {} for serviceID {} remains unresolved " +
              "for node ID {} Check your ozone-site.xml file to ensure scm " +
              "addresses are configured properly.",
          rpcAddress, serviceId, nodeId);
    }
  }

  public String toString() {
    return new StringBuilder()
        .append("nodeId=")
        .append(nodeId)
        .append(",nodeAddress=")
        .append(rpcAddrStr).toString();
  }

  public InetSocketAddress getAddress() {
    return rpcAddr;
  }
}
