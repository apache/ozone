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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_INTERNAL_SERVICE_ID;

/**
 * Construct SCM node details.
 */
public final class SCMNodeDetails {
  private String scmServiceId;
  private String scmNodeId;
  private InetSocketAddress rpcAddress;
  private int rpcPort;
  private int ratisPort;
  private String httpAddress;
  private String httpsAddress;

  public static final Logger LOG =
      LoggerFactory.getLogger(SCMNodeDetails.class);

  /**
   * Constructs SCMNodeDetails object.
   */
  private SCMNodeDetails(String serviceId, String nodeId,
                        InetSocketAddress rpcAddr, int rpcPort, int ratisPort,
                        String httpAddress, String httpsAddress) {
    this.scmServiceId = serviceId;
    this.scmNodeId = nodeId;
    this.rpcAddress = rpcAddr;
    this.rpcPort = rpcPort;
    this.ratisPort = ratisPort;
    this.httpAddress = httpAddress;
    this.httpsAddress = httpsAddress;
  }

  @Override
  public String toString() {
    return "SCMNodeDetails["
        + "scmServiceId=" + scmServiceId +
        ", scmNodeId=" + scmNodeId +
        ", rpcAddress=" + rpcAddress +
        ", rpcPort=" + rpcPort +
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
    private int rpcPort;
    private int ratisPort;
    private String httpAddr;
    private String httpsAddr;

    public Builder setRpcAddress(InetSocketAddress rpcAddr) {
      this.rpcAddress = rpcAddr;
      this.rpcPort = rpcAddress.getPort();
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
      return new SCMNodeDetails(scmServiceId, scmNodeId, rpcAddress, rpcPort,
          ratisPort, httpAddr, httpsAddr);
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

  public int getRpcPort() {
    return rpcPort;
  }

  public String getRpcAddressString() {
    return NetUtils.getHostPortString(rpcAddress);
  }

  public static SCMNodeDetails initStandAlone(
      OzoneConfiguration conf) throws IOException {
    String localSCMServiceId = conf.getTrimmed(OZONE_SCM_INTERNAL_SERVICE_ID);
    int ratisPort = conf.getInt(
        ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY,
        ScmConfigKeys.OZONE_SCM_RATIS_PORT_DEFAULT);
    InetSocketAddress rpcAddress = new InetSocketAddress(
        InetAddress.getLocalHost(), 0);
    SCMNodeDetails scmNodeDetails = new SCMNodeDetails.Builder()
        .setRatisPort(ratisPort)
        .setRpcAddress(rpcAddress)
        .setSCMNodeId(localSCMServiceId)
        .setSCMServiceId(OzoneConsts.SCM_SERVICE_ID_DEFAULT)
        .build();
    return scmNodeDetails;
  }
}
