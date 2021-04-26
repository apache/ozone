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

package org.apache.hadoop.ozone.om.ha;

import org.apache.commons.lang3.StringUtils;

import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.hdds.NodeDetails;

import java.net.InetSocketAddress;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_DB_CHECKPOINT_HTTP_ENDPOINT;

/**
 * This class stores OM node details.
 */
public final class OMNodeDetails extends NodeDetails {
  private int rpcPort;

  /**
   * Constructs OMNodeDetails object.
   */
  private OMNodeDetails(String serviceId, String nodeId,
      InetSocketAddress rpcAddr, int rpcPort, int ratisPort,
      String httpAddress, String httpsAddress) {
    super(serviceId, nodeId, rpcAddr, ratisPort, httpAddress, httpsAddress);
    this.rpcPort = rpcPort;
  }

  @Override
  public String toString() {
    return "OMNodeDetails["
        + "omServiceId=" + getServiceId() +
        ", omNodeId=" + getNodeId() +
        ", rpcAddress=" + getRpcAddressString() +
        ", rpcPort=" + getRpcPort() +
        ", ratisPort=" + getRatisPort() +
        ", httpAddress=" + getHttpAddress() +
        ", httpsAddress=" + getHttpsAddress() +
        "]";
  }

  public int getRpcPort() {
    return rpcPort;
  }

  /**
   * Builder class for OMNodeDetails.
   */
  public static class Builder {
    private String omServiceId;
    private String omNodeId;
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

    public Builder setOMServiceId(String serviceId) {
      this.omServiceId = serviceId;
      return this;
    }

    public Builder setOMNodeId(String nodeId) {
      this.omNodeId = nodeId;
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

    public OMNodeDetails build() {
      return new OMNodeDetails(omServiceId, omNodeId, rpcAddress, rpcPort,
          ratisPort, httpAddr, httpsAddr);
    }
  }

  public String getOMDBCheckpointEnpointUrl(HttpConfig.Policy httpPolicy) {
    if (httpPolicy.isHttpEnabled()) {
      if (StringUtils.isNotEmpty(getHttpAddress())) {
        return "http://" + getHttpAddress() +
            OZONE_OM_DB_CHECKPOINT_HTTP_ENDPOINT +
            "?" + OZONE_DB_CHECKPOINT_REQUEST_FLUSH + "=true";
      }
    } else {
      if (StringUtils.isNotEmpty(getHttpsAddress())) {
        return "https://" + getHttpsAddress() +
            OZONE_OM_DB_CHECKPOINT_HTTP_ENDPOINT +
            "?" + OZONE_DB_CHECKPOINT_REQUEST_FLUSH + "=true";
      }
    }
    return null;
  }
}
