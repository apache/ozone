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

package org.apache.hadoop.ozone.om.helpers;

import org.apache.commons.lang3.StringUtils;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.hdds.NodeDetails;

import java.net.InetSocketAddress;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_DB_CHECKPOINT_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_PORT_KEY;

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

  /**
   * Constructs OMNodeDetails object.
   */
  private OMNodeDetails(String serviceId, String nodeId, String hostAddr,
      int rpcPort, int ratisPort, String httpAddress, String httpsAddress) {
    super(serviceId, nodeId, hostAddr, rpcPort, ratisPort, httpAddress,
        httpsAddress);
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
    private String hostAddress;
    private InetSocketAddress rpcAddress;
    private int rpcPort;
    private int ratisPort;
    private String httpAddr;
    private String httpsAddr;

    public Builder setHostAddress(String hostName) {
      this.hostAddress = hostName;
      return this;
    }

    public Builder setRpcAddress(InetSocketAddress rpcAddr) {
      this.rpcAddress = rpcAddr;
      this.rpcPort = rpcAddress.getPort();
      return this;
    }

    public Builder setRatisAddress(InetSocketAddress ratisAddr) {
      this.hostAddress = ratisAddr.getHostName();
      this.ratisPort = ratisAddr.getPort();
      return this;
    }

    public Builder setRpcPort(int port) {
      this.rpcPort = port;
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
      if (rpcAddress != null) {
        return new OMNodeDetails(omServiceId, omNodeId, rpcAddress, rpcPort,
            ratisPort, httpAddr, httpsAddr);
      } else {
        return new OMNodeDetails(omServiceId, omNodeId, hostAddress, rpcPort,
            ratisPort, httpAddr, httpsAddr);
      }
    }
  }

  public String getOMDBCheckpointEnpointUrl(boolean isHttpPolicy) {
    if (isHttpPolicy) {
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

  public static OMNodeDetails getOMNodeDetailsFromConf(OzoneConfiguration conf,
      String omServiceId, String omNodeId) {
    String rpcAddrKey = ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
        omServiceId, omNodeId);
    String rpcAddrStr = OmUtils.getOmRpcAddress(conf, rpcAddrKey);
    if (rpcAddrStr == null || rpcAddrStr.isEmpty()) {
      return null;
    }

    String ratisPortKey = ConfUtils.addKeySuffixes(OZONE_OM_RATIS_PORT_KEY,
        omServiceId, omNodeId);
    int ratisPort = conf.getInt(ratisPortKey, OZONE_OM_RATIS_PORT_DEFAULT);

    InetSocketAddress omRpcAddress = null;
    try {
      omRpcAddress = NetUtils.createSocketAddr(rpcAddrStr);
    } catch (Exception e) {
      throw new IllegalArgumentException("Couldn't create socket address" +
          " for OM " + omNodeId + " at " + rpcAddrStr, e);
    }

    String httpAddr = OmUtils.getHttpAddressForOMPeerNode(conf,
        omServiceId, omNodeId, omRpcAddress.getHostName());
    String httpsAddr = OmUtils.getHttpsAddressForOMPeerNode(conf,
        omServiceId, omNodeId, omRpcAddress.getHostName());

    return new OMNodeDetails.Builder()
        .setOMNodeId(omNodeId)
        .setRpcAddress(omRpcAddress)
        .setRatisPort(ratisPort)
        .setHttpAddress(httpAddr)
        .setHttpsAddress(httpsAddr)
        .setOMServiceId(omServiceId)
        .build();
  }
}
