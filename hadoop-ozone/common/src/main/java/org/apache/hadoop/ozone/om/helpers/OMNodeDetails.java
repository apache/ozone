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

package org.apache.hadoop.ozone.om.helpers;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_HTTP_ENDPOINT_V2;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_PORT_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;
import org.apache.hadoop.hdds.NodeDetails;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.NodeState;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.OMNodeInfo;
import org.apache.http.client.utils.URIBuilder;

/**
 * This class stores OM node details.
 */
public final class OMNodeDetails extends NodeDetails {
  private int rpcPort;
  private boolean isDecommissioned = false;
  private boolean isRatisListener = false;

  /**
   * Constructs OMNodeDetails object.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private OMNodeDetails(String serviceId, String nodeId,
      InetSocketAddress rpcAddr, int rpcPort, int ratisPort,
      String httpAddress, String httpsAddress, boolean isRatisListener) {
    super(serviceId, nodeId, rpcAddr, ratisPort, httpAddress, httpsAddress);
    this.rpcPort = rpcPort;
    this.isRatisListener = isRatisListener;
  }

  /**
   * Constructs OMNodeDetails object.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private OMNodeDetails(String serviceId, String nodeId, String hostAddr,
      int rpcPort, int ratisPort, String httpAddress, String httpsAddress, boolean isRatisListener) {
    super(serviceId, nodeId, hostAddr, rpcPort, ratisPort, httpAddress,
        httpsAddress);
    this.rpcPort = rpcPort;
    this.isRatisListener = isRatisListener;
  }

  public void setDecommissioningState() {
    isDecommissioned = true;
  }

  public boolean isDecommissioned() {
    return isDecommissioned;
  }

  public void setRatisListener() {
    isRatisListener = true;
  }

  public boolean isRatisListener() {
    return isRatisListener;
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
        ", isListener=" + isRatisListener() +
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
    private boolean isListener = false;

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

    public Builder setIsListener(boolean isListener) {
      this.isListener = isListener;
      return this;
    }

    public OMNodeDetails build() {
      if (rpcAddress != null) {
        return new OMNodeDetails(omServiceId, omNodeId, rpcAddress, rpcPort,
            ratisPort, httpAddr, httpsAddr, isListener);
      } else {
        return new OMNodeDetails(omServiceId, omNodeId, hostAddress, rpcPort,
            ratisPort, httpAddr, httpsAddr, isListener);
      }
    }
  }

  public URL getOMDBCheckpointEndpointUrl(boolean isHttp, boolean flush)
      throws IOException {
    URL url;
    try {
      URIBuilder urlBuilder = new URIBuilder().
          setScheme(isHttp ? "http" : "https").
          setHost(isHttp ? getHttpAddress() : getHttpsAddress()).
          setPath(OZONE_DB_CHECKPOINT_HTTP_ENDPOINT_V2).
          addParameter(OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA, "true").
          addParameter(OZONE_DB_CHECKPOINT_REQUEST_FLUSH,
              flush ? "true" : "false");

      url = urlBuilder.build().toURL();
    } catch (URISyntaxException | MalformedURLException e) {
      throw new IOException("Could not get OM DB Checkpoint Endpoint Url", e);
    }
    return url;
  }

  public String getOMPrintInfo() {
    return getNodeId() + "[" + getHostAddress() + ":" + getRpcPort() + "]";
  }

  public static String getOMNodeAddressFromConf(OzoneConfiguration conf,
      String omServiceId, String omNodeId) {
    String rpcAddrKey = ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
        omServiceId, omNodeId);
    return OmUtils.getOmRpcAddress(conf, rpcAddrKey);
  }

  public static OMNodeDetails getOMNodeDetailsFromConf(OzoneConfiguration conf,
      String omServiceId, String omNodeId) throws IOException {

    String rpcAddrStr = getOMNodeAddressFromConf(conf, omServiceId, omNodeId);
    if (rpcAddrStr == null || rpcAddrStr.isEmpty()) {
      return null;
    }

    InetSocketAddress omRpcAddress;
    try {
      omRpcAddress = NetUtils.createSocketAddr(rpcAddrStr);
    } catch (Exception e) {
      throw new IOException("Couldn't create socket address" +
          " for OM " + omNodeId + " at " + rpcAddrStr, e);
    }

    String ratisPortKey = ConfUtils.addKeySuffixes(OZONE_OM_RATIS_PORT_KEY,
        omServiceId, omNodeId);
    int ratisPort = conf.getInt(ratisPortKey, OZONE_OM_RATIS_PORT_DEFAULT);

    String httpAddr = OmUtils.getHttpAddressForOMPeerNode(conf,
        omServiceId, omNodeId, omRpcAddress.getHostName());
    String httpsAddr = OmUtils.getHttpsAddressForOMPeerNode(conf,
        omServiceId, omNodeId, omRpcAddress.getHostName());

    Collection<String> listenerOmNodeIds = OmUtils.getListenerOMNodeIds(conf, omServiceId);
    boolean isListener = listenerOmNodeIds.contains(omNodeId);

    return new Builder()
        .setOMNodeId(omNodeId)
        .setRatisPort(ratisPort)
        .setHttpAddress(httpAddr)
        .setHttpsAddress(httpsAddr)
        .setOMServiceId(omServiceId)
        .setRpcAddress(omRpcAddress)
        .setIsListener(isListener)
        .build();
  }

  public OMNodeInfo getProtobuf() {
    return OMNodeInfo.newBuilder()
        .setNodeID(getNodeId())
        .setHostAddress(getHostAddress())
        .setRpcPort(getRpcPort())
        .setRatisPort(getRatisPort())
        .setNodeState(isDecommissioned ?
            NodeState.DECOMMISSIONED : NodeState.ACTIVE)
        .setIsListener(isRatisListener)
        .build();
  }

  public static OMNodeDetails getFromProtobuf(OMNodeInfo omNodeInfo) {
    OMNodeDetails nodeDetails = new Builder()
        .setOMNodeId(omNodeInfo.getNodeID())
        .setHostAddress(omNodeInfo.getHostAddress())
        .setRpcPort(omNodeInfo.getRpcPort())
        .setRatisPort(omNodeInfo.getRatisPort())
        .setIsListener(omNodeInfo.getIsListener())
        .build();
    if (omNodeInfo.hasNodeState() &&
        omNodeInfo.getNodeState().equals(NodeState.DECOMMISSIONED)) {
      nodeDetails.setDecommissioningState();
    }
    return nodeDetails;
  }
}
