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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.NodeState;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.OMNodeInfo;
import org.junit.jupiter.api.Test;

/**
 * Test OMNodeDetails.
 */
public class TestOMNodeDetails {

  private static final String OM_SERVICE_ID = "om-service";
  private static final String OM_NODE_ID = "om-01";
  private static final String HOST_ADDRESS = "localhost";
  private static final int RPC_PORT = 9862;
  private static final int RATIS_PORT = 9873;
  private static final String HTTP_ADDRESS = "0.0.0.0:9874";
  private static final String HTTPS_ADDRESS = "0.0.0.0:9875";

  /**
   * Test builder with InetSocketAddress.
   */
  @Test
  public void testBuilderWithInetSocketAddress() {
    InetSocketAddress rpcAddr = new InetSocketAddress(HOST_ADDRESS, RPC_PORT);
    
    OMNodeDetails nodeDetails = new OMNodeDetails.Builder()
        .setOMServiceId(OM_SERVICE_ID)
        .setOMNodeId(OM_NODE_ID)
        .setRpcAddress(rpcAddr)
        .setRatisPort(RATIS_PORT)
        .setHttpAddress(HTTP_ADDRESS)
        .setHttpsAddress(HTTPS_ADDRESS)
        .setIsListener(false)
        .build();

    assertEquals(OM_SERVICE_ID, nodeDetails.getServiceId());
    assertEquals(OM_NODE_ID, nodeDetails.getNodeId());
    assertEquals(RPC_PORT, nodeDetails.getRpcPort());
    assertEquals(RATIS_PORT, nodeDetails.getRatisPort());
    assertEquals(HTTP_ADDRESS, nodeDetails.getHttpAddress());
    assertEquals(HTTPS_ADDRESS, nodeDetails.getHttpsAddress());
    assertEquals(HOST_ADDRESS, nodeDetails.getHostAddress());
  }

  /**
   * Test builder with host address string.
   */
  @Test
  public void testBuilderWithHostAddressString() {
    OMNodeDetails nodeDetails = new OMNodeDetails.Builder()
        .setOMServiceId(OM_SERVICE_ID)
        .setOMNodeId(OM_NODE_ID)
        .setHostAddress(HOST_ADDRESS)
        .setRpcPort(RPC_PORT)
        .setRatisPort(RATIS_PORT)
        .setHttpAddress(HTTP_ADDRESS)
        .setHttpsAddress(HTTPS_ADDRESS)
        .build();

    assertEquals(OM_SERVICE_ID, nodeDetails.getServiceId());
    assertEquals(OM_NODE_ID, nodeDetails.getNodeId());
    assertEquals(RPC_PORT, nodeDetails.getRpcPort());
    assertEquals(RATIS_PORT, nodeDetails.getRatisPort());
  }

  /**
   * Test isRatisListener flag.
   */
  @Test
  public void testRatisListenerFlag() {
    OMNodeDetails nonListener = new OMNodeDetails.Builder()
        .setOMServiceId(OM_SERVICE_ID)
        .setOMNodeId(OM_NODE_ID)
        .setHostAddress(HOST_ADDRESS)
        .setRpcPort(RPC_PORT)
        .setRatisPort(RATIS_PORT)
        .setHttpAddress(HTTP_ADDRESS)
        .setHttpsAddress(HTTPS_ADDRESS)
        .setIsListener(false)
        .build();

    assertFalse(nonListener.isRatisListener());

    OMNodeDetails listener = new OMNodeDetails.Builder()
        .setOMServiceId(OM_SERVICE_ID)
        .setOMNodeId(OM_NODE_ID + "-listener")
        .setHostAddress(HOST_ADDRESS)
        .setRpcPort(RPC_PORT + 1)
        .setRatisPort(RATIS_PORT + 1)
        .setHttpAddress(HTTP_ADDRESS)
        .setHttpsAddress(HTTPS_ADDRESS)
        .setIsListener(true)
        .build();

    assertTrue(listener.isRatisListener());

    nonListener.setRatisListener();
    assertTrue(nonListener.isRatisListener());
  }

  /**
   * Test decommissioned state.
   */
  @Test
  public void testDecommissionedState() {
    OMNodeDetails nodeDetails = new OMNodeDetails.Builder()
        .setOMServiceId(OM_SERVICE_ID)
        .setOMNodeId(OM_NODE_ID)
        .setHostAddress(HOST_ADDRESS)
        .setRpcPort(RPC_PORT)
        .setRatisPort(RATIS_PORT)
        .setHttpAddress(HTTP_ADDRESS)
        .setHttpsAddress(HTTPS_ADDRESS)
        .build();

    assertFalse(nodeDetails.isDecommissioned());

    nodeDetails.setDecommissioningState();
    assertTrue(nodeDetails.isDecommissioned());
  }

  /**
   * Test toString method.
   */
  @Test
  public void testToString() {
    OMNodeDetails nodeDetails = new OMNodeDetails.Builder()
        .setOMServiceId(OM_SERVICE_ID)
        .setOMNodeId(OM_NODE_ID)
        .setHostAddress(HOST_ADDRESS)
        .setRpcPort(RPC_PORT)
        .setRatisPort(RATIS_PORT)
        .setHttpAddress(HTTP_ADDRESS)
        .setHttpsAddress(HTTPS_ADDRESS)
        .setIsListener(true)
        .build();

    String result = nodeDetails.toString();
    assertTrue(result.contains("omServiceId=" + OM_SERVICE_ID));
    assertTrue(result.contains("omNodeId=" + OM_NODE_ID));
    assertTrue(result.contains("rpcPort=" + RPC_PORT));
    assertTrue(result.contains("ratisPort=" + RATIS_PORT));
    assertTrue(result.contains("httpAddress=" + HTTP_ADDRESS));
    assertTrue(result.contains("httpsAddress=" + HTTPS_ADDRESS));
    assertTrue(result.contains("isListener=true"));
  }

  /**
   * Test getOMPrintInfo method.
   */
  @Test
  public void testGetOMPrintInfo() {
    OMNodeDetails nodeDetails = new OMNodeDetails.Builder()
        .setOMServiceId(OM_SERVICE_ID)
        .setOMNodeId(OM_NODE_ID)
        .setHostAddress(HOST_ADDRESS)
        .setRpcPort(RPC_PORT)
        .setRatisPort(RATIS_PORT)
        .setHttpAddress(HTTP_ADDRESS)
        .setHttpsAddress(HTTPS_ADDRESS)
        .build();

    String result = nodeDetails.getOMPrintInfo();
    assertEquals(OM_NODE_ID + "[" + HOST_ADDRESS + ":" + RPC_PORT + "]", result);
  }

  /**
   * Test getRpcPort method.
   */
  @Test
  public void testGetRpcPort() {
    OMNodeDetails nodeDetails = new OMNodeDetails.Builder()
        .setOMServiceId(OM_SERVICE_ID)
        .setOMNodeId(OM_NODE_ID)
        .setHostAddress(HOST_ADDRESS)
        .setRpcPort(RPC_PORT)
        .setRatisPort(RATIS_PORT)
        .setHttpAddress(HTTP_ADDRESS)
        .setHttpsAddress(HTTPS_ADDRESS)
        .build();

    assertEquals(RPC_PORT, nodeDetails.getRpcPort());
  }

  /**
   * Test protobuf conversion for active node.
   */
  @Test
  public void testProtobufConversionActiveNode() {
    OMNodeDetails nodeDetails = new OMNodeDetails.Builder()
        .setOMServiceId(OM_SERVICE_ID)
        .setOMNodeId(OM_NODE_ID)
        .setHostAddress(HOST_ADDRESS)
        .setRpcPort(RPC_PORT)
        .setRatisPort(RATIS_PORT)
        .setHttpAddress(HTTP_ADDRESS)
        .setHttpsAddress(HTTPS_ADDRESS)
        .setIsListener(false)
        .build();

    OMNodeInfo protobuf = nodeDetails.getProtobuf();

    assertEquals(OM_NODE_ID, protobuf.getNodeID());
    assertEquals(HOST_ADDRESS, protobuf.getHostAddress());
    assertEquals(RPC_PORT, protobuf.getRpcPort());
    assertEquals(RATIS_PORT, protobuf.getRatisPort());
    assertEquals(NodeState.ACTIVE, protobuf.getNodeState());
    assertFalse(protobuf.getIsListener());

    OMNodeDetails restored = OMNodeDetails.getFromProtobuf(protobuf);
    assertEquals(nodeDetails.getNodeId(), restored.getNodeId());
    assertEquals(nodeDetails.getHostAddress(), restored.getHostAddress());
    assertEquals(nodeDetails.getRpcPort(), restored.getRpcPort());
    assertEquals(nodeDetails.getRatisPort(), restored.getRatisPort());
    assertEquals(nodeDetails.isDecommissioned(), restored.isDecommissioned());
    assertEquals(nodeDetails.isRatisListener(), restored.isRatisListener());
  }

  /**
   * Test protobuf conversion for decommissioned node.
   */
  @Test
  public void testProtobufConversionDecommissionedNode() {
    OMNodeDetails nodeDetails = new OMNodeDetails.Builder()
        .setOMServiceId(OM_SERVICE_ID)
        .setOMNodeId(OM_NODE_ID)
        .setHostAddress(HOST_ADDRESS)
        .setRpcPort(RPC_PORT)
        .setRatisPort(RATIS_PORT)
        .setHttpAddress(HTTP_ADDRESS)
        .setHttpsAddress(HTTPS_ADDRESS)
        .build();

    nodeDetails.setDecommissioningState();

    OMNodeInfo protobuf = nodeDetails.getProtobuf();
    assertEquals(NodeState.DECOMMISSIONED, protobuf.getNodeState());

    OMNodeDetails restored = OMNodeDetails.getFromProtobuf(protobuf);
    assertTrue(restored.isDecommissioned());
  }

  /**
   * Test protobuf conversion for listener node.
   */
  @Test
  public void testProtobufConversionListenerNode() {
    OMNodeDetails nodeDetails = new OMNodeDetails.Builder()
        .setOMServiceId(OM_SERVICE_ID)
        .setOMNodeId(OM_NODE_ID)
        .setHostAddress(HOST_ADDRESS)
        .setRpcPort(RPC_PORT)
        .setRatisPort(RATIS_PORT)
        .setHttpAddress(HTTP_ADDRESS)
        .setHttpsAddress(HTTPS_ADDRESS)
        .setIsListener(true)
        .build();

    OMNodeInfo protobuf = nodeDetails.getProtobuf();
    assertTrue(protobuf.getIsListener());

    OMNodeDetails restored = OMNodeDetails.getFromProtobuf(protobuf);
    assertTrue(restored.isRatisListener());
  }

  /**
   * Test getOMDBCheckpointEndpointUrl for HTTP.
   */
  @Test
  public void testGetOMDBCheckpointEndpointUrlHttp() throws IOException {
    OMNodeDetails nodeDetails = new OMNodeDetails.Builder()
        .setOMServiceId(OM_SERVICE_ID)
        .setOMNodeId(OM_NODE_ID)
        .setHostAddress(HOST_ADDRESS)
        .setRpcPort(RPC_PORT)
        .setRatisPort(RATIS_PORT)
        .setHttpAddress(HOST_ADDRESS + ":9874")
        .setHttpsAddress(HOST_ADDRESS + ":9875")
        .build();

    URL urlWithoutFlush = nodeDetails.getOMDBCheckpointEndpointUrl(true, false);
    assertNotNull(urlWithoutFlush);
    assertEquals("http", urlWithoutFlush.getProtocol());
    assertEquals(HOST_ADDRESS + ":9874", urlWithoutFlush.getAuthority());
    assertNotNull(urlWithoutFlush.getQuery());
    assertTrue(urlWithoutFlush.getQuery().contains("flushBeforeCheckpoint=false"));

    URL urlWithFlush = nodeDetails.getOMDBCheckpointEndpointUrl(true, true);
    assertNotNull(urlWithFlush);
    assertTrue(urlWithFlush.getQuery().contains("flushBeforeCheckpoint=true"));
    assertTrue(urlWithFlush.getQuery().contains("includeSnapshotData=true"));
  }

  /**
   * Test getOMDBCheckpointEndpointUrl for HTTPS.
   */
  @Test
  public void testGetOMDBCheckpointEndpointUrlHttps() throws IOException {
    OMNodeDetails nodeDetails = new OMNodeDetails.Builder()
        .setOMServiceId(OM_SERVICE_ID)
        .setOMNodeId(OM_NODE_ID)
        .setHostAddress(HOST_ADDRESS)
        .setRpcPort(RPC_PORT)
        .setRatisPort(RATIS_PORT)
        .setHttpAddress(HOST_ADDRESS + ":9874")
        .setHttpsAddress(HOST_ADDRESS + ":9875")
        .build();

    URL url = nodeDetails.getOMDBCheckpointEndpointUrl(false, false);
    assertNotNull(url);
    assertEquals("https", url.getProtocol());
    assertEquals(HOST_ADDRESS + ":9875", url.getAuthority());
  }

  /**
   * Test getOMNodeAddressFromConf.
   */
  @Test
  public void testGetOMNodeAddressFromConf() {
    OzoneConfiguration conf = new OzoneConfiguration();
    
    String configKey = "ozone.om.address.om-service.om-01";
    String expectedAddress = "localhost:9862";
    conf.set(configKey, expectedAddress);

    String address = OMNodeDetails.getOMNodeAddressFromConf(conf, "om-service", "om-01");
    assertEquals(expectedAddress, address);

    String missingAddress = OMNodeDetails.getOMNodeAddressFromConf(conf, "nonexistent", "node");
    assertNull(missingAddress);
  }

  /**
   * Test getOMNodeDetailsFromConf with valid configuration.
   */
  @Test
  public void testGetOMNodeDetailsFromConfValid() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    
    String serviceId = "om-service";
    String nodeId = "om-01";
    
    conf.set(ConfUtils.addKeySuffixes(OMConfigKeys.OZONE_OM_ADDRESS_KEY, serviceId, nodeId), 
        "localhost:9862");
    conf.set(ConfUtils.addKeySuffixes(OMConfigKeys.OZONE_OM_RATIS_PORT_KEY, serviceId, nodeId), 
        "9873");
    conf.set(ConfUtils.addKeySuffixes(OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, serviceId, nodeId),
        "localhost:9874");
    conf.set(ConfUtils.addKeySuffixes(OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY, serviceId, nodeId),
        "localhost:9875");

    OMNodeDetails nodeDetails = OMNodeDetails.getOMNodeDetailsFromConf(conf, serviceId, nodeId);

    assertNotNull(nodeDetails);
    assertEquals(serviceId, nodeDetails.getServiceId());
    assertEquals(nodeId, nodeDetails.getNodeId());
    assertEquals(9862, nodeDetails.getRpcPort());
    assertEquals(9873, nodeDetails.getRatisPort());
  }

  /**
   * Test getOMNodeDetailsFromConf with missing configuration.
   */
  @Test
  public void testGetOMNodeDetailsFromConfMissing() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    
    OMNodeDetails nodeDetails = OMNodeDetails.getOMNodeDetailsFromConf(conf, "nonexistent", "node");
    assertNull(nodeDetails);
    
    String serviceId = "om-service";
    String nodeId = "om-01";
    
    nodeDetails = OMNodeDetails.getOMNodeDetailsFromConf(conf, serviceId, null);
    assertNull(nodeDetails);
    nodeDetails = OMNodeDetails.getOMNodeDetailsFromConf(conf, null, nodeId);
    assertNull(nodeDetails);
  }

  /**
   * Test setRatisAddress in builder.
   */
  @Test
  public void testSetRatisAddress() {
    InetSocketAddress ratisAddr = new InetSocketAddress("192.168.1.100", 9873);
    
    OMNodeDetails nodeDetails = new OMNodeDetails.Builder()
        .setOMServiceId(OM_SERVICE_ID)
        .setOMNodeId(OM_NODE_ID)
        .setRatisAddress(ratisAddr)
        .setRpcPort(RPC_PORT)
        .setHttpAddress(HTTP_ADDRESS)
        .setHttpsAddress(HTTPS_ADDRESS)
        .build();

    assertEquals("192.168.1.100", nodeDetails.getHostAddress());
    assertEquals(9873, nodeDetails.getRatisPort());
  }
}
