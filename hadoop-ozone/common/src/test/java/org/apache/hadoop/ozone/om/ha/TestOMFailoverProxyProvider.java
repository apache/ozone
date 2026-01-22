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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.StringJoiner;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests OMFailoverProxyProvider failover behaviour.
 */
public class TestOMFailoverProxyProvider {
  private static final String OM_SERVICE_ID = "om-service-test1";
  private static final String NODE_ID_BASE_STR = "omNode-";
  private static final String DUMMY_NODE_ADDR = "0.0.0.0:8080";
  private HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> provider;
  private long waitBetweenRetries;
  private int numNodes = 3;
  private OzoneConfiguration config;

  @BeforeEach
  public void init() throws Exception {
    config = new OzoneConfiguration();
    waitBetweenRetries = config.getLong(
        OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_KEY,
        OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT);
    StringJoiner allNodeIds = new StringJoiner(",");
    for (int i = 1; i <= numNodes; i++) {
      String nodeId = NODE_ID_BASE_STR + i;
      config.set(ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY, OM_SERVICE_ID,
          nodeId), DUMMY_NODE_ADDR);
      allNodeIds.add(nodeId);
    }
    config.set(ConfUtils.addKeySuffixes(OZONE_OM_NODES_KEY, OM_SERVICE_ID),
        allNodeIds.toString());
    provider = new HadoopRpcOMFailoverProxyProvider<>(config,
        UserGroupInformation.getCurrentUser(), OM_SERVICE_ID,
        OzoneManagerProtocolPB.class);
  }

  /**
   * Tests waitTime when fail over to next node.
   */
  @Test
  public void testWaitTimeWithNextNode() {
    failoverToNextNode(numNodes - 1, 0);
    // After 3 attempts done, wait time should be waitBetweenRetries.
    failoverToNextNode(1, waitBetweenRetries);
    // From 4th Attempt waitTime should reset to 0.
    failoverToNextNode(numNodes - 1, 0);
    // After 2nd round of 3attempts done, wait time should be
    // waitBetweenRetries.
    failoverToNextNode(1, waitBetweenRetries);
  }

  /**
   * Tests failover to next node and same node.
   */
  @Test
  public void testWaitTimeWithNextNodeAndSameNodeFailover() {
    failoverToNextNode(1, 0);
    // 1 Failover attempt to same OM, waitTime should increase.
    failoverToSameNode(2);
  }

  /**
   * Tests wait time should reset in the following case:
   * 1. Do a couple same node failover attempts.
   * 2. Next node failover should reset wait time to 0.
   */
  @Test
  public void testWaitTimeResetWhenNextNodeFailoverAfterSameNode() {
    // 2 failover attempts to same OM, waitTime should increase.
    failoverToSameNode(2);
    // Failover to next node, should reset waitTime to 0.
    failoverToNextNode(1, 0);
  }

  /**
   * Tests wait time should be 0 in the following case:
   * 1. Do failover to suggest new node.
   * 2. WaitTime should be 0.
   */
  @Test
  public void testWaitTimeWithSuggestedNewNode() {
    Collection<String> allNodeIds = config.getTrimmedStringCollection(ConfUtils.
        addKeySuffixes(OZONE_OM_NODES_KEY, OM_SERVICE_ID));
    allNodeIds.remove(provider.getCurrentProxyOMNodeId());
    assertTrue(!allNodeIds.isEmpty(),
        "This test needs at least 2 OMs");
    provider.setNextOmProxy(allNodeIds.iterator().next());
    assertEquals(0, provider.getWaitTime());
  }

  /**
   * Tests waitTime reset after same node failover.
   */
  @Test
  public void testWaitTimeResetWhenAllNodeFailoverAndSameNode() {
    // Next node failover wait time should be 0.
    failoverToNextNode(numNodes - 1, 0);
    // Once all numNodes failover done, waitTime should be waitBetweenRetries
    failoverToNextNode(1, waitBetweenRetries);
    // 4 failover attempts to same OM, waitTime should increase.
    failoverToSameNode(4);
    // Next node failover should reset wait time.
    failoverToNextNode(numNodes - 1, 0);
    failoverToNextNode(1, waitBetweenRetries);
  }

  /**
   * Ensure listener nodes are excluded from provider's proxy list.
   */
  @Test
  public void testExcludesListenerNodes() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    StringJoiner allNodeIds = new StringJoiner(",");
    for (int i = 1; i <= numNodes; i++) {
      String nodeId = NODE_ID_BASE_STR + i;
      conf.set(ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY, OM_SERVICE_ID,
          nodeId), DUMMY_NODE_ADDR);
      allNodeIds.add(nodeId);
    }
    conf.set(ConfUtils.addKeySuffixes(OZONE_OM_NODES_KEY, OM_SERVICE_ID),
        allNodeIds.toString());
    // Mark one of the nodes as listener (omNode-2)
    String listenerNode = NODE_ID_BASE_STR + 2;
    conf.set(ConfUtils.addKeySuffixes(
        org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_LISTENER_NODES_KEY,
        OM_SERVICE_ID), listenerNode);

    try (HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> providerWithListeners =
             new HadoopRpcOMFailoverProxyProvider<>(conf,
                 UserGroupInformation.getCurrentUser(), OM_SERVICE_ID,
                 OzoneManagerProtocolPB.class)) {
      // Verify listener node is not included in proxy map
      assertTrue(providerWithListeners.getOMProxyMap().containsKey(NODE_ID_BASE_STR + 1));
      assertTrue(providerWithListeners.getOMProxyMap().containsKey(NODE_ID_BASE_STR + 3));
      assertFalse(providerWithListeners.getOMProxyMap().containsKey(listenerNode));
    }
  }

  /**
   * Failover to next node and wait time should be same as waitTimeAfter.
   */
  private void failoverToNextNode(int numNextNodeFailoverTimes,
                                  long waitTimeAfter) {
    for (int attempt = 0; attempt < numNextNodeFailoverTimes; attempt++) {
      provider.selectNextOmProxy();
      assertEquals(waitTimeAfter, provider.getWaitTime());
      provider.performFailover(null);
    }
  }

  /**
   * Failover to same node and wait time will be attempt*waitBetweenRetries.
   */
  private void failoverToSameNode(int numSameNodeFailoverTimes) {
    provider.performFailover(null);
    for (int attempt = 1; attempt <= numSameNodeFailoverTimes; attempt++) {
      provider.setNextOmProxy(provider.getCurrentProxyOMNodeId());
      assertEquals(attempt * waitBetweenRetries,
          provider.getWaitTime());
    }
  }

  /**
   * Tests canonical delegation token service name in is consistently ordered.
   */
  @Test
  public void testCanonicalTokenServiceName() throws IOException {
    OzoneConfiguration ozoneConf = new OzoneConfiguration();
    ArrayList<String> nodeAddrs = new ArrayList<>(
        Arrays.asList("4.3.2.1:9862", "2.1.0.5:9862", "3.2.1.0:9862"));
    assertEquals(numNodes, nodeAddrs.size());

    StringJoiner allNodeIds = new StringJoiner(",");
    for (int i = 1; i <= numNodes; i++) {
      String nodeId = NODE_ID_BASE_STR + i;
      ozoneConf.set(
          ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY, OM_SERVICE_ID,
              nodeId), nodeAddrs.get(i - 1));
      allNodeIds.add(nodeId);
    }
    ozoneConf.set(ConfUtils.addKeySuffixes(OZONE_OM_NODES_KEY, OM_SERVICE_ID),
        allNodeIds.toString());
    try (HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> prov =
             new HadoopRpcOMFailoverProxyProvider<>(ozoneConf,
                 UserGroupInformation.getCurrentUser(),
                 OM_SERVICE_ID,
                 OzoneManagerProtocolPB.class)) {
      Text dtService = prov.getCurrentProxyDelegationToken();

      Collections.sort(nodeAddrs);
      String expectedDtService = String.join(",", nodeAddrs);
      assertEquals(expectedDtService, dtService.toString());
    }
  }

}
