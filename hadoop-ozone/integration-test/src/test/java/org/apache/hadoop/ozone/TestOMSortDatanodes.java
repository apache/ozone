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

package org.apache.hadoop.ozone;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_LEVEL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.KeyManagerImpl;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link org.apache.hadoop.hdds.scm.server.TestSCMBlockProtocolServer}
 * sortDatanodes tests for
 * {@link org.apache.hadoop.ozone.om.KeyManagerImpl#sortDatanodes(List, String)}.
 */
public class TestOMSortDatanodes {

  @TempDir
  private static File dir;
  private static OzoneConfiguration config;
  private static StorageContainerManager scm;
  private static NodeManager nodeManager;
  private static KeyManagerImpl keyManager;
  private static OzoneManager om;
  private static final int NODE_COUNT = 10;
  private static final Map<String, String> EDGE_NODES = ImmutableMap.of(
      "edge0", "/rack0",
      "edge1", "/rack1"
  );

  private static OzoneClient ozoneClient;

  @BeforeAll
  public static void setup() throws Exception {
    config = new OzoneConfiguration();
    config.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.toString());
    config.set(NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        StaticMapping.class.getName());
    config.set(OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY, "true");
    List<DatanodeDetails> datanodes = new ArrayList<>(NODE_COUNT);
    List<String> nodeMapping = new ArrayList<>(NODE_COUNT);
    for (int i = 0; i < NODE_COUNT; i++) {
      DatanodeDetails dn = randomDatanodeDetails();
      final String rack = "/rack" + (i % 2);
      nodeMapping.add(dn.getHostName() + "=" + rack);
      nodeMapping.add(dn.getIpAddress() + "=" + rack);
      datanodes.add(dn);
    }
    EDGE_NODES.forEach((n, r) -> nodeMapping.add(n + "=" + r));
    config.set(StaticMapping.KEY_HADOOP_CONFIGURED_NODE_MAPPING,
        String.join(",", nodeMapping));

    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setSCMHAManager(SCMHAManagerStub.getInstance(true));
    configurator.setScmContext(SCMContext.emptyContext());
    scm = HddsTestUtils.getScm(config, configurator);
    scm.start();
    scm.exitSafeMode();
    nodeManager = scm.getScmNodeManager();
    datanodes.forEach(dn -> nodeManager.register(dn, null, null));
    StorageContainerLocationProtocol mockScmContainerClient = mock(StorageContainerLocationProtocol.class);
    OmTestManagers omTestManagers
        = new OmTestManagers(config, scm.getBlockProtocolServer(),
        mockScmContainerClient);
    om = omTestManagers.getOzoneManager();
    ozoneClient = omTestManagers.getRpcClient();
    keyManager = (KeyManagerImpl)omTestManagers.getKeyManager();
  }

  @AfterAll
  public static void cleanup() throws Exception {
    if (ozoneClient != null) {
      ozoneClient.close();
    }
    if (scm != null) {
      scm.stop();
      scm.join();
    }
    if (om != null) {
      om.stop();
    }
  }

  @Test
  public void sortDatanodesRelativeToDatanode() {
    for (DatanodeDetails dn : nodeManager.getAllNodes()) {
      assertEquals(ROOT_LEVEL + 2, dn.getLevel());
      List<? extends DatanodeDetails> sorted =
          keyManager.sortDatanodes(nodeManager.getAllNodes(), nodeAddress(dn));
      assertEquals(dn, sorted.get(0),
          "Source node should be sorted very first");
      assertRackOrder(dn.getNetworkLocation(), sorted);
    }
  }

  @Test
  public void sortDatanodesRelativeToNonDatanode() {
    for (Map.Entry<String, String> entry : EDGE_NODES.entrySet()) {
      assertRackOrder(entry.getValue(),
          keyManager.sortDatanodes(nodeManager.getAllNodes(), entry.getKey()));
    }
  }

  @Test
  public void testSortDatanodes() {
    List<? extends DatanodeDetails> nodes = nodeManager.getAllNodes();

    // sort normal datanodes
    String client;
    client = nodeManager.getAllNodes().get(0).getIpAddress();
    List<? extends DatanodeDetails> datanodeDetails =
        keyManager.sortDatanodes(nodes, client);
    assertEquals(NODE_COUNT, datanodeDetails.size());

    // illegal client 1
    client += "X";
    datanodeDetails = keyManager.sortDatanodes(nodes, client);
    assertEquals(NODE_COUNT, datanodeDetails.size());

    // illegal client 2
    client = "/default-rack";
    datanodeDetails = keyManager.sortDatanodes(nodes, client);
    assertEquals(NODE_COUNT, datanodeDetails.size());
  }

  private static void assertRackOrder(String rack, List<? extends DatanodeDetails> list) {
    int size = list.size();
    for (int i = 0; i < size / 2; i++) {
      assertEquals(rack, list.get(i).getNetworkLocation(),
          "Nodes in the same rack should be sorted first");
    }
    for (int i = size / 2; i < size; i++) {
      assertNotEquals(rack, list.get(i).getNetworkLocation(),
          "Nodes in the other rack should be sorted last");
    }
  }

  private String nodeAddress(DatanodeDetails dn) {
    boolean useHostname = config.getBoolean(
        HddsConfigKeys.HDDS_DATANODE_USE_DN_HOSTNAME,
        HddsConfigKeys.HDDS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
    return useHostname ? dn.getHostName() : dn.getIpAddress();
  }
}
