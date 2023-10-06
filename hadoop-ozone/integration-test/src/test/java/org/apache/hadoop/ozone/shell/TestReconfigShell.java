/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package org.apache.hadoop.ozone.shell;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.ReconfigurableBase;
import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.HddsDatanodeClientProtocolServer;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.ozone.test.GenericTestUtils.SystemOutCapturer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.apache.ozone.test.JUnit5AwareTimeout;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;

/**
 * * Integration test for {@code ozone admin reconfig} command. HA enabled.
 */
public class TestReconfigShell {

  private static final int DATANODE_COUNT = 3;

  /**
   * Set a timeout for each test.
   */
  @Rule
  public TestRule timeout = new JUnit5AwareTimeout(Timeout.seconds(300));

  private static MiniOzoneCluster cluster;
  private static List<HddsDatanodeService> datanodeServices;
  private static OzoneAdmin ozoneAdmin;
  private static OzoneManager ozoneManager;
  private static StorageContainerManager storageContainerManager;
  private static NodeManager nm;


  /**
   * Create a Mini Cluster for testing.
   */
  @BeforeClass
  public static void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    String omServiceId = UUID.randomUUID().toString();
    cluster = MiniOzoneCluster.newOMHABuilder(conf)
        .setClusterId(UUID.randomUUID().toString())
        .setScmId(UUID.randomUUID().toString())
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(1)
        .setNumOfStorageContainerManagers(1)
        .setNumDatanodes(DATANODE_COUNT)
        .build();
    cluster.waitForClusterToBeReady();
    ozoneAdmin = new OzoneAdmin(cluster.getConf());
    ozoneManager = cluster.getOzoneManager();
    storageContainerManager = cluster.getStorageContainerManager();
    datanodeServices = cluster.getHddsDatanodes();
    nm = storageContainerManager.getScmNodeManager();
  }

  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testDataNodeGetReconfigurableProperties() throws Exception {
    try (SystemOutCapturer capture = new SystemOutCapturer()) {
      for (HddsDatanodeService datanodeService : datanodeServices) {
        HddsDatanodeClientProtocolServer server =
            datanodeService.getClientProtocolServer();
        InetSocketAddress socket = server.getClientRpcAddress();
        executeAndAssertProperties(datanodeService.getReconfigurationHandler(),
            socket, capture);
      }
    }
  }

  @Test
  public void testOzoneManagerGetReconfigurationProperties() throws Exception {
    try (SystemOutCapturer capture = new SystemOutCapturer()) {
      InetSocketAddress socket = ozoneManager.getOmRpcServerAddr();
      executeAndAssertProperties(ozoneManager.getReconfigurationHandler(),
          socket, capture);
    }
  }

  @Test
  public void testStorageContainerManagerGetReconfigurationProperties()
      throws Exception {
    try (SystemOutCapturer capture = new SystemOutCapturer()) {
      InetSocketAddress socket = storageContainerManager.getClientRpcAddress();
      executeAndAssertProperties(
          storageContainerManager.getReconfigurationHandler(), socket, capture);
    }
  }

  private void executeAndAssertProperties(
      ReconfigurableBase reconfigurableBase,
      InetSocketAddress socket, SystemOutCapturer capture)
      throws UnsupportedEncodingException {
    String address = socket.getHostString() + ":" + socket.getPort();
    ozoneAdmin.execute(
        new String[] {"reconfig", "--address", address, "properties"});
    assertReconfigurablePropertiesOutput(
        reconfigurableBase.getReconfigurableProperties(), capture.getOutput());
  }

  private void assertReconfigurablePropertiesOutput(
      Collection<String> except, String output) {
    List<String> outs =
        Arrays.asList(output.split(System.getProperty("line.separator")));
    for (String property : except) {
      Assert.assertTrue(
          String.format("Not found %s in output: %s", property, output),
          outs.contains(property));
    }
  }

  @Test
  public void testDatanodeBulkReconfig() throws Exception {
    // All Dn are normal, So All the Dn will be reconfig
    List<HddsDatanodeService> dns = cluster.getHddsDatanodes();
    Assert.assertEquals(DATANODE_COUNT, dns.size());
    executeAndAssertBulkReconfigCount(DATANODE_COUNT);

    // Shutdown a Dn, it will not be reconfig,
    // so only (datanodeCount - 1) Dn will be configured successfully
    cluster.shutdownHddsDatanode(0);
    executeAndAssertBulkReconfigCount(DATANODE_COUNT - 1);
    cluster.restartHddsDatanode(0, true);
    executeAndAssertBulkReconfigCount(DATANODE_COUNT);

    // DECOMMISSIONED a Dn, it will not be reconfig,
    // so only (datanodeCount - 1) Dn will be configured successfully
    DatanodeDetails details = dns.get(1).getDatanodeDetails();
    storageContainerManager.getScmDecommissionManager()
        .startDecommission(details);
    nm.setNodeOperationalState(details, DECOMMISSIONED);
    executeAndAssertBulkReconfigCount(DATANODE_COUNT - 1);
    storageContainerManager.getScmDecommissionManager()
        .recommission(details);
    nm.setNodeOperationalState(details, IN_SERVICE);
    executeAndAssertBulkReconfigCount(DATANODE_COUNT);
  }

  private void executeAndAssertBulkReconfigCount(int except)
      throws Exception {
    try (SystemOutCapturer capture = new SystemOutCapturer()) {
      ozoneAdmin.execute(new String[] {
          "reconfig",  "--in-service-datanodes", "properties"});
      String output = capture.getOutput();

      Assert.assertTrue(String.format(
          "Excepted successfully %d. output: %s%n", except, output),
          capture.getOutput().contains(
              String.format("successfully %d", except)));
    }
  }
}
