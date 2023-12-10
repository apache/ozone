/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.DatanodeAdminError;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.Arrays;
import java.util.ArrayList;

import static java.util.Collections.singletonList;
import static org.apache.hadoop.ozone.container.upgrade.UpgradeUtils.defaultLayoutVersionProto;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the decommission manager.
 */

public class TestNodeDecommissionManager {

  private NodeDecommissionManager decom;
  private StorageContainerManager scm;
  private NodeManager nodeManager;
  private OzoneConfiguration conf;

  @BeforeEach
  void setup(@TempDir File dir) throws Exception {
    conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.getAbsolutePath());
    nodeManager = createNodeManager(conf);
    decom = new NodeDecommissionManager(conf, nodeManager, null,
        SCMContext.emptyContext(), new EventQueue(), null);
  }

  @Test
  public void testHostStringsParseCorrectly()
      throws InvalidHostStringException {
    NodeDecommissionManager.HostDefinition def =
        new NodeDecommissionManager.HostDefinition("foobar");
    assertEquals("foobar", def.getHostname());
    assertEquals(-1, def.getPort());

    def = new NodeDecommissionManager.HostDefinition(" foobar ");
    assertEquals("foobar", def.getHostname());
    assertEquals(-1, def.getPort());

    def = new NodeDecommissionManager.HostDefinition("foobar:1234");
    assertEquals("foobar", def.getHostname());
    assertEquals(1234, def.getPort());

    def = new NodeDecommissionManager.HostDefinition(
        "foobar.mycompany.com:1234");
    assertEquals("foobar.mycompany.com", def.getHostname());
    assertEquals(1234, def.getPort());

    assertThrows(InvalidHostStringException.class,
        () -> new NodeDecommissionManager.HostDefinition("foobar:abcd"));
  }

  @Test
  public void testAnyInvalidHostThrowsException() {
    List<DatanodeDetails> dns = generateDatanodes();

    // Try to decommission a host that does exist, but give incorrect port
    List<DatanodeAdminError> error =
        decom.decommissionNodes(
            singletonList(dns.get(1).getIpAddress() + ":10"));
    assertEquals(1, error.size());
    assertTrue(error.get(0).getHostname().contains(dns.get(1).getIpAddress()));

    // Try to decommission a host that does not exist
    error = decom.decommissionNodes(singletonList("123.123.123.123"));
    assertEquals(1, error.size());
    assertTrue(error.get(0).getHostname().contains("123.123.123.123"));

    // Try to decommission a host that does exist and a host that does not
    error  = decom.decommissionNodes(Arrays.asList(dns.get(1).getIpAddress(),
        "123,123,123,123"));
    assertEquals(1, error.size());
    assertTrue(error.get(0).getHostname().contains("123,123,123,123"));

    // Try to decommission a host with many DNs on the address with no port
    error = decom.decommissionNodes(singletonList(dns.get(0).getIpAddress()));
    assertEquals(1, error.size());
    assertTrue(error.get(0).getHostname().contains(dns.get(0).getIpAddress()));

    // Try to decommission a host with many DNs on the address with a port
    // that does not exist
    error = decom.decommissionNodes(singletonList(dns.get(0).getIpAddress()
        + ":10"));
    assertEquals(1, error.size());
    assertTrue(error.get(0).getHostname().contains(dns.get(0).getIpAddress()
        + ":10"));

    // Try to decommission 2 hosts with address that does not exist
    // Both should return error
    error  = decom.decommissionNodes(Arrays.asList(
        "123.123.123.123", "234.234.234.234"));
    assertEquals(2, error.size());
    assertTrue(error.get(0).getHostname().contains("123.123.123.123") &&
        error.get(1).getHostname().contains("234.234.234.234"));
  }

  @Test
  public void testNodesCanBeDecommissionedAndRecommissioned()
      throws InvalidHostStringException, NodeNotFoundException {
    List<DatanodeDetails> dns = generateDatanodes();

    // Decommission 2 valid nodes
    decom.decommissionNodes(Arrays.asList(dns.get(1).getIpAddress(),
        dns.get(2).getIpAddress()));
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());

    // Running the command again gives no error - nodes already decommissioning
    // are silently ignored.
    decom.decommissionNodes(Arrays.asList(dns.get(1).getIpAddress(),
        dns.get(2).getIpAddress()));

    // Attempt to decommission dn(10) which has multiple hosts on the same IP
    // and we hardcoded ports to 3456, 4567, 5678
    DatanodeDetails multiDn = dns.get(10);
    String multiAddr =
        multiDn.getIpAddress() + ":" + multiDn.getPorts().get(0).getValue();
    decom.decommissionNodes(singletonList(multiAddr));
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(multiDn).getOperationalState());

    // Attempt to decommission on dn(9) which has another instance at
    // dn(11) with identical ports.
    nodeManager.processHeartbeat(dns.get(9), defaultLayoutVersionProto());
    DatanodeDetails duplicatePorts = dns.get(9);
    decom.decommissionNodes(singletonList(duplicatePorts.getIpAddress()));
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(duplicatePorts).getOperationalState());

    // Recommission all 3 hosts
    decom.recommissionNodes(Arrays.asList(
        multiAddr, dns.get(1).getIpAddress(), dns.get(2).getIpAddress(),
        duplicatePorts.getIpAddress()));
    decom.getMonitor().run();
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(10)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(duplicatePorts).getOperationalState());
  }

  @Test
  public void testNodesCanBeDecommissionedAndRecommissionedMixedPorts()
      throws InvalidHostStringException, NodeNotFoundException {
    List<DatanodeDetails> dns = generateDatanodes();

    // From the generateDatanodes method we have DNs at index 9 and 11 with the
    // same IP and port. We can add another DN with a different port on the
    // same IP so we have 3 registered from the same host and 2 distinct ports.
    DatanodeDetails sourceDN = dns.get(9);
    int ratisPort = sourceDN
        .getPort(DatanodeDetails.Port.Name.RATIS).getValue();
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(UUID.randomUUID())
        .setHostName(sourceDN.getHostName())
        .setIpAddress(sourceDN.getIpAddress())
        .addPort(DatanodeDetails.newPort(
            DatanodeDetails.Port.Name.STANDALONE,
            sourceDN.getPort(DatanodeDetails.Port.Name.STANDALONE)
                .getValue() + 1))
        .addPort(DatanodeDetails.newPort(
            DatanodeDetails.Port.Name.RATIS,
            ratisPort + 1))
        .addPort(DatanodeDetails.newPort(
            DatanodeDetails.Port.Name.REST,
            sourceDN.getPort(DatanodeDetails.Port.Name.REST).getValue() + 1))
        .setNetworkLocation(sourceDN.getNetworkLocation());
    DatanodeDetails extraDN = builder.build();
    dns.add(extraDN);
    nodeManager.register(extraDN, null, null);

    // Attempt to decommission with just the IP, which should fail.
    List<DatanodeAdminError> error =
        decom.decommissionNodes(singletonList(extraDN.getIpAddress()));
    assertEquals(1, error.size());
    assertTrue(error.get(0).getHostname().contains(extraDN.getIpAddress()));

    // Now try the one with the unique port
    decom.decommissionNodes(
        singletonList(extraDN.getIpAddress() + ":" + ratisPort + 1));

    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(extraDN).getOperationalState());

    decom.recommissionNodes(
        singletonList(extraDN.getIpAddress() + ":" + ratisPort + 1));
    decom.getMonitor().run();
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(extraDN).getOperationalState());

    // Now decommission one of the DNs with the duplicate port
    DatanodeDetails expectedDN = dns.get(9);
    nodeManager.processHeartbeat(expectedDN, defaultLayoutVersionProto());

    decom.decommissionNodes(singletonList(
        expectedDN.getIpAddress() + ":" + ratisPort));
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(expectedDN).getOperationalState());
    // The other duplicate is still in service
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(11)).getOperationalState());

    decom.recommissionNodes(singletonList(
        expectedDN.getIpAddress() + ":" + ratisPort));
    decom.getMonitor().run();
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(expectedDN).getOperationalState());
  }

  @Test
  public void testNodesCanBePutIntoMaintenanceAndRecommissioned()
      throws InvalidHostStringException, NodeNotFoundException {
    List<DatanodeDetails> dns = generateDatanodes();

    // Put 2 valid nodes into maintenance
    decom.startMaintenanceNodes(Arrays.asList(dns.get(1).getIpAddress(),
        dns.get(2).getIpAddress()), 100);
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertNotEquals(0, nodeManager.getNodeStatus(
        dns.get(1)).getOpStateExpiryEpochSeconds());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());
    assertNotEquals(0, nodeManager.getNodeStatus(
        dns.get(2)).getOpStateExpiryEpochSeconds());

    // Running the command again gives no error - nodes already decommissioning
    // are silently ignored.
    decom.startMaintenanceNodes(Arrays.asList(dns.get(1).getIpAddress(),
        dns.get(2).getIpAddress()), 100);

    // Attempt to decommission dn(10) which has multiple hosts on the same IP
    // and we hardcoded ports to 3456, 4567, 5678
    DatanodeDetails multiDn = dns.get(10);
    String multiAddr =
        multiDn.getIpAddress() + ":" + multiDn.getPorts().get(0).getValue();
    decom.startMaintenanceNodes(singletonList(multiAddr), 100);
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(multiDn).getOperationalState());

    // Attempt to enable maintenance on dn(9) which has another instance at
    // dn(11) with identical ports.
    nodeManager.processHeartbeat(dns.get(9), defaultLayoutVersionProto());
    DatanodeDetails duplicatePorts = dns.get(9);
    decom.startMaintenanceNodes(singletonList(duplicatePorts.getIpAddress()),
        100);
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(duplicatePorts).getOperationalState());

    // Recommission all 3 hosts
    decom.recommissionNodes(Arrays.asList(
        multiAddr, dns.get(1).getIpAddress(), dns.get(2).getIpAddress(),
        duplicatePorts.getIpAddress()));
    decom.getMonitor().run();
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(10)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(duplicatePorts).getOperationalState());
  }

  @Test
  public void testNodesCannotTransitionFromDecomToMaint() throws Exception {
    List<DatanodeDetails> dns = generateDatanodes();

    // Put 1 node into maintenance and another into decom
    decom.startMaintenance(dns.get(1), 100);
    decom.startDecommission(dns.get(2));
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());

    // Try to go from maint to decom:
    List<String> dn = new ArrayList<>();
    dn.add(dns.get(1).getIpAddress());
    List<DatanodeAdminError> errors = decom.decommissionNodes(dn);
    assertEquals(1, errors.size());
    assertEquals(dns.get(1).getHostName(), errors.get(0).getHostname());

    // Try to go from decom to maint:
    dn = new ArrayList<>();
    dn.add(dns.get(2).getIpAddress());
    errors = decom.startMaintenanceNodes(dn, 100);
    assertEquals(1, errors.size());
    assertEquals(dns.get(2).getHostName(), errors.get(0).getHostname());

    // Ensure the states are still as before
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());
  }

  @Test
  public void testNodeDecommissionManagerOnBecomeLeader() throws Exception {
    List<DatanodeDetails> dns = generateDatanodes();

    long maintenanceEnd =
        (System.currentTimeMillis() / 1000L) + (100 * 60L * 60L);

    // Put 1 node into entering_maintenance, 1 node into decommissioning
    // and 1 node into in_maintenance.
    nodeManager.setNodeOperationalState(dns.get(1),
        HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE, maintenanceEnd);
    nodeManager.setNodeOperationalState(dns.get(2),
        HddsProtos.NodeOperationalState.DECOMMISSIONING, 0);
    nodeManager.setNodeOperationalState(dns.get(3),
        HddsProtos.NodeOperationalState.IN_MAINTENANCE, maintenanceEnd);

    // trackedNodes should be empty now.
    assertEquals(decom.getMonitor().getTrackedNodes().size(), 0);

    // all nodes with decommissioning, entering_maintenance and in_maintenance
    // should be added to trackedNodes
    decom.onBecomeLeader();
    decom.getMonitor().run();

    // so size of trackedNodes will be 3.
    assertEquals(decom.getMonitor().getTrackedNodes().size(), 3);
  }

  private SCMNodeManager createNodeManager(OzoneConfiguration config)
      throws IOException, AuthenticationException {
    scm = HddsTestUtils.getScm(config);
    return (SCMNodeManager) scm.getScmNodeManager();
  }

  /**
   * Generate a list of random DNs and return the list. A total of 11 DNs will
   * be generated and registered with the node manager. Index 0 and 10 will
   * have the same IP and host and the rest will have unique IPs and Hosts.
   * The DN at index 10, has 3 hard coded ports of 3456, 4567, 5678. All other
   * DNs will have ports set to 0.
   * @return The list of DatanodeDetails Generated
   */
  private List<DatanodeDetails> generateDatanodes() {
    List<DatanodeDetails> dns = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      dns.add(dn);
      nodeManager.register(dn, null, null);
    }
    // We have 10 random DNs, we want to create another one that is on the same
    // host as some of the others, but with a different port
    DatanodeDetails multiDn = dns.get(0);

    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(UUID.randomUUID())
        .setHostName(multiDn.getHostName())
        .setIpAddress(multiDn.getIpAddress())
        .addPort(DatanodeDetails.newPort(
            DatanodeDetails.Port.Name.STANDALONE, 3456))
        .addPort(DatanodeDetails.newPort(
            DatanodeDetails.Port.Name.RATIS, 4567))
        .addPort(DatanodeDetails.newPort(
            DatanodeDetails.Port.Name.REST, 5678))
        .setNetworkLocation(multiDn.getNetworkLocation());

    DatanodeDetails dn = builder.build();
    dns.add(dn);
    nodeManager.register(dn, null, null);

    // Now add another DN with the same host and IP as dns(9), and with the
    // same port.
    DatanodeDetails duplicatePorts = dns.get(9);
    builder = DatanodeDetails.newBuilder();
    builder.setUuid(UUID.randomUUID())
        .setHostName(duplicatePorts.getHostName())
        .setIpAddress(duplicatePorts.getIpAddress())
        .addPort(DatanodeDetails.newPort(
            DatanodeDetails.Port.Name.STANDALONE,
            duplicatePorts.getPort(DatanodeDetails.Port.Name.STANDALONE)
                .getValue()))
        .addPort(DatanodeDetails.newPort(
            DatanodeDetails.Port.Name.RATIS,
            duplicatePorts.getPort(DatanodeDetails.Port.Name.RATIS).getValue()))
        .addPort(DatanodeDetails.newPort(
            DatanodeDetails.Port.Name.REST,
            duplicatePorts.getPort(DatanodeDetails.Port.Name.REST).getValue()))
        .setNetworkLocation(multiDn.getNetworkLocation());
    dn = builder.build();
    dns.add(dn);
    nodeManager.register(dn, null, null);

    return dns;
  }
}
