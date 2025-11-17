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

package org.apache.hadoop.hdds.scm.node;

import static java.util.Collections.singletonList;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.OPEN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.DatanodeAdminError;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * Unit tests for the decommission manager.
 */
public class TestNodeDecommissionManager {

  private NodeDecommissionManager decom;
  private SCMNodeManager nodeManager;
  private ContainerManager containerManager;
  private OzoneConfiguration conf;
  private static int id = 1;

  @BeforeEach
  void setup(@TempDir File dir) throws Exception {
    conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.getAbsolutePath());
    StorageContainerManager scm = HddsTestUtils.getScm(conf);
    nodeManager = (SCMNodeManager) scm.getScmNodeManager();
    containerManager = mock(ContainerManager.class);
    decom = new NodeDecommissionManager(conf, nodeManager, containerManager,
        SCMContext.emptyContext(), new EventQueue(), null);
    when(containerManager.allocateContainer(any(ReplicationConfig.class), anyString()))
        .thenAnswer(invocation -> createMockContainer((ReplicationConfig)invocation.getArguments()[0],
            (String) invocation.getArguments()[1]));
  }

  void setContainers(DatanodeDetails datanode, Set<ContainerID> containers) throws NodeNotFoundException {
    ScmNodeTestUtil.setContainers(nodeManager, datanode, containers);
  }

  private ContainerInfo createMockContainer(ReplicationConfig rep, String owner) {
    ContainerInfo.Builder builder = new ContainerInfo.Builder()
        .setReplicationConfig(rep)
        .setContainerID(id)
        .setPipelineID(PipelineID.randomId())
        .setState(OPEN)
        .setOwner(owner);
    id++;
    return builder.build();
  }

  private ContainerInfo getMockContainer(ReplicationConfig rep, ContainerID conId) {
    ContainerInfo.Builder builder = new ContainerInfo.Builder()
        .setReplicationConfig(rep)
        .setContainerID(conId.getId())
        .setPipelineID(PipelineID.randomId())
        .setState(OPEN)
        .setOwner("admin");
    return builder.build();
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
            singletonList(dns.get(1).getIpAddress() + ":10"), false);
    assertEquals(1, error.size());
    assertThat(error.get(0).getHostname()).contains(dns.get(1).getIpAddress());

    // Try to decommission a host that does not exist
    error = decom.decommissionNodes(singletonList("123.123.123.123"), false);
    assertEquals(1, error.size());
    assertThat(error.get(0).getHostname()).contains("123.123.123.123");

    // Try to decommission a host that does exist and a host that does not
    error  = decom.decommissionNodes(Arrays.asList(dns.get(1).getIpAddress(),
        "123,123,123,123"), false);
    assertEquals(1, error.size());
    assertThat(error.get(0).getHostname()).contains("123,123,123,123");

    // Try to decommission a host with many DNs on the address with no port
    error = decom.decommissionNodes(singletonList(dns.get(0).getIpAddress()), false);
    assertEquals(1, error.size());
    assertThat(error.get(0).getHostname()).contains(dns.get(0).getIpAddress());

    // Try to decommission a host with many DNs on the address with a port
    // that does not exist
    error = decom.decommissionNodes(singletonList(dns.get(0).getIpAddress()
        + ":10"), false);
    assertEquals(1, error.size());
    assertThat(error.get(0).getHostname()).contains(dns.get(0).getIpAddress() + ":10");

    // Try to decommission 2 hosts with address that does not exist
    // Both should return error
    error  = decom.decommissionNodes(Arrays.asList(
        "123.123.123.123", "234.234.234.234"), false);
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
        dns.get(2).getIpAddress()), false);
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());

    // Running the command again gives no error - nodes already decommissioning
    // are silently ignored.
    decom.decommissionNodes(Arrays.asList(dns.get(1).getIpAddress(),
        dns.get(2).getIpAddress()), false);

    // Attempt to decommission dn(10) which has multiple hosts on the same IP
    // and we hardcoded ports to 3456, 4567, 5678
    DatanodeDetails multiDn = dns.get(10);
    String multiAddr =
        multiDn.getIpAddress() + ":" + multiDn.getPorts().get(0).getValue();
    decom.decommissionNodes(singletonList(multiAddr), false);
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(multiDn).getOperationalState());

    // Attempt to decommission on dn(9) which has another instance at
    // dn(11) with identical ports.
    nodeManager.processHeartbeat(dns.get(9));
    DatanodeDetails duplicatePorts = dns.get(9);
    decom.decommissionNodes(singletonList(duplicatePorts.getIpAddress()), false);
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
        .getRatisPort().getValue();
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(UUID.randomUUID())
        .setHostName(sourceDN.getHostName())
        .setIpAddress(sourceDN.getIpAddress())
        .addPort(DatanodeDetails.newStandalonePort(sourceDN.getStandalonePort()
            .getValue() + 1))
        .addPort(DatanodeDetails.newRatisPort(ratisPort + 1))
        .addPort(DatanodeDetails.newRestPort(sourceDN.getRestPort().getValue() + 1))
        .setNetworkLocation(sourceDN.getNetworkLocation());
    DatanodeDetails extraDN = builder.build();
    dns.add(extraDN);
    nodeManager.register(extraDN, null, null);

    // Attempt to decommission with just the IP, which should fail.
    List<DatanodeAdminError> error =
        decom.decommissionNodes(singletonList(extraDN.getIpAddress()), false);
    assertEquals(1, error.size());
    assertThat(error.get(0).getHostname()).contains(extraDN.getIpAddress());

    // Now try the one with the unique port
    decom.decommissionNodes(
        singletonList(extraDN.getIpAddress() + ":" + ratisPort + 1), false);

    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(extraDN).getOperationalState());

    decom.recommissionNodes(
        singletonList(extraDN.getIpAddress() + ":" + ratisPort + 1));
    decom.getMonitor().run();
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(extraDN).getOperationalState());

    // Now decommission one of the DNs with the duplicate port
    DatanodeDetails expectedDN = dns.get(9);
    nodeManager.processHeartbeat(expectedDN);

    decom.decommissionNodes(singletonList(
        expectedDN.getIpAddress() + ":" + ratisPort), false);
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
        dns.get(2).getIpAddress()), 100, true);
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
        dns.get(2).getIpAddress()), 100, true);

    // Attempt to decommission dn(10) which has multiple hosts on the same IP
    // and we hardcoded ports to 3456, 4567, 5678
    DatanodeDetails multiDn = dns.get(10);
    String multiAddr =
        multiDn.getIpAddress() + ":" + multiDn.getPorts().get(0).getValue();
    decom.startMaintenanceNodes(singletonList(multiAddr), 100, true);
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(multiDn).getOperationalState());

    // Attempt to enable maintenance on dn(9) which has another instance at
    // dn(11) with identical ports.
    nodeManager.processHeartbeat(dns.get(9));
    DatanodeDetails duplicatePorts = dns.get(9);
    decom.startMaintenanceNodes(singletonList(duplicatePorts.getIpAddress()),
        100, true);
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
    List<DatanodeAdminError> errors = decom.decommissionNodes(dn, false);
    assertEquals(1, errors.size());
    assertEquals(dns.get(1).getHostName(), errors.get(0).getHostname());

    // Try to go from decom to maint:
    dn = new ArrayList<>();
    dn.add(dns.get(2).getIpAddress());
    errors = decom.startMaintenanceNodes(dn, 100, true);
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

  @Test
  public void testInsufficientNodeDecommissionThrowsExceptionForRatis() throws
      NodeNotFoundException, IOException {
    when(containerManager.getContainer(any(ContainerID.class)))
        .thenAnswer(invocation -> getMockContainer(RatisReplicationConfig
                .getInstance(HddsProtos.ReplicationFactor.THREE), (ContainerID)invocation.getArguments()[0]));
    List<DatanodeAdminError> error;
    List<DatanodeDetails> dns = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      dns.add(dn);
      nodeManager.register(dn, null, null);
    }

    Set<ContainerID> idsRatis = new HashSet<>();
    for (int i = 0; i < 5; i++) {
      ContainerInfo container = containerManager.allocateContainer(
          RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE), "admin");
      idsRatis.add(container.containerID());
    }

    for (DatanodeDetails dn  : nodeManager.getAllNodes().subList(0, 3)) {
      setContainers(dn, idsRatis);
    }

    error = decom.decommissionNodes(Arrays.asList(dns.get(1).getIpAddress(),
        dns.get(2).getIpAddress(), dns.get(3).getIpAddress(), dns.get(4).getIpAddress()), false);
    assertTrue(error.get(0).getHostname().contains("AllHosts"));
    String errorMsg = String.format("%d IN-SERVICE HEALTHY and %d not IN-SERVICE or not HEALTHY nodes.", 5, 0);
    assertTrue(error.get(0).getError().contains(errorMsg));
    errorMsg = String.format("Cannot decommission as a minimum of %d IN-SERVICE HEALTHY nodes are required", 3);
    assertTrue(error.get(0).getError().contains(errorMsg));
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(3)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(4)).getOperationalState());

    error = decom.decommissionNodes(Arrays.asList(dns.get(1).getIpAddress(),
        dns.get(2).getIpAddress(), dns.get(3).getIpAddress(), dns.get(4).getIpAddress()), true);
    assertEquals(0, error.size());
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dns.get(3)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dns.get(4)).getOperationalState());
  }

  @Test
  public void testInsufficientNodeDecommissionThrowsExceptionForEc() throws
      NodeNotFoundException, IOException {
    when(containerManager.getContainer(any(ContainerID.class)))
        .thenAnswer(invocation -> getMockContainer(new ECReplicationConfig(3, 2),
            (ContainerID)invocation.getArguments()[0]));
    List<DatanodeAdminError> error;
    List<DatanodeDetails> dns = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      dns.add(dn);
      nodeManager.register(dn, null, null);
    }

    Set<ContainerID> idsEC = new HashSet<>();
    for (int i = 0; i < 5; i++) {
      ContainerInfo container = containerManager.allocateContainer(new ECReplicationConfig(3, 2), "admin");
      idsEC.add(container.containerID());
    }

    for (DatanodeDetails dn  : nodeManager.getAllNodes()) {
      setContainers(dn, idsEC);
    }

    error = decom.decommissionNodes(Arrays.asList(dns.get(1).getIpAddress()), false);
    assertTrue(error.get(0).getHostname().contains("AllHosts"));
    String errorMsg = String.format("%d IN-SERVICE HEALTHY and %d not IN-SERVICE or not HEALTHY nodes.", 5, 0);
    assertTrue(error.get(0).getError().contains(errorMsg));
    errorMsg = String.format("Cannot decommission as a minimum of %d IN-SERVICE HEALTHY nodes are required", 5);
    assertTrue(error.get(0).getError().contains(errorMsg));
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    error = decom.decommissionNodes(Arrays.asList(dns.get(1).getIpAddress()), true);
    assertEquals(0, error.size());
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
  }

  @Test
  public void testInsufficientNodeDecommissionThrowsExceptionRatisAndEc() throws
      NodeNotFoundException, IOException {
    List<DatanodeAdminError> error;
    List<DatanodeDetails> dns = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      dns.add(dn);
      nodeManager.register(dn, null, null);
    }

    Set<ContainerID> idsRatis = new HashSet<>();
    ContainerInfo containerRatis = containerManager.allocateContainer(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE), "admin");
    idsRatis.add(containerRatis.containerID());
    Set<ContainerID> idsEC = new HashSet<>();
    ContainerInfo containerEC = containerManager.allocateContainer(new ECReplicationConfig(3, 2), "admin");
    idsEC.add(containerEC.containerID());

    when(containerManager.getContainer(any(ContainerID.class)))
        .thenAnswer(invocation -> {
          ContainerID containerID = (ContainerID)invocation.getArguments()[0];
          if (idsEC.contains(containerID)) {
            return getMockContainer(new ECReplicationConfig(3, 2),
                (ContainerID)invocation.getArguments()[0]);
          }
          return getMockContainer(RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
              (ContainerID)invocation.getArguments()[0]);
        });

    for (DatanodeDetails dn  : nodeManager.getAllNodes().subList(0, 3)) {
      setContainers(dn, idsRatis);
    }
    for (DatanodeDetails dn  : nodeManager.getAllNodes()) {
      setContainers(dn, idsEC);
    }

    error = decom.decommissionNodes(Arrays.asList(dns.get(1).getIpAddress()), false);
    assertTrue(error.get(0).getHostname().contains("AllHosts"));
    String errorMsg = String.format("%d IN-SERVICE HEALTHY and %d not IN-SERVICE or not HEALTHY nodes.", 5, 0);
    assertTrue(error.get(0).getError().contains(errorMsg));
    errorMsg = String.format("Cannot decommission as a minimum of %d IN-SERVICE HEALTHY nodes are required", 5);
    assertTrue(error.get(0).getError().contains(errorMsg));
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    error = decom.decommissionNodes(Arrays.asList(dns.get(1).getIpAddress()), true);
    assertEquals(0, error.size());
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
  }

  @Test
  public void testInsufficientNodeDecommissionChecksNotInService() throws
      NodeNotFoundException, IOException {
    when(containerManager.getContainer(any(ContainerID.class)))
        .thenAnswer(invocation -> getMockContainer(RatisReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.THREE), (ContainerID)invocation.getArguments()[0]));

    List<DatanodeAdminError> error;
    List<DatanodeDetails> dns = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      dns.add(dn);
      nodeManager.register(dn, null, null);
    }

    Set<ContainerID> idsRatis = new HashSet<>();
    for (int i = 0; i < 5; i++) {
      ContainerInfo container = containerManager.allocateContainer(
          RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE), "admin");
      idsRatis.add(container.containerID());
    }

    for (DatanodeDetails dn  : nodeManager.getAllNodes().subList(0, 3)) {
      setContainers(dn, idsRatis);
    }

    // decommission one node successfully
    error = decom.decommissionNodes(Arrays.asList(dns.get(0).getIpAddress()), false);
    assertEquals(0, error.size());
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dns.get(0)).getOperationalState());
    // try to decommission 2 nodes, one in service and one in decommissioning state, should be successful.
    error = decom.decommissionNodes(Arrays.asList(dns.get(0).getIpAddress(),
        dns.get(1).getIpAddress()), false);
    assertEquals(0, error.size());
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dns.get(0)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
  }

  @Test
  public void testInsufficientNodeDecommissionChecksForNNF() throws
      NodeNotFoundException, IOException {
    List<DatanodeAdminError> error;
    List<DatanodeDetails> dns = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      dns.add(dn);
    }
    Set<ContainerID> idsRatis = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      ContainerInfo container = containerManager.allocateContainer(
          RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE), "admin");
      idsRatis.add(container.containerID());
    }

    nodeManager = mock(SCMNodeManager.class);
    decom = new NodeDecommissionManager(conf, nodeManager, containerManager,
        SCMContext.emptyContext(), new EventQueue(), null);
    when(containerManager.getContainer(any(ContainerID.class)))
        .thenAnswer(invocation -> getMockContainer(RatisReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.THREE), (ContainerID)invocation.getArguments()[0]));
    when(nodeManager.getNodesByAddress(any())).thenAnswer(invocation ->
        getDatanodeDetailsList((String)invocation.getArguments()[0], dns));
    when(nodeManager.getContainers(any())).thenReturn(idsRatis);
    when(nodeManager.getNodeCount(any())).thenReturn(5);

    when(nodeManager.getNodeStatus(any())).thenAnswer(invocation ->
        getNodeOpState((DatanodeDetails) invocation.getArguments()[0], dns));
    Mockito.doAnswer(invocation -> {
      setNodeOpState((DatanodeDetails)invocation.getArguments()[0],
          (HddsProtos.NodeOperationalState)invocation.getArguments()[1], dns);
      return null;
    }).when(nodeManager).setNodeOperationalState(any(DatanodeDetails.class), any(
        HddsProtos.NodeOperationalState.class));

    error = decom.decommissionNodes(Arrays.asList(dns.get(1).getIpAddress(),
        dns.get(2).getIpAddress(), dns.get(3).getIpAddress()), false);
    assertTrue(error.get(0).getHostname().contains("AllHosts"));
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(3)).getOperationalState());

    error = decom.decommissionNodes(Arrays.asList(dns.get(0).getIpAddress(),
        dns.get(1).getIpAddress(), dns.get(2).getIpAddress()), false);
    assertFalse(error.get(0).getHostname().contains("AllHosts"));
    assertTrue(error.get(0).getError().contains("The host was not found in SCM"));
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());
  }

  @Test
  public void testInsufficientNodeMaintenanceThrowsExceptionForRatis() throws
      NodeNotFoundException, IOException {
    when(containerManager.getContainer(any(ContainerID.class)))
        .thenAnswer(invocation -> getMockContainer(RatisReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.THREE), (ContainerID)invocation.getArguments()[0]));
    List<DatanodeAdminError> error;
    List<DatanodeDetails> dns = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      dns.add(dn);
      nodeManager.register(dn, null, null);
    }
    Set<ContainerID> idsRatis = new HashSet<>();
    for (int i = 0; i < 5; i++) {
      ContainerInfo container = containerManager.allocateContainer(
          RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE), "admin");
      idsRatis.add(container.containerID());
    }
    for (DatanodeDetails dn  : nodeManager.getAllNodes().subList(0, 3)) {
      setContainers(dn, idsRatis);
    }

    decom.setMaintenanceConfigs(2, 1); // default config
    // putting 4 DNs into maintenance leave the cluster with 1 DN,
    // it should not be allowed as maintenance.replica.minimum is 2
    error = decom.startMaintenanceNodes(Arrays.asList(dns.get(1).getIpAddress(),
        dns.get(2).getIpAddress(), dns.get(3).getIpAddress(), dns.get(4).getIpAddress()), 100, false);
    assertTrue(error.get(0).getHostname().contains("AllHosts"));
    String errorMsg = String.format("%d IN-SERVICE HEALTHY and %d not IN-SERVICE or not HEALTHY nodes.", 5, 0);
    assertTrue(error.get(0).getError().contains(errorMsg));
    errorMsg = String.format("Cannot enter maintenance mode as a minimum of %d IN-SERVICE HEALTHY nodes are required",
        2);
    assertTrue(error.get(0).getError().contains(errorMsg));
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(3)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(4)).getOperationalState());
    // putting 3 DNs into maintenance leave the cluster with 2 DN,
    // it should be allowed
    error = decom.startMaintenanceNodes(Arrays.asList(dns.get(1).getIpAddress(),
        dns.get(2).getIpAddress(), dns.get(3).getIpAddress()), 100, false);
    assertEquals(0, error.size());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(3)).getOperationalState());

    decom.recommissionNodes(Arrays.asList(dns.get(1).getIpAddress(),
        dns.get(2).getIpAddress(), dns.get(3).getIpAddress(), dns.get(4).getIpAddress()));
    decom.getMonitor().run();
    assertEquals(5, nodeManager.getNodeCount(NodeStatus.inServiceHealthy()));

    decom.setMaintenanceConfigs(3, 1); // non-default config
    // putting 3 DNs into maintenance leave the cluster with 2 DN,
    // it should not be allowed as maintenance.replica.minimum is 3
    error = decom.startMaintenanceNodes(Arrays.asList(dns.get(1).getIpAddress(),
        dns.get(2).getIpAddress(), dns.get(3).getIpAddress()), 100, false);
    assertTrue(error.get(0).getHostname().contains("AllHosts"));
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(3)).getOperationalState());
    // putting 2 DNs into maintenance leave the cluster with 2 DN,
    // it should be allowed
    error = decom.startMaintenanceNodes(Arrays.asList(dns.get(1).getIpAddress(),
        dns.get(2).getIpAddress()), 100, false);
    assertEquals(0, error.size());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());

    decom.recommissionNodes(Arrays.asList(dns.get(1).getIpAddress(),
        dns.get(2).getIpAddress(), dns.get(3).getIpAddress(), dns.get(4).getIpAddress()));
    decom.getMonitor().run();
    assertEquals(5, nodeManager.getNodeCount(NodeStatus.inServiceHealthy()));

    // forcing 4 DNs into maintenance should be allowed
    error = decom.startMaintenanceNodes(Arrays.asList(dns.get(1).getIpAddress(),
        dns.get(2).getIpAddress(), dns.get(3).getIpAddress(), dns.get(4).getIpAddress()), 100, true);
    assertEquals(0, error.size());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(3)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(4)).getOperationalState());
  }

  @Test
  public void testInsufficientNodeMaintenanceThrowsExceptionForEc() throws
      NodeNotFoundException, IOException {
    when(containerManager.getContainer(any(ContainerID.class)))
        .thenAnswer(invocation -> getMockContainer(new ECReplicationConfig(3, 2),
            (ContainerID)invocation.getArguments()[0]));
    List<DatanodeAdminError> error;
    List<DatanodeDetails> dns = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      dns.add(dn);
      nodeManager.register(dn, null, null);
    }
    Set<ContainerID> idsEC = new HashSet<>();
    for (int i = 0; i < 5; i++) {
      ContainerInfo container = containerManager.allocateContainer(new ECReplicationConfig(3, 2), "admin");
      idsEC.add(container.containerID());
    }
    for (DatanodeDetails dn  : nodeManager.getAllNodes()) {
      setContainers(dn, idsEC);
    }

    decom.setMaintenanceConfigs(2, 1); // default config
    // putting 2 DNs into maintenance leave the cluster with 3 DN,
    // it should not be allowed as maintenance.remaining.redundancy is 1 => 3+1=4 DNs are required
    error = decom.startMaintenanceNodes(Arrays.asList(dns.get(1).getIpAddress(), dns.get(2).getIpAddress()),
        100, false);
    assertTrue(error.get(0).getHostname().contains("AllHosts"));
    String errorMsg = String.format("%d IN-SERVICE HEALTHY and %d not IN-SERVICE or not HEALTHY nodes.", 5, 0);
    assertTrue(error.get(0).getError().contains(errorMsg));
    errorMsg = String.format("Cannot enter maintenance mode as a minimum of %d IN-SERVICE HEALTHY nodes are required",
        4);
    assertTrue(error.get(0).getError().contains(errorMsg));
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());
    // putting 1 DN into maintenance leave the cluster with 4 DN,
    // it should be allowed
    error = decom.startMaintenanceNodes(Arrays.asList(dns.get(1).getIpAddress()), 100, false);
    assertEquals(0, error.size());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());

    decom.recommissionNodes(Arrays.asList(dns.get(1).getIpAddress(), dns.get(2).getIpAddress()));
    decom.getMonitor().run();
    assertEquals(5, nodeManager.getNodeCount(NodeStatus.inServiceHealthy()));

    decom.setMaintenanceConfigs(2, 2); // non-default config
    // putting 1 DNs into maintenance leave the cluster with 4 DN,
    // it should not be allowed as maintenance.remaining.redundancy is 2 => 3+2=5 DNs are required
    error = decom.startMaintenanceNodes(Arrays.asList(dns.get(1).getIpAddress()), 100, false);
    assertTrue(error.get(0).getHostname().contains("AllHosts"));
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());

    decom.recommissionNodes(Arrays.asList(dns.get(1).getIpAddress(), dns.get(2).getIpAddress()));
    decom.getMonitor().run();
    assertEquals(5, nodeManager.getNodeCount(NodeStatus.inServiceHealthy()));

    // forcing 2 DNs into maintenance should be allowed
    error = decom.startMaintenanceNodes(Arrays.asList(dns.get(1).getIpAddress(), dns.get(2).getIpAddress()), 100, true);
    assertEquals(0, error.size());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());
  }

  @Test
  public void testInsufficientNodeMaintenanceThrowsExceptionForRatisAndEc() throws
      NodeNotFoundException, IOException {
    when(containerManager.getContainer(any(ContainerID.class)))
        .thenAnswer(invocation -> getMockContainer(new ECReplicationConfig(3, 2),
            (ContainerID)invocation.getArguments()[0]));
    List<DatanodeAdminError> error;
    List<DatanodeDetails> dns = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      dns.add(dn);
      nodeManager.register(dn, null, null);
    }
    Set<ContainerID> idsRatis = new HashSet<>();
    ContainerInfo containerRatis = containerManager.allocateContainer(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE), "admin");
    idsRatis.add(containerRatis.containerID());
    Set<ContainerID> idsEC = new HashSet<>();
    ContainerInfo containerEC = containerManager.allocateContainer(new ECReplicationConfig(3, 2), "admin");
    idsEC.add(containerEC.containerID());

    when(containerManager.getContainer(any(ContainerID.class)))
        .thenAnswer(invocation -> {
          ContainerID containerID = (ContainerID)invocation.getArguments()[0];
          if (idsEC.contains(containerID)) {
            return getMockContainer(new ECReplicationConfig(3, 2),
                (ContainerID)invocation.getArguments()[0]);
          }
          return getMockContainer(RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
              (ContainerID)invocation.getArguments()[0]);
        });
    for (DatanodeDetails dn  : nodeManager.getAllNodes().subList(0, 3)) {
      setContainers(dn, idsRatis);
    }
    for (DatanodeDetails dn  : nodeManager.getAllNodes()) {
      setContainers(dn, idsEC);
    }

    decom.setMaintenanceConfigs(2, 1); // default config
    // putting 2 DNs into maintenance leave the cluster with 3 DN,
    // it should not be allowed as maintenance.remaining.redundancy is 1 => 3+1=4 DNs are required
    error = decom.startMaintenanceNodes(Arrays.asList(dns.get(1).getIpAddress(), dns.get(2).getIpAddress()),
        100, false);
    assertTrue(error.get(0).getHostname().contains("AllHosts"));
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());
    // putting 1 DN into maintenance leave the cluster with 4 DN,
    // it should be allowed
    error = decom.startMaintenanceNodes(Arrays.asList(dns.get(1).getIpAddress()), 100, false);
    assertEquals(0, error.size());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());

    decom.recommissionNodes(Arrays.asList(dns.get(1).getIpAddress(), dns.get(2).getIpAddress()));
    decom.getMonitor().run();
    assertEquals(5, nodeManager.getNodeCount(NodeStatus.inServiceHealthy()));

    decom.setMaintenanceConfigs(3, 2); // non-default config
    // putting 1 DNs into maintenance leave the cluster with 4 DN,
    // it should not be allowed as for EC, maintenance.remaining.redundancy is 2 => 3+2=5 DNs are required
    error = decom.startMaintenanceNodes(Arrays.asList(dns.get(1).getIpAddress()), 100, false);
    assertTrue(error.get(0).getHostname().contains("AllHosts"));
    String errorMsg = String.format("%d IN-SERVICE HEALTHY and %d not IN-SERVICE or not HEALTHY nodes.", 5, 0);
    assertTrue(error.get(0).getError().contains(errorMsg));
    errorMsg = String.format("Cannot enter maintenance mode as a minimum of %d IN-SERVICE HEALTHY nodes are required",
        5);
    assertTrue(error.get(0).getError().contains(errorMsg));
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());

    decom.recommissionNodes(Arrays.asList(dns.get(1).getIpAddress(), dns.get(2).getIpAddress()));
    decom.getMonitor().run();
    assertEquals(5, nodeManager.getNodeCount(NodeStatus.inServiceHealthy()));

    // forcing 2 DNs into maintenance should be allowed
    error = decom.startMaintenanceNodes(Arrays.asList(dns.get(1).getIpAddress(), dns.get(2).getIpAddress()), 100, true);
    assertEquals(0, error.size());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());
  }

  @Test
  public void testInsufficientNodeMaintenanceChecksNotInService() throws
      NodeNotFoundException, IOException {
    when(containerManager.getContainer(any(ContainerID.class)))
        .thenAnswer(invocation -> getMockContainer(RatisReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.THREE), (ContainerID)invocation.getArguments()[0]));

    List<DatanodeAdminError> error;
    List<DatanodeDetails> dns = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      dns.add(dn);
      nodeManager.register(dn, null, null);
    }
    Set<ContainerID> idsRatis = new HashSet<>();
    for (int i = 0; i < 5; i++) {
      ContainerInfo container = containerManager.allocateContainer(
          RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE), "admin");
      idsRatis.add(container.containerID());
    }
    for (DatanodeDetails dn  : nodeManager.getAllNodes().subList(0, 3)) {
      setContainers(dn, idsRatis);
    }

    // put 2 nodes into maintenance successfully
    error = decom.startMaintenanceNodes(Arrays.asList(dns.get(0).getIpAddress(), dns.get(1).getIpAddress()),
        100, false);
    assertEquals(0, error.size());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(0)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    // try to put 3 nodes into maintenance, 1 in service and 2 in ENTER_MAINTENANCE state, should be successful.
    error = decom.startMaintenanceNodes(Arrays.asList(dns.get(0).getIpAddress(),
        dns.get(1).getIpAddress(), dns.get(2).getIpAddress()), 100, false);
    assertEquals(0, error.size());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(0)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());
  }

  @Test
  public void testInsufficientNodeMaintenanceChecksForNNF() throws
      NodeNotFoundException, IOException {
    List<DatanodeAdminError> error;
    List<DatanodeDetails> dns = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      dns.add(dn);
    }
    Set<ContainerID> idsRatis = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      ContainerInfo container = containerManager.allocateContainer(
          RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE), "admin");
      idsRatis.add(container.containerID());
    }

    nodeManager = mock(SCMNodeManager.class);
    decom = new NodeDecommissionManager(conf, nodeManager, containerManager,
        SCMContext.emptyContext(), new EventQueue(), null);
    when(containerManager.getContainer(any(ContainerID.class)))
        .thenAnswer(invocation -> getMockContainer(RatisReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.THREE), (ContainerID)invocation.getArguments()[0]));
    when(nodeManager.getNodesByAddress(any())).thenAnswer(invocation ->
        getDatanodeDetailsList((String)invocation.getArguments()[0], dns));
    when(nodeManager.getContainers(any())).thenReturn(idsRatis);
    when(nodeManager.getNodeCount(any())).thenReturn(5);
    when(nodeManager.getNodeStatus(any())).thenAnswer(invocation ->
        getNodeOpState((DatanodeDetails) invocation.getArguments()[0], dns));
    Mockito.doAnswer(invocation -> {
      setNodeOpState((DatanodeDetails)invocation.getArguments()[0],
          (HddsProtos.NodeOperationalState)invocation.getArguments()[1], dns);
      return null;
    }).when(nodeManager).setNodeOperationalState(any(DatanodeDetails.class), any(
        HddsProtos.NodeOperationalState.class));
    Mockito.doAnswer(invocation -> {
      setNodeOpState((DatanodeDetails)invocation.getArguments()[0],
          (HddsProtos.NodeOperationalState)invocation.getArguments()[1], dns);
      return null;
    }).when(nodeManager).setNodeOperationalState(any(DatanodeDetails.class), any(
        HddsProtos.NodeOperationalState.class), any(Long.class));

    // trying to put 4 available DNs into maintenance,
    // it should not be allowed as it leaves the cluster with 1 DN and maintenance.replica.minimum is 2
    error = decom.startMaintenanceNodes(Arrays.asList(dns.get(1).getIpAddress(),
        dns.get(2).getIpAddress(), dns.get(3).getIpAddress(), dns.get(4).getIpAddress()), 100, false);
    assertTrue(error.get(0).getHostname().contains("AllHosts"));
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(3)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(4)).getOperationalState());
    // trying to put 4 DNs (3 available + 1 not found) into maintenance,
    // it should be allowed as effectively, it tries to move 3 DNs to maintenance,
    // leaving the cluster with 2 DNs
    error = decom.startMaintenanceNodes(Arrays.asList(dns.get(0).getIpAddress(),
        dns.get(1).getIpAddress(), dns.get(2).getIpAddress(), dns.get(3).getIpAddress()), 100, false);
    assertEquals(0, error.size());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(3)).getOperationalState());
  }

  private List<DatanodeDetails> getDatanodeDetailsList(String ipaddress, List<DatanodeDetails> dns) {
    List<DatanodeDetails> datanodeDetails = new ArrayList<>();
    for (DatanodeDetails dn : dns) {
      if (dn.getIpAddress().equals(ipaddress)) {
        datanodeDetails.add(dn);
        break;
      }
    }
    return datanodeDetails;
  }

  private void setNodeOpState(DatanodeDetails dn, HddsProtos.NodeOperationalState newState, List<DatanodeDetails> dns) {
    for (DatanodeDetails datanode : dns) {
      if (datanode.equals(dn)) {
        datanode.setPersistedOpState(newState);
        break;
      }
    }
  }

  private NodeStatus getNodeOpState(DatanodeDetails dn, List<DatanodeDetails> dns) throws NodeNotFoundException {
    if (dn.equals(dns.get(0))) {
      throw new NodeNotFoundException(dn.getID());
    }
    for (DatanodeDetails datanode : dns) {
      if (datanode.equals(dn)) {
        return NodeStatus.valueOf(datanode.getPersistedOpState(), HddsProtos.NodeState.HEALTHY);
      }
    }
    return null;
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
        .addPort(DatanodeDetails.newStandalonePort(3456))
        .addPort(DatanodeDetails.newRatisPort(4567))
        .addPort(DatanodeDetails.newRestPort(5678))
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
        .addPort(DatanodeDetails.newStandalonePort(duplicatePorts.getStandalonePort().getValue()))
        .addPort(DatanodeDetails.newRatisPort(duplicatePorts.getRatisPort().getValue()))
        .addPort(DatanodeDetails.newRestPort(duplicatePorts.getRestPort().getValue()))
        .setNetworkLocation(multiDn.getNetworkLocation());
    dn = builder.build();
    dns.add(dn);
    nodeManager.register(dn, null, null);

    return dns;
  }
}
