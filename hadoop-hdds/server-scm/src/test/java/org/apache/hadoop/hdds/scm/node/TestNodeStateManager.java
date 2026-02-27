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

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;
import static org.apache.hadoop.ozone.container.upgrade.UpgradeUtils.defaultLayoutVersionProto;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.states.NodeAlreadyExistsException;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationCheckpoint;
import org.apache.hadoop.hdds.server.events.Event;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.container.upgrade.UpgradeUtils;
import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Class to test the NodeStateManager, which is an internal class used by
 * the SCMNodeManager.
 */
public class TestNodeStateManager {

  private NodeStateManager nsm;
  private ConfigurationSource conf;
  private MockEventPublisher eventPublisher;

  @BeforeEach
  public void setUp() {
    conf = new ConfigurationSource() {
      @Override
      public String get(String key) {
        return null;
      }

      @Override
      public Collection<String> getConfigKeys() {
        return null;
      }

      @Override
      public char[] getPassword(String key) throws IOException {
        return new char[0];
      }
    };
    // Make NodeStateManager behave as if SCM has completed finalization,
    // unless a test changes the value of this variable.
    SCMContext scmContext = SCMContext.emptyContext();
    scmContext.setFinalizationCheckpoint(
        FinalizationCheckpoint.FINALIZATION_COMPLETE);
    eventPublisher = new MockEventPublisher();
    int scmSlv = maxLayoutVersion();
    int scmMlv = maxLayoutVersion();
    LayoutVersionManager mockVersionManager = mock(HDDSLayoutVersionManager.class);
    when(mockVersionManager.getMetadataLayoutVersion()).thenReturn(scmMlv);
    when(mockVersionManager.getSoftwareLayoutVersion()).thenReturn(scmSlv);
    nsm = new NodeStateManager(conf, eventPublisher, mockVersionManager,
        scmContext);
  }

  @Test
  public void testNodeCanBeAddedAndRetrieved()
      throws NodeAlreadyExistsException, NodeNotFoundException {
    // Create a datanode, then add and retrieve it
    DatanodeDetails dn = generateDatanode();
    nsm.addNode(dn, UpgradeUtils.defaultLayoutVersionProto());
    assertEquals(dn.getUuid(), nsm.getNode(dn).getUuid());
    // Now get the status of the newly added node and it should be
    // IN_SERVICE and HEALTHY
    NodeStatus expectedState = NodeStatus.inServiceHealthy();
    assertEquals(expectedState, nsm.getNodeStatus(dn));
  }

  @Test
  public void testGetAllNodesReturnsCorrectly()
      throws NodeAlreadyExistsException {
    DatanodeDetails dn = generateDatanode();
    nsm.addNode(dn, UpgradeUtils.defaultLayoutVersionProto());
    dn = generateDatanode();
    nsm.addNode(dn, UpgradeUtils.defaultLayoutVersionProto());
    assertEquals(2, nsm.getAllNodes().size());
    assertEquals(2, nsm.getTotalNodeCount());
  }

  @Test
  public void testGetNodeCountReturnsCorrectly()
      throws NodeAlreadyExistsException {
    DatanodeDetails dn = generateDatanode();
    nsm.addNode(dn, UpgradeUtils.defaultLayoutVersionProto());
    assertEquals(1, nsm.getNodes(NodeStatus.inServiceHealthy()).size());
    assertEquals(0, nsm.getNodes(NodeStatus.inServiceStale()).size());
  }

  @Test
  public void testGetNodeCount() throws NodeAlreadyExistsException {
    DatanodeDetails dn = generateDatanode();
    nsm.addNode(dn, UpgradeUtils.defaultLayoutVersionProto());
    assertEquals(1, nsm.getNodeCount(NodeStatus.inServiceHealthy()));
    assertEquals(0, nsm.getNodeCount(NodeStatus.inServiceStale()));
  }

  @Test
  public void testNodesMarkedDeadAndStale()
      throws NodeAlreadyExistsException, NodeNotFoundException {
    long now = Time.monotonicNow();

    // Set the dead and stale limits to be 1 second larger than configured
    long staleLimit = HddsServerUtil.getStaleNodeInterval(conf) + 1000;
    long deadLimit = HddsServerUtil.getDeadNodeInterval(conf) + 1000;

    DatanodeDetails staleDn = generateDatanode();
    nsm.addNode(staleDn, defaultLayoutVersionProto());
    nsm.getNode(staleDn).updateLastHeartbeatTime(now - staleLimit);

    DatanodeDetails deadDn = generateDatanode();
    nsm.addNode(deadDn, defaultLayoutVersionProto());
    nsm.getNode(deadDn).updateLastHeartbeatTime(now - deadLimit);

    DatanodeDetails healthyDn = generateDatanode();
    nsm.addNode(healthyDn, defaultLayoutVersionProto());
    nsm.getNode(healthyDn).updateLastHeartbeatTime();

    nsm.checkNodesHealth();
    assertEquals(healthyDn, nsm.getHealthyNodes().get(0));
    // A node cannot go directly to dead. It must be marked stale first
    // due to the allowed state transitions. Therefore we will initially have 2
    // stale nodesCheck it is in stale nodes
    assertEquals(2, nsm.getStaleNodes().size());
    // Now check health again and it should be in deadNodes()
    nsm.checkNodesHealth();
    assertEquals(staleDn, nsm.getStaleNodes().get(0));
    assertEquals(deadDn, nsm.getDeadNodes().get(0));
  }

  @Test
  public void testNodeCanTransitionThroughHealthStatesAndFiresEvents()
      throws NodeAlreadyExistsException, NodeNotFoundException {
    long now = Time.monotonicNow();

    // Set the dead and stale limits to be 1 second larger than configured
    long staleLimit = HddsServerUtil.getStaleNodeInterval(conf) + 1000;
    long deadLimit = HddsServerUtil.getDeadNodeInterval(conf) + 1000;

    DatanodeDetails dn = generateDatanode();
    nsm.addNode(dn, defaultLayoutVersionProto());
    DatanodeInfo dni = nsm.getNode(dn);
    dni.updateLastHeartbeatTime();

    // Ensure node is initially healthy
    eventPublisher.clearEvents();
    nsm.checkNodesHealth();
    assertEquals(NodeState.HEALTHY, nsm.getNodeStatus(dn).getHealth());
    assertNull(eventPublisher.getLastEvent());

    // Set the heartbeat old enough to make it stale
    dni.updateLastHeartbeatTime(now - staleLimit);
    nsm.checkNodesHealth();
    assertEquals(NodeState.STALE, nsm.getNodeStatus(dn).getHealth());
    assertEquals(SCMEvents.STALE_NODE, eventPublisher.getLastEvent());

    // Now make it dead
    dni.updateLastHeartbeatTime(now - deadLimit);
    nsm.checkNodesHealth();
    assertEquals(NodeState.DEAD, nsm.getNodeStatus(dn).getHealth());
    assertEquals(SCMEvents.DEAD_NODE, eventPublisher.getLastEvent());

    // Transition to healthy from dead
    dni.updateLastHeartbeatTime();
    nsm.checkNodesHealth();
    assertEquals(NodeState.HEALTHY, nsm.getNodeStatus(dn).getHealth());
    assertEquals(SCMEvents.UNHEALTHY_TO_HEALTHY_NODE, eventPublisher.getLastEvent());

    // Make the node stale again, and transition to healthy.
    dni.updateLastHeartbeatTime(now - staleLimit);
    nsm.checkNodesHealth();
    assertEquals(NodeState.STALE, nsm.getNodeStatus(dn).getHealth());
    assertEquals(SCMEvents.STALE_NODE, eventPublisher.getLastEvent());
    dni.updateLastHeartbeatTime();
    nsm.checkNodesHealth();
    assertEquals(NodeState.HEALTHY, nsm.getNodeStatus(dn).getHealth());
    assertEquals(SCMEvents.UNHEALTHY_TO_HEALTHY_NODE, eventPublisher.getLastEvent());
    eventPublisher.clearEvents();
  }

  @Test
  public void testNodeOpStateCanBeSet()
      throws NodeAlreadyExistsException, NodeNotFoundException {
    DatanodeDetails dn = generateDatanode();
    nsm.addNode(dn, UpgradeUtils.defaultLayoutVersionProto());

    nsm.setNodeOperationalState(dn,
        HddsProtos.NodeOperationalState.DECOMMISSIONED);

    NodeStatus newStatus = nsm.getNodeStatus(dn);
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONED, newStatus.getOperationalState());
    assertEquals(NodeState.HEALTHY, newStatus.getHealth());
  }

  @Test
  public void testContainerCanBeAddedAndRemovedFromDN()
      throws NodeAlreadyExistsException, NodeNotFoundException {
    DatanodeDetails dn = generateDatanode();
    nsm.addNode(dn, UpgradeUtils.defaultLayoutVersionProto());

    nsm.addContainer(dn.getID(), ContainerID.valueOf(1));
    nsm.addContainer(dn.getID(), ContainerID.valueOf(2));

    Set<ContainerID> containerSet = nsm.getContainers(dn.getID());
    assertEquals(2, containerSet.size());
    assertThat(containerSet).contains(ContainerID.valueOf(1));
    assertThat(containerSet).contains(ContainerID.valueOf(2));

    nsm.removeContainer(dn.getID(), ContainerID.valueOf(2));
    containerSet = nsm.getContainers(dn.getID());
    assertEquals(1, containerSet.size());
    assertThat(containerSet).contains(ContainerID.valueOf(1));
    assertThat(containerSet).doesNotContain(ContainerID.valueOf(2));
  }

  @Test
  public void testHealthEventsFiredWhenOpStateChanged()
      throws NodeAlreadyExistsException, NodeNotFoundException {
    DatanodeDetails dn = generateDatanode();
    nsm.addNode(dn, UpgradeUtils.defaultLayoutVersionProto());

    // First set the node to decommissioned, then run through all op states in
    // order and ensure the unhealthy_to_healthy event gets fired
    nsm.setNodeOperationalState(dn,
        HddsProtos.NodeOperationalState.DECOMMISSIONED);
    for (HddsProtos.NodeOperationalState s :
        HddsProtos.NodeOperationalState.values()) {
      eventPublisher.clearEvents();
      nsm.setNodeOperationalState(dn, s);
      assertEquals(SCMEvents.UNHEALTHY_TO_HEALTHY_NODE, eventPublisher.getLastEvent());
    }

    // Now make the node stale and run through all states again ensuring the
    // stale event gets fired
    long now = Time.monotonicNow();
    long staleLimit = HddsServerUtil.getStaleNodeInterval(conf) + 1000;
    long deadLimit = HddsServerUtil.getDeadNodeInterval(conf) + 1000;
    DatanodeInfo dni = nsm.getNode(dn);
    dni.updateLastHeartbeatTime(now - staleLimit);
    nsm.checkNodesHealth();
    assertEquals(NodeState.STALE, nsm.getNodeStatus(dn).getHealth());
    nsm.setNodeOperationalState(dn,
        HddsProtos.NodeOperationalState.DECOMMISSIONED);
    for (HddsProtos.NodeOperationalState s :
        HddsProtos.NodeOperationalState.values()) {
      eventPublisher.clearEvents();
      nsm.setNodeOperationalState(dn, s);
      assertEquals(SCMEvents.STALE_NODE, eventPublisher.getLastEvent());
    }

    // Finally make the node dead and run through all the op states again
    dni.updateLastHeartbeatTime(now - deadLimit);
    nsm.checkNodesHealth();
    assertEquals(NodeState.DEAD, nsm.getNodeStatus(dn).getHealth());
    nsm.setNodeOperationalState(dn,
        HddsProtos.NodeOperationalState.DECOMMISSIONED);
    for (HddsProtos.NodeOperationalState s :
        HddsProtos.NodeOperationalState.values()) {
      eventPublisher.clearEvents();
      nsm.setNodeOperationalState(dn, s);
      assertEquals(SCMEvents.DEAD_NODE, eventPublisher.getLastEvent());
    }
  }

  @Test
  public void testUpdateNode() throws NodeAlreadyExistsException,
          NodeNotFoundException {
    UUID dnUuid = UUID.randomUUID();
    String ipAddress = "1.2.3.4";
    String hostName = "test-host";
    StorageContainerDatanodeProtocolProtos.LayoutVersionProto
            layoutVersionProto =
            UpgradeUtils.toLayoutVersionProto(1, 2);
    DatanodeDetails dn = DatanodeDetails.newBuilder()
            .setUuid(dnUuid)
            .setIpAddress(ipAddress)
            .setHostName(hostName)
            .setPersistedOpState(HddsProtos.NodeOperationalState.IN_MAINTENANCE)
            .build();
    nsm.addNode(dn, layoutVersionProto);

    String newIpAddress = "2.3.4.5";
    String newHostName = "new-host";
    StorageContainerDatanodeProtocolProtos.LayoutVersionProto
            newLayoutVersionProto = UpgradeUtils.defaultLayoutVersionProto();
    DatanodeDetails newDn = DatanodeDetails.newBuilder()
            .setUuid(dnUuid)
            .setIpAddress(newIpAddress)
            .setHostName(newHostName)
            .setPersistedOpState(HddsProtos.NodeOperationalState.IN_SERVICE)
            .build();
    nsm.updateNode(newDn, newLayoutVersionProto);

    DatanodeInfo updatedDn = nsm.getNode(dn);
    assertEquals(newIpAddress, updatedDn.getIpAddress());
    assertEquals(newHostName, updatedDn.getHostName());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE, updatedDn.getPersistedOpState());
    assertEquals(NodeStatus.inServiceHealthy(), updatedDn.getNodeStatus());
  }

  private DatanodeDetails generateDatanode() {
    return DatanodeDetails.newBuilder().setUuid(UUID.randomUUID()).build();
  }

  static class  MockEventPublisher implements EventPublisher {

    private List<Event> events = new ArrayList<>();
    private List<Object> payloads = new ArrayList<>();

    public void clearEvents() {
      events.clear();
      payloads.clear();
    }

    public List<Event> getEvents() {
      return events;
    }

    public Event getLastEvent() {
      if (events.isEmpty()) {
        return null;
      } else {
        return events.get(events.size() - 1);
      }
    }

    @Override
    public <PAYLOAD, EVENT_TYPE extends Event<PAYLOAD>> void
        fireEvent(EVENT_TYPE event, PAYLOAD payload) {
      events.add(event);
      payloads.add(payload);
    }
  }

}
