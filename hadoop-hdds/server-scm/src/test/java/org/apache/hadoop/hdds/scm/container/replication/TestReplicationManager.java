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
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.ozone.test.TestClock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerInfo;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createReplicas;

/**
 * Tests for the ReplicationManager.
 */
public class TestReplicationManager {

  private OzoneConfiguration configuration;
  private ReplicationManager replicationManager;
  private LegacyReplicationManager legacyReplicationManager;
  private ContainerManager containerManager;
  private PlacementPolicy placementPolicy;
  private EventPublisher eventPublisher;
  private SCMContext scmContext;
  private NodeManager nodeManager;
  private TestClock clock;
  private ContainerReplicaPendingOps containerReplicaPendingOps;

  private Map<ContainerID, Set<ContainerReplica>> containerReplicaMap;
  private ReplicationConfig repConfig;
  private ReplicationManagerReport repReport;
  private List<ContainerHealthResult.UnderReplicatedHealthResult> underRep;
  private List<ContainerHealthResult.OverReplicatedHealthResult> overRep;

  @Before
  public void setup() throws IOException {
    configuration = new OzoneConfiguration();
    configuration.set(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, "0s");
    containerManager = Mockito.mock(ContainerManager.class);
    placementPolicy = Mockito.mock(PlacementPolicy.class);
    eventPublisher = Mockito.mock(EventPublisher.class);
    scmContext = Mockito.mock(SCMContext.class);
    nodeManager = Mockito.mock(NodeManager.class);
    legacyReplicationManager = Mockito.mock(LegacyReplicationManager.class);
    clock = new TestClock(Instant.now(), ZoneId.systemDefault());
    containerReplicaPendingOps =
        new ContainerReplicaPendingOps(configuration, clock);

    Mockito.when(containerManager
        .getContainerReplicas(Mockito.any(ContainerID.class))).thenAnswer(
          invocation -> {
            ContainerID cid = invocation.getArgument(0);
            return containerReplicaMap.get(cid);
          });

    replicationManager = new ReplicationManager(
        configuration,
        containerManager,
        placementPolicy,
        eventPublisher,
        scmContext,
        nodeManager,
        clock,
        legacyReplicationManager,
        containerReplicaPendingOps);
    containerReplicaMap = new HashMap<>();
    repConfig = new ECReplicationConfig(3, 2);
    repReport = new ReplicationManagerReport();
    underRep = new ArrayList<>();
    overRep = new ArrayList<>();
  }

  @Test
  public void testHealthyContainer() throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, 1, 2, 3, 4, 5);

    ContainerHealthResult result = replicationManager.processContainer(
        container, underRep, overRep, repReport);
    Assert.assertEquals(ContainerHealthResult.HealthState.HEALTHY,
        result.getHealthState());
    Assert.assertEquals(0, underRep.size());
    Assert.assertEquals(0, overRep.size());
  }

  @Test
  public void testUnderReplicatedContainer() throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, 1, 2, 3, 4);

    ContainerHealthResult result = replicationManager.processContainer(
        container, underRep, overRep, repReport);
    Assert.assertEquals(ContainerHealthResult.HealthState.UNDER_REPLICATED,
        result.getHealthState());
    Assert.assertEquals(1, underRep.size());
    Assert.assertEquals(0, overRep.size());
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
  }

  @Test
  public void testUnderReplicatedContainerFixedByPending()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, 1, 2, 3, 4);
    containerReplicaPendingOps.scheduleAddReplica(container.containerID(),
        MockDatanodeDetails.randomDatanodeDetails(), 5);

    ContainerHealthResult result = replicationManager.processContainer(
        container, underRep, overRep, repReport);
    Assert.assertEquals(ContainerHealthResult.HealthState.UNDER_REPLICATED,
        result.getHealthState());
    // As the pending replication fixes the under replication, nothing is added
    // to the under replication list.
    Assert.assertEquals(0, underRep.size());
    Assert.assertEquals(0, overRep.size());
    Assert.assertTrue(((ContainerHealthResult.UnderReplicatedHealthResult)
        result).isSufficientlyReplicatedAfterPending());
    // As the container is still under replicated, as the pending have not
    // completed yet, the container is still marked as under-replciated in the
    // report.
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
  }

  @Test
  public void testUnderReplicatedAndUnrecoverable()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, 1, 2);

    ContainerHealthResult result = replicationManager.processContainer(
        container, underRep, overRep, repReport);
    Assert.assertEquals(ContainerHealthResult.HealthState.UNDER_REPLICATED,
        result.getHealthState());
    // If it is unrecoverable, there is no point in putting it into the under
    // replication list. It will be checked again on the next RM run.
    Assert.assertEquals(0, underRep.size());
    Assert.assertEquals(0, overRep.size());
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.MISSING));
  }

  @Test
  public void testUnderAndOverReplicated()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, 1, 2, 3, 5, 5);

    ContainerHealthResult result = replicationManager.processContainer(
        container, underRep, overRep, repReport);
    // If it is both under and over replicated, we set it to the most important
    // state, which is under-replicated. When that is fixed, over replication
    // will be handled.
    Assert.assertEquals(ContainerHealthResult.HealthState.UNDER_REPLICATED,
        result.getHealthState());
    Assert.assertEquals(1, underRep.size());
    Assert.assertEquals(0, overRep.size());
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    Assert.assertEquals(0, repReport.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
  }

  @Test
  public void testOverReplicated() throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, 1, 2, 3, 4, 5, 5);
    ContainerHealthResult result = replicationManager.processContainer(
        container, underRep, overRep, repReport);
    Assert.assertEquals(ContainerHealthResult.HealthState.OVER_REPLICATED,
        result.getHealthState());
    Assert.assertEquals(0, underRep.size());
    Assert.assertEquals(1, overRep.size());
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
  }

  @Test
  public void testOverReplicatedFixByPending()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, 1, 2, 3, 4, 5, 5);
    containerReplicaPendingOps.scheduleDeleteReplica(container.containerID(),
        MockDatanodeDetails.randomDatanodeDetails(), 5);
    ContainerHealthResult result = replicationManager.processContainer(
        container, underRep, overRep, repReport);
    Assert.assertEquals(ContainerHealthResult.HealthState.OVER_REPLICATED,
        result.getHealthState());
    Assert.assertEquals(0, underRep.size());
    // If the pending replication fixes the over-replication, nothing is added
    // to the over replication list.
    Assert.assertEquals(0, overRep.size());
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
  }

  private Set<ContainerReplica> addReplicas(ContainerInfo container,
      int... indexes) {
    final Set<ContainerReplica> replicas =
        createReplicas(container.containerID(), indexes);
    containerReplicaMap.put(container.containerID(), replicas);
    return replicas;
  }

}
