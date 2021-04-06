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
package org.apache.hadoop.hdds.scm.container.states;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaCount;
import org.junit.Before;
import org.junit.Test;
import java.util.*;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos
    .NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos
    .NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos
    .NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos
    .NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos
    .NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.OPEN;
import static org.junit.Assert.assertFalse;

/**
 * Class used to test the ContainerReplicaCount class.
 */
public class TestContainerReplicaCount {

  @Before
  public void setup() {
  }

  @Test
  public void testThreeHealthyReplica() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, true, 0, false);
  }

  @Test
  public void testTwoHealthyReplica() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, false, 1, false);
  }

  @Test
  public void testOneHealthyReplica() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, false, 2, false);
  }

  @Test
  public void testTwoHealthyAndInflightAdd() {

    Set<ContainerReplica> replica = registerNodes(IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 1, 0, 3, 2);
    validate(rcnt, false, 0, false);
  }

  @Test
  /**
   * This does not schedule a container to be removed, as the inFlight add may
   * fail and then the delete would make things under-replicated. Once the add
   * completes there will be 4 healthy and it will get taken care of then.
   */
  public void testThreeHealthyAndInflightAdd() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 1, 0, 3, 2);
    validate(rcnt, true, 0, false);
  }

  @Test
  /**
   * As the inflight delete may fail, but as it will make the the container
   * under replicated, we go ahead and schedule another replica to be added.
   */
  public void testThreeHealthyAndInflightDelete() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 1, 3, 2);
    validate(rcnt, false, 1, false);
  }

  @Test
  /**
   * This is NOT sufficiently replicated as the inflight add may fail and the
   * inflight del could succeed, leaving only 2 healthy replicas.
   */
  public void testThreeHealthyAndInflightAddAndInFlightDelete() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 1, 1, 3, 2);
    validate(rcnt, false, 0, false);
  }

  @Test
  public void testFourHealthyReplicas() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, true, -1, true);
  }

  @Test
  public void testFourHealthyReplicasAndInFlightDelete() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 1, 3, 2);
    validate(rcnt, true, 0, false);
  }

  @Test
  public void testFourHealthyReplicasAndTwoInFlightDelete() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 2, 3, 2);
    validate(rcnt, false, 1, false);
  }

  @Test
  public void testOneHealthyReplicaRepFactorOne() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 0, 1, 2);
    validate(rcnt, true, 0, false);
  }

  @Test
  public void testOneHealthyReplicaRepFactorOneInFlightDelete() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 1, 1, 2);
    validate(rcnt, false, 1, false);
  }

  @Test
  public void testTwoHealthyReplicaTwoInflightAdd() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 2, 0, 3, 2);
    validate(rcnt, false, 0, false);
  }

  /**
   * From here consider decommission replicas.
   */

  @Test
  public void testThreeHealthyAndTwoDecommission() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE, IN_SERVICE,
        IN_SERVICE, DECOMMISSIONING, DECOMMISSIONING);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, true, 0, false);
  }

  @Test
  public void testOneDecommissionedReplica() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, DECOMMISSIONING);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, false, 1, false);
  }

  @Test
  public void testTwoHealthyOneDecommissionedneInFlightAdd() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, DECOMMISSIONED);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 1, 0, 3, 2);
    validate(rcnt, false, 0, false);
  }

  @Test
  public void testAllDecommissioned() {
    Set<ContainerReplica> replica =
        registerNodes(DECOMMISSIONED, DECOMMISSIONED, DECOMMISSIONED);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, false, 3, false);
  }

  @Test
  public void testAllDecommissionedRepFactorOne() {
    Set<ContainerReplica> replica = registerNodes(DECOMMISSIONED);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 0, 1, 2);
    validate(rcnt, false, 1, false);

  }

  @Test
  public void testAllDecommissionedRepFactorOneInFlightAdd() {
    Set<ContainerReplica> replica = registerNodes(DECOMMISSIONED);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 1, 0, 1, 2);
    validate(rcnt, false, 0, false);
  }

  @Test
  public void testOneHealthyOneDecommissioningRepFactorOne() {
    Set<ContainerReplica> replica = registerNodes(DECOMMISSIONED, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 0, 1, 2);
    validate(rcnt, true, 0, false);
  }

  /**
   * Maintenance tests from here.
   */

  @Test
  public void testOneHealthyTwoMaintenanceMinRepOfTwo() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_MAINTENANCE, IN_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, false, 1, false);
  }

  @Test
  public void testOneHealthyThreeMaintenanceMinRepOfTwo() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE,
        IN_MAINTENANCE, IN_MAINTENANCE, ENTERING_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, false, 1, false);
  }

  @Test
  public void testOneHealthyTwoMaintenanceMinRepOfOne() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_MAINTENANCE, ENTERING_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 0, 3, 1);
    validate(rcnt, true, 0, false);
  }

  @Test
  public void testOneHealthyThreeMaintenanceMinRepOfTwoInFlightAdd() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE,
        IN_MAINTENANCE, ENTERING_MAINTENANCE, IN_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 1, 0, 3, 2);
    validate(rcnt, false, 0, false);
  }

  @Test
  public void testAllMaintenance() {
    Set<ContainerReplica> replica =
        registerNodes(IN_MAINTENANCE, ENTERING_MAINTENANCE, IN_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, false, 2, false);
  }

  @Test
  /**
   * As we have exactly 3 healthy, but then an excess of maintenance copies
   * we ignore the over-replication caused by the maintenance copies until they
   * come back online, and then deal with them.
   */
  public void testThreeHealthyTwoInMaintenance() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE, IN_SERVICE,
        IN_SERVICE, IN_MAINTENANCE, ENTERING_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, true, 0, false);
  }

  @Test
  /**
   * This is somewhat similar to testThreeHealthyTwoInMaintenance() except now
   * one of the maintenance copies has become healthy and we will need to remove
   * the over-replicated healthy container.
   */
  public void testFourHealthyOneInMaintenance() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE, IN_SERVICE,
            IN_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, true, -1, true);
  }

  @Test
  public void testOneMaintenanceMinRepOfTwoRepFactorOne() {
    Set<ContainerReplica> replica = registerNodes(IN_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 0, 1, 2);
    validate(rcnt, false, 1, false);
  }

  @Test
  public void testOneMaintenanceMinRepOfTwoRepFactorOneInFlightAdd() {
    Set<ContainerReplica> replica = registerNodes(IN_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 1, 0, 1, 2);
    validate(rcnt, false, 0, false);
  }

  @Test
  public void testOneHealthyOneMaintenanceRepFactorOne() {
    Set<ContainerReplica> replica = registerNodes(IN_MAINTENANCE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 0, 1, 2);
    validate(rcnt, true, 0, false);
  }

  @Test
  public void testTwoDecomTwoMaintenanceOneInflightAdd() {
    Set<ContainerReplica> replica =
        registerNodes(DECOMMISSIONED, DECOMMISSIONING,
            IN_MAINTENANCE, ENTERING_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 1, 0, 3, 2);
    validate(rcnt, false, 1, false);
  }

  @Test
  public void testHealthyContainerIsHealthy() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
    assertTrue(rcnt.isHealthy());
  }

  @Test
  public void testIsHealthyWithDifferentReplicaStateNotHealthy() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE);
    for (ContainerReplica r : replica) {
      DatanodeDetails dn = r.getDatanodeDetails();

      ContainerReplica replace = new ContainerReplica.ContainerReplicaBuilder()
          .setContainerID(ContainerID.valueOf(1))
          .setContainerState(OPEN)
          .setDatanodeDetails(dn)
          .setOriginNodeId(dn.getUuid())
          .setSequenceId(1)
          .build();
      replica.remove(r);
      replica.add(replace);
      break;
    }
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
    assertFalse(rcnt.isHealthy());
  }

  @Test
  public void testIsHealthyWithMaintReplicaIsHealthy() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_MAINTENANCE,
            ENTERING_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
    assertTrue(rcnt.isHealthy());
  }

  private void validate(ContainerReplicaCount rcnt,
      boolean sufficientlyReplicated, int replicaDelta,
      boolean overReplicated) {
    assertEquals(sufficientlyReplicated, rcnt.isSufficientlyReplicated());
    assertEquals(overReplicated, rcnt.isOverReplicated());
    assertEquals(replicaDelta, rcnt.additionalReplicaNeeded());
  }

  private Set<ContainerReplica> registerNodes(
      HddsProtos.NodeOperationalState... states) {
    Set<ContainerReplica> replica = new HashSet<>();
    for (HddsProtos.NodeOperationalState s : states) {
      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      dn.setPersistedOpState(s);
      replica.add(new ContainerReplica.ContainerReplicaBuilder()
          .setContainerID(ContainerID.valueOf(1))
          .setContainerState(CLOSED)
          .setDatanodeDetails(dn)
          .setOriginNodeId(dn.getUuid())
          .setSequenceId(1)
          .build());
    }
    return replica;
  }

  private ContainerInfo createContainer(HddsProtos.LifeCycleState state) {
    return new ContainerInfo.Builder()
        .setContainerID(ContainerID.valueOf(1).getId())
        .setState(state)
        .build();
  }
}
