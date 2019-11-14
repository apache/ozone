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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaCount;
import org.junit.Before;
import org.junit.Test;
import java.util.*;
import static junit.framework.TestCase.assertEquals;
import static org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State
    .DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State
    .MAINTENANCE;

/**
 * Class used to test the ContainerReplicaCount class.
 */
public class TestContainerReplicaCount {

  private OzoneConfiguration conf;
  private List<DatanodeDetails> dns;
  private Set<ContainerReplica> replica;
  private ContainerReplicaCount rcount;


  public TestContainerReplicaCount() {
  }

  @Before
  public void setup() {
    conf = new OzoneConfiguration();
    replica = new HashSet<>();
    dns = new LinkedList<>();
  }

  @Test
  public void testThreeHealthyReplica() {
    registerNodes(CLOSED, CLOSED, CLOSED);
    rcount = new ContainerReplicaCount(replica, 0, 0, 3, 2);
    validate(true, 0, false);
  }

  @Test
  public void testTwoHealthyReplica() {
    registerNodes(CLOSED, CLOSED);
    rcount = new ContainerReplicaCount(replica, 0, 0, 3, 2);
    validate(false, 1, false);
  }

  @Test
  public void testOneHealthyReplica() {
    registerNodes(CLOSED);
    rcount = new ContainerReplicaCount(replica, 0, 0, 3, 2);
    validate(false, 2, false);
  }

  @Test
  public void testTwoHealthyAndInflightAdd() {
    registerNodes(CLOSED, CLOSED);
    rcount = new ContainerReplicaCount(replica, 1, 0, 3, 2);
    validate(false, 0, false);
  }

  @Test
  /**
   * This does not schedule a container to be removed, as the inFlight add may
   * fail and then the delete would make things under-replicated. Once the add
   * completes there will be 4 healthy and it will get taken care of then.
   */
  public void testThreeHealthyAndInflightAdd() {
    registerNodes(CLOSED, CLOSED, CLOSED);
    rcount = new ContainerReplicaCount(replica, 1, 0, 3, 2);
    validate(true, 0, false);
  }

  @Test
  /**
   * As the inflight delete may fail, but as it will make the the container
   * under replicated, we go ahead and schedule another replica to be added.
   */
  public void testThreeHealthyAndInflightDelete() {
    registerNodes(CLOSED, CLOSED, CLOSED);
    rcount = new ContainerReplicaCount(replica, 0, 1, 3, 2);
    validate(false, 1, false);
  }

  @Test
  /**
   * This is NOT sufficiently replicated as the inflight add may fail and the
   * inflight del could succeed, leaving only 2 healthy replicas.
   */
  public void testThreeHealthyAndInflightAddAndInFlightDelete() {
    registerNodes(CLOSED, CLOSED, CLOSED);
    rcount = new ContainerReplicaCount(replica, 1, 1, 3, 2);
    validate(false, 0, false);
  }

  @Test
  public void testFourHealthyReplicas() {
    registerNodes(CLOSED, CLOSED, CLOSED, CLOSED);
    rcount = new ContainerReplicaCount(replica, 0, 0, 3, 2);
    validate(true, -1, true);
  }

  @Test
  public void testFourHealthyReplicasAndInFlightDelete() {
    registerNodes(CLOSED, CLOSED, CLOSED, CLOSED);
    rcount = new ContainerReplicaCount(replica, 0, 1, 3, 2);
    validate(true, 0, false);
  }

  @Test
  public void testFourHealthyReplicasAndTwoInFlightDelete() {
    registerNodes(CLOSED, CLOSED, CLOSED, CLOSED);
    rcount = new ContainerReplicaCount(replica, 0, 2, 3, 2);
    validate(false, 1, false);
  }

  @Test
  public void testOneHealthyReplicaRepFactorOne() {
    registerNodes(CLOSED);
    rcount = new ContainerReplicaCount(replica, 0, 0, 1, 2);
    validate(true, 0, false);
  }

  @Test
  public void testOneHealthyReplicaRepFactorOneInFlightDelete() {
    registerNodes(CLOSED);
    rcount = new ContainerReplicaCount(replica, 0, 1, 1, 2);
    validate(false, 1, false);
  }

  /**
   * From here consider decommission replicas.
   */

  @Test
  public void testThreeHealthyAndTwoDecommission() {
    registerNodes(CLOSED, CLOSED, CLOSED,
        DECOMMISSIONED, DECOMMISSIONED);
    rcount = new ContainerReplicaCount(replica, 0, 0, 3, 2);
    validate(true, 0, false);
  }

  @Test
  public void testOneDecommissionedReplica() {
    registerNodes(CLOSED, CLOSED, DECOMMISSIONED);
    rcount = new ContainerReplicaCount(replica, 0, 0, 3, 2);
    validate(false, 1, false);
  }

  @Test
  public void testTwoHealthyOneDecommissionedneInFlightAdd() {
    registerNodes(CLOSED, CLOSED, DECOMMISSIONED);
    rcount = new ContainerReplicaCount(replica, 1, 0, 3, 2);
    validate(false, 0, false);
  }

  @Test
  public void testAllDecommissioned() {
    registerNodes(DECOMMISSIONED, DECOMMISSIONED, DECOMMISSIONED);
    rcount = new ContainerReplicaCount(replica, 0, 0, 3, 2);
    validate(false, 3, false);
  }

  @Test
  public void testAllDecommissionedRepFactorOne() {
    registerNodes(DECOMMISSIONED);
    rcount = new ContainerReplicaCount(replica, 0, 0, 1, 2);
    validate(false, 1, false);
  }

  @Test
  public void testAllDecommissionedRepFactorOneInFlightAdd() {
    registerNodes(DECOMMISSIONED);
    rcount = new ContainerReplicaCount(replica, 1, 0, 1, 2);
    validate(false, 0, false);
  }

  @Test
  public void testOneHealthyOneDecommissioningRepFactorOne() {
    registerNodes(DECOMMISSIONED, CLOSED);
    rcount = new ContainerReplicaCount(replica, 0, 0, 1, 2);
    validate(true, 0, false);
  }

  /**
   * Maintenance tests from here.
   */

  @Test
  public void testOneHealthyTwoMaintenanceMinRepOfTwo() {
    registerNodes(CLOSED, MAINTENANCE, MAINTENANCE);
    rcount = new ContainerReplicaCount(replica, 0, 0, 3, 2);
    validate(false, 1, false);
  }

  @Test
  public void testOneHealthyThreeMaintenanceMinRepOfTwo() {
    registerNodes(CLOSED,
        MAINTENANCE, MAINTENANCE, MAINTENANCE);
    rcount = new ContainerReplicaCount(replica, 0, 0, 3, 2);
    validate(false, 1, false);
  }

  @Test
  public void testOneHealthyTwoMaintenanceMinRepOfOne() {
    registerNodes(CLOSED, MAINTENANCE, MAINTENANCE);
    rcount = new ContainerReplicaCount(replica, 0, 0, 3, 1);
    validate(true, 0, false);
  }

  @Test
  public void testOneHealthyThreeMaintenanceMinRepOfTwoInFlightAdd() {
    registerNodes(CLOSED,
        MAINTENANCE, MAINTENANCE, MAINTENANCE);
    rcount = new ContainerReplicaCount(replica, 1, 0, 3, 2);
    validate(false, 0, false);
  }

  @Test
  public void testAllMaintenance() {
    registerNodes(MAINTENANCE, MAINTENANCE, MAINTENANCE);
    rcount = new ContainerReplicaCount(replica, 0, 0, 3, 2);
    validate(false, 2, false);
  }

  @Test
  /**
   * As we have exactly 3 healthy, but then an excess of maintenance copies
   * we ignore the over-replication caused by the maintenance copies until they
   * come back online, and then deal with them.
   */
  public void testThreeHealthyTwoInMaintenance() {
    registerNodes(CLOSED, CLOSED, CLOSED,
        MAINTENANCE, MAINTENANCE);
    rcount = new ContainerReplicaCount(replica, 0, 0, 3, 2);
    validate(true, 0, false);
  }

  @Test
  /**
   * This is somewhat similar to testThreeHealthyTwoInMaintenance() except now
   * one of the maintenance copies has become healthy and we will need to remove
   * the over-replicated healthy container.
   */
  public void testFourHealthyOneInMaintenance() {
    registerNodes(CLOSED, CLOSED, CLOSED, CLOSED,
        MAINTENANCE);
    rcount = new ContainerReplicaCount(replica, 0, 0, 3, 2);
    validate(true, -1, true);
  }

  @Test
  public void testOneMaintenanceMinRepOfTwoRepFactorOne() {
    registerNodes(MAINTENANCE);
    rcount = new ContainerReplicaCount(replica, 0, 0, 1, 2);
    validate(false, 1, false);
  }

  @Test
  public void testOneMaintenanceMinRepOfTwoRepFactorOneInFlightAdd() {
    registerNodes(MAINTENANCE);
    rcount = new ContainerReplicaCount(replica, 1, 0, 1, 2);
    validate(false, 0, false);
  }

  @Test
  public void testOneHealthyOneMaintenanceRepFactorOne() {
    registerNodes(MAINTENANCE, CLOSED);
    rcount = new ContainerReplicaCount(replica, 0, 0, 1, 2);
    validate(true, 0, false);
  }

  @Test
  public void testTwoDecomTwoMaintenanceOneInflightAdd() {
    registerNodes(DECOMMISSIONED, DECOMMISSIONED,
        MAINTENANCE, MAINTENANCE);
    rcount = new ContainerReplicaCount(replica, 1, 0, 3, 2);
    validate(false, 1, false);
  }

  private void validate(boolean sufficientlyReplicated, int replicaDelta,
      boolean overRelicated) {
    assertEquals(sufficientlyReplicated, rcount.isSufficientlyReplicated());
    assertEquals(replicaDelta, rcount.additionalReplicaNeeded());
  }

  private void registerNodes(ContainerReplicaProto.State... states) {
    for (ContainerReplicaProto.State s : states) {
      DatanodeDetails dn = TestUtils.randomDatanodeDetails();
      replica.add(new ContainerReplica.ContainerReplicaBuilder()
          .setContainerID(new ContainerID(1))
          .setContainerState(s)
          .setDatanodeDetails(dn)
          .setOriginNodeId(dn.getUuid())
          .setSequenceId(1)
          .build());
    }
  }
}
