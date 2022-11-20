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
package org.apache.hadoop.hdds.scm.container.move;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.PlacementPolicyValidateProxy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.common.helpers.MoveDataNodePair;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaCount;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerInfo;
import static org.mockito.Mockito.doNothing;

/**
 * test class for MoveManagerImpl.
 * */
public class TestMoveManagerImpl {
  private MoveManagerImpl moveManager;
  private ContainerReplicaPendingOps containerReplicaPendingOps;
  private ContainerManager containerManager;
  private NodeManager nodeManager;
  private PlacementPolicyValidateProxy placementPolicyValidateProxy;
  private SCMContext scmContext;
  private SCMHAManager scmhaManager;
  private SCMMetadataStore scmMetadataStore;
  private DBStore dbStore;
  private File testDir;
  private ReplicationConfig repConfig;
  private ContainerInfo cif;
  private EventPublisher eventPublisher;

  @BeforeEach
  public void setup() throws IOException, NodeNotFoundException {
    OzoneConfiguration conf = new OzoneConfiguration();
    testDir = GenericTestUtils.getTestDir(
        TestMoveManagerImpl.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
    dbStore = DBStoreBuilder.createDBStore(conf, new SCMDBDefinition());
    StorageContainerManager scm = Mockito.mock(StorageContainerManager.class);
    nodeManager = Mockito.mock(NodeManager.class);
    repConfig = new ECReplicationConfig(3, 2);
    Mockito.when(nodeManager.getNodeStatus(Mockito.any(DatanodeDetails.class)))
        .thenReturn(NodeStatus.inServiceHealthy());
    cif = createContainerInfo(repConfig, 1, HddsProtos.LifeCycleState.CLOSED);
    containerManager = Mockito.mock(ContainerManager.class);
    Mockito.when(containerManager.getContainer(Mockito.any(ContainerID.class)))
        .thenReturn(cif);
    Mockito.when(containerManager.getContainerReplicaIndex(
        Mockito.any(ContainerID.class),
        Mockito.any(DatanodeDetails.class))).thenReturn(1);
    scmContext = Mockito.mock(SCMContext.class);
    placementPolicyValidateProxy =
        Mockito.mock(PlacementPolicyValidateProxy.class);
    Mockito.when(placementPolicyValidateProxy
            .validateContainerPlacement(Mockito.any(), Mockito.any()))
        .thenAnswer(invocation ->
            new ContainerPlacementStatusDefault(2, 2, 3));
    eventPublisher = Mockito.mock(EventPublisher.class);
    scmhaManager = SCMHAManagerStub.getInstance(true);
    scmMetadataStore = Mockito.mock(SCMMetadataStore.class);
    Table<ContainerID, MoveDataNodePair> movetable =
        SCMDBDefinition.MOVE.getTable(dbStore);
    Mockito.when(scmMetadataStore.getMoveTable()).thenReturn(movetable);
    containerReplicaPendingOps = Mockito.mock(ContainerReplicaPendingOps.class);
    Mockito.when(scmContext.isLeaderReady()).thenReturn(true);
    Mockito.when(scm.getScmNodeManager()).thenReturn(nodeManager);
    Mockito.when(scm.getContainerManager()).thenReturn(containerManager);
    Mockito.when(scm.getScmContext()).thenReturn(scmContext);
    Mockito.when(scm.getPlacementPolicyValidateProxy())
        .thenReturn(placementPolicyValidateProxy);
    Mockito.when(scm.getScmMetadataStore()).thenReturn(scmMetadataStore);
    Mockito.when(scm.getScmHAManager()).thenReturn(scmhaManager);
    Mockito.when(scm.getContainerReplicaPendingOps())
        .thenReturn(containerReplicaPendingOps);
    Mockito.when(scm.getEventQueue()).thenReturn(eventPublisher);

    moveManager = new MoveManagerImpl(scm);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (dbStore != null) {
      dbStore.close();
    }

    FileUtil.fullyDelete(testDir);
  }

  /**
   * if all the prerequisites are satisfied, move should work as expected.
   */
  @Test
  public void testMove() throws NodeNotFoundException,
      IOException, TimeoutException, ExecutionException, InterruptedException {
    ContainerID cid = cif.containerID();
    Mockito.when(containerReplicaPendingOps.getPendingOps(cid))
        .thenReturn(Collections.emptyList());
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails dn2 = MockDatanodeDetails.randomDatanodeDetails();
    ContainerReplica cp1 = new ContainerReplica.ContainerReplicaBuilder()
        .setDatanodeDetails(dn1).setContainerID(cid).setReplicaIndex(3)
        .setContainerState(State.CLOSED).build();
    ContainerReplica cp2 = new ContainerReplica.ContainerReplicaBuilder()
        .setDatanodeDetails(dn2).setContainerID(cid).setReplicaIndex(3)
        .setContainerState(State.CLOSED).build();
    Set<ContainerReplica> currentReplicas = new HashSet<>();
    currentReplicas.add(cp1);
    Mockito.when(containerManager.getContainerReplicas(cid))
        .thenReturn(currentReplicas);
    doNothing().when(eventPublisher).fireEvent(Mockito.any(), Mockito.any());
    doNothing().when(containerReplicaPendingOps)
        .scheduleDeleteReplica(cid, dn1, 1);
    doNothing().when(containerReplicaPendingOps)
        .scheduleAddReplica(cid, dn2, 1);
    moveManager.onLeaderReady();

    //1. test normal move
    CompletableFuture<MoveManager.MoveResult> ret =
        moveManager.move(cid, dn1, dn2);
    //replication command should be sent;
    Mockito.verify(containerReplicaPendingOps, Mockito.times(1))
        .scheduleAddReplica(cid, dn2, 1);

    ContainerReplicaOp opAdd = new ContainerReplicaOp(
        ContainerReplicaOp.PendingOpType.ADD, dn2, 3, 0);
    ContainerReplicaOp opDel = new ContainerReplicaOp(
        ContainerReplicaOp.PendingOpType.DELETE, dn1, 3, 0);
    //successfully replicate
    currentReplicas.add(cp2);
    ContainerReplicaCount replicaCount =
        Mockito.mock(ContainerReplicaCount.class);
    Mockito.when(replicaCount.isOverReplicatedWithIndex(1)).thenReturn(true);
    Mockito.when(containerManager.getContainerReplicaCount(cid))
        .thenReturn(replicaCount);
    moveManager.notifyContainerOpCompleted(opAdd, cid);
    //delete command should be sent;
    Mockito.verify(containerReplicaPendingOps, Mockito.times(1))
        .scheduleDeleteReplica(cid, dn1, 1);
    moveManager.notifyContainerOpCompleted(opDel, cid);

    Assert.assertTrue(ret.get().equals(MoveManager.MoveResult.COMPLETED));
    Assert.assertTrue(moveManager.getPendingMove().isEmpty());

    //2. test expired ADD when moving
    currentReplicas.remove(cp2);
    ret = moveManager.move(cid, dn1, dn2);
    //replication command should be sent, we have sent a replication command
    //at line 159, so times here should be 1+1=2;
    Mockito.verify(containerReplicaPendingOps, Mockito.times(2))
        .scheduleAddReplica(cid, dn2, 1);
    Assert.assertFalse(moveManager.getPendingMove().isEmpty());
    moveManager.notifyContainerOpExpired(opAdd, cid);
    Assert.assertTrue(ret.get()
        .equals(MoveManager.MoveResult.REPLICATION_FAIL_TIME_OUT));
    Assert.assertTrue(moveManager.getPendingMove().isEmpty());

    //3. test expired DELETE when moving
    ret = moveManager.move(cid, dn1, dn2);
    Mockito.verify(containerReplicaPendingOps, Mockito.times(3))
        .scheduleAddReplica(cid, dn2, 1);
    Assert.assertFalse(moveManager.getPendingMove().isEmpty());
    currentReplicas.add(cp2);
    moveManager.notifyContainerOpCompleted(opAdd, cid);
    Mockito.verify(containerReplicaPendingOps, Mockito.times(2))
        .scheduleDeleteReplica(cid, dn1, 1);
    Assert.assertFalse(moveManager.getPendingMove().isEmpty());
    moveManager.notifyContainerOpExpired(opDel, cid);
    Assert.assertTrue(ret.get()
        .equals(MoveManager.MoveResult.DELETION_FAIL_TIME_OUT));
    Assert.assertTrue(moveManager.getPendingMove().isEmpty());
  }
}