/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container.replication.health;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerManagerImpl;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.MockPipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.DELETED;

/**
 * Tests for {@link DeletingContainerHandler}.
 */
public class TestDeletingContainerHandler {
  private ReplicationManager replicationManager;
  private File testDir;
  private ContainerManager containerManager;
  private DeletingContainerHandler deletingContainerHandler;
  private ECReplicationConfig ecReplicationConfig;
  private RatisReplicationConfig ratisReplicationConfig;
  private ContainerReplicaPendingOps pendingOpsMock;
  private SequenceIdGenerator sequenceIdGen;
  private SCMHAManager scmhaManager;
  private NodeManager nodeManager;
  private PipelineManager pipelineManager;
  private DBStore dbStore;

  @BeforeEach
  public void setup() throws IOException {
    final OzoneConfiguration conf = SCMTestUtils.getConf();
    testDir = GenericTestUtils.getTestDir(
        TestDeletingContainerHandler.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    dbStore = DBStoreBuilder.createDBStore(
        conf, new SCMDBDefinition());
    ecReplicationConfig = new ECReplicationConfig(3, 2);
    ratisReplicationConfig = RatisReplicationConfig.getInstance(
        HddsProtos.ReplicationFactor.THREE);
    scmhaManager = SCMHAManagerStub.getInstance(true);
    replicationManager = Mockito.mock(ReplicationManager.class);
    pendingOpsMock = Mockito.mock(ContainerReplicaPendingOps.class);
    nodeManager = new MockNodeManager(true, 10);
    sequenceIdGen = new SequenceIdGenerator(
        conf, scmhaManager, SCMDBDefinition.SEQUENCE_ID.getTable(dbStore));
    pipelineManager =
        new MockPipelineManager(dbStore, scmhaManager, nodeManager);
    containerManager = new ContainerManagerImpl(conf,
        scmhaManager, sequenceIdGen, pipelineManager,
        SCMDBDefinition.CONTAINERS.getTable(dbStore), pendingOpsMock);

    Mockito.doAnswer(invocation -> {
      containerManager.updateContainerState(
          ((ContainerID)invocation.getArguments()[0]),
          (HddsProtos.LifeCycleEvent) invocation.getArguments()[1]);
      return null;
    }).when(replicationManager).updateContainerState(
        Mockito.any(ContainerID.class),
        Mockito.any(HddsProtos.LifeCycleEvent.class));

    deletingContainerHandler =
        new DeletingContainerHandler(replicationManager);
  }

  @AfterEach
  public void cleanup() throws Exception {
    if (containerManager != null) {
      containerManager.close();
    }

    if (dbStore != null) {
      dbStore.close();
    }

    FileUtil.fullyDelete(testDir);
  }

  /**
   * If a container is not in Deleting state, it should not be handled by
   * DeletingContainerHandler. It should return false so the request can be
   * passed to the next handler in the chain.
   */
  @Test
  public void testNonDeletingContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ecReplicationConfig, 1, CLOSED);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSING, 1, 2, 3, 4, 5);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.EMPTY_LIST)
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    Assert.assertFalse(deletingContainerHandler.handle(request));
  }

  @Test
  public void testNonDeletingRatisContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, CLOSED);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSING, 0, 0, 0);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.EMPTY_LIST)
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    Assert.assertFalse(deletingContainerHandler.handle(request));
  }

  /**
   * If a container is in Deleting state and no replica exists,
   * change the state of the container to DELETED.
   */
  @Test
  public void testCleanupIfNoReplicaExist()
      throws IOException, TimeoutException, InvalidStateTransitionException {
    //ratis container
    cleanupIfNoReplicaExist(RatisReplicationConfig.getInstance(
        HddsProtos.ReplicationFactor.THREE));

    //ec container
    cleanupIfNoReplicaExist(ecReplicationConfig);
  }


  private void cleanupIfNoReplicaExist(ReplicationConfig replicationConfig)
      throws IOException, TimeoutException, InvalidStateTransitionException {
    ContainerInfo containerInfo = containerManager.allocateContainer(
        replicationConfig, "admin");
    ContainerID cID = containerInfo.containerID();

    //change the state of the container to Deleting
    containerManager.updateContainerState(cID,
        HddsProtos.LifeCycleEvent.FINALIZE);
    containerManager.updateContainerState(cID, HddsProtos.LifeCycleEvent.CLOSE);
    containerManager.updateContainerState(cID,
        HddsProtos.LifeCycleEvent.DELETE);

    Set<ContainerReplica> containerReplicas = new HashSet<>();
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.EMPTY_LIST)
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    Assert.assertTrue(deletingContainerHandler.handle(request));
    Assert.assertTrue(containerInfo.getState() == DELETED);
  }

  /**
   * If a container is in Deleting state , some replicas exist and
   * for each replica there is a pending delete, then do nothing.
   */
  @Test
  public void testNoNeedResendDeleteCommand()
      throws IOException, TimeoutException, InvalidStateTransitionException {
    //ratis container
    ContainerInfo containerInfo = containerManager.allocateContainer(
        RatisReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.THREE), "admin");
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSING, 0, 0, 0);
    List<ContainerReplicaOp> pendingOps = new ArrayList<>();
    containerReplicas.forEach(r -> pendingOps.add(
        ContainerReplicaOp.create(ContainerReplicaOp.PendingOpType.DELETE,
            r.getDatanodeDetails(), r.getReplicaIndex())));
    resendDeleteCommand(containerInfo, containerReplicas, pendingOps, 0);

    //EC container
    containerInfo = containerManager.allocateContainer(
        ecReplicationConfig, "admin");
    containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSING, 1, 2, 3, 4, 5);
    pendingOps.clear();
    containerReplicas.forEach(r -> pendingOps.add(
        ContainerReplicaOp.create(ContainerReplicaOp.PendingOpType.DELETE,
            r.getDatanodeDetails(), r.getReplicaIndex())));
    resendDeleteCommand(containerInfo, containerReplicas, pendingOps, 0);

  }

  /**
   * If a container is in Deleting state , some replicas exist and
   * for some replica there is no pending delete, then resending delete
   * command.
   */
  @Test
  public void testResendDeleteCommand()
      throws IOException, TimeoutException, InvalidStateTransitionException {
    //ratis container
    ContainerInfo containerInfo = containerManager.allocateContainer(
        RatisReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.THREE), "admin");
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSING, 0, 0, 0);
    List<ContainerReplicaOp> pendingOps = new ArrayList<>();
    Set<ContainerReplica> tempContainerReplicas = new HashSet<>();
    tempContainerReplicas.addAll(containerReplicas);
    Iterator iter = tempContainerReplicas.iterator();
    iter.next();
    iter.remove();
    Assert.assertEquals(2, tempContainerReplicas.size());
    tempContainerReplicas.forEach(r -> pendingOps.add(
        ContainerReplicaOp.create(ContainerReplicaOp.PendingOpType.DELETE,
            r.getDatanodeDetails(), r.getReplicaIndex())));
    resendDeleteCommand(containerInfo, containerReplicas, pendingOps, 1);

    //EC container
    containerInfo = containerManager.allocateContainer(
        ecReplicationConfig, "admin");
    containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSING, 1, 2, 3, 4, 5);
    pendingOps.clear();
    tempContainerReplicas.clear();
    tempContainerReplicas.addAll(containerReplicas);
    iter = tempContainerReplicas.iterator();
    iter.next();
    iter.remove();
    iter.next();
    iter.remove();
    Assert.assertEquals(3, tempContainerReplicas.size());

    tempContainerReplicas.forEach(r -> pendingOps.add(
        ContainerReplicaOp.create(ContainerReplicaOp.PendingOpType.DELETE,
            r.getDatanodeDetails(), r.getReplicaIndex())));
    //since one delete command is end when testing ratis container, so
    //here should be 1+2 = 3 times
    resendDeleteCommand(containerInfo, containerReplicas, pendingOps, 3);

  }

  private void resendDeleteCommand(ContainerInfo containerInfo,
                                   Set<ContainerReplica> containerReplicas,
                                   List<ContainerReplicaOp> pendingOps,
                                   int times)
      throws InvalidStateTransitionException, IOException, TimeoutException {
    ContainerID cID = containerInfo.containerID();
    //change the state of the container to Deleting
    containerManager.updateContainerState(cID,
        HddsProtos.LifeCycleEvent.FINALIZE);
    containerManager.updateContainerState(cID, HddsProtos.LifeCycleEvent.CLOSE);
    containerManager.updateContainerState(cID,
        HddsProtos.LifeCycleEvent.DELETE);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(pendingOps)
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    Assert.assertTrue(deletingContainerHandler.handle(request));

    Mockito.verify(replicationManager, Mockito.times(times))
        .sendDeleteCommand(Mockito.any(ContainerInfo.class), Mockito.anyInt(),
            Mockito.any(DatanodeDetails.class));
  }
}
