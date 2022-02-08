/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.scm.ha.MockSCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.MockPipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .IncrementalContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getContainer;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getReplicas;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;

/**
 * Test cases to verify the functionality of IncrementalContainerReportHandler.
 */
public class TestIncrementalContainerReportHandler {

  private NodeManager nodeManager;
  private ContainerManager containerManager;
  private ContainerStateManager containerStateManager;
  private EventPublisher publisher;
  private HDDSLayoutVersionManager versionManager;
  private SCMContext scmContext = SCMContext.emptyContext();
  private PipelineManager pipelineManager;
  private File testDir;
  private DBStore dbStore;
  private SCMHAManager scmhaManager;

  @Before
  public void setup() throws IOException, InvalidStateTransitionException {
    final OzoneConfiguration conf = new OzoneConfiguration();
    final String path =
        GenericTestUtils.getTempPath(UUID.randomUUID().toString());
    Path scmPath = Paths.get(path, "scm-meta");
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, scmPath.toString());
    this.containerManager = Mockito.mock(ContainerManager.class);
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    EventQueue eventQueue = new EventQueue();
    SCMStorageConfig storageConfig = new SCMStorageConfig(conf);
    this.versionManager =
        Mockito.mock(HDDSLayoutVersionManager.class);
    Mockito.when(versionManager.getMetadataLayoutVersion())
        .thenReturn(maxLayoutVersion());
    Mockito.when(versionManager.getSoftwareLayoutVersion())
        .thenReturn(maxLayoutVersion());
    this.nodeManager =
        new SCMNodeManager(conf, storageConfig, eventQueue, clusterMap,
            scmContext, versionManager);
    scmhaManager = MockSCMHAManager.getInstance(true);
    testDir = GenericTestUtils.getTestDir(
        TestContainerManagerImpl.class.getSimpleName() + UUID.randomUUID());
    dbStore = DBStoreBuilder.createDBStore(
        conf, new SCMDBDefinition());

    pipelineManager =
        new MockPipelineManager(dbStore, scmhaManager, nodeManager);

    this.containerStateManager = ContainerStateManagerImpl.newBuilder()
        .setConfiguration(conf)
        .setPipelineManager(pipelineManager)
        .setRatisServer(scmhaManager.getRatisServer())
        .setContainerStore(SCMDBDefinition.CONTAINERS.getTable(dbStore))
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .build();

    this.publisher = Mockito.mock(EventPublisher.class);

    Mockito.when(containerManager.getContainer(Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainer(((ContainerID)invocation
                .getArguments()[0]).getProtobuf()));

    Mockito.when(containerManager.getContainerReplicas(
        Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainerReplicas(((ContainerID)invocation
                .getArguments()[0]).getProtobuf()));

    Mockito.doAnswer(invocation -> {
      containerStateManager
          .removeContainerReplica(((ContainerID)invocation
                  .getArguments()[0]).getProtobuf(),
              (ContainerReplica)invocation.getArguments()[1]);
      return null;
    }).when(containerManager).removeContainerReplica(
        Mockito.any(ContainerID.class),
        Mockito.any(ContainerReplica.class));

    Mockito.doAnswer(invocation -> {
      containerStateManager
          .updateContainerState(((ContainerID)invocation
                  .getArguments()[0]).getProtobuf(),
              (HddsProtos.LifeCycleEvent)invocation.getArguments()[1]);
      return null;
    }).when(containerManager).updateContainerState(
        Mockito.any(ContainerID.class),
        Mockito.any(HddsProtos.LifeCycleEvent.class));

    Mockito.doAnswer(invocation -> {
      containerStateManager
          .updateContainerReplica(((ContainerID)invocation
                  .getArguments()[0]).getProtobuf(),
              (ContainerReplica) invocation.getArguments()[1]);
      return null;
    }).when(containerManager).updateContainerReplica(
        Mockito.any(ContainerID.class),
        Mockito.any(ContainerReplica.class));

  }

  @After
  public void tearDown() throws Exception {
    containerStateManager.close();
    if (dbStore != null) {
      dbStore.close();
    }

    FileUtil.fullyDelete(testDir);
  }


  @Test
  public void testClosingToClosed() throws IOException {
    final IncrementalContainerReportHandler reportHandler =
        new IncrementalContainerReportHandler(
            nodeManager, containerManager, scmContext);
    final ContainerInfo container = getContainer(LifeCycleState.CLOSING);
    final DatanodeDetails datanodeOne = randomDatanodeDetails();
    final DatanodeDetails datanodeTwo = randomDatanodeDetails();
    final DatanodeDetails datanodeThree = randomDatanodeDetails();
    nodeManager.register(datanodeOne, null, null);
    nodeManager.register(datanodeTwo, null, null);
    nodeManager.register(datanodeThree, null, null);
    final Set<ContainerReplica> containerReplicas = getReplicas(
        container.containerID(),
        ContainerReplicaProto.State.CLOSING,
        datanodeOne, datanodeTwo, datanodeThree);

    containerStateManager.addContainer(container.getProtobuf());
    containerReplicas.forEach(r -> containerStateManager.updateContainerReplica(
        container.containerID().getProtobuf(), r));

    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            ContainerReplicaProto.State.CLOSED,
            datanodeOne.getUuidString());
    final IncrementalContainerReportFromDatanode icrFromDatanode =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icrFromDatanode, publisher);
    Assert.assertEquals(LifeCycleState.CLOSED,
        containerManager.getContainer(container.containerID()).getState());
  }

  @Test
  public void testClosingToQuasiClosed() throws IOException {
    final IncrementalContainerReportHandler reportHandler =
        new IncrementalContainerReportHandler(
            nodeManager, containerManager, scmContext);
    final ContainerInfo container = getContainer(LifeCycleState.CLOSING);
    final DatanodeDetails datanodeOne = randomDatanodeDetails();
    final DatanodeDetails datanodeTwo = randomDatanodeDetails();
    final DatanodeDetails datanodeThree = randomDatanodeDetails();
    nodeManager.register(datanodeOne, null, null);
    nodeManager.register(datanodeTwo, null, null);
    nodeManager.register(datanodeThree, null, null);
    final Set<ContainerReplica> containerReplicas = getReplicas(
        container.containerID(),
        ContainerReplicaProto.State.CLOSING,
        datanodeOne, datanodeTwo, datanodeThree);

    containerStateManager.addContainer(container.getProtobuf());
    containerReplicas.forEach(r -> containerStateManager.updateContainerReplica(
        container.containerID().getProtobuf(), r));


    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            ContainerReplicaProto.State.QUASI_CLOSED,
            datanodeOne.getUuidString());
    final IncrementalContainerReportFromDatanode icrFromDatanode =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icrFromDatanode, publisher);
    Assert.assertEquals(LifeCycleState.QUASI_CLOSED,
        containerManager.getContainer(container.containerID()).getState());
  }

  @Test
  public void testQuasiClosedToClosed() throws IOException {
    final IncrementalContainerReportHandler reportHandler =
        new IncrementalContainerReportHandler(
            nodeManager, containerManager, scmContext);
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final DatanodeDetails datanodeOne = randomDatanodeDetails();
    final DatanodeDetails datanodeTwo = randomDatanodeDetails();
    final DatanodeDetails datanodeThree = randomDatanodeDetails();
    nodeManager.register(datanodeOne, null, null);
    nodeManager.register(datanodeTwo, null, null);
    nodeManager.register(datanodeThree, null, null);
    final Set<ContainerReplica> containerReplicas = getReplicas(
        container.containerID(),
        ContainerReplicaProto.State.CLOSING,
        datanodeOne, datanodeTwo);
    containerReplicas.addAll(getReplicas(
        container.containerID(),
        ContainerReplicaProto.State.QUASI_CLOSED,
        datanodeThree));

    containerStateManager.addContainer(container.getProtobuf());
    containerReplicas.forEach(r -> containerStateManager.updateContainerReplica(
        container.containerID().getProtobuf(), r));

    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            CLOSED,
            datanodeThree.getUuidString());
    final IncrementalContainerReportFromDatanode icr =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icr, publisher);
    Assert.assertEquals(LifeCycleState.CLOSED,
        containerManager.getContainer(container.containerID()).getState());
  }

  @Test
  public void testDeleteContainer() throws IOException {
    final IncrementalContainerReportHandler reportHandler =
        new IncrementalContainerReportHandler(
            nodeManager, containerManager, scmContext);
    final ContainerInfo container = getContainer(LifeCycleState.CLOSED);
    final DatanodeDetails datanodeOne = randomDatanodeDetails();
    final DatanodeDetails datanodeTwo = randomDatanodeDetails();
    final DatanodeDetails datanodeThree = randomDatanodeDetails();
    nodeManager.register(datanodeOne, null, null);
    nodeManager.register(datanodeTwo, null, null);
    nodeManager.register(datanodeThree, null, null);
    final Set<ContainerReplica> containerReplicas = getReplicas(
        container.containerID(),
        CLOSED,
        datanodeOne, datanodeTwo, datanodeThree);

    containerStateManager.addContainer(container.getProtobuf());
    containerReplicas.forEach(r -> containerStateManager.updateContainerReplica(
        container.containerID().getProtobuf(), r));
    Assert.assertEquals(3, containerStateManager
        .getContainerReplicas(container.containerID().getProtobuf()).size());
    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            ContainerReplicaProto.State.DELETED,
            datanodeThree.getUuidString());
    final IncrementalContainerReportFromDatanode icr =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icr, publisher);
    Assert.assertEquals(2, containerStateManager
        .getContainerReplicas(container.containerID().getProtobuf()).size());
  }

  @Test
  // HDDS-5249 - This test reproduces the race condition mentioned in the Jira
  // until the code was changed to fix the race condition.
  public void testICRFCRRace() throws IOException, NodeNotFoundException,
      ExecutionException, InterruptedException {
    final IncrementalContainerReportHandler reportHandler =
        new IncrementalContainerReportHandler(
            nodeManager, containerManager, scmContext);
    final ContainerReportHandler fullReportHandler =
        new ContainerReportHandler(nodeManager, containerManager);

    final ContainerInfo container = getContainer(LifeCycleState.CLOSED);
    final ContainerInfo containerTwo = getContainer(LifeCycleState.CLOSED);
    final DatanodeDetails datanode = randomDatanodeDetails();
    nodeManager.register(datanode, null, null);

    containerStateManager.addContainer(container.getProtobuf());
    containerStateManager.addContainer(containerTwo.getProtobuf());

    Assert.assertEquals(0, nodeManager.getContainers(datanode).size());

    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            CLOSED,
            datanode.getUuidString());
    final IncrementalContainerReportFromDatanode icr =
        new IncrementalContainerReportFromDatanode(
            datanode, containerReport);

    final ContainerReportsProto fullReport = TestContainerReportHandler
        .getContainerReportsProto(containerTwo.containerID(), CLOSED,
            datanode.getUuidString());
    final ContainerReportFromDatanode fcr =new ContainerReportFromDatanode(
        datanode, fullReport);

    // We need to run the FCR and ICR at the same time via the executor so we
    // can try to simulate the race condition.
    ThreadPoolExecutor executor =
        (ThreadPoolExecutor)Executors.newFixedThreadPool(2);
    try {
      // Running this test 10 times to ensure the race condition we are testing
      // for does not occur. In local tests, before the code was fixed, this
      // test failed consistently every time (reproducing the issue).
      for (int i=0; i<10; i++) {
        Future<?> t1 =
            executor.submit(() -> fullReportHandler.onMessage(fcr, publisher));
        Future<?> t2 =
            executor.submit(() -> reportHandler.onMessage(icr, publisher));
        t1.get();
        t2.get();

        Set<ContainerID> nmContainers = nodeManager.getContainers(datanode);
        if (nmContainers.contains(container.containerID())) {
          // If we find "container" in the NM, then we must also have it in
          // Container Manager.
          Assert.assertEquals(1, containerStateManager
              .getContainerReplicas(container.containerID().getProtobuf())
              .size());
          Assert.assertEquals(2, nmContainers.size());
        } else {
          // If the race condition occurs as mentioned in HDDS-5249, then this
          // assert should fail. We will have found nothing for "container" in
          // NM, but have found something for it in ContainerManager, and that
          // should not happen. It should be in both, or neither.
          Assert.assertEquals(0, containerStateManager
              .getContainerReplicas(container.containerID().getProtobuf())
              .size());
          Assert.assertEquals(1, nmContainers.size());
        }
        Assert.assertEquals(1, containerStateManager
            .getContainerReplicas(containerTwo.containerID().getProtobuf())
            .size());
      }
    } finally {
      executor.shutdown();
    }
  }

  private static IncrementalContainerReportProto
      getIncrementalContainerReportProto(
          final ContainerID containerId,
          final ContainerReplicaProto.State state,
          final String originNodeId) {
    final IncrementalContainerReportProto.Builder crBuilder =
        IncrementalContainerReportProto.newBuilder();
    final ContainerReplicaProto replicaProto =
        ContainerReplicaProto.newBuilder()
            .setContainerID(containerId.getId())
            .setState(state)
            .setOriginNodeId(originNodeId)
            .setFinalhash("e16cc9d6024365750ed8dbd194ea46d2")
            .setSize(5368709120L)
            .setUsed(2000000000L)
            .setKeyCount(100000000L)
            .setReadCount(100000000L)
            .setWriteCount(100000000L)
            .setReadBytes(2000000000L)
            .setWriteBytes(2000000000L)
            .setBlockCommitSequenceId(10000L)
            .setDeleteTransactionId(0)
            .build();
    return crBuilder.addReport(replicaProto).build();
  }
}