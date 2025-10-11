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

package org.apache.hadoop.hdds.scm.container;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.UNHEALTHY;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getContainer;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getECContainer;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getReplicas;
import static org.apache.hadoop.hdds.scm.container.TestContainerReportHandler.createMatchingDataChecksumForReplica;
import static org.apache.hadoop.hdds.scm.container.TestContainerReportHandler.createUniqueDataChecksumForReplica;
import static org.apache.hadoop.hdds.scm.container.TestContainerReportHandler.getContainerReportsProto;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.MockPipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.IncrementalContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test cases to verify the functionality of IncrementalContainerReportHandler.
 */
public class TestIncrementalContainerReportHandler {

  private NodeManager nodeManager;
  private ContainerManager containerManager;
  private ContainerStateManager containerStateManager;
  private EventPublisher publisher;
  private SCMContext scmContext = SCMContext.emptyContext();
  private PipelineManager pipelineManager;
  @TempDir
  private File testDir;
  private DBStore dbStore;

  @BeforeEach
  public void setup() throws IOException, InvalidStateTransitionException,
      TimeoutException {
    final OzoneConfiguration conf = new OzoneConfiguration();
    Path scmPath = Paths.get(testDir.getPath(), "scm-meta");
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, scmPath.toString());
    this.containerManager = mock(ContainerManager.class);
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    EventQueue eventQueue = new EventQueue();
    SCMStorageConfig storageConfig = new SCMStorageConfig(conf);
    HDDSLayoutVersionManager versionManager = mock(HDDSLayoutVersionManager.class);
    when(versionManager.getMetadataLayoutVersion()).thenReturn(maxLayoutVersion());
    when(versionManager.getSoftwareLayoutVersion()).thenReturn(maxLayoutVersion());
    this.nodeManager =
        new SCMNodeManager(conf, storageConfig, eventQueue, clusterMap,
            scmContext, versionManager);
    SCMHAManager scmhaManager = SCMHAManagerStub.getInstance(true);
    dbStore = DBStoreBuilder.createDBStore(conf, SCMDBDefinition.get());

    pipelineManager =
        new MockPipelineManager(dbStore, scmhaManager, nodeManager);

    this.containerStateManager = ContainerStateManagerImpl.newBuilder()
        .setConfiguration(conf)
        .setPipelineManager(pipelineManager)
        .setRatisServer(scmhaManager.getRatisServer())
        .setContainerStore(SCMDBDefinition.CONTAINERS.getTable(dbStore))
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .setContainerReplicaPendingOps(new ContainerReplicaPendingOps(
            Clock.system(ZoneId.systemDefault()), null))
        .build();

    this.publisher = mock(EventPublisher.class);

    when(containerManager.getContainer(any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainer(((ContainerID)invocation
                .getArguments()[0])));

    when(containerManager.getContainerReplicas(
        any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainerReplicas(((ContainerID)invocation
                .getArguments()[0])));

    doAnswer(invocation -> {
      containerStateManager
          .removeContainerReplica(
              (ContainerReplica)invocation.getArguments()[1]);
      return null;
    }).when(containerManager).removeContainerReplica(
        any(ContainerID.class),
        any(ContainerReplica.class));

    doAnswer(invocation -> {
      containerStateManager
          .updateContainerState(((ContainerID)invocation
                  .getArguments()[0]).getProtobuf(),
              (HddsProtos.LifeCycleEvent)invocation.getArguments()[1]);
      return null;
    }).when(containerManager).updateContainerState(
        any(ContainerID.class),
        any(HddsProtos.LifeCycleEvent.class));

    doAnswer(invocation -> {
      containerStateManager
          .updateContainerReplica(
              (ContainerReplica) invocation.getArguments()[1]);
      return null;
    }).when(containerManager).updateContainerReplica(
        any(ContainerID.class),
        any(ContainerReplica.class));

  }

  @AfterEach
  public void tearDown() throws Exception {
    nodeManager.close();
    if (dbStore != null) {
      dbStore.close();
    }
  }

  @Test
  public void testClosingToClosed() throws IOException, TimeoutException {
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
    containerReplicas.forEach(containerStateManager::updateContainerReplica);

    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            ContainerReplicaProto.State.CLOSED,
            datanodeOne.getUuidString());
    final IncrementalContainerReportFromDatanode icrFromDatanode =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icrFromDatanode, publisher);
    assertEquals(LifeCycleState.CLOSED, containerManager.getContainer(container.containerID()).getState());
  }

  /**
   * Tests that CLOSING to CLOSED transition for an EC container happens only
   * when a CLOSED replica with first index or parity indexes is reported.
   */
  @Test
  public void testClosingToClosedForECContainer()
      throws NodeNotFoundException, IOException, TimeoutException {
    // Create an EC 3-2 container
    ECReplicationConfig replicationConfig = new ECReplicationConfig(3, 2);
    final ContainerInfo container = getECContainer(LifeCycleState.CLOSING,
        PipelineID.randomId(), replicationConfig);
    List<DatanodeDetails> dns = setupECContainerForTesting(container);

    createAndHandleICR(container.containerID(),
        ContainerReplicaProto.State.CLOSED, dns.get(1), 2);
    // index is 2; container shouldn't transition to CLOSED
    assertEquals(LifeCycleState.CLOSING, containerManager.getContainer(container.containerID()).getState());

    createAndHandleICR(container.containerID(),
        ContainerReplicaProto.State.CLOSED, dns.get(2), 3);
    // index is 3; container shouldn't transition to CLOSED
    assertEquals(LifeCycleState.CLOSING, containerManager.getContainer(container.containerID()).getState());

    createAndHandleICR(container.containerID(),
        ContainerReplicaProto.State.CLOSED, dns.get(0), 1);
    // index is 1; container should transition to CLOSED
    assertEquals(LifeCycleState.CLOSED, containerManager.getContainer(container.containerID()).getState());

    // Test with an EC 6-3 container
    replicationConfig = new ECReplicationConfig(6, 3);
    final ContainerInfo container2 = getECContainer(LifeCycleState.CLOSING,
        PipelineID.randomId(), replicationConfig);
    dns = setupECContainerForTesting(container2);

    createAndHandleICR(container2.containerID(),
        ContainerReplicaProto.State.CLOSED, dns.get(5), 6);
    // index is 6, container shouldn't transition to CLOSED
    assertEquals(LifeCycleState.CLOSING, containerManager.getContainer(container2.containerID()).getState());

    createAndHandleICR(container2.containerID(),
        ContainerReplicaProto.State.CLOSED, dns.get(8), 9);
    // index is 9, container should transition to CLOSED
    assertEquals(LifeCycleState.CLOSED, containerManager.getContainer(container2.containerID()).getState());
  }

  /**
   * Creates an ICR from the specified datanodeDetails for replica with the
   * specified containerID. The replica's state is reported as the specified
   * state. Then, creates an {@link IncrementalContainerReportHandler} and
   * handles the ICR.
   * @param containerID id of the replica
   * @param state state of the replica to be reported
   * @param datanodeDetails DN that hosts this replica
   * @param replicaIndex index of the replica
   */
  private void createAndHandleICR(ContainerID containerID,
                                  ContainerReplicaProto.State state,
                                  DatanodeDetails datanodeDetails,
                                  int replicaIndex) {
    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(containerID, state,
            datanodeDetails.getUuidString(), true, replicaIndex);
    final IncrementalContainerReportFromDatanode icrFromDatanode =
        new IncrementalContainerReportFromDatanode(datanodeDetails,
            containerReport);

    final IncrementalContainerReportHandler reportHandler =
        new IncrementalContainerReportHandler(
            nodeManager, containerManager, scmContext);
    reportHandler.onMessage(icrFromDatanode, publisher);
  }

  /**
   * Creates the required number of DNs that will hold a replica each for the
   * specified EC container. Registers these DNs with the NodeManager, adds
   * this container and its replicas to ContainerStateManager etc.
   * @param container must be an EC container
   * @return List of datanodes that host replicas of this container
   */
  private List<DatanodeDetails> setupECContainerForTesting(
      ContainerInfo container)
      throws IOException, TimeoutException, NodeNotFoundException {
    assertEquals(HddsProtos.ReplicationType.EC, container.getReplicationType());
    final int numDatanodes =
        container.getReplicationConfig().getRequiredNodes();
    // Register required number of datanodes with NodeManager
    List<DatanodeDetails> dns = new ArrayList<>(numDatanodes);
    for (int i = 0; i < numDatanodes; i++) {
      dns.add(randomDatanodeDetails());
      nodeManager.register(dns.get(i), null, null);
    }

    // Add this container to ContainerStateManager
    containerStateManager.addContainer(container.getProtobuf());

    // Create its replicas and add them to ContainerStateManager
    Set<ContainerReplica> replicas =
        HddsTestUtils.getReplicasWithReplicaIndex(container.containerID(),
            ContainerReplicaProto.State.CLOSING,
            HddsTestUtils.CONTAINER_USED_BYTES_DEFAULT,
            HddsTestUtils.CONTAINER_NUM_KEYS_DEFAULT,
            container.getSequenceId(),
            dns.toArray(new DatanodeDetails[0]));
    for (ContainerReplica r : replicas) {
      containerStateManager.updateContainerReplica(r);
    }

    // Tell NodeManager that each DN hosts a replica of this container
    for (DatanodeDetails dn : dns) {
      nodeManager.addContainer(dn, container.containerID());
    }
    return dns;
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
    containerReplicas.forEach(containerStateManager::updateContainerReplica);

    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            ContainerReplicaProto.State.QUASI_CLOSED,
            datanodeOne.getUuidString());
    final IncrementalContainerReportFromDatanode icrFromDatanode =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icrFromDatanode, publisher);
    assertEquals(LifeCycleState.QUASI_CLOSED, containerManager.getContainer(container.containerID()).getState());
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
    containerReplicas.forEach(containerStateManager::updateContainerReplica);

    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            CLOSED,
            datanodeThree.getUuidString());
    final IncrementalContainerReportFromDatanode icr =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icr, publisher);
    assertEquals(LifeCycleState.CLOSED, containerManager.getContainer(container.containerID()).getState());
  }

  @ParameterizedTest
  @EnumSource(value = LifeCycleState.class, names = {"CLOSING", "QUASI_CLOSED"})
  public void testContainerStateTransitionToClosedWithMismatchingBCSID(LifeCycleState lcState) throws IOException {
    /*
     * Negative test. When a replica with a (lower) mismatching bcsId gets reported,
     * expect the ContainerReportHandler thread to not throw uncaught exception.
     * (That exception lead to ContainerReportHandler thread crash before HDDS-12150.)
     */
    final IncrementalContainerReportHandler reportHandler =
        new IncrementalContainerReportHandler(nodeManager, containerManager, scmContext);

    // Initial sequenceId 10000L is set here
    final ContainerInfo container = getContainer(lcState);
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
    containerReplicas.forEach(containerStateManager::updateContainerReplica);

    // Generate incremental container report with replica in CLOSED state with intentional lower bcsId
    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            CLOSED, datanodeThree.getUuidString(), false, 0,
            2000L);
    final IncrementalContainerReportFromDatanode icr =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);

    // Handler should NOT throw IllegalArgumentException
    try {
      reportHandler.onMessage(icr, publisher);
    } catch (IllegalArgumentException iaEx) {
      fail("Handler should not throw IllegalArgumentException: " + iaEx.getMessage());
    }

    // Because the container report is ignored, the container remains in the same previous state in SCM
    assertEquals(lcState, containerManager.getContainer(container.containerID()).getState());
  }

  @Test
  public void testOpenWithUnhealthyReplica() throws IOException {
    final IncrementalContainerReportHandler reportHandler =
        new IncrementalContainerReportHandler(
            nodeManager, containerManager, scmContext);

    RatisReplicationConfig replicationConfig =
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);
    Pipeline pipeline = pipelineManager.createPipeline(replicationConfig);
    List<DatanodeDetails> nodes = pipeline.getNodes();

    final DatanodeDetails datanodeOne = nodes.get(0);
    final DatanodeDetails datanodeTwo = nodes.get(1);
    final DatanodeDetails datanodeThree = nodes.get(2);

    final ContainerInfo container = getContainer(LifeCycleState.OPEN,
        pipeline.getId());

    nodeManager.register(datanodeOne, null, null);
    nodeManager.register(datanodeTwo, null, null);
    nodeManager.register(datanodeThree, null, null);
    final Set<ContainerReplica> containerReplicas = getReplicas(
        container.containerID(), ContainerReplicaProto.State.OPEN,
        datanodeOne, datanodeTwo, datanodeThree);

    containerStateManager.addContainer(container.getProtobuf());
    containerReplicas.forEach(containerStateManager::updateContainerReplica);

    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            UNHEALTHY,
            datanodeThree.getUuidString());
    final IncrementalContainerReportFromDatanode icr =
        new IncrementalContainerReportFromDatanode(
            datanodeThree, containerReport);
    reportHandler.onMessage(icr, publisher);
    assertEquals(LifeCycleState.CLOSING, containerManager.getContainer(container.containerID()).getState());
  }

  @Test
  public void testDeleteContainer() throws IOException, TimeoutException,
      NodeNotFoundException {
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
    containerReplicas.forEach(r -> {
      containerStateManager.updateContainerReplica(r);

      assertDoesNotThrow(() -> nodeManager.addContainer(r.getDatanodeDetails(), container.containerID()),
          "Node should be found");
    });
    assertEquals(3, containerStateManager
        .getContainerReplicas(container.containerID()).size());
    assertEquals(1, nodeManager.getContainers(datanodeOne).size());
    assertEquals(1, nodeManager.getContainers(datanodeTwo).size());
    assertEquals(1, nodeManager.getContainers(datanodeThree)
        .size());
    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            ContainerReplicaProto.State.DELETED,
            datanodeOne.getUuidString());
    final IncrementalContainerReportFromDatanode icr =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icr, publisher);
    assertEquals(2, containerStateManager
        .getContainerReplicas(container.containerID()).size());
    assertEquals(0, nodeManager.getContainers(datanodeOne).size());
    assertEquals(1, nodeManager.getContainers(datanodeTwo).size());
    assertEquals(1, nodeManager.getContainers(datanodeThree)
        .size());
  }

  @Test
  // HDDS-5249 - This test reproduces the race condition mentioned in the Jira
  // until the code was changed to fix the race condition.
  public void testICRFCRRace() throws IOException, NodeNotFoundException,
      ExecutionException, InterruptedException, TimeoutException {
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

    assertEquals(0, nodeManager.getContainers(datanode).size());

    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            CLOSED,
            datanode.getUuidString());
    final IncrementalContainerReportFromDatanode icr =
        new IncrementalContainerReportFromDatanode(
            datanode, containerReport);

    final ContainerReportsProto fullReport = getContainerReportsProto(
            containerTwo.containerID(), CLOSED, datanode.getUuidString());
    final ContainerReportFromDatanode fcr = new ContainerReportFromDatanode(
        datanode, fullReport);

    // We need to run the FCR and ICR at the same time via the executor so we
    // can try to simulate the race condition.
    ThreadPoolExecutor executor =
        (ThreadPoolExecutor)Executors.newFixedThreadPool(2);
    try {
      // Running this test 10 times to ensure the race condition we are testing
      // for does not occur. In local tests, before the code was fixed, this
      // test failed consistently every time (reproducing the issue).
      for (int i = 0; i < 10; i++) {
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
          assertEquals(1, containerStateManager
              .getContainerReplicas(container.containerID())
              .size());
          assertEquals(2, nmContainers.size());
        } else {
          // If the race condition occurs as mentioned in HDDS-5249, then this
          // assert should fail. We will have found nothing for "container" in
          // NM, but have found something for it in ContainerManager, and that
          // should not happen. It should be in both, or neither.
          assertEquals(0, containerStateManager
              .getContainerReplicas(container.containerID())
              .size());
          assertEquals(1, nmContainers.size());
        }
        assertEquals(1, containerStateManager
            .getContainerReplicas(containerTwo.containerID())
            .size());
      }
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testWithNoContainerDataChecksum() throws Exception {
    final IncrementalContainerReportHandler reportHandler = new IncrementalContainerReportHandler(nodeManager,
        containerManager, scmContext);

    final int numNodes = 3;

    // Create a container which will have one replica on each datanode.
    final ContainerInfo container = getContainer(LifeCycleState.CLOSED);
    ContainerID contID = container.containerID();

    List<DatanodeDetails> datanodes = new ArrayList<>();
    for (int i = 0; i < numNodes; i++) {
      DatanodeDetails dn = randomDatanodeDetails();
      nodeManager.register(dn, null, null);
      datanodes.add(dn);
    }

    containerStateManager.addContainer(container.getProtobuf());

    getReplicas(contID, ContainerReplicaProto.State.CLOSED, 0, datanodes)
        .forEach(containerStateManager::updateContainerReplica);

    // Container manager should now be aware of 3 replicas of each container.
    assertEquals(numNodes, containerManager.getContainerReplicas(contID).size());

    // All replicas should start with an empty data checksum in SCM.
    boolean contOneDataChecksumsEmpty = containerManager.getContainerReplicas(contID).stream()
        .allMatch(r -> r.getDataChecksum() == 0);
    assertTrue(contOneDataChecksumsEmpty, "Replicas of container one should not yet have any data checksums.");

    // Send a report to SCM from one datanode that still does not have a data checksum.
    for (DatanodeDetails dn: datanodes) {
      final IncrementalContainerReportProto dnReportProto = getIncrementalContainerReportProto(
          contID, ContainerReplicaProto.State.CLOSED, dn.getUuidString());
      final IncrementalContainerReportFromDatanode dnReport = new IncrementalContainerReportFromDatanode(dn,
          dnReportProto);
      reportHandler.onMessage(dnReport, publisher);
    }

    // Regardless of which datanode sent the report, none of them have checksums, so all replica's data checksums
    // should remain empty.
    boolean containerDataChecksumEmpty = containerManager.getContainerReplicas(contID).stream()
        .allMatch(r -> r.getDataChecksum() == 0);
    assertTrue(containerDataChecksumEmpty, "Replicas of the container should not have any data checksums.");
  }

  @Test
  public void testWithContainerDataChecksum() throws Exception {
    final IncrementalContainerReportHandler reportHandler = new IncrementalContainerReportHandler(nodeManager,
        containerManager, scmContext);

    final int numNodes = 3;

    // Create a container which will have one replica on each datanode.
    final ContainerInfo container = getContainer(LifeCycleState.CLOSED);
    ContainerID contID = container.containerID();

    List<DatanodeDetails> datanodes = new ArrayList<>();
    for (int i = 0; i < numNodes; i++) {
      DatanodeDetails dn = randomDatanodeDetails();
      nodeManager.register(dn, null, null);
      datanodes.add(dn);
    }

    containerStateManager.addContainer(container.getProtobuf());

    getReplicas(contID, ContainerReplicaProto.State.CLOSED, 0, datanodes)
        .forEach(containerStateManager::updateContainerReplica);

    // Container manager should now be aware of 3 replicas of each container.
    assertEquals(3, containerManager.getContainerReplicas(contID).size());

    // All replicas should start with a zero data checksum in SCM.
    boolean dataChecksumsEmpty = containerManager.getContainerReplicas(contID).stream()
        .allMatch(r -> r.getDataChecksum() == 0);
    assertTrue(dataChecksumsEmpty, "Replicas of container one should not yet have any data checksums.");

    // For each datanode, send a container report with a mismatched checksum.
    for (DatanodeDetails dn: datanodes) {
      IncrementalContainerReportProto dnReportProto = getIncrementalContainerReportProto(
          contID, ContainerReplicaProto.State.CLOSED, dn.getUuidString());
      ContainerReplicaProto replicaWithChecksum = dnReportProto.getReport(0).toBuilder()
          .setDataChecksum(createUniqueDataChecksumForReplica(contID, dn.getUuidString()))
          .build();
      IncrementalContainerReportProto reportWithChecksum = dnReportProto.toBuilder()
          .clearReport()
          .addReport(replicaWithChecksum)
          .build();
      final IncrementalContainerReportFromDatanode dnReport = new IncrementalContainerReportFromDatanode(dn,
          reportWithChecksum);
      reportHandler.onMessage(dnReport, publisher);
    }

    // All the replicas should have different checksums.
    // Since the containers don't have any data in this test, different checksums are based on container ID and
    // datanode ID.
    int numReplicasChecked = 0;
    for (ContainerReplica replica: containerManager.getContainerReplicas(contID)) {
      long expectedChecksum = createUniqueDataChecksumForReplica(
          contID, replica.getDatanodeDetails().getUuidString());
      assertEquals(expectedChecksum, replica.getDataChecksum());
      numReplicasChecked++;
    }
    assertEquals(numNodes, numReplicasChecked);

    // For each datanode, send a container report with a matching checksum.
    // This simulates reconciliation running.
    for (DatanodeDetails dn: datanodes) {
      IncrementalContainerReportProto dnReportProto = getIncrementalContainerReportProto(
          contID, ContainerReplicaProto.State.CLOSED, dn.getUuidString());
      ContainerReplicaProto replicaWithChecksum = dnReportProto.getReport(0).toBuilder()
          .setDataChecksum(createMatchingDataChecksumForReplica(contID))
          .build();
      IncrementalContainerReportProto reportWithChecksum = dnReportProto.toBuilder()
          .clearReport()
          .addReport(replicaWithChecksum)
          .build();
      IncrementalContainerReportFromDatanode dnReport = new IncrementalContainerReportFromDatanode(dn,
          reportWithChecksum);
      reportHandler.onMessage(dnReport, publisher);
    }

    // All the replicas should now have matching checksums.
    // Since the containers don't have any data in this test, the matching checksums are based on container ID only.
    numReplicasChecked = 0;
    for (ContainerReplica replica: containerManager.getContainerReplicas(contID)) {
      long expectedChecksum = createMatchingDataChecksumForReplica(contID);
      assertEquals(expectedChecksum, replica.getDataChecksum());
      numReplicasChecked++;
    }
    assertEquals(numNodes, numReplicasChecked);
  }

  private static IncrementalContainerReportProto
      getIncrementalContainerReportProto(ContainerReplicaProto replicaProto) {
    final IncrementalContainerReportProto.Builder crBuilder =
            IncrementalContainerReportProto.newBuilder();
    return crBuilder.addReport(replicaProto).build();
  }

  private static IncrementalContainerReportProto
      getIncrementalContainerReportProto(
          final ContainerID containerId,
          final ContainerReplicaProto.State state,
          final String originNodeId,
          final boolean hasReplicaIndex,
          final int replicaIndex) {
    return getIncrementalContainerReportProto(containerId, state, originNodeId,
        hasReplicaIndex, replicaIndex, 10000L);
  }

  private static IncrementalContainerReportProto
      getIncrementalContainerReportProto(
          final ContainerID containerId,
          final ContainerReplicaProto.State state,
          final String originNodeId,
          final boolean hasReplicaIndex,
          final int replicaIndex,
          final long bcsId) {
    final ContainerReplicaProto.Builder replicaProto =
            ContainerReplicaProto.newBuilder()
                    .setContainerID(containerId.getId())
                    .setState(state)
                    .setOriginNodeId(originNodeId)
                    .setSize(5368709120L)
                    .setUsed(2000000000L)
                    .setKeyCount(100000000L)
                    .setReadCount(100000000L)
                    .setWriteCount(100000000L)
                    .setReadBytes(2000000000L)
                    .setWriteBytes(2000000000L)
                    .setBlockCommitSequenceId(bcsId)
                    .setDeleteTransactionId(0);
    if (hasReplicaIndex) {
      replicaProto.setReplicaIndex(replicaIndex);
    }
    return getIncrementalContainerReportProto(replicaProto.build());
  }

  private static IncrementalContainerReportProto
      getIncrementalContainerReportProto(
          final ContainerID containerId,
          final ContainerReplicaProto.State state,
          final String originNodeId) {
    return getIncrementalContainerReportProto(containerId, state, originNodeId,
            false, 0);
  }

  private void testReplicaIndexUpdate(ContainerInfo container,
         DatanodeDetails dn, int replicaIndex,
         Map<DatanodeDetails, Integer> expectedReplicaMap) {
    final IncrementalContainerReportProto containerReport =
            getIncrementalContainerReportProto(container.containerID(),
                    ContainerReplicaProto.State.CLOSED, dn.getUuidString(),
                    true, replicaIndex);
    final IncrementalContainerReportFromDatanode containerReportFromDatanode =
            new IncrementalContainerReportFromDatanode(dn, containerReport);
    final IncrementalContainerReportHandler reportHandler =
            new IncrementalContainerReportHandler(nodeManager,
                    containerManager, scmContext);
    reportHandler.onMessage(containerReportFromDatanode, publisher);
    assertEquals(containerStateManager.getContainerReplicas(container.containerID()).stream()
        .collect(Collectors.toMap(ContainerReplica::getDatanodeDetails,
            ContainerReplica::getReplicaIndex)), expectedReplicaMap);

  }

  @Test
  public void testECReplicaIndexValidation() throws NodeNotFoundException,
          IOException, TimeoutException {
    List<DatanodeDetails> dns = IntStream.range(0, 5)
        .mapToObj(i -> randomDatanodeDetails()).collect(Collectors.toList());
    dns.forEach(dn -> nodeManager.register(dn, null, null));
    ECReplicationConfig replicationConfig = new ECReplicationConfig(3, 2);
    final ContainerInfo container = getECContainer(LifeCycleState.CLOSED,
            PipelineID.randomId(), replicationConfig);
    nodeManager.addContainer(dns.get(0), container.containerID());
    nodeManager.addContainer(dns.get(1), container.containerID());
    nodeManager.addContainer(dns.get(2), container.containerID());
    nodeManager.addContainer(dns.get(3), container.containerID());
    nodeManager.addContainer(dns.get(4), container.containerID());
    containerStateManager.addContainer(container.getProtobuf());
    Set<ContainerReplica> replicas =
            HddsTestUtils.getReplicasWithReplicaIndex(container.containerID(),
                    ContainerReplicaProto.State.CLOSED,
                    HddsTestUtils.CONTAINER_USED_BYTES_DEFAULT,
                    HddsTestUtils.CONTAINER_NUM_KEYS_DEFAULT,
                    1000000L,
                    dns.toArray(new DatanodeDetails[0]));
    Map<DatanodeDetails, Integer> replicaMap = replicas.stream()
            .collect(Collectors.toMap(ContainerReplica::getDatanodeDetails,
                    ContainerReplica::getReplicaIndex));
    replicas.forEach(containerStateManager::updateContainerReplica);
    testReplicaIndexUpdate(container, dns.get(0), 0, replicaMap);
    testReplicaIndexUpdate(container, dns.get(0), 6, replicaMap);
    replicaMap.put(dns.get(0), 2);
    testReplicaIndexUpdate(container, dns.get(0), 2, replicaMap);
  }
}
