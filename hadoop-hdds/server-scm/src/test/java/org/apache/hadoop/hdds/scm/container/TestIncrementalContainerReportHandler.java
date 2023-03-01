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
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.MockPipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
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
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getContainer;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getECContainer;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getReplicas;
import static org.apache.hadoop.hdds.scm.container.TestContainerReportHandler.getContainerReportsProto;
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

  @BeforeEach
  public void setup() throws IOException, InvalidStateTransitionException,
      TimeoutException {
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
    scmhaManager = SCMHAManagerStub.getInstance(true);
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
        .setContainerReplicaPendingOps(new ContainerReplicaPendingOps(
            conf, Clock.system(ZoneId.systemDefault())))
        .build();

    this.publisher = Mockito.mock(EventPublisher.class);

    Mockito.when(containerManager.getContainer(Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainer(((ContainerID)invocation
                .getArguments()[0])));

    Mockito.when(containerManager.getContainerReplicas(
        Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainerReplicas(((ContainerID)invocation
                .getArguments()[0])));

    Mockito.doAnswer(invocation -> {
      containerStateManager
          .removeContainerReplica(((ContainerID)invocation
                  .getArguments()[0]),
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
                  .getArguments()[0]),
              (ContainerReplica) invocation.getArguments()[1]);
      return null;
    }).when(containerManager).updateContainerReplica(
        Mockito.any(ContainerID.class),
        Mockito.any(ContainerReplica.class));

  }

  @AfterEach
  public void tearDown() throws Exception {
    containerStateManager.close();
    if (dbStore != null) {
      dbStore.close();
    }

    FileUtil.fullyDelete(testDir);
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
    containerReplicas.forEach(r -> containerStateManager.updateContainerReplica(
        container.containerID(), r));

    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            ContainerReplicaProto.State.CLOSED,
            datanodeOne.getUuidString());
    final IncrementalContainerReportFromDatanode icrFromDatanode =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icrFromDatanode, publisher);
    Assertions.assertEquals(LifeCycleState.CLOSED,
        containerManager.getContainer(container.containerID()).getState());
  }

  @Test
  public void testClosingToQuasiClosed() throws IOException, TimeoutException {
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
        container.containerID(), r));


    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            ContainerReplicaProto.State.QUASI_CLOSED,
            datanodeOne.getUuidString());
    final IncrementalContainerReportFromDatanode icrFromDatanode =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icrFromDatanode, publisher);
    Assertions.assertEquals(LifeCycleState.QUASI_CLOSED,
        containerManager.getContainer(container.containerID()).getState());
  }

  @Test
  public void testQuasiClosedToClosed() throws IOException, TimeoutException {
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
        container.containerID(), r));

    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            CLOSED,
            datanodeThree.getUuidString());
    final IncrementalContainerReportFromDatanode icr =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icr, publisher);
    Assertions.assertEquals(LifeCycleState.CLOSED,
        containerManager.getContainer(container.containerID()).getState());
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
      containerStateManager.updateContainerReplica(container.containerID(), r);
      try {
        nodeManager.addContainer(
            r.getDatanodeDetails(), container.containerID());
      } catch (NodeNotFoundException e) {
        Assertions.fail("Node should be found");
      }
    });
    Assertions.assertEquals(3, containerStateManager
        .getContainerReplicas(container.containerID()).size());
    Assertions.assertEquals(1, nodeManager.getContainers(datanodeOne).size());
    Assertions.assertEquals(1, nodeManager.getContainers(datanodeTwo).size());
    Assertions.assertEquals(1, nodeManager.getContainers(datanodeThree)
        .size());
    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            ContainerReplicaProto.State.DELETED,
            datanodeOne.getUuidString());
    final IncrementalContainerReportFromDatanode icr =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icr, publisher);
    Assertions.assertEquals(2, containerStateManager
        .getContainerReplicas(container.containerID()).size());
    Assertions.assertEquals(0, nodeManager.getContainers(datanodeOne).size());
    Assertions.assertEquals(1, nodeManager.getContainers(datanodeTwo).size());
    Assertions.assertEquals(1, nodeManager.getContainers(datanodeThree)
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

    Assertions.assertEquals(0, nodeManager.getContainers(datanode).size());

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
          Assertions.assertEquals(1, containerStateManager
              .getContainerReplicas(container.containerID())
              .size());
          Assertions.assertEquals(2, nmContainers.size());
        } else {
          // If the race condition occurs as mentioned in HDDS-5249, then this
          // assert should fail. We will have found nothing for "container" in
          // NM, but have found something for it in ContainerManager, and that
          // should not happen. It should be in both, or neither.
          Assertions.assertEquals(0, containerStateManager
              .getContainerReplicas(container.containerID())
              .size());
          Assertions.assertEquals(1, nmContainers.size());
        }
        Assertions.assertEquals(1, containerStateManager
            .getContainerReplicas(containerTwo.containerID())
            .size());
      }
    } finally {
      executor.shutdown();
    }
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
    final ContainerReplicaProto.Builder replicaProto =
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
    Assert.assertEquals(containerStateManager
            .getContainerReplicas(container.containerID()).stream()
            .collect(Collectors.toMap(ContainerReplica::getDatanodeDetails,
                    ContainerReplica::getReplicaIndex)), expectedReplicaMap);

  }

  @Test
  public void testECReplicaIndexValidation() throws NodeNotFoundException,
          IOException, TimeoutException {
    List<DatanodeDetails> dns = IntStream.range(0, 5)
        .mapToObj(i -> randomDatanodeDetails()).collect(Collectors.toList());
    dns.stream().forEach(dn -> nodeManager.register(dn, null, null));
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
    replicas.forEach(r -> containerStateManager.updateContainerReplica(
            container.containerID(), r));
    testReplicaIndexUpdate(container, dns.get(0), 0, replicaMap);
    testReplicaIndexUpdate(container, dns.get(0), 6, replicaMap);
    replicaMap.put(dns.get(0), 2);
    testReplicaIndexUpdate(container, dns.get(0), 2, replicaMap);
  }
}