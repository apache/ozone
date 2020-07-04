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

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .IncrementalContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.UUID;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.scm.TestUtils.getContainer;
import static org.apache.hadoop.hdds.scm.TestUtils.getReplicas;

/**
 * Test cases to verify the functionality of IncrementalContainerReportHandler.
 */
public class TestIncrementalContainerReportHandler {

  private NodeManager nodeManager;
  private ContainerManager containerManager;
  private ContainerStateManager containerStateManager;
  private EventPublisher publisher;

  @Before
  public void setup() throws IOException {
    final OzoneConfiguration conf = new OzoneConfiguration();
    final String path =
        GenericTestUtils.getTempPath(UUID.randomUUID().toString());
    Path scmPath = Paths.get(path, "scm-meta");
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, scmPath.toString());
    this.containerManager = Mockito.mock(ContainerManager.class);
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    EventQueue eventQueue = new EventQueue();
    SCMStorageConfig storageConfig = new SCMStorageConfig(conf);
    this.nodeManager =
        new SCMNodeManager(conf, storageConfig, eventQueue, clusterMap);

    this.containerStateManager = new ContainerStateManager(conf);
    this.publisher = Mockito.mock(EventPublisher.class);


    Mockito.when(containerManager.getContainer(Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainer((ContainerID)invocation.getArguments()[0]));

    Mockito.when(containerManager.getContainerReplicas(
        Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainerReplicas((ContainerID)invocation.getArguments()[0]));

    Mockito.doAnswer(invocation -> {
      containerStateManager
          .removeContainerReplica((ContainerID)invocation.getArguments()[0],
              (ContainerReplica)invocation.getArguments()[1]);
      return null;
    }).when(containerManager).removeContainerReplica(
        Mockito.any(ContainerID.class),
        Mockito.any(ContainerReplica.class));

    Mockito.doAnswer(invocation -> {
      containerStateManager
          .updateContainerState((ContainerID)invocation.getArguments()[0],
              (HddsProtos.LifeCycleEvent)invocation.getArguments()[1]);
      return null;
    }).when(containerManager).updateContainerState(
        Mockito.any(ContainerID.class),
        Mockito.any(HddsProtos.LifeCycleEvent.class));

  }

  @After
  public void tearDown() throws IOException {
    containerStateManager.close();
  }


  @Test
  public void testClosingToClosed() throws IOException {
    final IncrementalContainerReportHandler reportHandler =
        new IncrementalContainerReportHandler(nodeManager, containerManager);
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

    containerStateManager.loadContainer(container);
    containerReplicas.forEach(r -> {
      try {
        containerStateManager.updateContainerReplica(
            container.containerID(), r);
      } catch (ContainerNotFoundException ignored) {

      }
    });

    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            ContainerReplicaProto.State.CLOSED,
            datanodeOne.getUuidString());
    final IncrementalContainerReportFromDatanode icrFromDatanode =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icrFromDatanode, publisher);
    Assert.assertEquals(LifeCycleState.CLOSED, container.getState());
  }

  @Test
  public void testClosingToQuasiClosed() throws IOException {
    final IncrementalContainerReportHandler reportHandler =
        new IncrementalContainerReportHandler(nodeManager, containerManager);
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

    containerStateManager.loadContainer(container);
    containerReplicas.forEach(r -> {
      try {
        containerStateManager.updateContainerReplica(
            container.containerID(), r);
      } catch (ContainerNotFoundException ignored) {

      }
    });


    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            ContainerReplicaProto.State.QUASI_CLOSED,
            datanodeOne.getUuidString());
    final IncrementalContainerReportFromDatanode icrFromDatanode =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icrFromDatanode, publisher);
    Assert.assertEquals(LifeCycleState.QUASI_CLOSED, container.getState());
  }

  @Test
  public void testQuasiClosedToClosed() throws IOException {
    final IncrementalContainerReportHandler reportHandler =
        new IncrementalContainerReportHandler(nodeManager, containerManager);
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

    containerStateManager.loadContainer(container);
    containerReplicas.forEach(r -> {
      try {
        containerStateManager.updateContainerReplica(
            container.containerID(), r);
      } catch (ContainerNotFoundException ignored) {

      }
    });

    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            ContainerReplicaProto.State.CLOSED,
            datanodeThree.getUuidString());
    final IncrementalContainerReportFromDatanode icr =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icr, publisher);
    Assert.assertEquals(LifeCycleState.CLOSED, container.getState());
  }

  @Test
  public void testDeleteContainer() throws IOException {
    final IncrementalContainerReportHandler reportHandler =
        new IncrementalContainerReportHandler(nodeManager, containerManager);
    final ContainerInfo container = getContainer(LifeCycleState.CLOSED);
    final DatanodeDetails datanodeOne = randomDatanodeDetails();
    final DatanodeDetails datanodeTwo = randomDatanodeDetails();
    final DatanodeDetails datanodeThree = randomDatanodeDetails();
    nodeManager.register(datanodeOne, null, null);
    nodeManager.register(datanodeTwo, null, null);
    nodeManager.register(datanodeThree, null, null);
    final Set<ContainerReplica> containerReplicas = getReplicas(
        container.containerID(),
        ContainerReplicaProto.State.CLOSED,
        datanodeOne, datanodeTwo, datanodeThree);

    containerStateManager.loadContainer(container);
    containerReplicas.forEach(r -> {
      try {
        containerStateManager.updateContainerReplica(
            container.containerID(), r);
      } catch (ContainerNotFoundException ignored) {

      }
    });
    Assert.assertEquals(3, containerStateManager
        .getContainerReplicas(container.containerID()).size());
    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            ContainerReplicaProto.State.DELETED,
            datanodeThree.getUuidString());
    final IncrementalContainerReportFromDatanode icr =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    reportHandler.onMessage(icr, publisher);
    Assert.assertEquals(2, containerStateManager
        .getContainerReplicas(container.containerID()).size());
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