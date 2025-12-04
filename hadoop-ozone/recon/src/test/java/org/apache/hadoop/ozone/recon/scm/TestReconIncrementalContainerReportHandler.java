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

package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.OPEN;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.IncrementalContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.recon.TestReconUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test Recon ICR handler.
 */
public class TestReconIncrementalContainerReportHandler
    extends AbstractReconContainerManagerTest {

  @Test
  public void testProcessICR(@TempDir Path scmPath)
      throws IOException, NodeNotFoundException, TimeoutException {

    ContainerID containerID = ContainerID.valueOf(100L);
    DatanodeDetails datanodeDetails = randomDatanodeDetails();
    IncrementalContainerReportFromDatanode reportMock =
        mock(IncrementalContainerReportFromDatanode.class);
    when(reportMock.getDatanodeDetails()).thenReturn(datanodeDetails);

    ContainerWithPipeline containerWithPipeline = getTestContainer(
        containerID.getId(), OPEN);
    List<ContainerWithPipeline> containerWithPipelineList = new ArrayList<>();
    containerWithPipelineList.add(containerWithPipeline);
    ReconContainerManager containerManager = getContainerManager();
    IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(containerID,
            State.OPEN,
            datanodeDetails.getUuidString());
    when(reportMock.getReport()).thenReturn(containerReport);
    when(getContainerManager().getScmClient()
        .getExistContainerWithPipelinesInBatch(any(
            ArrayList.class))).thenReturn(containerWithPipelineList);

    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, scmPath.toString());
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    EventQueue eventQueue = new EventQueue();
    SCMStorageConfig storageConfig = new SCMStorageConfig(conf);
    HDDSLayoutVersionManager versionManager = mock(HDDSLayoutVersionManager.class);
    when(versionManager.getMetadataLayoutVersion())
        .thenReturn(maxLayoutVersion());
    when(versionManager.getSoftwareLayoutVersion())
        .thenReturn(maxLayoutVersion());

    NodeManager nodeManager = new SCMNodeManager(conf, storageConfig,
        eventQueue, clusterMap, SCMContext.emptyContext(), versionManager);

    nodeManager.register(datanodeDetails, null, null);

    ReconIncrementalContainerReportHandler reconIcr =
        new ReconIncrementalContainerReportHandler(nodeManager,
            containerManager, SCMContext.emptyContext());
    EventPublisher eventPublisherMock = mock(EventPublisher.class);

    reconIcr.onMessage(reportMock, eventPublisherMock);
    nodeManager.addContainer(datanodeDetails, containerID);
    assertTrue(containerManager.containerExist(containerID));
    assertEquals(1, containerManager.getContainerReplicas(containerID).size());
    assertEquals(OPEN, containerManager.getContainer(containerID).getState());
  }

  @Test
  public void testProcessICRStateMismatch()
      throws IOException, TimeoutException {

    // Recon container state is "OPEN".
    // Replica state could be any Non OPEN state.
    long containerId = 11;
    for (State state : Arrays.asList(State.CLOSING, State.QUASI_CLOSED,
        State.CLOSED)) {
      ContainerWithPipeline containerWithPipeline = getTestContainer(
          containerId++, OPEN);
      ContainerID containerID =
          containerWithPipeline.getContainerInfo().containerID();

      ReconContainerManager containerManager = getContainerManager();
      containerManager.addNewContainer(containerWithPipeline);

      DatanodeDetails datanodeDetails =
          containerWithPipeline.getPipeline().getFirstNode();
      NodeManager nodeManagerMock = mock(NodeManager.class);
      when(nodeManagerMock.getNode(any(DatanodeID.class)))
          .thenReturn(datanodeDetails);
      IncrementalContainerReportFromDatanode reportMock =
          mock(IncrementalContainerReportFromDatanode.class);
      when(reportMock.getDatanodeDetails())
          .thenReturn(containerWithPipeline.getPipeline().getFirstNode());

      IncrementalContainerReportProto containerReport =
          getIncrementalContainerReportProto(containerID, state,
              datanodeDetails.getUuidString());
      when(reportMock.getReport()).thenReturn(containerReport);
      ReconIncrementalContainerReportHandler reconIcr =
          new ReconIncrementalContainerReportHandler(nodeManagerMock,
              containerManager, SCMContext.emptyContext());

      reconIcr.onMessage(reportMock, mock(EventPublisher.class));
      assertTrue(containerManager.containerExist(containerID));
      assertEquals(1,
          containerManager.getContainerReplicas(containerID).size());
      LifeCycleState expectedState = getContainerStateFromReplicaState(state);
      LifeCycleState actualState =
          containerManager.getContainer(containerID).getState();
      assertEquals(expectedState, actualState,
          String.format("Expecting %s in container state for replica state %s",
              expectedState, state));
    }
  }

  @Test
  public void testMergeMultipleICRs() {
    final ContainerInfo container = TestReconUtils.getContainer(LifeCycleState.OPEN);
    final DatanodeDetails datanodeOne = randomDatanodeDetails();
    final IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(container.containerID(),
            ContainerReplicaProto.State.CLOSED,
            datanodeOne.getUuidString());
    final IncrementalContainerReportFromDatanode icrFromDatanode1 =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    final IncrementalContainerReportFromDatanode icrFromDatanode2 =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    assertEquals(1, icrFromDatanode1.getReport().getReportList().size());
    icrFromDatanode1.mergeReport(icrFromDatanode2);
    assertEquals(2, icrFromDatanode1.getReport().getReportList().size());

    final IncrementalContainerReportFromDatanode icrFromDatanode3 =
        new IncrementalContainerReportFromDatanode(
            datanodeOne, containerReport);
    icrFromDatanode1.mergeReport(icrFromDatanode3);
    assertEquals(3, icrFromDatanode1.getReport().getReportList().size());
  }

  private LifeCycleState getContainerStateFromReplicaState(
      State state) {
    switch (state) {
    case CLOSING: return LifeCycleState.CLOSING;
    case QUASI_CLOSED: return LifeCycleState.QUASI_CLOSED;
    case CLOSED: return LifeCycleState.CLOSED;
    default: return null;
    }
  }

  private static IncrementalContainerReportProto
      getIncrementalContainerReportProto(final ContainerID containerId,
                                         final State state,
                                         final String originNodeId) {
    final IncrementalContainerReportProto.Builder crBuilder =
        IncrementalContainerReportProto.newBuilder();
    final ContainerReplicaProto replicaProto =
        ContainerReplicaProto.newBuilder()
            .setContainerID(containerId.getId())
            .setState(state)
            .setOriginNodeId(originNodeId)
            .build();
    return crBuilder.addReport(replicaProto).build();
  }
}
