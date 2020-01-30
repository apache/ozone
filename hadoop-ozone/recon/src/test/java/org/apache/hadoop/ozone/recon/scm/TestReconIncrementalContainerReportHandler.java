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

package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.OPEN;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.STAND_ALONE;
import static org.apache.hadoop.ozone.recon.AbstractOMMetadataManagerTest.getRandomPipeline;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.IncrementalContainerReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.junit.Test;

/**
 * Test Recon ICR handler.
 */
public class TestReconIncrementalContainerReportHandler
    extends AbstractReconContainerManagerTest {

  @Test
  public void testProcessICR() throws IOException, NodeNotFoundException {

    Pipeline pipeline = getRandomPipeline();
    ReconPipelineManager pipelineManager = getPipelineManager();
    pipelineManager.addPipeline(pipeline);

    ContainerID containerID = new ContainerID(100L);
    ContainerInfo containerInfo =
        new ContainerInfo.Builder()
            .setContainerID(containerID.getId())
            .setNumberOfKeys(10)
            .setPipelineID(pipeline.getId())
            .setReplicationFactor(ONE)
            .setOwner("test")
            .setState(OPEN)
            .setReplicationType(STAND_ALONE)
            .build();
    ContainerWithPipeline containerWithPipeline =
        new ContainerWithPipeline(containerInfo, pipeline);

    StorageContainerServiceProvider scmServiceProviderMock = mock(
        StorageContainerServiceProvider.class);
    when(scmServiceProviderMock.getContainerWithPipeline(100L))
        .thenReturn(containerWithPipeline);

    DatanodeDetails datanodeDetails = randomDatanodeDetails();
    IncrementalContainerReportFromDatanode reportMock =
        mock(IncrementalContainerReportFromDatanode.class);
    when(reportMock.getDatanodeDetails()).thenReturn(datanodeDetails);
    IncrementalContainerReportProto containerReport =
        getIncrementalContainerReportProto(containerID,
            State.OPEN,
            datanodeDetails.getUuidString());
    when(reportMock.getReport()).thenReturn(containerReport);

    NodeManager nodeManagerMock = mock(NodeManager.class);

    ReconContainerManager containerManager = getContainerManager();
    ReconIncrementalContainerReportHandler recconIcr =
        new ReconIncrementalContainerReportHandler(nodeManagerMock,
            containerManager, scmServiceProviderMock);
    EventPublisher eventPublisherMock = mock(EventPublisher.class);

    recconIcr.onMessage(reportMock, eventPublisherMock);
    verify(nodeManagerMock, times(1))
        .addContainer(datanodeDetails, containerID);
    assertTrue(containerManager.exists(containerID));
    assertEquals(1, containerManager.getContainerReplicas(containerID).size());
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