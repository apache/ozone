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
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.recon.AbstractOMMetadataManagerTest.getRandomPipeline;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.IncrementalContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test Recon ICR handler.
 */
public class TestReconIncrementalContainerReportHandler {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private OzoneConfiguration conf;
  private SCMStorageConfig scmStorageConfig;
  private ReconPipelineManager pipelineManager;
  private ReconContainerManager containerManager;

  @Before
  public void setUp() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS,
        temporaryFolder.newFolder().getAbsolutePath());
    conf.set(OZONE_SCM_NAMES, "localhost");
    scmStorageConfig = new ReconStorageConfig(conf);
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    EventQueue eventQueue = new EventQueue();
    NodeManager nodeManager =
        new SCMNodeManager(conf, scmStorageConfig, eventQueue, clusterMap);
    pipelineManager = new ReconPipelineManager(conf, nodeManager, eventQueue);
    containerManager = new ReconContainerManager(conf, pipelineManager);
  }

  @After
  public void tearDown() throws IOException {
    containerManager.close();
    pipelineManager.close();
  }

  @Test
  public void testProcessICR() throws IOException, NodeNotFoundException {

    Pipeline pipeline = getRandomPipeline();
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