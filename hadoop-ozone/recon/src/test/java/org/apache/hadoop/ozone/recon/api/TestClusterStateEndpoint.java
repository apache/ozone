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

package org.apache.hadoop.ozone.recon.api;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getRandomPipeline;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.ClusterStateResponse;
import org.apache.hadoop.ozone.recon.api.types.ClusterStorageReport;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManagerV2;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.scm.ReconPipelineManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.ReconGlobalStatsManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit test for ClusterStateEndpoint ContainerStateCounts.
 */
public class TestClusterStateEndpoint extends AbstractReconSqlDBTest {

  private static final long KEY_COUNT = 5L;

  @TempDir
  private Path temporaryFolder;
  private ClusterStateEndpoint clusterStateEndpoint;
  private ReconContainerManager reconContainerManager;
  private Pipeline pipeline;
  private PipelineID pipelineID;
  private OzoneConfiguration conf;
  private int count = 0;
  private static final int NUM_OPEN_CONTAINERS = 3;
  private static final int NUM_DELETED_CONTAINERS = 4;
  private static final int NUM_CLOSED_CONTAINERS = 3;

  public TestClusterStateEndpoint() {
    super();
  }

  @BeforeEach
  public void setUp() throws Exception {
    ReconOMMetadataManager reconOMMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(Files.createDirectory(
            temporaryFolder.resolve("JunitOmDBDir")).toFile()),
        Files.createDirectory(temporaryFolder.resolve("NewDir")).toFile());
    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder.toFile())
            .withReconSqlDb()
            .withReconOm(reconOMMetadataManager)
            .withOmServiceProvider(mock(OzoneManagerServiceProviderImpl.class))
            // No longer using mock reconSCM as we need nodeDB in Facade
            //  to establish datanode UUID to hostname mapping
            .addBinding(OzoneStorageContainerManager.class,
                ReconStorageContainerManagerFacade.class)
            .withContainerDB()
            .addBinding(StorageContainerServiceProvider.class,
                mock(StorageContainerServiceProviderImpl.class))
            .addBinding(ClusterStateEndpoint.class)
            .addBinding(ContainerHealthSchemaManagerV2.class)
            .build();
    OzoneStorageContainerManager ozoneStorageContainerManager =
        reconTestInjector.getInstance(OzoneStorageContainerManager.class);
    reconContainerManager = (ReconContainerManager)
        ozoneStorageContainerManager.getContainerManager();
    ReconPipelineManager reconPipelineManager = (ReconPipelineManager)
                                                    ozoneStorageContainerManager.getPipelineManager();
    ContainerHealthSchemaManagerV2 containerHealthSchemaManagerV2 =
        reconTestInjector.getInstance(ContainerHealthSchemaManagerV2.class);
    ReconGlobalStatsManager reconGlobalStatsManager = 
        reconTestInjector.getInstance(ReconGlobalStatsManager.class);
    conf = mock(OzoneConfiguration.class);
    clusterStateEndpoint =
        new ClusterStateEndpoint(ozoneStorageContainerManager,
            reconGlobalStatsManager, containerHealthSchemaManagerV2, conf);
    pipeline = getRandomPipeline();
    pipelineID = pipeline.getId();
    reconPipelineManager.addPipeline(pipeline);
  }

  @Test
  public void testGetContainerCounts() throws Exception {
    putContainerInfos(NUM_OPEN_CONTAINERS,
        HddsProtos.LifeCycleState.OPEN);
    putContainerInfos(NUM_DELETED_CONTAINERS,
        HddsProtos.LifeCycleState.DELETED);
    putContainerInfos(NUM_CLOSED_CONTAINERS,
        HddsProtos.LifeCycleState.CLOSED);

    // Get the cluster state using the ClusterStateEndpoint
    Response response1 = clusterStateEndpoint.getClusterState();
    ClusterStateResponse clusterStateResponse1 =
        (ClusterStateResponse) response1.getEntity();

    // Calculate expected counts
    int expectedTotalContainers = NUM_OPEN_CONTAINERS + NUM_CLOSED_CONTAINERS;
    int expectedOpenContainers = NUM_OPEN_CONTAINERS;
    int expectedDeletedContainers = NUM_DELETED_CONTAINERS;

    // Verify counts using assertions
    assertEquals(expectedTotalContainers, clusterStateResponse1.getContainers());
    assertEquals(expectedOpenContainers, clusterStateResponse1.getOpenContainers());
    assertEquals(expectedDeletedContainers, clusterStateResponse1.getDeletedContainers());
  }

  @Test
  public void testScmAndOmServiceId() {
    // given
    when(conf.get(eq(OZONE_SCM_SERVICE_IDS_KEY))).thenReturn("scmServiceId");
    when(conf.get(eq(OZONE_OM_SERVICE_IDS_KEY))).thenReturn("omServiceId");

    // when
    Response clusterState = clusterStateEndpoint.getClusterState();

    // then
    ClusterStateResponse clusterStateResponse = (ClusterStateResponse) clusterState.getEntity();
    assertEquals("scmServiceId", clusterStateResponse.getScmServiceId());
    assertEquals("omServiceId", clusterStateResponse.getOmServiceId());
  }

  @Test
  public void testStorageReportIsClusterStorageReport() {
    OzoneStorageContainerManager mockScm = mock(OzoneStorageContainerManager.class);
    ReconNodeManager mockNodeManager = mock(ReconNodeManager.class);
    ReconPipelineManager mockPipelineManager = mock(ReconPipelineManager.class);
    ReconContainerManager mockContainerManager = mock(ReconContainerManager.class);
    ContainerHealthSchemaManagerV2 mockContainerHealthSchemaManagerV2 =
        mock(ContainerHealthSchemaManagerV2.class);
    ReconGlobalStatsManager mockGlobalStatsManager =
        mock(ReconGlobalStatsManager.class);
    OzoneConfiguration mockConf = mock(OzoneConfiguration.class);
    DatanodeInfo mockDatanode = mock(DatanodeInfo.class);

    when(mockScm.getScmNodeManager()).thenReturn(mockNodeManager);
    when(mockScm.getPipelineManager()).thenReturn(mockPipelineManager);
    when(mockScm.getContainerManager()).thenReturn(mockContainerManager);

    when(mockPipelineManager.getPipelines()).thenReturn(Collections.emptyList());
    when(mockContainerManager.getContainers()).thenReturn(Collections.emptyList());
    when(mockContainerManager.getContainerStateCount(HddsProtos.LifeCycleState.OPEN))
        .thenReturn(0);
    when(mockContainerManager.getContainerStateCount(HddsProtos.LifeCycleState.DELETED))
        .thenReturn(0);
    when(mockContainerHealthSchemaManagerV2.getUnhealthyContainers(
        any(), anyLong(), anyLong(), anyInt())).thenReturn(Collections.emptyList());

    SCMNodeStat scmNodeStat = new SCMNodeStat(
        1000L, 400L, 600L, 300L, 50L, 20L);
    when(mockNodeManager.getStats()).thenReturn(scmNodeStat);
    when(mockNodeManager.getNodeCount(NodeStatus.inServiceHealthy())).thenReturn(1);
    when(mockNodeManager.getNodeCount(NodeStatus.inServiceHealthyReadOnly()))
        .thenReturn(0);
    when(mockNodeManager.getAllNodeCount()).thenReturn(1);
    when(mockNodeManager.getAllNodes()).thenReturn(Collections.singletonList(mockDatanode));
    when(mockNodeManager.getTotalFilesystemUsage(mockDatanode))
        .thenReturn(new SpaceUsageSource.Fixed(2000L, 1500L, 500L));

    ClusterStateEndpoint endpoint = new ClusterStateEndpoint(
        mockScm, mockGlobalStatsManager, mockContainerHealthSchemaManagerV2, mockConf);

    ClusterStateResponse response =
        (ClusterStateResponse) endpoint.getClusterState().getEntity();

    assertNotNull(response.getStorageReport());
    assertInstanceOf(ClusterStorageReport.class, response.getStorageReport());

    ClusterStorageReport report = response.getStorageReport();
    assertEquals(1000L, report.getCapacity());
    assertEquals(400L, report.getUsed());
    assertEquals(600L, report.getRemaining());
    assertEquals(300L, report.getCommitted());
    assertEquals(50L, report.getMinimumFreeSpace());
    assertEquals(20L, report.getReserved());
    assertEquals(2000L, report.getFilesystemCapacity());
    assertEquals(500L, report.getFilesystemUsed());
    assertEquals(1500L, report.getFilesystemAvailable());
  }

  ContainerInfo newContainerInfo(long containerId,
                                 HddsProtos.LifeCycleState state) {
    return new ContainerInfo.Builder()
        .setContainerID(containerId)
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.THREE))
        .setState(state)
        .setOwner("owner1")
        .setNumberOfKeys(KEY_COUNT)
        .setPipelineID(pipelineID)
        .build();
  }

  void putContainerInfos(int num, HddsProtos.LifeCycleState state)
      throws IOException, TimeoutException {
    for (int i = 1; i <= num; i++) {
      final ContainerInfo info = newContainerInfo(count + i, state);
      reconContainerManager.addNewContainer(
          new ContainerWithPipeline(info, pipeline));
    }
    count += num;
  }

}
