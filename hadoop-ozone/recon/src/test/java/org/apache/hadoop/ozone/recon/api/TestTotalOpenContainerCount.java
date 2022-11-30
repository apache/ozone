/*
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

package org.apache.hadoop.ozone.recon.api;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos
    .ExtendedDatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.PipelineID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageTypeProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.ClusterStateResponse;
import org.apache.hadoop.ozone.recon.api.types.DatanodesResponse;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.ozone.container.upgrade.UpgradeUtils.defaultLayoutVersionProto;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getRandomPipeline;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import java.net.HttpURLConnection;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test for Recon API endpoints.
 */
public class TestTotalOpenContainerCount extends AbstractReconSqlDBTest {
  private NodeEndpoint nodeEndpoint;
  private ClusterStateEndpoint clusterStateEndpoint;
  private ReconOMMetadataManager reconOMMetadataManager;
  private ReconStorageContainerManagerFacade reconScm;
  private boolean isSetupDone = false;
  private String pipelineId, pipelineId2;
  private DatanodeDetails datanodeDetails;
  private DatanodeDetails datanodeDetails2;
  private ContainerReportsProto containerReportsProto;
  private ExtendedDatanodeDetailsProto extendedDatanodeDetailsProto;
  private Pipeline pipeline, pipeline2;
  private static final String HOST1 = "host1.datanode";
  private static final String HOST2 = "host2.datanode";
  private static final String IP1 = "1.1.1.1";
  private static final String IP2 = "2.2.2.2";
  private ReconUtils reconUtilsMock;
  private ContainerHealthSchemaManager containerHealthSchemaManager;
  private List<Long> containerIDs;
  private List<ContainerWithPipeline> cpw;
  private StorageContainerServiceProvider mockScmServiceProvider;
  private ContainerReportsProto.Builder builder;

  private void initializeInjector() throws Exception {
    reconOMMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(temporaryFolder.newFolder()),
        temporaryFolder.newFolder());
    datanodeDetails = randomDatanodeDetails();
    datanodeDetails2 = randomDatanodeDetails();
    datanodeDetails.setHostName(HOST1);
    datanodeDetails.setIpAddress(IP1);
    datanodeDetails2.setHostName(HOST2);
    datanodeDetails2.setIpAddress(IP2);
    pipeline = getRandomPipeline(datanodeDetails);
    pipelineId = pipeline.getId().getId().toString();
    pipeline2 = getRandomPipeline(datanodeDetails2);
    pipelineId2 = pipeline2.getId().getId().toString();

    StorageContainerLocationProtocol mockScmClient = mock(
        StorageContainerLocationProtocol.class);
    mockScmServiceProvider = mock(
        StorageContainerServiceProviderImpl.class);

    when(mockScmServiceProvider.getPipeline(
        pipeline.getId().getProtobuf())).thenReturn(pipeline);
    when(mockScmServiceProvider.getPipeline(
        pipeline2.getId().getProtobuf())).thenReturn(pipeline2);

    // Open 5 containers on pipeline 1
    containerIDs = new LinkedList<>();
    cpw = new LinkedList<>();
    for (long i = 1L; i <= 5L; ++i) {
      ContainerInfo containerInfo = new ContainerInfo.Builder()
          .setContainerID(i)
          .setReplicationConfig(
              RatisReplicationConfig.getInstance(ReplicationFactor.ONE))
          .setState(LifeCycleState.OPEN)
          .setOwner("test")
          .setPipelineID(pipeline.getId())
          .build();
      ContainerWithPipeline containerWithPipeline =
          new ContainerWithPipeline(containerInfo, pipeline);
      when(mockScmServiceProvider.getContainerWithPipeline(i))
          .thenReturn(containerWithPipeline);
      containerIDs.add(i);
      cpw.add(containerWithPipeline);
    }

    // Open 5 containers on pipeline 2
    for (long i = 6L; i <= 10L; ++i) {
      ContainerInfo containerInfo = new ContainerInfo.Builder()
          .setContainerID(i)
          .setReplicationConfig(
              RatisReplicationConfig.getInstance(ReplicationFactor.ONE))
          .setState(LifeCycleState.OPEN)
          .setOwner("test")
          .setPipelineID(pipeline2.getId())
          .build();
      ContainerWithPipeline containerWithPipeline =
          new ContainerWithPipeline(containerInfo, pipeline2);
      when(mockScmServiceProvider.getContainerWithPipeline(i))
          .thenReturn(containerWithPipeline);
      containerIDs.add(i);
      cpw.add(containerWithPipeline);
    }

    when(mockScmServiceProvider
        .getExistContainerWithPipelinesInBatch(containerIDs))
        .thenReturn(cpw);

    reconUtilsMock = mock(ReconUtils.class);
    HttpURLConnection urlConnectionMock = mock(HttpURLConnection.class);
    when(urlConnectionMock.getResponseCode())
        .thenReturn(HttpServletResponse.SC_OK);
    when(reconUtilsMock.makeHttpCall(any(URLConnectionFactory.class),
        anyString(), anyBoolean())).thenReturn(urlConnectionMock);
    when(reconUtilsMock.getReconDbDir(any(OzoneConfiguration.class),
        anyString())).thenReturn(GenericTestUtils.getRandomizedTestDir());

    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder)
            .withReconSqlDb()
            .withReconOm(reconOMMetadataManager)
            .withOmServiceProvider(mock(OzoneManagerServiceProviderImpl.class))
            .addBinding(StorageContainerServiceProvider.class,
                mockScmServiceProvider)
            .addBinding(OzoneStorageContainerManager.class,
                ReconStorageContainerManagerFacade.class)
            .withContainerDB()
            .addBinding(ClusterStateEndpoint.class)
            .addBinding(NodeEndpoint.class)
            .addBinding(ContainerHealthSchemaManager.class)
            .addBinding(ReconUtils.class, reconUtilsMock)
            .addBinding(StorageContainerLocationProtocol.class, mockScmClient)
            .build();

    nodeEndpoint = reconTestInjector.getInstance(NodeEndpoint.class);
    GlobalStatsDao globalStatsDao = getDao(GlobalStatsDao.class);
    reconScm = (ReconStorageContainerManagerFacade)
        reconTestInjector.getInstance(OzoneStorageContainerManager.class);
    containerHealthSchemaManager =
        reconTestInjector.getInstance(ContainerHealthSchemaManager.class);
    clusterStateEndpoint =
        new ClusterStateEndpoint(reconScm, globalStatsDao,
            containerHealthSchemaManager);
  }

  @BeforeEach
  public void setUp() throws Exception {
    // The following setup runs only once
    if (!isSetupDone) {
      initializeInjector();
      isSetupDone = true;
    }
    String datanodeId = datanodeDetails.getUuid().toString();
    String datanodeId2 = datanodeDetails2.getUuid().toString();

    // initialize container report
    builder = ContainerReportsProto.newBuilder();
    for (long i = 1L; i <= 10L; i++) {
      if (i >= 1L && i < 6L) {
        builder.addReports(
            ContainerReplicaProto.newBuilder()
                .setContainerID(i)
                .setState(ContainerReplicaProto.State.OPEN)
                .setOriginNodeId(datanodeId)
                .build()
        );
      } else {
        builder.addReports(
            ContainerReplicaProto.newBuilder()
                .setContainerID(i)
                .setState(ContainerReplicaProto.State.OPEN)
                .setOriginNodeId(datanodeId2)
                .build()
        );
      }
    }
    containerReportsProto = builder.build();

    UUID pipelineUuid = UUID.fromString(pipelineId);
    HddsProtos.UUID uuid128 = HddsProtos.UUID.newBuilder()
        .setMostSigBits(pipelineUuid.getMostSignificantBits())
        .setLeastSigBits(pipelineUuid.getLeastSignificantBits())
        .build();

    PipelineReport pipelineReport = PipelineReport.newBuilder()
        .setPipelineID(
            PipelineID.newBuilder().setId(pipelineId).setUuid128(uuid128)
                .build())
        .setIsLeader(true)
        .build();
    DatanodeDetailsProto datanodeDetailsProto =
        DatanodeDetailsProto.newBuilder()
            .setHostName(HOST1)
            .setUuid(datanodeId)
            .setIpAddress(IP1)
            .build();
    extendedDatanodeDetailsProto =
        ExtendedDatanodeDetailsProto.newBuilder()
            .setDatanodeDetails(datanodeDetailsProto)
            .setVersion("0.6.0")
            .setSetupTime(1596347628802L)
            .setBuildDate("2020-08-01T08:50Z")
            .setRevision("3346f493fa1690358add7bb9f3e5b52545993f36")
            .build();
    StorageReportProto storageReportProto1 =
        StorageReportProto.newBuilder().setStorageType(StorageTypeProto.DISK)
            .setStorageLocation("/disk1").setScmUsed(10000).setRemaining(5400)
            .setCapacity(25000)
            .setStorageUuid(UUID.randomUUID().toString())
            .setFailed(false).build();
    StorageReportProto storageReportProto2 =
        StorageReportProto.newBuilder().setStorageType(StorageTypeProto.DISK)
            .setStorageLocation("/disk2").setScmUsed(25000).setRemaining(10000)
            .setCapacity(50000)
            .setStorageUuid(UUID.randomUUID().toString())
            .setFailed(false).build();
    NodeReportProto nodeReportProto =
        NodeReportProto.newBuilder()
            .addStorageReport(storageReportProto1)
            .addStorageReport(storageReportProto2).build();

    UUID pipelineUuid2 = UUID.fromString(pipelineId2);
    uuid128 = HddsProtos.UUID.newBuilder()
        .setMostSigBits(pipelineUuid2.getMostSignificantBits())
        .setLeastSigBits(pipelineUuid2.getLeastSignificantBits())
        .build();
    PipelineReport pipelineReport2 = PipelineReport.newBuilder()
        .setPipelineID(
            PipelineID.newBuilder().setId(pipelineId2).setUuid128(uuid128)
                .build()).setIsLeader(false).build();
    PipelineReportsProto pipelineReportsProto =
        PipelineReportsProto.newBuilder()
            .addPipelineReport(pipelineReport)
            .addPipelineReport(pipelineReport2)
            .build();
    DatanodeDetailsProto datanodeDetailsProto2 =
        DatanodeDetailsProto.newBuilder()
            .setHostName(HOST2)
            .setUuid(datanodeId2)
            .setIpAddress(IP2)
            .build();
    ExtendedDatanodeDetailsProto extendedDatanodeDetailsProto2 =
        ExtendedDatanodeDetailsProto.newBuilder()
            .setDatanodeDetails(datanodeDetailsProto2)
            .setVersion("0.6.0")
            .setSetupTime(1596347636802L)
            .setBuildDate("2020-08-01T08:50Z")
            .setRevision("3346f493fa1690358add7bb9f3e5b52545993f36")
            .build();
    StorageReportProto storageReportProto3 =
        StorageReportProto.newBuilder().setStorageType(StorageTypeProto.DISK)
            .setStorageLocation("/disk1").setScmUsed(20000).setRemaining(7800)
            .setCapacity(50000)
            .setStorageUuid(UUID.randomUUID().toString())
            .setFailed(false).build();
    StorageReportProto storageReportProto4 =
        StorageReportProto.newBuilder().setStorageType(StorageTypeProto.DISK)
            .setStorageLocation("/disk2").setScmUsed(60000).setRemaining(10000)
            .setCapacity(80000)
            .setStorageUuid(UUID.randomUUID().toString())
            .setFailed(false).build();
    NodeReportProto nodeReportProto2 =
        NodeReportProto.newBuilder()
            .addStorageReport(storageReportProto3)
            .addStorageReport(storageReportProto4).build();
    LayoutVersionProto layoutInfo = defaultLayoutVersionProto();

    try {
      reconScm.getDatanodeProtocolServer()
          .register(extendedDatanodeDetailsProto, nodeReportProto,
              containerReportsProto, pipelineReportsProto, layoutInfo);
      reconScm.getDatanodeProtocolServer()
          .register(extendedDatanodeDetailsProto2,
              nodeReportProto2, containerReportsProto, pipelineReportsProto,
              layoutInfo);
      // Process all events in the event queue
      reconScm.getEventQueue().processAll(1000);
    } catch (Exception ex) {
      Assertions.fail(ex.getMessage());
    }
  }

  @Test
  public void testOpenContainerCount() throws Exception {

    waitAndCheckConditionAfterHeartbeat(() -> {
      Response response1 = clusterStateEndpoint.getClusterState();
      ClusterStateResponse clusterStateResponse1 =
          (ClusterStateResponse) response1.getEntity();
      return (clusterStateResponse1.getContainers() == 10);
    });

    Response response = clusterStateEndpoint.getClusterState();
    response = nodeEndpoint.getDatanodes();
    DatanodesResponse datanodesResponse =
        (DatanodesResponse) response.getEntity();
    Assertions.assertEquals(2, datanodesResponse.getTotalCount());
    AtomicInteger expectedCount = new AtomicInteger();

    Response response1 = clusterStateEndpoint.getClusterState();
    ClusterStateResponse clusterStateResponse1 =
        (ClusterStateResponse) response1.getEntity();
    // Get the total count of Open containers across all DataNodes
    datanodesResponse.getDatanodes().forEach(datanodeMetadata -> {
      expectedCount.set(
          expectedCount.get() +
              datanodeMetadata.getOpenContainers());
    });

    Assertions.assertEquals(expectedCount.intValue(),
        clusterStateResponse1.getOpenContainers());
  }

  private void waitAndCheckConditionAfterHeartbeat(Callable<Boolean> check)
      throws Exception {
    // if container report is processed first, and pipeline does not exist
    // then container is not added until the next container report is processed
    SCMHeartbeatRequestProto heartbeatRequestProto =
        SCMHeartbeatRequestProto.newBuilder()
            .setContainerReport(containerReportsProto)
            .setDatanodeDetails(extendedDatanodeDetailsProto
                .getDatanodeDetails())
            .setDataNodeLayoutVersion(defaultLayoutVersionProto())
            .build();
    reconScm.getDatanodeProtocolServer().sendHeartbeat(heartbeatRequestProto);
    LambdaTestUtils.await(30000, 1000, check);
  }
}
