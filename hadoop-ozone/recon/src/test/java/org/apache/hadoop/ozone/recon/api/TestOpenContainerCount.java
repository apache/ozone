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

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.ozone.container.upgrade.UpgradeUtils.defaultLayoutVersionProto;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getRandomPipeline;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ExtendedDatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.PipelineID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto.Builder;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.recon.MetricsServiceProviderFactory;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.DatanodeMetadata;
import org.apache.hadoop.ozone.recon.api.types.DatanodesResponse;
import org.apache.hadoop.ozone.recon.common.ReconTestUtils;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconPipelineManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test for Open Container count per Datanode.
 */
public class TestOpenContainerCount {
  @TempDir
  private Path temporaryFolder;

  private NodeEndpoint nodeEndpoint;
  private ReconStorageContainerManagerFacade reconScm;
  private boolean isSetupDone = false;
  private String pipelineId;
  private String pipelineId2;
  private DatanodeDetails datanodeDetails;
  private String datanodeId;
  private ContainerReportsProto containerReportsProto;
  private Builder builder;
  private ExtendedDatanodeDetailsProto extendedDatanodeDetailsProto;
  private NodeReportProto nodeReportProto;
  private PipelineReportsProto pipelineReportsProto;
  private Pipeline pipeline;
  private Pipeline pipeline2;
  private static final String HOST1 = "host1.datanode";
  private static final String IP1 = "1.1.1.1";
  private StorageContainerServiceProvider mockScmServiceProvider;

  private List<Long> containerIDs;

  private List<ContainerWithPipeline> cpw;

  private void initializeInjector() throws Exception {
    ReconOMMetadataManager reconOMMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(Files.createDirectory(
            temporaryFolder.resolve("JunitOmDBDir")).toFile()),
        Files.createDirectory(temporaryFolder.resolve("NewDir")).toFile());
    datanodeDetails = randomDatanodeDetails();
    datanodeDetails.setHostName(HOST1);
    datanodeDetails.setIpAddress(IP1);
    pipeline = getRandomPipeline(datanodeDetails);
    pipelineId = pipeline.getId().getId().toString();

    pipeline2 = getRandomPipeline(datanodeDetails);
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

    ReconUtils reconUtilsMock = mock(ReconUtils.class);

    HttpURLConnection urlConnectionMock = mock(HttpURLConnection.class);
    when(urlConnectionMock.getResponseCode())
            .thenReturn(HttpServletResponse.SC_OK);
    when(reconUtilsMock.makeHttpCall(any(URLConnectionFactory.class),
            anyString(), anyBoolean())).thenReturn(urlConnectionMock);
    when(reconUtilsMock.getReconDbDir(any(OzoneConfiguration.class),
        anyString())).thenReturn(temporaryFolder.resolve("reconDbDir").toFile());
    when(reconUtilsMock.getReconNodeDetails(
        any(OzoneConfiguration.class))).thenReturn(
        ReconTestUtils.getReconNodeDetails());

    ReconTestInjector reconTestInjector =
            new ReconTestInjector.Builder(temporaryFolder.toFile())
                    .withReconSqlDb()
                    .withReconOm(reconOMMetadataManager)
                    .withOmServiceProvider(
                            mock(OzoneManagerServiceProviderImpl.class))
                    .addBinding(StorageContainerServiceProvider.class,
                            mockScmServiceProvider)
                    .addBinding(OzoneStorageContainerManager.class,
                            ReconStorageContainerManagerFacade.class)
                    .withContainerDB()
                    .addBinding(NodeEndpoint.class)
                    .addBinding(MetricsServiceProviderFactory.class)
                    .addBinding(ContainerHealthSchemaManager.class)
                    .addBinding(ReconUtils.class, reconUtilsMock)
                    .addBinding(StorageContainerLocationProtocol.class,
                            mockScmClient)
                    .build();

    nodeEndpoint = reconTestInjector.getInstance(NodeEndpoint.class);
    reconScm = (ReconStorageContainerManagerFacade)
            reconTestInjector.getInstance(OzoneStorageContainerManager.class);
    PipelineManager pipelineManager = reconScm.getPipelineManager();
    ReconPipelineManager reconPipelineManager = (ReconPipelineManager) pipelineManager;
    reconPipelineManager.addPipeline(pipeline);
    reconPipelineManager.addPipeline(pipeline2);
  }

  @BeforeEach
  public void setUp() throws Exception {
    // The following setup runs only once
    if (!isSetupDone) {
      initializeInjector();
      isSetupDone = true;
    }
    datanodeId = datanodeDetails.getUuid().toString();

    // initialize container report
    builder = ContainerReportsProto.newBuilder();
    for (long i = 1L; i <= 10L; i++) {
      builder.addReports(
              ContainerReplicaProto.newBuilder()
                      .setContainerID(i)
                      .setState(ContainerReplicaProto.State.OPEN)
                      .setOriginNodeId(datanodeId)
                      .build()
      );
    }
    containerReportsProto = builder.build();

    UUID pipelineUuid = UUID.fromString(pipelineId);
    HddsProtos.UUID uuid128 = HddsProtos.UUID.newBuilder()
            .setMostSigBits(pipelineUuid.getMostSignificantBits())
            .setLeastSigBits(pipelineUuid.getLeastSignificantBits())
            .build();

    UUID pipelineUuid2 = UUID.fromString(pipelineId2);
    HddsProtos.UUID uuid1282 = HddsProtos.UUID.newBuilder()
            .setMostSigBits(pipelineUuid2.getMostSignificantBits())
            .setLeastSigBits(pipelineUuid2.getLeastSignificantBits())
            .build();

    PipelineReport pipelineReport = PipelineReport.newBuilder()
            .setPipelineID(
                    PipelineID.newBuilder()
                            .setId(pipelineId)
                            .setUuid128(uuid128)
                            .build())
            .setIsLeader(true)
            .build();

    PipelineReport pipelineReport2 = PipelineReport.newBuilder()
            .setPipelineID(
                    PipelineID
                            .newBuilder()
                            .setId(pipelineId2)
                            .setUuid128(uuid1282)
                            .build())
            .setIsLeader(false)
            .build();

    pipelineReportsProto =
            PipelineReportsProto.newBuilder()
                    .addPipelineReport(pipelineReport)
                    .addPipelineReport(pipelineReport2)
                    .build();

    DatanodeDetailsProto datanodeDetailsProto =
            DatanodeDetailsProto.newBuilder()
                    .setHostName(HOST1)
                    .setUuid(datanodeId)
                    .setIpAddress(IP1)
                    .build();

    extendedDatanodeDetailsProto =
            HddsProtos.ExtendedDatanodeDetailsProto.newBuilder()
                    .setDatanodeDetails(datanodeDetailsProto)
                    .setVersion("0.6.0")
                    .setSetupTime(1596347628802L)
                    .setRevision("3346f493fa1690358add7bb9f3e5b52545993f36")
                    .build();

    StorageReportProto storageReportProto1 =
            StorageReportProto.newBuilder()
                    .setStorageType(StorageTypeProto.DISK)
                    .setStorageLocation("/disk1")
                    .setScmUsed(10 * OzoneConsts.GB)
                    .setRemaining(90 * OzoneConsts.GB)
                    .setCapacity(100 * OzoneConsts.GB)
                    .setStorageUuid(UUID.randomUUID().toString())
                    .setFailed(false).build();

    StorageReportProto storageReportProto2 =
            StorageReportProto.newBuilder()
                    .setStorageType(StorageTypeProto.DISK)
                    .setStorageLocation("/disk2")
                    .setScmUsed(10 * OzoneConsts.GB)
                    .setRemaining(90 * OzoneConsts.GB)
                    .setCapacity(100 * OzoneConsts.GB)
                    .setStorageUuid(UUID.randomUUID().toString())
                    .setFailed(false).build();

    nodeReportProto =
            NodeReportProto.newBuilder()
                    .addStorageReport(storageReportProto1)
                    .addStorageReport(storageReportProto2).build();

    assertDoesNotThrow(() -> {
      reconScm.getDatanodeProtocolServer()
          .register(extendedDatanodeDetailsProto, nodeReportProto,
              containerReportsProto, pipelineReportsProto,
              defaultLayoutVersionProto());
      // Process all events in the event queue
      reconScm.getEventQueue().processAll(1000);
    });
  }

  @Test
  public void testOpenContainerCount() throws Exception {
    // In case of pipeline doesn't exist
    waitAndCheckConditionAfterHeartbeat(() -> {

      DatanodeMetadata datanodeMetadata1 = getDatanodeMetadata();
      return datanodeMetadata1.getContainers() == 10
              && datanodeMetadata1.getPipelines().size() == 2;
    });

    DatanodeMetadata datanodeMetadata = getDatanodeMetadata();

    int expectedCnt = datanodeMetadata.getOpenContainers();

    // check if open container's count decrement according
    for (long id = 1L; id <= 10L; ++id) {
      --expectedCnt;
      closeContainer(id);
      DatanodeMetadata metadata = getDatanodeMetadata();
      assertEquals(expectedCnt, metadata.getOpenContainers());
    }
  }

  private DatanodeMetadata getDatanodeMetadata() {
    Response response = nodeEndpoint.getDatanodes();
    DatanodesResponse datanodesResponse =
            (DatanodesResponse) response.getEntity();

    DatanodeMetadata datanodeMetadata =
            datanodesResponse.getDatanodes().stream().filter(metadata ->
                    metadata.getHostname().equals("host1.datanode"))
                    .findFirst().orElse(null);
    return datanodeMetadata;
  }

  private void closeContainer(long containerID) throws IOException {

    if (containerID >= 1L && containerID <= 5L) {
      ContainerInfo closedContainer = new ContainerInfo.Builder()
              .setContainerID(containerID)
              .setReplicationConfig(
                      RatisReplicationConfig.getInstance(ReplicationFactor.ONE))
              .setState(LifeCycleState.CLOSED)
              .setOwner("test")
              .setPipelineID(pipeline.getId())
              .build();
      ContainerWithPipeline containerWithPipeline =
              new ContainerWithPipeline(closedContainer, pipeline);
      when(mockScmServiceProvider.getContainerWithPipeline(containerID))
              .thenReturn(containerWithPipeline);
      cpw.set((int) containerID - 1, containerWithPipeline);
    } else if (containerID >= 6L && containerID <= 10L) {
      ContainerInfo closedContainer = new ContainerInfo.Builder()
              .setContainerID(containerID)
              .setReplicationConfig(
                      RatisReplicationConfig.getInstance(ReplicationFactor.ONE))
              .setState(LifeCycleState.CLOSED)
              .setOwner("test")
              .setPipelineID(pipeline2.getId())
              .build();
      ContainerWithPipeline containerWithPipeline =
              new ContainerWithPipeline(closedContainer, pipeline2);
      when(mockScmServiceProvider.getContainerWithPipeline(containerID))
              .thenReturn(containerWithPipeline);
      cpw.set((int) containerID - 1, containerWithPipeline);
    }
    when(mockScmServiceProvider
            .getExistContainerWithPipelinesInBatch(containerIDs))
            .thenReturn(cpw);
    updateContainerReport(containerID);
  }

  private void updateContainerReport(long containerId) {
    containerReportsProto = builder.setReports((int) containerId - 1,
            ContainerReplicaProto.newBuilder()
                    .setContainerID(containerId)
                    .setState(ContainerReplicaProto.State.CLOSED)
                    .setOriginNodeId(datanodeId)
                    .build())
            .build();
    assertDoesNotThrow(() -> {
      reconScm.getDatanodeProtocolServer()
          .register(extendedDatanodeDetailsProto, nodeReportProto,
              containerReportsProto, pipelineReportsProto,
              defaultLayoutVersionProto());
      // Process all events in the event queue
      reconScm.getEventQueue().processAll(1000);
    });
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
                    .build();

    reconScm.getDatanodeProtocolServer().sendHeartbeat(heartbeatRequestProto);
    LambdaTestUtils.await(30000, 1000, check);
  }
}
