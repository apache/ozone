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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.PipelineID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
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
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.ClusterStateResponse;
import org.apache.hadoop.ozone.recon.api.types.DatanodeMetadata;
import org.apache.hadoop.ozone.recon.api.types.DatanodesResponse;
import org.apache.hadoop.ozone.recon.api.types.PipelineMetadata;
import org.apache.hadoop.ozone.recon.api.types.PipelinesResponse;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.persistence.ContainerSchemaManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getRandomPipeline;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDataToOm;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Test for Recon API endpoints.
 */
public class TestEndpoints extends AbstractReconSqlDBTest {
  private NodeEndpoint nodeEndpoint;
  private PipelineEndpoint pipelineEndpoint;
  private ClusterStateEndpoint clusterStateEndpoint;
  private ReconOMMetadataManager reconOMMetadataManager;
  private ReconStorageContainerManagerFacade reconScm;
  private boolean isSetupDone = false;
  private String pipelineId;
  private DatanodeDetails datanodeDetails;
  private DatanodeDetails datanodeDetails2;
  private long containerId = 1L;
  private ContainerReportsProto containerReportsProto;
  private DatanodeDetailsProto datanodeDetailsProto;
  private Pipeline pipeline;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private void initializeInjector() throws IOException {
    reconOMMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(temporaryFolder.newFolder()),
        temporaryFolder.newFolder());
    datanodeDetails = randomDatanodeDetails();
    datanodeDetails2 = randomDatanodeDetails();
    pipeline = getRandomPipeline(datanodeDetails);
    pipelineId = pipeline.getId().getId().toString();

    ContainerInfo containerInfo = new ContainerInfo.Builder()
        .setContainerID(containerId)
        .setReplicationFactor(ReplicationFactor.ONE)
        .setState(LifeCycleState.OPEN)
        .setOwner("test")
        .setPipelineID(pipeline.getId())
        .setReplicationType(ReplicationType.RATIS)
        .build();
    ContainerWithPipeline containerWithPipeline =
        new ContainerWithPipeline(containerInfo, pipeline);

    StorageContainerLocationProtocol mockScmClient = mock(
        StorageContainerLocationProtocol.class);
    StorageContainerServiceProvider mockScmServiceProvider = mock(
        StorageContainerServiceProviderImpl.class);
    when(mockScmServiceProvider.getPipeline(
        pipeline.getId().getProtobuf())).thenReturn(pipeline);
    when(mockScmServiceProvider.getContainerWithPipeline(containerId))
        .thenReturn(containerWithPipeline);

    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder)
            .withReconSqlDb()
            .withReconOm(reconOMMetadataManager)
            .withOmServiceProvider(mock(OzoneManagerServiceProviderImpl.class))
            .addBinding(StorageContainerServiceProvider.class,
                mockScmServiceProvider)
            .addBinding(OzoneStorageContainerManager.class,
                ReconStorageContainerManagerFacade.class)
            .addBinding(ClusterStateEndpoint.class)
            .addBinding(NodeEndpoint.class)
            .addBinding(ContainerSchemaManager.class)
            .addBinding(StorageContainerLocationProtocol.class, mockScmClient)
            .build();

    nodeEndpoint = reconTestInjector.getInstance(NodeEndpoint.class);
    pipelineEndpoint = reconTestInjector.getInstance(PipelineEndpoint.class);
    clusterStateEndpoint =
        reconTestInjector.getInstance(ClusterStateEndpoint.class);
    reconScm = (ReconStorageContainerManagerFacade)
        reconTestInjector.getInstance(OzoneStorageContainerManager.class);
  }

  @Before
  public void setUp() throws Exception {
    // The following setup runs only once
    if (!isSetupDone) {
      initializeInjector();
      isSetupDone = true;
    }
    String datanodeId = datanodeDetails.getUuid().toString();
    String datanodeId2 = datanodeDetails2.getUuid().toString();
    containerReportsProto =
        ContainerReportsProto.newBuilder()
            .addReports(
                ContainerReplicaProto.newBuilder()
                    .setContainerID(containerId)
                    .setState(ContainerReplicaProto.State.OPEN)
                    .setOriginNodeId(datanodeId)
                    .build())
            .build();

    PipelineReport pipelineReport = PipelineReport.newBuilder()
        .setPipelineID(
            PipelineID.newBuilder().setId(pipelineId).build())
        .setIsLeader(true)
        .build();
    PipelineReportsProto pipelineReportsProto =
        PipelineReportsProto.newBuilder()
            .addPipelineReport(pipelineReport).build();
    datanodeDetailsProto =
        DatanodeDetailsProto.newBuilder()
            .setHostName("host1.datanode")
            .setUuid(datanodeId)
            .setIpAddress("1.1.1.1")
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

    DatanodeDetailsProto datanodeDetailsProto2 =
        DatanodeDetailsProto.newBuilder()
        .setHostName("host2.datanode")
        .setUuid(datanodeId2)
        .setIpAddress("2.2.2.2")
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

    try {
      reconScm.getDatanodeProtocolServer()
          .register(datanodeDetailsProto, nodeReportProto,
              containerReportsProto, pipelineReportsProto);
      reconScm.getDatanodeProtocolServer()
          .register(datanodeDetailsProto2, nodeReportProto2,
              ContainerReportsProto.newBuilder().build(),
              PipelineReportsProto.newBuilder().build());
      // Process all events in the event queue
      reconScm.getEventQueue().processAll(1000);
    } catch (Exception ex) {
      Assert.fail(ex.getMessage());
    }

    // Write Data to OM

    // A sample volume (sampleVol) and a bucket (bucketOne) is already created
    // in AbstractOMMetadataManagerTest.
    // Create a new volume and bucket and then write keys to the bucket.
    String volumeKey = reconOMMetadataManager.getVolumeKey("sampleVol2");
    OmVolumeArgs args =
        OmVolumeArgs.newBuilder()
            .setVolume("sampleVol2")
            .setAdminName("TestUser")
            .setOwnerName("TestUser")
            .build();
    reconOMMetadataManager.getVolumeTable().put(volumeKey, args);

    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("sampleVol2")
        .setBucketName("bucketOne")
        .build();

    String bucketKey = reconOMMetadataManager.getBucketKey(
        bucketInfo.getVolumeName(), bucketInfo.getBucketName());

    reconOMMetadataManager.getBucketTable().put(bucketKey, bucketInfo);

    // key = key_one
    writeDataToOm(reconOMMetadataManager, "key_one");

    // key = key_two
    writeDataToOm(reconOMMetadataManager, "key_two");

    // key = key_three
    writeDataToOm(reconOMMetadataManager, "key_three");
  }

  private void testDatanodeResponse(DatanodeMetadata datanodeMetadata) {
    String hostname = datanodeMetadata.getHostname();
    switch (hostname) {
    case "host1.datanode":
      Assert.assertEquals(75000,
          datanodeMetadata.getDatanodeStorageReport().getCapacity());
      Assert.assertEquals(15400,
          datanodeMetadata.getDatanodeStorageReport().getRemaining());
      Assert.assertEquals(35000,
          datanodeMetadata.getDatanodeStorageReport().getUsed());

      Assert.assertEquals(1, datanodeMetadata.getPipelines().size());
      Assert.assertEquals(pipelineId,
          datanodeMetadata.getPipelines().get(0).getPipelineID().toString());
      Assert.assertEquals(pipeline.getFactor().getNumber(),
          datanodeMetadata.getPipelines().get(0).getReplicationFactor());
      Assert.assertEquals(pipeline.getType().toString(),
          datanodeMetadata.getPipelines().get(0).getReplicationType());
      break;
    case "host2.datanode":
      Assert.assertEquals(130000,
          datanodeMetadata.getDatanodeStorageReport().getCapacity());
      Assert.assertEquals(17800,
          datanodeMetadata.getDatanodeStorageReport().getRemaining());
      Assert.assertEquals(80000,
          datanodeMetadata.getDatanodeStorageReport().getUsed());

      Assert.assertEquals(0, datanodeMetadata.getPipelines().size());
      break;
    default:
      Assert.fail(String.format("Datanode %s not registered",
          hostname));
    }
  }

  @Test
  public void testGetDatanodes() throws Exception {
    Response response = nodeEndpoint.getDatanodes();
    DatanodesResponse datanodesResponse =
        (DatanodesResponse) response.getEntity();
    Assert.assertEquals(2, datanodesResponse.getTotalCount());
    Assert.assertEquals(2, datanodesResponse.getDatanodes().size());

    datanodesResponse.getDatanodes().forEach(this::testDatanodeResponse);

    waitAndCheckConditionAfterHeartbeat(() -> {
      Response response1 = nodeEndpoint.getDatanodes();
      DatanodesResponse datanodesResponse1 =
          (DatanodesResponse) response1.getEntity();
      DatanodeMetadata datanodeMetadata1 =
          datanodesResponse1.getDatanodes().stream().filter(datanodeMetadata ->
              datanodeMetadata.getHostname().equals("host1.datanode"))
              .findFirst().orElse(null);
      return (datanodeMetadata1 != null &&
          datanodeMetadata1.getContainers() == 1);
    });
    Assert.assertEquals(1,
        reconScm.getPipelineManager()
            .getContainersInPipeline(pipeline.getId()).size());
  }

  @Test
  public void testGetPipelines() throws Exception {
    Response response = pipelineEndpoint.getPipelines();
    PipelinesResponse pipelinesResponse =
        (PipelinesResponse) response.getEntity();
    Assert.assertEquals(1, pipelinesResponse.getTotalCount());
    Assert.assertEquals(1, pipelinesResponse.getPipelines().size());
    PipelineMetadata pipelineMetadata =
        pipelinesResponse.getPipelines().iterator().next();
    Assert.assertEquals(1, pipelineMetadata.getDatanodes().size());
    Assert.assertEquals(pipeline.getType().toString(),
        pipelineMetadata.getReplicationType());
    Assert.assertEquals(pipeline.getFactor().getNumber(),
        pipelineMetadata.getReplicationFactor());
    Assert.assertEquals(datanodeDetails.getHostName(),
        pipelineMetadata.getLeaderNode());
    Assert.assertEquals(pipeline.getId().getId(),
        pipelineMetadata.getPipelineId());

    waitAndCheckConditionAfterHeartbeat(() -> {
      Response response1 = pipelineEndpoint.getPipelines();
      PipelinesResponse pipelinesResponse1 =
          (PipelinesResponse) response1.getEntity();
      PipelineMetadata pipelineMetadata1 =
          pipelinesResponse1.getPipelines().iterator().next();
      return (pipelineMetadata1.getContainers() == 1);
    });
  }

  @Test
  public void testGetClusterState() throws Exception {
    Response response = clusterStateEndpoint.getClusterState();
    ClusterStateResponse clusterStateResponse =
        (ClusterStateResponse) response.getEntity();

    Assert.assertEquals(1, clusterStateResponse.getPipelines());
    Assert.assertEquals(2, clusterStateResponse.getVolumes());
    Assert.assertEquals(2, clusterStateResponse.getBuckets());
    Assert.assertEquals(3, clusterStateResponse.getKeys());
    Assert.assertEquals(2, clusterStateResponse.getTotalDatanodes());
    Assert.assertEquals(2, clusterStateResponse.getHealthyDatanodes());

    waitAndCheckConditionAfterHeartbeat(() -> {
      Response response1 = clusterStateEndpoint.getClusterState();
      ClusterStateResponse clusterStateResponse1 =
          (ClusterStateResponse) response1.getEntity();
      return (clusterStateResponse1.getContainers() == 1);
    });
  }

  private void waitAndCheckConditionAfterHeartbeat(Callable<Boolean> check)
      throws Exception {
    // if container report is processed first, and pipeline does not exist
    // then container is not added until the next container report is processed
    SCMHeartbeatRequestProto heartbeatRequestProto =
        SCMHeartbeatRequestProto.newBuilder()
            .setContainerReport(containerReportsProto)
            .setDatanodeDetails(datanodeDetailsProto)
            .build();
    reconScm.getDatanodeProtocolServer().sendHeartbeat(heartbeatRequestProto);
    LambdaTestUtils.await(30000, 1000, check);
  }
}
