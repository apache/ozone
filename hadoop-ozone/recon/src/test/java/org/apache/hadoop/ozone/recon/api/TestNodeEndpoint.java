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

package org.apache.hadoop.ozone.recon.api;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.PipelineID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageTypeProto;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
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
import org.apache.hadoop.ozone.recon.GuiceInjectorUtilsForTestsImpl;
import org.apache.hadoop.ozone.recon.api.types.DatanodeMetadata;
import org.apache.hadoop.ozone.recon.api.types.DatanodesResponse;
import org.apache.hadoop.ozone.recon.persistence.AbstractSqlDatabaseTest;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.hadoop.test.LambdaTestUtils;
import org.hadoop.ozone.recon.schema.ReconTaskSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.MissingContainersDao;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.jooq.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.ozone.recon.AbstractOMMetadataManagerTest.getRandomPipeline;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.ws.rs.core.Response;
import java.util.UUID;

/**
 * Test for Node Endpoint.
 */
public class TestNodeEndpoint extends AbstractSqlDatabaseTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private NodeEndpoint nodeEndpoint;
  private ReconStorageContainerManagerFacade reconScm;
  private boolean isSetupDone = false;
  private String pipelineId;
  private DatanodeDetails datanodeDetails;
  private GuiceInjectorUtilsForTestsImpl guiceInjectorTest =
      new GuiceInjectorUtilsForTestsImpl();
  private long containerId = 1L;
  private ContainerReportsProto containerReportsProto;
  private DatanodeDetailsProto datanodeDetailsProto;
  private Pipeline pipeline;
  private void initializeInjector() {

    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        try {
          datanodeDetails = randomDatanodeDetails();
          pipeline = getRandomPipeline(datanodeDetails);
          pipelineId = pipeline.getId().getId().toString();

          Configuration sqlConfiguration =
              getInjector().getInstance((Configuration.class));

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

          ReconTaskSchemaDefinition taskSchemaDefinition = getInjector()
              .getInstance(ReconTaskSchemaDefinition.class);
          taskSchemaDefinition.initializeSchema();

          ReconTaskStatusDao reconTaskStatusDao =
              new ReconTaskStatusDao(sqlConfiguration);
          MissingContainersDao missingContainersDao =
              new MissingContainersDao(sqlConfiguration);

          bind(ReconTaskStatusDao.class).toInstance(reconTaskStatusDao);
          bind(MissingContainersDao.class).toInstance(missingContainersDao);

          StorageContainerLocationProtocol mockScmClient = mock(
              StorageContainerLocationProtocol.class);
          StorageContainerServiceProvider mockScmServiceProvider = mock(
              StorageContainerServiceProviderImpl.class);
          when(mockScmServiceProvider.getPipeline(
              pipeline.getId().getProtobuf())).thenReturn(pipeline);
          when(mockScmServiceProvider.getContainerWithPipeline(containerId))
              .thenReturn(containerWithPipeline);

          OzoneConfiguration testOzoneConfiguration =
              guiceInjectorTest.getTestOzoneConfiguration(temporaryFolder);
          testOzoneConfiguration.set(OZONE_RECON_DATANODE_ADDRESS_KEY,
              "0.0.0.0:0");
          bind(OzoneConfiguration.class).toInstance(testOzoneConfiguration);
          bind(StorageContainerLocationProtocol.class)
              .toInstance(mockScmClient);
          bind(StorageContainerServiceProvider.class)
              .toInstance(mockScmServiceProvider);
          bind(OzoneStorageContainerManager.class)
              .to(ReconStorageContainerManagerFacade.class).in(Singleton.class);
          bind(NodeEndpoint.class);
        } catch (Exception e) {
          Assert.fail(e.getMessage());
        }
      }
    });

    nodeEndpoint = injector.getInstance(NodeEndpoint.class);
    reconScm = (ReconStorageContainerManagerFacade)
        injector.getInstance(OzoneStorageContainerManager.class);
  }

  @Before
  public void setUp() {
    // The following setup runs only once
    if (!isSetupDone) {
      initializeInjector();
      isSetupDone = true;
    }
    String datanodeId = datanodeDetails.getUuid().toString();
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

    try {
      reconScm.getDatanodeProtocolServer()
          .register(datanodeDetailsProto, nodeReportProto,
              containerReportsProto, pipelineReportsProto);
      // Process all events in the event queue
      reconScm.getEventQueue().processAll(1000);
    } catch (Exception ex) {
      Assert.fail(ex.getMessage());
    }
  }

  @Test
  public void testGetDatanodes() throws Exception {
    Response response = nodeEndpoint.getDatanodes();
    DatanodesResponse datanodesResponse =
        (DatanodesResponse) response.getEntity();
    Assert.assertEquals(1, datanodesResponse.getTotalCount());
    Assert.assertEquals(1, datanodesResponse.getDatanodes().size());
    DatanodeMetadata datanodeMetadata =
        datanodesResponse.getDatanodes().iterator().next();
    Assert.assertEquals("host1.datanode", datanodeMetadata.getHostname());
    Assert.assertEquals(75000,
        datanodeMetadata.getDatanodeStorageReport().getCapacity());
    Assert.assertEquals(15400,
        datanodeMetadata.getDatanodeStorageReport().getRemaining());
    Assert.assertEquals(35000,
        datanodeMetadata.getDatanodeStorageReport().getUsed());

    Assert.assertEquals(1, datanodeMetadata.getPipelineIDs().size());
    Assert.assertEquals(pipelineId,
        datanodeMetadata.getPipelineIDs().get(0).toString());

    // if container report is processed first, and pipeline does not exist
    // then container is not added until the next container report is processed
    SCMHeartbeatRequestProto heartbeatRequestProto =
        SCMHeartbeatRequestProto.newBuilder()
            .setContainerReport(containerReportsProto)
            .setDatanodeDetails(datanodeDetailsProto)
            .build();
    reconScm.getDatanodeProtocolServer()
        .sendHeartbeat(heartbeatRequestProto);

    LambdaTestUtils.await(30000, 5000, () -> {
      Response response1 = nodeEndpoint.getDatanodes();
      DatanodesResponse datanodesResponse1 =
          (DatanodesResponse) response1.getEntity();
      DatanodeMetadata datanodeMetadata1 =
          datanodesResponse1.getDatanodes().iterator().next();
      return (datanodeMetadata1.getContainers() == 1);
    });
    Assert.assertEquals(1,
        reconScm.getPipelineManager()
            .getContainersInPipeline(pipeline.getId()).size());
  }
}
