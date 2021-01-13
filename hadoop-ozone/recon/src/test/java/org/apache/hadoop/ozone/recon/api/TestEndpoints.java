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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos
    .ExtendedDatanodeDetailsProto;
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
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.MetricsServiceProviderFactory;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.ClusterStateResponse;
import org.apache.hadoop.ozone.recon.api.types.DatanodeMetadata;
import org.apache.hadoop.ozone.recon.api.types.DatanodesResponse;
import org.apache.hadoop.ozone.recon.api.types.PipelineMetadata;
import org.apache.hadoop.ozone.recon.api.types.PipelinesResponse;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.FileSizeCountTask;
import org.apache.hadoop.ozone.recon.tasks.TableCountTask;
import org.apache.hadoop.test.LambdaTestUtils;
import org.hadoop.ozone.recon.schema.UtilizationSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.FileCountBySizeDao;
import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.hadoop.ozone.recon.schema.tables.pojos.FileCountBySize;
import org.jooq.Configuration;
import org.jooq.DSLContext;
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
import static org.apache.hadoop.ozone.recon.spi.impl.PrometheusServiceProviderImpl.PROMETHEUS_INSTANT_QUERY_API;
import static org.hadoop.ozone.recon.schema.tables.GlobalStatsTable.GLOBAL_STATS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Test for Recon API endpoints.
 */
public class TestEndpoints extends AbstractReconSqlDBTest {
  private NodeEndpoint nodeEndpoint;
  private PipelineEndpoint pipelineEndpoint;
  private ClusterStateEndpoint clusterStateEndpoint;
  private UtilizationEndpoint utilizationEndpoint;
  private MetricsProxyEndpoint metricsProxyEndpoint;
  private ReconOMMetadataManager reconOMMetadataManager;
  private FileSizeCountTask fileSizeCountTask;
  private TableCountTask tableCountTask;
  private ReconStorageContainerManagerFacade reconScm;
  private boolean isSetupDone = false;
  private String pipelineId;
  private DatanodeDetails datanodeDetails;
  private DatanodeDetails datanodeDetails2;
  private long containerId = 1L;
  private ContainerReportsProto containerReportsProto;
  private ExtendedDatanodeDetailsProto extendedDatanodeDetailsProto;
  private Pipeline pipeline;
  private FileCountBySizeDao fileCountBySizeDao;
  private DSLContext dslContext;
  private final String host1 = "host1.datanode";
  private final String host2 = "host2.datanode";
  private final String ip1 = "1.1.1.1";
  private final String ip2 = "2.2.2.2";
  private final String prometheusTestResponseFile =
      "prometheus-test-response.txt";
  private ReconUtils reconUtilsMock;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private void initializeInjector() throws Exception {
    reconOMMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(temporaryFolder.newFolder()),
        temporaryFolder.newFolder());
    datanodeDetails = randomDatanodeDetails();
    datanodeDetails2 = randomDatanodeDetails();
    datanodeDetails.setHostName(host1);
    datanodeDetails.setIpAddress(ip1);
    datanodeDetails2.setHostName(host2);
    datanodeDetails2.setIpAddress(ip2);
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

    InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(
            prometheusTestResponseFile);
    reconUtilsMock = mock(ReconUtils.class);
    HttpURLConnection urlConnectionMock = mock(HttpURLConnection.class);
    when(urlConnectionMock.getResponseCode())
        .thenReturn(HttpServletResponse.SC_OK);
    when(urlConnectionMock.getInputStream()).thenReturn(inputStream);
    when(reconUtilsMock.makeHttpCall(any(URLConnectionFactory.class),
        anyString(), anyBoolean())).thenReturn(urlConnectionMock);

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
            .addBinding(MetricsServiceProviderFactory.class)
            .addBinding(ContainerHealthSchemaManager.class)
            .addBinding(UtilizationEndpoint.class)
            .addBinding(ReconUtils.class, reconUtilsMock)
            .addBinding(StorageContainerLocationProtocol.class, mockScmClient)
            .build();

    nodeEndpoint = reconTestInjector.getInstance(NodeEndpoint.class);
    pipelineEndpoint = reconTestInjector.getInstance(PipelineEndpoint.class);
    fileCountBySizeDao = getDao(FileCountBySizeDao.class);
    GlobalStatsDao globalStatsDao = getDao(GlobalStatsDao.class);
    UtilizationSchemaDefinition utilizationSchemaDefinition =
        getSchemaDefinition(UtilizationSchemaDefinition.class);
    Configuration sqlConfiguration =
        reconTestInjector.getInstance(Configuration.class);
    utilizationEndpoint = new UtilizationEndpoint(
        fileCountBySizeDao, utilizationSchemaDefinition);
    fileSizeCountTask =
        new FileSizeCountTask(fileCountBySizeDao, utilizationSchemaDefinition);
    tableCountTask = new TableCountTask(
        globalStatsDao, sqlConfiguration, reconOMMetadataManager);
    reconScm = (ReconStorageContainerManagerFacade)
        reconTestInjector.getInstance(OzoneStorageContainerManager.class);
    clusterStateEndpoint =
        new ClusterStateEndpoint(reconScm, globalStatsDao);
    MetricsServiceProviderFactory metricsServiceProviderFactory =
        reconTestInjector.getInstance(MetricsServiceProviderFactory.class);
    metricsProxyEndpoint =
        new MetricsProxyEndpoint(metricsServiceProviderFactory);
    dslContext = getDslContext();
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
    PipelineReportsProto pipelineReportsProto =
        PipelineReportsProto.newBuilder()
            .addPipelineReport(pipelineReport).build();
    DatanodeDetailsProto datanodeDetailsProto =
        DatanodeDetailsProto.newBuilder()
            .setHostName(host1)
            .setUuid(datanodeId)
            .setIpAddress(ip1)
            .build();
    extendedDatanodeDetailsProto =
        HddsProtos.ExtendedDatanodeDetailsProto.newBuilder()
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

    DatanodeDetailsProto datanodeDetailsProto2 =
        DatanodeDetailsProto.newBuilder()
            .setHostName(host2)
            .setUuid(datanodeId2)
            .setIpAddress(ip2)
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

    try {
      reconScm.getDatanodeProtocolServer()
          .register(extendedDatanodeDetailsProto, nodeReportProto,
              containerReportsProto, pipelineReportsProto);
      reconScm.getDatanodeProtocolServer()
          .register(extendedDatanodeDetailsProto2, nodeReportProto2,
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

    // Truncate global stats table before running each test
    dslContext.truncate(GLOBAL_STATS);
  }

  private void testDatanodeResponse(DatanodeMetadata datanodeMetadata)
      throws IOException {
    String hostname = datanodeMetadata.getHostname();
    switch (hostname) {
    case host1:
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
      Assert.assertEquals(pipeline.getLeaderNode().getHostName(),
          datanodeMetadata.getPipelines().get(0).getLeaderNode());
      Assert.assertEquals(1, datanodeMetadata.getLeaderCount());
      break;
    case host2:
      Assert.assertEquals(130000,
          datanodeMetadata.getDatanodeStorageReport().getCapacity());
      Assert.assertEquals(17800,
          datanodeMetadata.getDatanodeStorageReport().getRemaining());
      Assert.assertEquals(80000,
          datanodeMetadata.getDatanodeStorageReport().getUsed());

      Assert.assertEquals(0, datanodeMetadata.getPipelines().size());
      Assert.assertEquals(0, datanodeMetadata.getLeaderCount());
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

    datanodesResponse.getDatanodes().forEach(datanodeMetadata -> {
      try {
        testDatanodeResponse(datanodeMetadata);
      } catch (IOException e) {
        Assert.fail(e.getMessage());
      }
    });

    waitAndCheckConditionAfterHeartbeat(() -> {
      Response response1 = nodeEndpoint.getDatanodes();
      DatanodesResponse datanodesResponse1 =
          (DatanodesResponse) response1.getEntity();
      DatanodeMetadata datanodeMetadata1 =
          datanodesResponse1.getDatanodes().stream().filter(datanodeMetadata ->
              datanodeMetadata.getHostname().equals("host1.datanode"))
              .findFirst().orElse(null);
      return (datanodeMetadata1 != null &&
          datanodeMetadata1.getContainers() == 1 &&
          reconScm.getPipelineManager()
              .getContainersInPipeline(pipeline.getId()).size() == 1);
    });
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
    Assert.assertEquals(5, pipelineMetadata.getLeaderElections());

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
  public void testGetMetricsResponse() throws Exception {
    HttpServletResponse responseMock = mock(HttpServletResponse.class);
    ServletOutputStream outputStreamMock = mock(ServletOutputStream.class);
    when(responseMock.getOutputStream()).thenReturn(outputStreamMock);
    UriInfo uriInfoMock = mock(UriInfo.class);
    URI uriMock = mock(URI.class);
    when(uriMock.getQuery()).thenReturn("");
    when(uriInfoMock.getRequestUri()).thenReturn(uriMock);

    // Mock makeHttpCall to send a json response
    // when the prometheus endpoint is queried.
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    InputStream inputStream = classLoader
        .getResourceAsStream(prometheusTestResponseFile);
    HttpURLConnection urlConnectionMock = mock(HttpURLConnection.class);
    when(urlConnectionMock.getResponseCode())
        .thenReturn(HttpServletResponse.SC_OK);
    when(urlConnectionMock.getInputStream()).thenReturn(inputStream);
    when(reconUtilsMock.makeHttpCall(any(URLConnectionFactory.class),
        anyString(), anyBoolean())).thenReturn(urlConnectionMock);

    metricsProxyEndpoint.getMetricsResponse(PROMETHEUS_INSTANT_QUERY_API,
        uriInfoMock, responseMock);

    byte[] fileBytes = FileUtils.readFileToByteArray(
        new File(classLoader.getResource(prometheusTestResponseFile).getFile())
        );
    verify(outputStreamMock).write(fileBytes, 0, fileBytes.length);
  }

  @Test
  public void testGetClusterState() throws Exception {
    Response response = clusterStateEndpoint.getClusterState();
    ClusterStateResponse clusterStateResponse =
        (ClusterStateResponse) response.getEntity();

    Assert.assertEquals(1, clusterStateResponse.getPipelines());
    Assert.assertEquals(0, clusterStateResponse.getVolumes());
    Assert.assertEquals(0, clusterStateResponse.getBuckets());
    Assert.assertEquals(0, clusterStateResponse.getKeys());
    Assert.assertEquals(2, clusterStateResponse.getTotalDatanodes());
    Assert.assertEquals(2, clusterStateResponse.getHealthyDatanodes());

    waitAndCheckConditionAfterHeartbeat(() -> {
      Response response1 = clusterStateEndpoint.getClusterState();
      ClusterStateResponse clusterStateResponse1 =
          (ClusterStateResponse) response1.getEntity();
      return (clusterStateResponse1.getContainers() == 1);
    });

    // check volume, bucket and key count after running table count task
    Pair<String, Boolean> result =
        tableCountTask.reprocess(reconOMMetadataManager);
    assertTrue(result.getRight());
    response = clusterStateEndpoint.getClusterState();
    clusterStateResponse = (ClusterStateResponse) response.getEntity();
    Assert.assertEquals(2, clusterStateResponse.getVolumes());
    Assert.assertEquals(2, clusterStateResponse.getBuckets());
    Assert.assertEquals(3, clusterStateResponse.getKeys());
  }

  @Test
  public void testGetFileCounts() throws Exception {
    OmKeyInfo omKeyInfo1 = mock(OmKeyInfo.class);
    given(omKeyInfo1.getKeyName()).willReturn("key1");
    given(omKeyInfo1.getVolumeName()).willReturn("vol1");
    given(omKeyInfo1.getBucketName()).willReturn("bucket1");
    given(omKeyInfo1.getDataSize()).willReturn(1000L);

    OmKeyInfo omKeyInfo2 = mock(OmKeyInfo.class);
    given(omKeyInfo2.getKeyName()).willReturn("key2");
    given(omKeyInfo2.getVolumeName()).willReturn("vol1");
    given(omKeyInfo2.getBucketName()).willReturn("bucket1");
    given(omKeyInfo2.getDataSize()).willReturn(100000L);

    OmKeyInfo omKeyInfo3 = mock(OmKeyInfo.class);
    given(omKeyInfo3.getKeyName()).willReturn("key1");
    given(omKeyInfo3.getVolumeName()).willReturn("vol2");
    given(omKeyInfo3.getBucketName()).willReturn("bucket1");
    given(omKeyInfo3.getDataSize()).willReturn(1000L);

    OMMetadataManager omMetadataManager = mock(OmMetadataManagerImpl.class);
    TypedTable<String, OmKeyInfo> keyTable = mock(TypedTable.class);

    TypedTable.TypedTableIterator mockKeyIter = mock(TypedTable
        .TypedTableIterator.class);
    TypedTable.TypedKeyValue mockKeyValue = mock(
        TypedTable.TypedKeyValue.class);

    when(keyTable.iterator()).thenReturn(mockKeyIter);
    when(omMetadataManager.getKeyTable()).thenReturn(keyTable);
    when(mockKeyIter.hasNext())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);
    when(mockKeyIter.next()).thenReturn(mockKeyValue);
    when(mockKeyValue.getValue())
        .thenReturn(omKeyInfo1)
        .thenReturn(omKeyInfo2)
        .thenReturn(omKeyInfo3);

    Pair<String, Boolean> result =
        fileSizeCountTask.reprocess(omMetadataManager);
    assertTrue(result.getRight());

    assertEquals(3, fileCountBySizeDao.count());
    Response response = utilizationEndpoint.getFileCounts(null, null, 0);
    List<FileCountBySize> resultSet =
        (List<FileCountBySize>) response.getEntity();
    assertEquals(3, resultSet.size());
    assertTrue(resultSet.stream().anyMatch(o -> o.getVolume().equals("vol1") &&
        o.getBucket().equals("bucket1") && o.getFileSize() == 1024L &&
        o.getCount() == 1L));
    assertTrue(resultSet.stream().anyMatch(o -> o.getVolume().equals("vol1") &&
        o.getBucket().equals("bucket1") && o.getFileSize() == 131072 &&
        o.getCount() == 1L));
    assertTrue(resultSet.stream().anyMatch(o -> o.getVolume().equals("vol2") &&
        o.getBucket().equals("bucket1") && o.getFileSize() == 1024L &&
        o.getCount() == 1L));

    // Test for "volume" query param
    response = utilizationEndpoint.getFileCounts("vol1", null, 0);
    resultSet = (List<FileCountBySize>) response.getEntity();
    assertEquals(2, resultSet.size());
    assertTrue(resultSet.stream().allMatch(o -> o.getVolume().equals("vol1")));

    // Test for non-existent volume
    response = utilizationEndpoint.getFileCounts("vol", null, 0);
    resultSet = (List<FileCountBySize>) response.getEntity();
    assertEquals(0, resultSet.size());

    // Test for "volume" + "bucket" query param
    response = utilizationEndpoint.getFileCounts("vol1", "bucket1", 0);
    resultSet = (List<FileCountBySize>) response.getEntity();
    assertEquals(2, resultSet.size());
    assertTrue(resultSet.stream().allMatch(o -> o.getVolume().equals("vol1") &&
        o.getBucket().equals("bucket1")));

    // Test for non-existent bucket
    response = utilizationEndpoint.getFileCounts("vol1", "bucket", 0);
    resultSet = (List<FileCountBySize>) response.getEntity();
    assertEquals(0, resultSet.size());

    // Test for "volume" + "bucket" + "fileSize" query params
    response = utilizationEndpoint.getFileCounts("vol1", "bucket1", 131072);
    resultSet = (List<FileCountBySize>) response.getEntity();
    assertEquals(1, resultSet.size());
    FileCountBySize o = resultSet.get(0);
    assertTrue(o.getVolume().equals("vol1") && o.getBucket().equals(
        "bucket1") && o.getFileSize() == 131072);

    // Test for non-existent fileSize
    response = utilizationEndpoint.getFileCounts("vol1", "bucket1", 1310725);
    resultSet = (List<FileCountBySize>) response.getEntity();
    assertEquals(0, resultSet.size());
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
