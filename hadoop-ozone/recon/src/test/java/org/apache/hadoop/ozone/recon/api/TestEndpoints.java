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
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos
    .ExtendedDatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
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
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.MetricsServiceProviderFactory;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.AclMetadata;
import org.apache.hadoop.ozone.recon.api.types.BucketObjectDBInfo;
import org.apache.hadoop.ozone.recon.api.types.BucketsResponse;
import org.apache.hadoop.ozone.recon.api.types.ClusterStateResponse;
import org.apache.hadoop.ozone.recon.api.types.DatanodeMetadata;
import org.apache.hadoop.ozone.recon.api.types.DatanodesResponse;
import org.apache.hadoop.ozone.recon.api.types.PipelineMetadata;
import org.apache.hadoop.ozone.recon.api.types.PipelinesResponse;
import org.apache.hadoop.ozone.recon.api.types.VolumeObjectDBInfo;
import org.apache.hadoop.ozone.recon.api.types.VolumesResponse;
import org.apache.hadoop.ozone.recon.common.CommonUtils;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconPipelineManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.ContainerSizeCountTask;
import org.apache.hadoop.ozone.recon.tasks.FileSizeCountTask;
import org.apache.hadoop.ozone.recon.tasks.OmTableInsightTask;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.ozone.test.LambdaTestUtils;
import org.hadoop.ozone.recon.schema.UtilizationSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.ContainerCountBySizeDao;
import org.hadoop.ozone.recon.schema.tables.daos.FileCountBySizeDao;
import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ContainerCountBySize;
import org.hadoop.ozone.recon.schema.tables.pojos.FileCountBySize;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.ozone.container.upgrade.UpgradeUtils.defaultLayoutVersionProto;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDeletedDirToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getRandomPipeline;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDataToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDeletedKeysToOm;
import static org.apache.hadoop.ozone.recon.spi.impl.PrometheusServiceProviderImpl.PROMETHEUS_INSTANT_QUERY_API;
import static org.hadoop.ozone.recon.schema.tables.GlobalStatsTable.GLOBAL_STATS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test for Recon API endpoints.
 */
public class TestEndpoints extends AbstractReconSqlDBTest {
  @TempDir
  private Path temporaryFolder;
  private NodeEndpoint nodeEndpoint;
  private PipelineEndpoint pipelineEndpoint;
  private ClusterStateEndpoint clusterStateEndpoint;
  private UtilizationEndpoint utilizationEndpoint;
  private MetricsProxyEndpoint metricsProxyEndpoint;
  private VolumeEndpoint volumeEndpoint;
  private BucketEndpoint bucketEndpoint;
  private ReconOMMetadataManager reconOMMetadataManager;
  private FileSizeCountTask fileSizeCountTask;
  private ContainerSizeCountTask containerSizeCountTask;
  private OmTableInsightTask omTableInsightTask;
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
  private ContainerCountBySizeDao containerCountBySizeDao;
  private DSLContext dslContext;
  private static final String HOST1 = "host1.datanode";
  private static final String HOST2 = "host2.datanode";
  private static final String IP1 = "1.1.1.1";
  private static final String IP2 = "2.2.2.2";
  private static final String PROMETHEUS_TEST_RESPONSE_FILE =
      "prometheus-test-response.txt";
  private ReconUtils reconUtilsMock;

  private ContainerHealthSchemaManager containerHealthSchemaManager;
  private CommonUtils commonUtils;
  private PipelineManager pipelineManager;
  private ReconPipelineManager reconPipelineManager;

  public TestEndpoints() {
    super();
  }

  private void initializeInjector() throws Exception {
    reconOMMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(Files.createDirectory(
            temporaryFolder.resolve("JunitOmDBDir")).toFile()),
        Files.createDirectory(temporaryFolder.resolve("NewDir")).toFile());
    datanodeDetails = randomDatanodeDetails();
    datanodeDetails2 = randomDatanodeDetails();
    datanodeDetails.setHostName(HOST1);
    datanodeDetails.setIpAddress(IP1);
    datanodeDetails2.setHostName(HOST2);
    datanodeDetails2.setIpAddress(IP2);
    pipeline = getRandomPipeline(datanodeDetails);
    pipelineId = pipeline.getId().getId().toString();

    ContainerInfo containerInfo = new ContainerInfo.Builder()
        .setContainerID(containerId)
        .setReplicationConfig(RatisReplicationConfig
            .getInstance(ReplicationFactor.ONE))
        .setState(LifeCycleState.OPEN)
        .setOwner("test")
        .setPipelineID(pipeline.getId())
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
    List<Long> containerIDs = new LinkedList<>();
    containerIDs.add(containerId);
    List<ContainerWithPipeline> cpw = new LinkedList<>();
    cpw.add(containerWithPipeline);
    when(mockScmServiceProvider
        .getExistContainerWithPipelinesInBatch(containerIDs))
        .thenReturn(cpw);
    InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(
            PROMETHEUS_TEST_RESPONSE_FILE);
    reconUtilsMock = mock(ReconUtils.class);
    HttpURLConnection urlConnectionMock = mock(HttpURLConnection.class);
    when(urlConnectionMock.getResponseCode())
        .thenReturn(HttpServletResponse.SC_OK);
    when(urlConnectionMock.getInputStream()).thenReturn(inputStream);
    when(reconUtilsMock.makeHttpCall(any(URLConnectionFactory.class),
        anyString(), anyBoolean())).thenReturn(urlConnectionMock);
    when(reconUtilsMock.getReconDbDir(any(OzoneConfiguration.class),
        anyString())).thenReturn(temporaryFolder.resolve("reconDbDir").toFile());
    when(reconUtilsMock.getReconNodeDetails(
        any(OzoneConfiguration.class))).thenReturn(
        commonUtils.getReconNodeDetails());
    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder.toFile())
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
            .addBinding(VolumeEndpoint.class)
            .addBinding(BucketEndpoint.class)
            .addBinding(MetricsServiceProviderFactory.class)
            .addBinding(ContainerHealthSchemaManager.class)
            .addBinding(UtilizationEndpoint.class)
            .addBinding(ReconUtils.class, reconUtilsMock)
            .addBinding(StorageContainerLocationProtocol.class, mockScmClient)
            .build();

    reconScm = (ReconStorageContainerManagerFacade)
        reconTestInjector.getInstance(OzoneStorageContainerManager.class);
    nodeEndpoint = reconTestInjector.getInstance(NodeEndpoint.class);
    pipelineEndpoint = reconTestInjector.getInstance(PipelineEndpoint.class);
    volumeEndpoint = reconTestInjector.getInstance(VolumeEndpoint.class);
    bucketEndpoint = reconTestInjector.getInstance(BucketEndpoint.class);
    fileCountBySizeDao = getDao(FileCountBySizeDao.class);
    containerCountBySizeDao = reconScm.getContainerCountBySizeDao();
    GlobalStatsDao globalStatsDao = getDao(GlobalStatsDao.class);
    UtilizationSchemaDefinition utilizationSchemaDefinition =
        getSchemaDefinition(UtilizationSchemaDefinition.class);
    Configuration sqlConfiguration =
        reconTestInjector.getInstance(Configuration.class);
    utilizationEndpoint = new UtilizationEndpoint(
        fileCountBySizeDao,
        containerCountBySizeDao,
        utilizationSchemaDefinition);
    fileSizeCountTask =
        new FileSizeCountTask(fileCountBySizeDao, utilizationSchemaDefinition);
    omTableInsightTask =
        new OmTableInsightTask(globalStatsDao, sqlConfiguration,
            reconOMMetadataManager);
    containerHealthSchemaManager =
        reconTestInjector.getInstance(ContainerHealthSchemaManager.class);
    clusterStateEndpoint =
        new ClusterStateEndpoint(reconScm, globalStatsDao,
            containerHealthSchemaManager);
    containerSizeCountTask = reconScm.getContainerSizeCountTask();
    MetricsServiceProviderFactory metricsServiceProviderFactory =
        reconTestInjector.getInstance(MetricsServiceProviderFactory.class);
    metricsProxyEndpoint =
        new MetricsProxyEndpoint(metricsServiceProviderFactory);
    dslContext = getDslContext();
    pipelineManager = reconScm.getPipelineManager();
    reconPipelineManager = (ReconPipelineManager) pipelineManager;
    reconPipelineManager.addPipeline(pipeline);
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @BeforeEach
  public void setUp() throws Exception {
    // The following setup runs only once
    if (!isSetupDone) {
      commonUtils = new CommonUtils();
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
            .setHostName(HOST1)
            .setUuid(datanodeId)
            .setIpAddress(IP1)
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

    assertDoesNotThrow(() -> {
      reconScm.getDatanodeProtocolServer()
          .register(extendedDatanodeDetailsProto, nodeReportProto,
              containerReportsProto, pipelineReportsProto, layoutInfo);
      reconScm.getDatanodeProtocolServer()
          .register(extendedDatanodeDetailsProto2, nodeReportProto2,
              ContainerReportsProto.newBuilder().build(),
              PipelineReportsProto.newBuilder().build(),
              defaultLayoutVersionProto());
      // Process all events in the event queue
      reconScm.getEventQueue().processAll(1000);
    });
    // Write Data to OM
    // A sample volume (sampleVol) and a bucket (bucketOne) is already created
    // in AbstractOMMetadataManagerTest.
    // Create a new volume and bucket and then write keys to the bucket.
    String volumeKey = reconOMMetadataManager.getVolumeKey("sampleVol2");
    OmVolumeArgs args =
        OmVolumeArgs.newBuilder()
            .setVolume("sampleVol2")
            .setAdminName("TestUser2")
            .setOwnerName("TestUser2")
            .setQuotaInBytes(OzoneConsts.GB)
            .setQuotaInNamespace(1000)
            .setUsedNamespace(500)
            .addOzoneAcls(new OzoneAcl(
                IAccessAuthorizer.ACLIdentityType.USER,
                "TestUser2",
                IAccessAuthorizer.ACLType.WRITE,
                OzoneAcl.AclScope.ACCESS
            ))
            .addOzoneAcls(new OzoneAcl(
                IAccessAuthorizer.ACLIdentityType.USER,
                "TestUser2",
                IAccessAuthorizer.ACLType.READ,
                OzoneAcl.AclScope.ACCESS
            ))
            .build();
    reconOMMetadataManager.getVolumeTable().put(volumeKey, args);

    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("sampleVol2")
        .setBucketName("bucketOne")
        .addAcl(new OzoneAcl(
            IAccessAuthorizer.ACLIdentityType.GROUP,
            "TestGroup2",
            IAccessAuthorizer.ACLType.WRITE,
            OzoneAcl.AclScope.ACCESS
        ))
        .setQuotaInBytes(OzoneConsts.GB)
        .setUsedBytes(OzoneConsts.MB)
        .setQuotaInNamespace(5)
        .setStorageType(StorageType.DISK)
        .setUsedNamespace(3)
        .setBucketLayout(BucketLayout.LEGACY)
        .setOwner("TestUser2")
        .setIsVersionEnabled(false)
        .build();

    String bucketKey = reconOMMetadataManager.getBucketKey(
        bucketInfo.getVolumeName(), bucketInfo.getBucketName());

    reconOMMetadataManager.getBucketTable().put(bucketKey, bucketInfo);

    OmBucketInfo bucketInfo2 = OmBucketInfo.newBuilder()
        .setVolumeName("sampleVol2")
        .setBucketName("bucketTwo")
        .addAcl(new OzoneAcl(
            IAccessAuthorizer.ACLIdentityType.GROUP,
            "TestGroup2",
            IAccessAuthorizer.ACLType.READ,
            OzoneAcl.AclScope.ACCESS
        ))
        .setQuotaInBytes(OzoneConsts.GB)
        .setUsedBytes(100 * OzoneConsts.MB)
        .setQuotaInNamespace(5)
        .setStorageType(StorageType.SSD)
        .setUsedNamespace(3)
        .setBucketLayout(BucketLayout.OBJECT_STORE)
        .setOwner("TestUser2")
        .setIsVersionEnabled(false)
        .build();

    String bucketKey2 = reconOMMetadataManager.getBucketKey(
        bucketInfo2.getVolumeName(), bucketInfo2.getBucketName());

    reconOMMetadataManager.getBucketTable().put(bucketKey2, bucketInfo2);

    // key = key_one
    writeDataToOm(reconOMMetadataManager, "key_one");
    // key = key_two
    writeDataToOm(reconOMMetadataManager, "key_two");
    // key = key_three
    writeDataToOm(reconOMMetadataManager, "key_three");

    // Populate the deletedKeys table in OM DB
    List<String> deletedKeysList1 = Arrays.asList("key1");
    writeDeletedKeysToOm(reconOMMetadataManager,
        deletedKeysList1, "Bucket1", "Volume1");
    List<String> deletedKeysList2 = Arrays.asList("key2", "key2");
    writeDeletedKeysToOm(reconOMMetadataManager,
        deletedKeysList2, "Bucket2", "Volume2");
    List<String> deletedKeysList3 = Arrays.asList("key3", "key3", "key3");
    writeDeletedKeysToOm(reconOMMetadataManager,
        deletedKeysList3, "Bucket3", "Volume3");

    // Populate the deletedDirectories table in OM DB
    writeDeletedDirToOm(reconOMMetadataManager, "Bucket1", "Volume1", "dir1",
        3L, 2L, 1L, 23L);
    writeDeletedDirToOm(reconOMMetadataManager, "Bucket2", "Volume2", "dir2",
        6L, 5L, 4L, 22L);
    writeDeletedDirToOm(reconOMMetadataManager, "Bucket3", "Volume3", "dir3",
        9L, 8L, 7L, 21L);

    // Truncate global stats table before running each test
    dslContext.truncate(GLOBAL_STATS);
  }

  private void testDatanodeResponse(DatanodeMetadata datanodeMetadata)
      throws IOException {
    // Check NodeState and NodeOperationalState field existence
    assertEquals(NodeState.HEALTHY, datanodeMetadata.getState());
    assertEquals(NodeOperationalState.IN_SERVICE,
        datanodeMetadata.getOperationalState());

    String hostname = datanodeMetadata.getHostname();
    switch (hostname) {
    case HOST1:
      assertEquals(75000,
          datanodeMetadata.getDatanodeStorageReport().getCapacity());
      assertEquals(15400,
          datanodeMetadata.getDatanodeStorageReport().getRemaining());
      assertEquals(35000,
          datanodeMetadata.getDatanodeStorageReport().getUsed());

      assertEquals(1, datanodeMetadata.getPipelines().size());
      assertEquals(pipelineId,
          datanodeMetadata.getPipelines().get(0).getPipelineID().toString());
      assertEquals(pipeline.getReplicationConfig().getReplication(),
          datanodeMetadata.getPipelines().get(0).getReplicationFactor());
      assertEquals(pipeline.getType().toString(),
          datanodeMetadata.getPipelines().get(0).getReplicationType());
      assertEquals(pipeline.getLeaderNode().getHostName(),
          datanodeMetadata.getPipelines().get(0).getLeaderNode());
      assertEquals(1, datanodeMetadata.getLeaderCount());
      break;
    case HOST2:
      assertEquals(130000,
          datanodeMetadata.getDatanodeStorageReport().getCapacity());
      assertEquals(17800,
          datanodeMetadata.getDatanodeStorageReport().getRemaining());
      assertEquals(80000,
          datanodeMetadata.getDatanodeStorageReport().getUsed());

      assertEquals(0, datanodeMetadata.getPipelines().size());
      assertEquals(0, datanodeMetadata.getLeaderCount());
      break;
    default:
      fail(String.format("Datanode %s not registered",
          hostname));
    }
    assertEquals(HDDSLayoutVersionManager.maxLayoutVersion(),
        datanodeMetadata.getLayoutVersion());
  }

  @Test
  public void testGetDatanodes() throws Exception {
    Response response = nodeEndpoint.getDatanodes();
    DatanodesResponse datanodesResponse =
        (DatanodesResponse) response.getEntity();
    assertEquals(2, datanodesResponse.getTotalCount());
    assertEquals(2, datanodesResponse.getDatanodes().size());

    datanodesResponse.getDatanodes().forEach(datanodeMetadata -> {
      try {
        testDatanodeResponse(datanodeMetadata);
      } catch (IOException e) {
        fail(e.getMessage());
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
          datanodeMetadata1.getOpenContainers() == 1 &&
          reconScm.getPipelineManager()
              .getContainersInPipeline(pipeline.getId()).size() == 1);
    });

    // Change Node OperationalState with NodeManager
    final NodeManager nodeManager = reconScm.getScmNodeManager();
    final DatanodeDetails dnDetailsInternal =
        nodeManager.getNodeByUuid(datanodeDetails.getUuidString());
    // Backup existing state and sanity check
    final NodeStatus nStatus = nodeManager.getNodeStatus(dnDetailsInternal);
    final NodeOperationalState backupOpState =
        dnDetailsInternal.getPersistedOpState();
    final long backupOpStateExpiry =
        dnDetailsInternal.getPersistedOpStateExpiryEpochSec();
    assertEquals(backupOpState, nStatus.getOperationalState());
    assertEquals(backupOpStateExpiry, nStatus.getOpStateExpiryEpochSeconds());

    dnDetailsInternal.setPersistedOpState(NodeOperationalState.DECOMMISSIONING);
    dnDetailsInternal.setPersistedOpStateExpiryEpochSec(666L);
    nodeManager.setNodeOperationalState(dnDetailsInternal,
        NodeOperationalState.DECOMMISSIONING, 666L);
    // Check if the endpoint response reflects the change
    response = nodeEndpoint.getDatanodes();
    datanodesResponse = (DatanodesResponse) response.getEntity();
    // Order of datanodes in the response is random
    AtomicInteger count = new AtomicInteger();
    datanodesResponse.getDatanodes().forEach(metadata -> {
      if (metadata.getUuid().equals(dnDetailsInternal.getUuidString())) {
        count.incrementAndGet();
        assertEquals(NodeOperationalState.DECOMMISSIONING,
            metadata.getOperationalState());
      }
    });
    assertEquals(1, count.get());

    // Restore state
    dnDetailsInternal.setPersistedOpState(backupOpState);
    dnDetailsInternal.setPersistedOpStateExpiryEpochSec(backupOpStateExpiry);
    nodeManager.setNodeOperationalState(dnDetailsInternal,
        backupOpState, backupOpStateExpiry);
  }

  @Test
  public void testGetPipelines() throws Exception {
    Response response = pipelineEndpoint.getPipelines();
    PipelinesResponse pipelinesResponse =
        (PipelinesResponse) response.getEntity();
    assertEquals(1, pipelinesResponse.getTotalCount());
    assertEquals(1, pipelinesResponse.getPipelines().size());
    PipelineMetadata pipelineMetadata =
        pipelinesResponse.getPipelines().iterator().next();
    assertEquals(1, pipelineMetadata.getDatanodes().size());
    assertEquals(pipeline.getType().toString(),
        pipelineMetadata.getReplicationType());
    assertEquals(pipeline.getReplicationConfig().getReplication(),
        pipelineMetadata.getReplicationFactor());
    assertEquals(datanodeDetails.getHostName(),
        pipelineMetadata.getLeaderNode());
    assertEquals(pipeline.getId().getId(),
        pipelineMetadata.getPipelineId());
    assertEquals(5, pipelineMetadata.getLeaderElections());

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
        .getResourceAsStream(PROMETHEUS_TEST_RESPONSE_FILE);
    HttpURLConnection urlConnectionMock = mock(HttpURLConnection.class);
    when(urlConnectionMock.getResponseCode())
        .thenReturn(HttpServletResponse.SC_OK);
    when(urlConnectionMock.getInputStream()).thenReturn(inputStream);
    when(reconUtilsMock.makeHttpCall(any(URLConnectionFactory.class),
        anyString(), anyBoolean())).thenReturn(urlConnectionMock);

    metricsProxyEndpoint.getMetricsResponse(PROMETHEUS_INSTANT_QUERY_API,
        uriInfoMock, responseMock);

    byte[] fileBytes = FileUtils.readFileToByteArray(
        new File(classLoader.getResource(PROMETHEUS_TEST_RESPONSE_FILE)
            .getFile())
    );
    verify(outputStreamMock).write(fileBytes, 0, fileBytes.length);
  }

  @Test
  public void testGetClusterState() throws Exception {
    Response response = clusterStateEndpoint.getClusterState();
    ClusterStateResponse clusterStateResponse =
        (ClusterStateResponse) response.getEntity();

    assertEquals(1, clusterStateResponse.getPipelines());
    assertEquals(0, clusterStateResponse.getVolumes());
    assertEquals(0, clusterStateResponse.getBuckets());
    assertEquals(0, clusterStateResponse.getKeys());
    assertEquals(2, clusterStateResponse.getTotalDatanodes());
    assertEquals(2, clusterStateResponse.getHealthyDatanodes());
    assertEquals(0, clusterStateResponse.getMissingContainers());

    waitAndCheckConditionAfterHeartbeat(() -> {
      Response response1 = clusterStateEndpoint.getClusterState();
      ClusterStateResponse clusterStateResponse1 =
          (ClusterStateResponse) response1.getEntity();
      return (clusterStateResponse1.getContainers() == 1);
    });

    // check volume, bucket and key count after running table count task
    Pair<String, Boolean> result =
        omTableInsightTask.reprocess(reconOMMetadataManager);
    assertTrue(result.getRight());
    response = clusterStateEndpoint.getClusterState();
    clusterStateResponse = (ClusterStateResponse) response.getEntity();
    assertEquals(2, clusterStateResponse.getVolumes());
    assertEquals(3, clusterStateResponse.getBuckets());
    assertEquals(3, clusterStateResponse.getKeys());
    // Since a single RepeatedOmKeyInfo can contain multiple deleted keys with
    // the same name, the total count of pending deletion keys is determined by
    // summing the count of the keyInfoList. Each keyInfoList comprises
    // OmKeyInfo objects that represent the deleted keys.
    assertEquals(6, clusterStateResponse.getKeysPendingDeletion());
    assertEquals(3, clusterStateResponse.getDeletedDirs());
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
    TypedTable<String, OmKeyInfo> keyTableLegacy = mock(TypedTable.class);
    TypedTable<String, OmKeyInfo> keyTableFso = mock(TypedTable.class);

    TypedTable.TypedTableIterator mockKeyIterLegacy = mock(TypedTable
        .TypedTableIterator.class);
    TypedTable.TypedTableIterator mockKeyIterFso = mock(TypedTable
        .TypedTableIterator.class);
    TypedTable.TypedKeyValue mockKeyValueLegacy = mock(
        TypedTable.TypedKeyValue.class);
    TypedTable.TypedKeyValue mockKeyValueFso = mock(
        TypedTable.TypedKeyValue.class);

    when(keyTableLegacy.iterator()).thenReturn(mockKeyIterLegacy);
    when(keyTableFso.iterator()).thenReturn(mockKeyIterFso);

    when(omMetadataManager.getKeyTable(BucketLayout.LEGACY)).thenReturn(
        keyTableLegacy);
    when(omMetadataManager.getKeyTable(
        BucketLayout.FILE_SYSTEM_OPTIMIZED)).thenReturn(keyTableFso);

    when(mockKeyIterLegacy.hasNext())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);
    when(mockKeyIterFso.hasNext())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);
    when(mockKeyIterLegacy.next()).thenReturn(mockKeyValueLegacy);
    when(mockKeyIterFso.next()).thenReturn(mockKeyValueFso);

    when(mockKeyValueLegacy.getValue())
        .thenReturn(omKeyInfo1)
        .thenReturn(omKeyInfo2)
        .thenReturn(omKeyInfo3);
    when(mockKeyValueFso.getValue())
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
        o.getCount() == 2L));
    assertTrue(resultSet.stream().anyMatch(o -> o.getVolume().equals("vol1") &&
        o.getBucket().equals("bucket1") && o.getFileSize() == 131072 &&
        o.getCount() == 2L));
    assertTrue(resultSet.stream().anyMatch(o -> o.getVolume().equals("vol2") &&
        o.getBucket().equals("bucket1") && o.getFileSize() == 1024L &&
        o.getCount() == 2L));

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

  @Test
  public void testGetContainerCounts() throws Exception {
    // Mock container info objects with different sizes
    ContainerInfo omContainerInfo1 = mock(ContainerInfo.class);
    given(omContainerInfo1.containerID()).willReturn(new ContainerID(1));
    given(omContainerInfo1.getUsedBytes()).willReturn(1500000000L); // 1.5GB
    given(omContainerInfo1.getState()).willReturn(LifeCycleState.OPEN);

    ContainerInfo omContainerInfo2 = mock(ContainerInfo.class);
    given(omContainerInfo2.containerID()).willReturn(new ContainerID(2));
    given(omContainerInfo2.getUsedBytes()).willReturn(2500000000L); // 2.5GB
    given(omContainerInfo2.getState()).willReturn(LifeCycleState.OPEN);

    // Create a list of container info objects
    List<ContainerInfo> containers = new ArrayList<>();
    containers.add(omContainerInfo1);
    containers.add(omContainerInfo2);

    // Process the list of containers through the container size count task
    containerSizeCountTask.process(containers);

    // Test fetching all container counts
    Response response = utilizationEndpoint.getContainerCounts(0L);
    List<ContainerCountBySize> resultSet =
        (List<ContainerCountBySize>) response.getEntity();

    assertEquals(2, resultSet.size());
    // The UpperBound Stored for 1.5GB in the table is 2147483648L
    assertTrue(resultSet.stream().anyMatch(o -> o.getContainerSize() ==
        2147483648L && o.getCount() == 1L));
    // The UpperBound Stored for 2.5GB in the table is 4294967296L
    assertTrue(resultSet.stream().anyMatch(o -> o.getContainerSize() ==
        4294967296L && o.getCount() == 1L));

    // Test fetching a specific container size (1.5GB)
    response = utilizationEndpoint.getContainerCounts(1500000000L);
    resultSet = (List<ContainerCountBySize>) response.getEntity();
    assertEquals(1, resultSet.size());
    // The UpperBound Stored for 1.5GB is 2147483648L (2GB)
    assertEquals(2147483648L, resultSet.get(0).getContainerSize());
    assertEquals(1, resultSet.get(0).getCount());

    // Test fetching non-existent container size
    response = utilizationEndpoint.getContainerCounts(8192L);
    resultSet = (List<ContainerCountBySize>) response.getEntity();
    assertThat(resultSet).isEmpty();

  }

  private void testVolumeResponse(VolumeObjectDBInfo volumeMetadata)
      throws IOException {
    String volumeName = volumeMetadata.getVolume();
    switch (volumeName) {
    case "sampleVol":
      assertEquals("TestUser", volumeMetadata.getOwner());
      assertEquals("TestUser", volumeMetadata.getAdmin());
      assertEquals(-1, volumeMetadata.getQuotaInBytes());
      assertEquals(-1, volumeMetadata.getQuotaInNamespace());
      assertEquals(0, volumeMetadata.getUsedNamespace());
      assertEquals(0, volumeMetadata.getAcls().size());
      break;
    case "sampleVol2":
      assertEquals("TestUser2", volumeMetadata.getOwner());
      assertEquals("TestUser2", volumeMetadata.getAdmin());
      assertEquals(OzoneConsts.GB, volumeMetadata.getQuotaInBytes());
      assertEquals(1000, volumeMetadata.getQuotaInNamespace());
      assertEquals(500, volumeMetadata.getUsedNamespace());

      assertEquals(1, volumeMetadata.getAcls().size());
      AclMetadata acl = volumeMetadata.getAcls().get(0);
      assertEquals("TestUser2", acl.getName());
      assertEquals("USER", acl.getType());
      assertEquals("ACCESS", acl.getScope());

      assertEquals(2, acl.getAclList().size());

      if (acl.getAclList().get(0).equals("WRITE")) {
        assertEquals("READ", acl.getAclList().get(1));
      } else if (acl.getAclList().get(0).equals("READ")) {
        assertEquals("WRITE", acl.getAclList().get(1));
      } else {
        fail(String.format("ACL %s is not recognized",
            acl.getAclList().get(0)));
      }
      break;
    default:
      fail(String.format("Volume %s not registered",
          volumeName));
    }

  }

  @Test
  public void testGetVolumes() throws Exception {
    Response response = volumeEndpoint.getVolumes(10, "");
    assertEquals(200, response.getStatus());
    VolumesResponse volumesResponse =
        (VolumesResponse) response.getEntity();
    assertEquals(2, volumesResponse.getTotalCount());
    assertEquals(2, volumesResponse.getVolumes().size());

    volumesResponse.getVolumes().forEach(volumeMetadata -> {
      try {
        testVolumeResponse(volumeMetadata);
      } catch (IOException e) {
        fail(e.getMessage());
      }
    });
  }

  @Test
  public void testGetVolumesWithPrevKeyAndLimit() throws IOException {
    // Test limit
    Response responseWithLimit = volumeEndpoint.getVolumes(1, null);
    assertEquals(200, responseWithLimit.getStatus());
    VolumesResponse volumesResponseWithLimit =
        (VolumesResponse) responseWithLimit.getEntity();
    assertEquals(1, volumesResponseWithLimit.getTotalCount());
    assertEquals(1, volumesResponseWithLimit.getVolumes().size());

    // Test prevKey
    Response responseWithPrevKey = volumeEndpoint.getVolumes(1, "sampleVol");
    assertEquals(200, responseWithPrevKey.getStatus());
    VolumesResponse volumesResponseWithPrevKey =
        (VolumesResponse) responseWithPrevKey.getEntity();

    assertEquals(1, volumesResponseWithPrevKey.getTotalCount());
    assertEquals(1, volumesResponseWithPrevKey.getVolumes().size());
    assertEquals("sampleVol2",
        volumesResponseWithPrevKey.getVolumes().stream()
            .findFirst().get().getVolume());

  }

  private void testBucketResponse(BucketObjectDBInfo bucketMetadata)
      throws IOException {
    String bucketName = bucketMetadata.getName();
    switch (bucketName) {
    case "bucketOne":
      assertEquals("sampleVol2", bucketMetadata.getVolumeName());
      assertEquals(StorageType.DISK, bucketMetadata.getStorageType());
      assertNull(bucketMetadata.getSourceVolume());
      assertNull(bucketMetadata.getSourceBucket());
      assertEquals(OzoneConsts.GB, bucketMetadata.getQuotaInBytes());
      assertEquals(OzoneConsts.MB, bucketMetadata.getUsedBytes());
      assertEquals(5, bucketMetadata.getQuotaInNamespace());
      assertEquals(3, bucketMetadata.getUsedNamespace());
      assertFalse(bucketMetadata.isVersioningEnabled());
      assertEquals(BucketLayout.LEGACY, bucketMetadata.getBucketLayout());
      assertEquals("TestUser2", bucketMetadata.getOwner());


      assertEquals(1, bucketMetadata.getAcls().size());
      AclMetadata acl = bucketMetadata.getAcls().get(0);
      assertEquals("TestGroup2", acl.getName());
      assertEquals("GROUP", acl.getType());
      assertEquals("ACCESS", acl.getScope());
      assertEquals(1, acl.getAclList().size());
      assertEquals("WRITE", acl.getAclList().get(0));
      break;
    case "bucketTwo":
      assertEquals("sampleVol2", bucketMetadata.getVolumeName());
      assertEquals(StorageType.SSD, bucketMetadata.getStorageType());
      assertNull(bucketMetadata.getSourceVolume());
      assertNull(bucketMetadata.getSourceBucket());
      assertEquals(OzoneConsts.GB, bucketMetadata.getQuotaInBytes());
      assertEquals(100 * OzoneConsts.MB, bucketMetadata.getUsedBytes());
      assertEquals(5, bucketMetadata.getQuotaInNamespace());
      assertEquals(3, bucketMetadata.getUsedNamespace());
      assertFalse(bucketMetadata.isVersioningEnabled());
      assertEquals(BucketLayout.OBJECT_STORE, bucketMetadata.getBucketLayout());
      assertEquals("TestUser2", bucketMetadata.getOwner());

      assertEquals(1, bucketMetadata.getAcls().size());
      AclMetadata acl2 = bucketMetadata.getAcls().get(0);
      assertEquals("TestGroup2", acl2.getName());
      assertEquals("GROUP", acl2.getType());
      assertEquals("ACCESS", acl2.getScope());
      assertEquals(1, acl2.getAclList().size());
      assertEquals("READ", acl2.getAclList().get(0));
      break;
    default:
      fail(String.format("Bucket %s not registered",
          bucketName));
    }

  }

  @Test
  public void testGetBuckets() throws Exception {
    // Normal bucket under a volume
    Response response = bucketEndpoint.getBuckets("sampleVol2", 10, null);
    assertEquals(200, response.getStatus());
    BucketsResponse bucketUnderVolumeResponse =
        (BucketsResponse) response.getEntity();
    assertEquals(2, bucketUnderVolumeResponse.getTotalCount());
    assertEquals(2,
        bucketUnderVolumeResponse.getBuckets().size());

    bucketUnderVolumeResponse.getBuckets().forEach(bucketMetadata -> {
      try {
        testBucketResponse(bucketMetadata);
      } catch (IOException e) {
        fail(e.getMessage());
      }
    });

    // Listing all buckets
    Response response3 = bucketEndpoint.getBuckets(null, 10, null);
    assertEquals(200, response3.getStatus());
    BucketsResponse allBucketsResponse =
        (BucketsResponse) response3.getEntity();

    // Include bucketOne under sampleVol in
    // A sample volume (sampleVol) and a bucket (bucketOne) is already created
    // in initializeNewOmMetadataManager.
    assertEquals(3, allBucketsResponse.getTotalCount());
    assertEquals(3, allBucketsResponse.getBuckets().size());
  }

  @Test
  public void testGetBucketsWithPrevKeyAndLimit() throws IOException {
    // Case 1: Volume is not specified (prevKey is ignored)
    // Test limit
    Response responseLimitWithoutVolume = bucketEndpoint.getBuckets(null,
        1, null);
    assertEquals(200, responseLimitWithoutVolume.getStatus());

    BucketsResponse bucketsResponseLimitWithoutVolume =
        (BucketsResponse) responseLimitWithoutVolume.getEntity();
    assertEquals(1,
        bucketsResponseLimitWithoutVolume.getTotalCount());
    assertEquals(1,
        bucketsResponseLimitWithoutVolume.getBuckets().size());

    // Test prevKey (it should be ignored)
    Response responsePrevKeyWithoutVolume = bucketEndpoint.getBuckets(
        null, 3, "bucketOne");

    assertEquals(200,
        responsePrevKeyWithoutVolume.getStatus());

    BucketsResponse bucketResponsePrevKeyWithoutVolume =
        (BucketsResponse) responsePrevKeyWithoutVolume.getEntity();
    assertEquals(3,
        bucketResponsePrevKeyWithoutVolume.getTotalCount());
    assertEquals(3,
        bucketResponsePrevKeyWithoutVolume.getBuckets().size());
    assertEquals("sampleVol",
        bucketResponsePrevKeyWithoutVolume.getBuckets()
            .stream().findFirst().get().getVolumeName());
    assertEquals("bucketOne",
        bucketResponsePrevKeyWithoutVolume.getBuckets()
            .stream().findFirst().get().getName());

    // Case 2: Volume is specified (prevKey is not ignored)
    // Test limit
    Response responseLimitWithVolume = bucketEndpoint.getBuckets(
        "sampleVol2", 1, null);

    assertEquals(200, responseLimitWithVolume.getStatus());

    BucketsResponse bucketResponseLimitWithVolume =
        (BucketsResponse) responseLimitWithVolume.getEntity();
    assertEquals(1,
        bucketResponseLimitWithVolume.getTotalCount());
    assertEquals(1,
        bucketResponseLimitWithVolume.getBuckets().size());

    // Case 3: Test prevKey (it should not be ignored)
    Response responsePrevKeyWithVolume = bucketEndpoint.getBuckets(
        "sampleVol2", 1, "bucketOne");

    assertEquals(200, responsePrevKeyWithVolume.getStatus());

    BucketsResponse bucketPrevKeyResponseWithVolume =
        (BucketsResponse) responsePrevKeyWithVolume.getEntity();
    assertEquals(1,
        bucketPrevKeyResponseWithVolume.getTotalCount());
    assertEquals(1,
        bucketPrevKeyResponseWithVolume.getBuckets().size());
    assertEquals("sampleVol2",
        bucketPrevKeyResponseWithVolume.getBuckets()
            .stream().findFirst().get().getVolumeName());
    assertEquals("bucketTwo",
        bucketPrevKeyResponseWithVolume.getBuckets()
            .stream().findFirst().get().getName());

    // Case 4: Test volume does not exist
    Response responseVolumeNotExist = bucketEndpoint.getBuckets(
        "sampleVol3", 100, "bucketOne");

    assertEquals(200, responseVolumeNotExist.getStatus());

    BucketsResponse bucketVolumeNotExistResponse  =
        (BucketsResponse) responseVolumeNotExist.getEntity();
    assertEquals(0,
        bucketVolumeNotExistResponse.getTotalCount());
    assertEquals(0,
        bucketVolumeNotExistResponse.getBuckets().size());

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

  private BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }
}
