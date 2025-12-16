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
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDataToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDeletedDirToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDeletedKeysToOm;
import static org.apache.hadoop.ozone.recon.spi.impl.PrometheusServiceProviderImpl.PROMETHEUS_INSTANT_QUERY_API;
import static org.apache.ozone.recon.schema.generated.tables.GlobalStatsTable.GLOBAL_STATS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ExtendedDatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.PipelineID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
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
import org.apache.hadoop.hdds.utils.db.Table;
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
import org.apache.hadoop.ozone.recon.api.types.RemoveDataNodesResponseWrapper;
import org.apache.hadoop.ozone.recon.api.types.VolumeObjectDBInfo;
import org.apache.hadoop.ozone.recon.api.types.VolumesResponse;
import org.apache.hadoop.ozone.recon.common.ReconTestUtils;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconPipelineManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.ReconFileMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconGlobalStatsManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.ContainerSizeCountTask;
import org.apache.hadoop.ozone.recon.tasks.FileSizeCountKey;
import org.apache.hadoop.ozone.recon.tasks.FileSizeCountTaskFSO;
import org.apache.hadoop.ozone.recon.tasks.FileSizeCountTaskOBS;
import org.apache.hadoop.ozone.recon.tasks.OmTableInsightTask;
import org.apache.hadoop.ozone.recon.tasks.ReconOmTask;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.ozone.recon.schema.UtilizationSchemaDefinition;
import org.apache.ozone.recon.schema.generated.tables.daos.ContainerCountBySizeDao;
import org.apache.ozone.recon.schema.generated.tables.daos.GlobalStatsDao;
import org.apache.ozone.recon.schema.generated.tables.pojos.ContainerCountBySize;
import org.apache.ozone.recon.schema.generated.tables.pojos.FileCountBySize;
import org.apache.ozone.test.LambdaTestUtils;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

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
  private FileSizeCountTaskFSO fileSizeCountTaskFSO;
  private FileSizeCountTaskOBS fileSizeCountTaskOBS;
  private ContainerSizeCountTask containerSizeCountTask;
  private OmTableInsightTask omTableInsightTask;
  private ReconStorageContainerManagerFacade reconScm;
  private boolean isSetupDone = false;
  private String pipelineId;
  private DatanodeDetails datanodeDetails;
  private DatanodeDetails datanodeDetails2;
  private DatanodeDetails datanodeDetails3;
  private ReconFileMetadataManager reconFileMetadataManager;
  private DatanodeDetails datanodeDetails4;
  private long containerId = 1L;
  private ContainerReportsProto containerReportsProto;
  private ExtendedDatanodeDetailsProto extendedDatanodeDetailsProto;
  private ExtendedDatanodeDetailsProto extendedDatanodeDetailsProto3;
  private Pipeline pipeline;
  private DSLContext dslContext;
  private static final String HOST1 = "host1.datanode";
  private static final String HOST2 = "host2.datanode";
  private static final String HOST3 = "host3.datanode";
  private static final String HOST4 = "host4.datanode";
  private static final String IP1 = "1.1.1.1";
  private static final String IP2 = "2.2.2.2";
  private static final String IP3 = "3.3.3.3";
  private static final String IP4 = "4.4.4.4";
  private static final String PROMETHEUS_TEST_RESPONSE_FILE =
      "prometheus-test-response.txt";
  private ReconUtils reconUtilsMock;
  private StorageContainerLocationProtocol mockScmClient;

  private List<HddsProtos.Node> nodes = getNodeDetails(2);
  private Map<String, List<ContainerID>> containerOnDecom = getContainersOnDecomNodes();
  private ArrayList<String> metrics = getMetrics();

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
    datanodeDetails3 = randomDatanodeDetails();
    datanodeDetails4 = randomDatanodeDetails();
    datanodeDetails.setHostName(HOST1);
    datanodeDetails.setIpAddress(IP1);
    datanodeDetails2.setHostName(HOST2);
    datanodeDetails2.setIpAddress(IP2);
    datanodeDetails3.setHostName(HOST3);
    datanodeDetails3.setIpAddress(IP3);
    datanodeDetails4.setHostName(HOST4);
    datanodeDetails4.setIpAddress(IP4);
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

    mockScmClient = mock(
        StorageContainerLocationProtocol.class, Mockito.RETURNS_DEEP_STUBS);
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
        ReconTestUtils.getReconNodeDetails());
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
    ContainerCountBySizeDao containerCountBySizeDao = reconScm.getContainerCountBySizeDao();
    GlobalStatsDao globalStatsDao = getDao(GlobalStatsDao.class);
    UtilizationSchemaDefinition utilizationSchemaDefinition =
        getSchemaDefinition(UtilizationSchemaDefinition.class);
    reconFileMetadataManager = reconTestInjector.getInstance(ReconFileMetadataManager.class);
    ReconGlobalStatsManager reconGlobalStatsManager = reconTestInjector.getInstance(ReconGlobalStatsManager.class);
    utilizationEndpoint = new UtilizationEndpoint(
        containerCountBySizeDao,
        utilizationSchemaDefinition,
        reconFileMetadataManager);
    OzoneConfiguration configuration = reconTestInjector.getInstance(OzoneConfiguration.class);
    fileSizeCountTaskFSO =
        new FileSizeCountTaskFSO(reconFileMetadataManager, configuration);
    fileSizeCountTaskOBS =
        new FileSizeCountTaskOBS(reconFileMetadataManager, configuration);
    omTableInsightTask =
        new OmTableInsightTask(reconGlobalStatsManager,
            reconOMMetadataManager);
    ContainerHealthSchemaManager containerHealthSchemaManager =
        reconTestInjector.getInstance(ContainerHealthSchemaManager.class);
    clusterStateEndpoint =
        new ClusterStateEndpoint(reconScm, globalStatsDao, reconGlobalStatsManager,
            containerHealthSchemaManager, mock(OzoneConfiguration.class));
    containerSizeCountTask = reconScm.getContainerSizeCountTask();
    MetricsServiceProviderFactory metricsServiceProviderFactory =
        reconTestInjector.getInstance(MetricsServiceProviderFactory.class);
    metricsProxyEndpoint =
        new MetricsProxyEndpoint(metricsServiceProviderFactory);
    dslContext = getDslContext();
    PipelineManager pipelineManager = reconScm.getPipelineManager();
    ReconPipelineManager reconPipelineManager = (ReconPipelineManager) pipelineManager;
    reconPipelineManager.addPipeline(pipeline);
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @BeforeEach
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

    DatanodeDetailsProto datanodeDetailsProto3 =
        DatanodeDetailsProto.newBuilder()
            .setHostName(HOST3)
            .setUuid(datanodeDetails3.getUuid().toString())
            .setIpAddress(IP3)
            .build();
    extendedDatanodeDetailsProto3 =
        HddsProtos.ExtendedDatanodeDetailsProto.newBuilder()
            .setDatanodeDetails(datanodeDetailsProto3)
            .setVersion("0.6.0")
            .setSetupTime(1596347628802L)
            .setRevision("3346f493fa1690358add7bb9f3e5b52545993f36")
            .build();
    StorageReportProto storageReportProto5 =
        StorageReportProto.newBuilder().setStorageType(StorageTypeProto.DISK)
            .setStorageLocation("/disk1").setScmUsed(20000).setRemaining(7800)
            .setCapacity(50000)
            .setStorageUuid(UUID.randomUUID().toString())
            .setFailed(false).build();
    StorageReportProto storageReportProto6 =
        StorageReportProto.newBuilder().setStorageType(StorageTypeProto.DISK)
            .setStorageLocation("/disk2").setScmUsed(60000).setRemaining(10000)
            .setCapacity(80000)
            .setStorageUuid(UUID.randomUUID().toString())
            .setFailed(false).build();
    NodeReportProto nodeReportProto3 =
        NodeReportProto.newBuilder()
            .addStorageReport(storageReportProto5)
            .addStorageReport(storageReportProto6).build();

    assertDoesNotThrow(() -> {
      reconScm.getDatanodeProtocolServer()
          .register(extendedDatanodeDetailsProto, nodeReportProto,
              containerReportsProto, pipelineReportsProto, layoutInfo);
      reconScm.getDatanodeProtocolServer()
          .register(extendedDatanodeDetailsProto2, nodeReportProto2,
              ContainerReportsProto.newBuilder().build(),
              PipelineReportsProto.newBuilder().build(),
              defaultLayoutVersionProto());
      reconScm.getDatanodeProtocolServer()
          .register(extendedDatanodeDetailsProto3, nodeReportProto3,
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
            .addAcl(OzoneAcl.of(
                IAccessAuthorizer.ACLIdentityType.USER,
                "TestUser2",
                OzoneAcl.AclScope.ACCESS, IAccessAuthorizer.ACLType.WRITE
            ))
            .addAcl(OzoneAcl.of(
                IAccessAuthorizer.ACLIdentityType.USER,
                "TestUser2",
                OzoneAcl.AclScope.ACCESS, IAccessAuthorizer.ACLType.READ
            ))
            .build();
    reconOMMetadataManager.getVolumeTable().put(volumeKey, args);

    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("sampleVol2")
        .setBucketName("bucketOne")
        .addAcl(OzoneAcl.of(
            IAccessAuthorizer.ACLIdentityType.GROUP,
            "TestGroup2",
            OzoneAcl.AclScope.ACCESS, IAccessAuthorizer.ACLType.WRITE
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
        .addAcl(OzoneAcl.of(
            IAccessAuthorizer.ACLIdentityType.GROUP,
            "TestGroup2",
            OzoneAcl.AclScope.ACCESS, IAccessAuthorizer.ACLType.READ
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
    case HOST3:
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
    assertEquals(3, datanodesResponse.getTotalCount());
    assertEquals(3, datanodesResponse.getDatanodes().size());

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
        nodeManager.getNode(datanodeDetails.getID());
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
    assertEquals(3, clusterStateResponse.getTotalDatanodes());
    assertEquals(3, clusterStateResponse.getHealthyDatanodes());
    assertEquals(0, clusterStateResponse.getMissingContainers());

    waitAndCheckConditionAfterHeartbeat(() -> {
      Response response1 = clusterStateEndpoint.getClusterState();
      ClusterStateResponse clusterStateResponse1 =
          (ClusterStateResponse) response1.getEntity();
      return (clusterStateResponse1.getContainers() == 1);
    });
    omTableInsightTask.init();
    // check volume, bucket and key count after running table count task
    ReconOmTask.TaskResult result =
        omTableInsightTask.reprocess(reconOMMetadataManager);
    assertTrue(result.isTaskSuccess());
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
    final Table.KeyValue mockKeyValueLegacy = mock(Table.KeyValue.class);
    final Table.KeyValue mockKeyValueFso = mock(Table.KeyValue.class);

    when(keyTableLegacy.iterator()).thenReturn(mockKeyIterLegacy);
    when(keyTableFso.iterator()).thenReturn(mockKeyIterFso);

    when(omMetadataManager.getKeyTable(BucketLayout.OBJECT_STORE)).thenReturn(
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

    // Call reprocess on both endpoints.
    ReconOmTask.TaskResult resultOBS = fileSizeCountTaskOBS.reprocess(omMetadataManager);
    ReconOmTask.TaskResult resultFSO = fileSizeCountTaskFSO.reprocess(omMetadataManager);
    assertTrue(resultOBS.isTaskSuccess());
    assertTrue(resultFSO.isTaskSuccess());

    // Verify RocksDB data directly:
    // For vol1/bucket1 with fileSize 1000L, the upper bound is 1024L and expected count is 2.
    FileSizeCountKey rocksKey1 = new FileSizeCountKey("vol1", "bucket1", 1024L);
    Long rocksCount1 = reconFileMetadataManager.getFileSizeCount(rocksKey1);
    assertNotNull(rocksCount1, "Expected RocksDB bin 1024 to exist for vol1/bucket1");
    assertEquals(2L, rocksCount1.longValue(), "Expected RocksDB bin 1024 to have count 2 for vol1/bucket1");

    // For vol1/bucket1 with fileSize 100000L, the upper bound is 131072L and expected count is 2.
    FileSizeCountKey rocksKey2 = new FileSizeCountKey("vol1", "bucket1", 131072L);
    Long rocksCount2 = reconFileMetadataManager.getFileSizeCount(rocksKey2);
    assertNotNull(rocksCount2, "Expected RocksDB bin 131072 to exist for vol1/bucket1");
    assertEquals(2L, rocksCount2.longValue(), "Expected RocksDB bin 131072 to have count 2 for vol1/bucket1");

    // For vol2/bucket1 with fileSize 1000L, the upper bound is 1024L and expected count is 2.
    FileSizeCountKey rocksKey3 = new FileSizeCountKey("vol2", "bucket1", 1024L);
    Long rocksCount3 = reconFileMetadataManager.getFileSizeCount(rocksKey3);
    assertNotNull(rocksCount3, "Expected RocksDB bin 1024 to exist for vol2/bucket1");
    assertEquals(2L, rocksCount3.longValue(), "Expected RocksDB bin 1024 to have count 2 for vol2/bucket1");

    // --- Now test the query endpoints of the utilization service ---
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

    // Test for "volume" query param.
    response = utilizationEndpoint.getFileCounts("vol1", null, 0);
    resultSet = (List<FileCountBySize>) response.getEntity();
    assertEquals(2, resultSet.size());
    assertTrue(resultSet.stream().allMatch(o -> o.getVolume().equals("vol1")));

    // Test for non-existent volume.
    response = utilizationEndpoint.getFileCounts("vol", null, 0);
    resultSet = (List<FileCountBySize>) response.getEntity();
    assertEquals(0, resultSet.size());

    // Test for "volume" + "bucket" query param.
    response = utilizationEndpoint.getFileCounts("vol1", "bucket1", 0);
    resultSet = (List<FileCountBySize>) response.getEntity();
    assertEquals(2, resultSet.size());
    assertTrue(resultSet.stream().allMatch(o -> o.getVolume().equals("vol1") &&
        o.getBucket().equals("bucket1")));

    // Test for non-existent bucket.
    response = utilizationEndpoint.getFileCounts("vol1", "bucket", 0);
    resultSet = (List<FileCountBySize>) response.getEntity();
    assertEquals(0, resultSet.size());

    // Test for "volume" + "bucket" + "fileSize" query params.
    response = utilizationEndpoint.getFileCounts("vol1", "bucket1", 131072);
    resultSet = (List<FileCountBySize>) response.getEntity();
    assertEquals(1, resultSet.size());
    FileCountBySize o = resultSet.get(0);
    assertTrue(o.getVolume().equals("vol1") && o.getBucket().equals("bucket1") &&
        o.getFileSize() == 131072);

    // Test for non-existent fileSize.
    response = utilizationEndpoint.getFileCounts("vol1", "bucket1", 1310725);
    resultSet = (List<FileCountBySize>) response.getEntity();
    assertEquals(0, resultSet.size());
  }

  @Test
  public void testGetContainerCounts() throws Exception {
    // Mock container info objects with different sizes
    ContainerInfo omContainerInfo1 = mock(ContainerInfo.class);
    given(omContainerInfo1.containerID()).willReturn(ContainerID.valueOf(1));
    given(omContainerInfo1.getUsedBytes()).willReturn(1500000000L); // 1.5GB
    given(omContainerInfo1.getState()).willReturn(LifeCycleState.OPEN);

    ContainerInfo omContainerInfo2 = mock(ContainerInfo.class);
    given(omContainerInfo2.containerID()).willReturn(ContainerID.valueOf(2));
    given(omContainerInfo2.getUsedBytes()).willReturn(2500000000L); // 2.5GB
    given(omContainerInfo2.getState()).willReturn(LifeCycleState.OPEN);

    // Create a list of container info objects
    List<ContainerInfo> containers = new ArrayList<>();
    containers.add(omContainerInfo1);
    containers.add(omContainerInfo2);

    // Process the list of containers through the container size count task
    containerSizeCountTask.processContainers(containers);

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

  @Test
  public void testExplicitRemovalOfDecommissionedNode() throws Exception {
    Response response = nodeEndpoint.getDatanodes();

    DatanodesResponse datanodesResponse =
        (DatanodesResponse) response.getEntity();
    assertEquals(3, datanodesResponse.getTotalCount());
    assertEquals(3, datanodesResponse.getDatanodes().size());

    // Change Node3 OperationalState with NodeManager to NodeOperationalState.DECOMMISSIONED
    final NodeManager nodeManager = reconScm.getScmNodeManager();
    final DatanodeDetails dnDetailsInternal =
        nodeManager.getNode(datanodeDetails3.getID());
    // Backup existing state and sanity check
    final NodeStatus nStatus = nodeManager.getNodeStatus(dnDetailsInternal);
    final NodeOperationalState backupOpState =
        dnDetailsInternal.getPersistedOpState();
    final long backupOpStateExpiry =
        dnDetailsInternal.getPersistedOpStateExpiryEpochSec();
    assertEquals(backupOpState, nStatus.getOperationalState());
    assertEquals(backupOpStateExpiry, nStatus.getOpStateExpiryEpochSeconds());

    dnDetailsInternal.setPersistedOpState(NodeOperationalState.DECOMMISSIONED);
    dnDetailsInternal.setPersistedOpStateExpiryEpochSec(666L);
    nodeManager.setNodeOperationalState(dnDetailsInternal,
        NodeOperationalState.DECOMMISSIONED, 666L);

    Response removedDNResponse = nodeEndpoint.removeDatanodes(Arrays.asList(datanodeDetails3.getUuid().toString()));

    RemoveDataNodesResponseWrapper removeDataNodesResponseWrapper =
        (RemoveDataNodesResponseWrapper) removedDNResponse.getEntity();
    DatanodesResponse errorDataNodes = removeDataNodesResponseWrapper.getDatanodesResponseMap().get("failedDatanodes");
    DatanodesResponse removedNodes = removeDataNodesResponseWrapper.getDatanodesResponseMap().get("removedDatanodes");

    assertEquals(1, errorDataNodes.getTotalCount());
    assertNull(removedNodes);
    errorDataNodes.getDatanodes().forEach(datanodeMetadata -> {
      assertEquals("host3.datanode", datanodeMetadata.getHostname());
    });
  }

  @Test
  public void testExplicitRemovalOfInvalidStateNode() {
    String dnUUID = datanodeDetails2.getUuid().toString();
    Response removedDNResponse = nodeEndpoint.removeDatanodes(Arrays.asList(dnUUID));
    RemoveDataNodesResponseWrapper removeDataNodesResponseWrapper =
        (RemoveDataNodesResponseWrapper) removedDNResponse.getEntity();
    Map<String, DatanodesResponse> datanodesResponseMap = removeDataNodesResponseWrapper.getDatanodesResponseMap();
    assertFalse(datanodesResponseMap.isEmpty());
    DatanodesResponse failedDatanodes = datanodesResponseMap.get("failedDatanodes");
    Map<String, String> failedNodeErrorResponseMap = failedDatanodes.getFailedNodeErrorResponseMap();
    assertFalse(failedNodeErrorResponseMap.isEmpty());
    String nodeError = failedNodeErrorResponseMap.get(dnUUID);
    assertNotNull(nodeError);
    assertEquals("DataNode should be in DEAD node status.", nodeError);
    assertEquals(Response.Status.OK.getStatusCode(), removedDNResponse.getStatus());
  }

  @Test
  public void testExplicitRemovalOfNonExistingNode() {
    String dnUUID = datanodeDetails4.getUuid().toString();
    Response removedDNResponse = nodeEndpoint.removeDatanodes(Arrays.asList(dnUUID));
    RemoveDataNodesResponseWrapper removeDataNodesResponseWrapper =
        (RemoveDataNodesResponseWrapper) removedDNResponse.getEntity();
    DatanodesResponse notFoundDatanodes = removeDataNodesResponseWrapper.getDatanodesResponseMap()
        .get("notFoundDatanodes");
    assertEquals(1, notFoundDatanodes.getTotalCount());
    Collection<DatanodeMetadata> datanodes = notFoundDatanodes.getDatanodes();
    assertEquals(1, datanodes.size());
    DatanodeMetadata datanodeMetadata = datanodes.stream().findFirst().get();
    assertEquals(dnUUID, datanodeMetadata.getUuid());
  }

  @Test
  public void testSuccessWhenDecommissionStatus() throws IOException {
    when(mockScmClient.queryNode(any(), any(), any(), any(), any(Integer.class))).thenReturn(
        nodes); // 2 nodes decommissioning
    when(mockScmClient.getContainersOnDecomNode(any())).thenReturn(containerOnDecom);
    when(mockScmClient.getMetrics(any())).thenReturn(metrics.get(1));
    Response datanodesDecommissionInfo = nodeEndpoint.getDatanodesDecommissionInfo();
    Map<String, Object> responseMap = (Map<String, Object>) datanodesDecommissionInfo.getEntity();
    List<Map<String, Object>> dnDecommissionInfo =
        (List<Map<String, Object>>) responseMap.get("DatanodesDecommissionInfo");
    DatanodeDetails datanode = (DatanodeDetails) dnDecommissionInfo.get(0).get("datanodeDetails");
    Map<String, Object> dnMetrics = (Map<String, Object>) dnDecommissionInfo.get(0).get("metrics");
    Map<String, Object> containers = (Map<String, Object>) dnDecommissionInfo.get(0).get("containers");
    assertNotNull(datanode);
    assertNotNull(dnMetrics);
    assertNotNull(containers);
    assertFalse(datanode.getUuidString().isEmpty());
    assertFalse(((String) dnMetrics.get("decommissionStartTime")).isEmpty());
    assertEquals(1, dnMetrics.get("numOfUnclosedPipelines"));
    assertEquals(3.0, dnMetrics.get("numOfUnderReplicatedContainers"));
    assertEquals(3.0, dnMetrics.get("numOfUnclosedContainers"));

    assertEquals(3, ((List<String>) containers.get("UnderReplicated")).size());
    assertEquals(3, ((List<String>) containers.get("UnClosed")).size());
  }

  @Test
  public void testSuccessWhenDecommissionStatusWithUUID() throws IOException {
    when(mockScmClient.queryNode(any(), any(), any(), any(), any(Integer.class))).thenReturn(
        getNodeDetailsForUuid("654c4b89-04ef-4015-8a3b-50d0fb0e1684")); // 1 nodes decommissioning
    when(mockScmClient.getContainersOnDecomNode(any())).thenReturn(containerOnDecom);
    Response datanodesDecommissionInfo =
        nodeEndpoint.getDecommissionInfoForDatanode("654c4b89-04ef-4015-8a3b-50d0fb0e1684", "");
    Map<String, Object> responseMap = (Map<String, Object>) datanodesDecommissionInfo.getEntity();
    List<Map<String, Object>> dnDecommissionInfo =
        (List<Map<String, Object>>) responseMap.get("DatanodesDecommissionInfo");
    DatanodeDetails datanode = (DatanodeDetails) dnDecommissionInfo.get(0).get("datanodeDetails");
    Map<String, Object> containers = (Map<String, Object>) dnDecommissionInfo.get(0).get("containers");
    assertNotNull(datanode);
    assertNotNull(containers);
    assertFalse(datanode.getUuidString().isEmpty());
    assertEquals("654c4b89-04ef-4015-8a3b-50d0fb0e1684", datanode.getUuidString());

    assertEquals(3, ((List<String>) containers.get("UnderReplicated")).size());
    assertEquals(3, ((List<String>) containers.get("UnClosed")).size());
  }

  private List<HddsProtos.Node> getNodeDetailsForUuid(String uuid) {
    List<HddsProtos.Node> nodesList = new ArrayList<>();

    HddsProtos.DatanodeDetailsProto.Builder dnd =
        HddsProtos.DatanodeDetailsProto.newBuilder();
    dnd.setHostName("hostName");
    dnd.setIpAddress("1.2.3.5");
    dnd.setNetworkLocation("/default");
    dnd.setNetworkName("hostName");
    dnd.addPorts(HddsProtos.Port.newBuilder()
        .setName("ratis").setValue(5678).build());
    dnd.setUuid(uuid);

    HddsProtos.Node.Builder builder = HddsProtos.Node.newBuilder();
    builder.addNodeOperationalStates(
        HddsProtos.NodeOperationalState.DECOMMISSIONING);
    builder.addNodeStates(HddsProtos.NodeState.HEALTHY);
    builder.setNodeID(dnd.build());
    nodesList.add(builder.build());
    return nodesList;
  }

  private List<HddsProtos.Node> getNodeDetails(int n) {
    List<HddsProtos.Node> nodesList = new ArrayList<>();

    for (int i = 0; i < n; i++) {
      HddsProtos.DatanodeDetailsProto.Builder dnd =
          HddsProtos.DatanodeDetailsProto.newBuilder();
      dnd.setHostName("host" + i);
      dnd.setIpAddress("1.2.3." + i + 1);
      dnd.setNetworkLocation("/default");
      dnd.setNetworkName("host" + i);
      dnd.addPorts(HddsProtos.Port.newBuilder()
          .setName("ratis").setValue(5678).build());
      dnd.setUuid(UUID.randomUUID().toString());

      HddsProtos.Node.Builder builder  = HddsProtos.Node.newBuilder();
      builder.addNodeOperationalStates(
          HddsProtos.NodeOperationalState.DECOMMISSIONING);
      builder.addNodeStates(HddsProtos.NodeState.HEALTHY);
      builder.setNodeID(dnd.build());
      nodesList.add(builder.build());
    }
    return nodesList;
  }

  private Map<String, List<ContainerID>> getContainersOnDecomNodes() {
    Map<String, List<ContainerID>> containerMap = new HashMap<>();
    List<ContainerID> underReplicated = new ArrayList<>();
    underReplicated.add(ContainerID.valueOf(1L));
    underReplicated.add(ContainerID.valueOf(2L));
    underReplicated.add(ContainerID.valueOf(3L));
    containerMap.put("UnderReplicated", underReplicated);
    List<ContainerID> unclosed = new ArrayList<>();
    unclosed.add(ContainerID.valueOf(10L));
    unclosed.add(ContainerID.valueOf(11L));
    unclosed.add(ContainerID.valueOf(12L));
    containerMap.put("UnClosed", unclosed);
    return containerMap;
  }

  private ArrayList<String> getMetrics() {
    ArrayList<String> result = new ArrayList<>();
    // no nodes decommissioning
    result.add("{  \"beans\" : [ {    " +
        "\"name\" : \"Hadoop:service=StorageContainerManager,name=NodeDecommissionMetrics\",    " +
        "\"modelerType\" : \"NodeDecommissionMetrics\",    \"DecommissioningMaintenanceNodesTotal\" : 0,    " +
        "\"RecommissionNodesTotal\" : 0,    \"PipelinesWaitingToCloseTotal\" : 0,    " +
        "\"ContainersUnderReplicatedTotal\" : 0,    \"ContainersUnClosedTotal\" : 0,    " +
        "\"ContainersSufficientlyReplicatedTotal\" : 0  } ]}");
    // 2 nodes in decommisioning
    result.add("{  \"beans\" : [ {    " +
        "\"name\" : \"Hadoop:service=StorageContainerManager,name=NodeDecommissionMetrics\",    " +
        "\"modelerType\" : \"NodeDecommissionMetrics\",    \"DecommissioningMaintenanceNodesTotal\" : 2,    " +
        "\"RecommissionNodesTotal\" : 0,    \"PipelinesWaitingToCloseTotal\" : 2,    " +
        "\"ContainersUnderReplicatedTotal\" : 6,    \"ContainersUnclosedTotal\" : 6,    " +
        "\"ContainersSufficientlyReplicatedTotal\" : 10,   " +
        "\"tag.datanode.1\" : \"host0\",    \"tag.Hostname.1\" : \"host0\",    " +
        "\"PipelinesWaitingToCloseDN.1\" : 1,    \"UnderReplicatedDN.1\" : 3,    " +
        "\"SufficientlyReplicatedDN.1\" : 0,    \"UnclosedContainersDN.1\" : 3,    \"StartTimeDN.1\" : 111211,    " +
        "\"tag.datanode.2\" : \"host1\",    \"tag.Hostname.2\" : \"host1\",    " +
        "\"PipelinesWaitingToCloseDN.2\" : 1,    \"UnderReplicatedDN.2\" : 3,    " +
        "\"SufficientlyReplicatedDN.2\" : 0,    \"UnclosedContainersDN.2\" : 3,    \"StartTimeDN.2\" : 221221} ]}");
    // only host 1 decommissioning
    result.add("{  \"beans\" : [ {    " +
        "\"name\" : \"Hadoop:service=StorageContainerManager,name=NodeDecommissionMetrics\",    " +
        "\"modelerType\" : \"NodeDecommissionMetrics\",    \"DecommissioningMaintenanceNodesTotal\" : 1,    " +
        "\"RecommissionNodesTotal\" : 0,    \"PipelinesWaitingToCloseTotal\" : 1,    " +
        "\"ContainersUnderReplicatedTotal\" : 3,    \"ContainersUnclosedTotal\" : 3,    " +
        "\"ContainersSufficientlyReplicatedTotal\" : 10,   " +
        "\"tag.datanode.1\" : \"host0\",\n    \"tag.Hostname.1\" : \"host0\",\n    " +
        "\"PipelinesWaitingToCloseDN.1\" : 1,\n    \"UnderReplicatedDN.1\" : 3,\n    " +
        "\"SufficientlyReplicatedDN.1\" : 0,\n    \"UnclosedContainersDN.1\" : 3,    \"StartTimeDN.1\" : 221221} ]}");
    return result;
  }
}
