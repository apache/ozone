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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getOmKeyLocationInfo;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getRandomPipeline;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDataToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeKeyToOm;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.ContainerChecksums;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerStateManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.ContainerDiscrepancyInfo;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.api.types.ContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.ContainersResponse;
import org.apache.hadoop.ozone.recon.api.types.DeletedContainerInfo;
import org.apache.hadoop.ozone.recon.api.types.KeyMetadata;
import org.apache.hadoop.ozone.recon.api.types.KeysResponse;
import org.apache.hadoop.ozone.recon.api.types.MissingContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.MissingContainersResponse;
import org.apache.hadoop.ozone.recon.api.types.UnhealthyContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.UnhealthyContainersResponse;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.persistence.ContainerHistory;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.scm.ReconPipelineManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.ContainerKeyMapperHelper;
import org.apache.hadoop.ozone.recon.tasks.ContainerKeyMapperTaskFSO;
import org.apache.hadoop.ozone.recon.tasks.ContainerKeyMapperTaskOBS;
import org.apache.hadoop.ozone.recon.tasks.NSSummaryTaskWithFSO;
import org.apache.hadoop.ozone.recon.tasks.ReconOmTask;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.apache.ozone.recon.schema.generated.tables.pojos.UnhealthyContainers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for container endpoint.
 */
public class TestContainerEndpoint {

  @TempDir
  private Path temporaryFolder;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestContainerEndpoint.class);

  private ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private ReconContainerManager reconContainerManager;
  private ContainerStateManager containerStateManager;
  private ReconPipelineManager reconPipelineManager;
  private ReconContainerMetadataManager reconContainerMetadataManager;
  private ContainerEndpoint containerEndpoint;
  private boolean isSetupDone = false;
  private ContainerHealthSchemaManager containerHealthSchemaManager;
  private ReconOMMetadataManager reconOMMetadataManager;
  private OzoneConfiguration omConfiguration;

  private ContainerID containerID = ContainerID.valueOf(1L);
  private Pipeline pipeline;
  private PipelineID pipelineID;
  private long keyCount = 5L;
  private static final String FSO_KEY_NAME1 = "dir1/file7";
  private static final String FSO_KEY_NAME2 = "dir1/dir2/file8";
  private static final String FSO_KEY_NAME3 = "dir1/dir2/file9";
  private static final String FSO_KEY_NAME4 = "dir1/dir2/dir3/file10";
  private static final String BUCKET_NAME = "fsoBucket";
  private static final String VOLUME_NAME = "sampleVol2";
  private static final String FILE_NAME1 = "file7";
  private static final String FILE_NAME2 = "file8";
  private static final String FILE_NAME3 = "file9";
  private static final String FILE_NAME4 = "file10";
  private static final long FILE_ONE_OBJECT_ID = 13L;
  private static final long FILE_TWO_OBJECT_ID = 14L;
  private static final long FILE_THREE_OBJECT_ID = 15L;
  private static final long FILE_FOUR_OBJECT_ID = 16L;
  private static final long PARENT_OBJECT_ID = 2L;  // dir1 objectID
  private static final long PARENT_OBJECT_ID2 = 3L; // dir2 objectID
  private static final long PARENT_OBJECT_ID3 = 4L; // dir3 objectID
  private static final long BUCKET_OBJECT_ID = 1L;  // fsoBucket objectID
  private static final long VOL_OBJECT_ID = 0L;    // sampleVol2 objectID
  private static final long CONTAINER_ID_1 = 20L;
  private static final long CONTAINER_ID_2 = 21L;
  private static final long CONTAINER_ID_3 = 22L;
  private static final long LOCAL_ID = 0L;
  private static final long KEY_ONE_SIZE = 500L; // 500 bytes

  private UUID uuid1;
  private UUID uuid2;
  private UUID uuid3;
  private UUID uuid4;

  private void initializeInjector() throws Exception {
    reconOMMetadataManager = getTestReconOmMetadataManager(
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
            .addBinding(ContainerEndpoint.class)
            .addBinding(ContainerHealthSchemaManager.class)
            .build();

    OzoneStorageContainerManager ozoneStorageContainerManager =
        reconTestInjector.getInstance(OzoneStorageContainerManager.class);
    reconContainerManager = (ReconContainerManager)
        ozoneStorageContainerManager.getContainerManager();
    reconPipelineManager = (ReconPipelineManager)
        ozoneStorageContainerManager.getPipelineManager();
    reconContainerMetadataManager =
        reconTestInjector.getInstance(ReconContainerMetadataManager.class);
    containerEndpoint = reconTestInjector.getInstance(ContainerEndpoint.class);
    containerHealthSchemaManager =
        reconTestInjector.getInstance(ContainerHealthSchemaManager.class);
    this.reconNamespaceSummaryManager =
        reconTestInjector.getInstance(ReconNamespaceSummaryManager.class);

    pipeline = getRandomPipeline();
    pipelineID = pipeline.getId();
    reconPipelineManager.addPipeline(pipeline);
    containerStateManager = reconContainerManager
        .getContainerStateManager();
  }

  @BeforeEach
  public void setUp() throws Exception {
    // The following setup runs only once
    if (!isSetupDone) {
      initializeInjector();
      isSetupDone = true;
    } else {
      // Clear shared state before subsequent tests to prevent data leakage
      ContainerKeyMapperHelper.clearSharedContainerCountMap();
      ReconConstants.resetTableTruncatedFlags();

      // Reinitialize container tables to clear RocksDB data
      reconContainerMetadataManager.reinitWithNewContainerDataFromOm(Collections.emptyMap());
    }

    omConfiguration = new OzoneConfiguration();

    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();
    BlockID blockID1 = new BlockID(1, 101);
    OmKeyLocationInfo omKeyLocationInfo1 = getOmKeyLocationInfo(blockID1,
        pipeline);
    omKeyLocationInfoList.add(omKeyLocationInfo1);

    BlockID blockID2 = new BlockID(2, 102);
    OmKeyLocationInfo omKeyLocationInfo2 = getOmKeyLocationInfo(blockID2,
        pipeline);
    omKeyLocationInfoList.add(omKeyLocationInfo2);

    OmKeyLocationInfoGroup omKeyLocationInfoGroup = new
        OmKeyLocationInfoGroup(0, omKeyLocationInfoList);

    //key = key_one, Blocks = [ {CID = 1, LID = 101}, {CID = 2, LID = 102} ]
    writeDataToOm(reconOMMetadataManager,
        "key_one", "bucketOne", "sampleVol",
        Collections.singletonList(omKeyLocationInfoGroup));

    List<OmKeyLocationInfoGroup> infoGroups = new ArrayList<>();
    BlockID blockID3 = new BlockID(1, 103);
    OmKeyLocationInfo omKeyLocationInfo3 = getOmKeyLocationInfo(blockID3,
        pipeline);

    List<OmKeyLocationInfo> omKeyLocationInfoListNew = new ArrayList<>();
    omKeyLocationInfoListNew.add(omKeyLocationInfo3);
    infoGroups.add(new OmKeyLocationInfoGroup(0,
        omKeyLocationInfoListNew));

    BlockID blockID4 = new BlockID(1, 104);
    OmKeyLocationInfo omKeyLocationInfo4 = getOmKeyLocationInfo(blockID4,
        pipeline);

    omKeyLocationInfoListNew = new ArrayList<>();
    omKeyLocationInfoListNew.add(omKeyLocationInfo4);
    infoGroups.add(new OmKeyLocationInfoGroup(1,
        omKeyLocationInfoListNew));

    //key = key_two, Blocks = [ {CID = 1, LID = 103}, {CID = 1, LID = 104} ]
    writeDataToOm(reconOMMetadataManager,
        "key_two", "bucketOne", "sampleVol", infoGroups);

    List<OmKeyLocationInfo> omKeyLocationInfoList2 = new ArrayList<>();
    BlockID blockID5 = new BlockID(2, 2);
    OmKeyLocationInfo omKeyLocationInfo5 = getOmKeyLocationInfo(blockID5,
        pipeline);
    omKeyLocationInfoList2.add(omKeyLocationInfo5);

    BlockID blockID6 = new BlockID(2, 3);
    OmKeyLocationInfo omKeyLocationInfo6 = getOmKeyLocationInfo(blockID6,
        pipeline);
    omKeyLocationInfoList2.add(omKeyLocationInfo6);

    OmKeyLocationInfoGroup omKeyLocationInfoGroup2 = new
        OmKeyLocationInfoGroup(0, omKeyLocationInfoList2);

    //key = key_three, Blocks = [ {CID = 2, LID = 2}, {CID = 2, LID = 3} ]
    writeDataToOm(reconOMMetadataManager,
        "key_three", "bucketOne", "sampleVol",
        Collections.singletonList(omKeyLocationInfoGroup2));

    //Generate Recon container DB data.
    OMMetadataManager omMetadataManagerMock = mock(OMMetadataManager.class);
    Table keyTableMock = mock(Table.class);
    Table fileTableMock = mock(Table.class);

    when(keyTableMock.getName()).thenReturn("KeyTable");
    when(fileTableMock.getName()).thenReturn("FileTable");

    when(omMetadataManagerMock.getKeyTable(BucketLayout.LEGACY))
        .thenReturn(keyTableMock);

    when(omMetadataManagerMock.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED))
        .thenReturn(fileTableMock);

    reprocessContainerKeyMapper();
  }

  private void reprocessContainerKeyMapper() throws Exception {
    ContainerKeyMapperTaskOBS containerKeyMapperTaskOBS =
        new ContainerKeyMapperTaskOBS(reconContainerMetadataManager, omConfiguration);
    ContainerKeyMapperTaskFSO containerKeyMapperTaskFSO =
        new ContainerKeyMapperTaskFSO(reconContainerMetadataManager, omConfiguration);

    // Run both tasks in parallel (like production)
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<ReconOmTask.TaskResult> obsFuture = executor.submit(
          () -> containerKeyMapperTaskOBS.reprocess(reconOMMetadataManager));
      Future<ReconOmTask.TaskResult> fsoFuture = executor.submit(
          () -> containerKeyMapperTaskFSO.reprocess(reconOMMetadataManager));

      // Wait for both to complete
      obsFuture.get();
      fsoFuture.get();
    } finally {
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  private void setUpFSOData() throws IOException {

    // Create another new volume and add it to the volume table
    String volumeKey = reconOMMetadataManager.getVolumeKey(VOLUME_NAME);
    OmVolumeArgs args = OmVolumeArgs.newBuilder()
        .setVolume(VOLUME_NAME)
        .setAdminName("TestUser")
        .setOwnerName("TestUser")
        .setObjectID(0L)
        .build();
    reconOMMetadataManager.getVolumeTable().put(volumeKey, args);

    // Create another new bucket and add it to the bucket table
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .setObjectID(1L)
        .build();
    String bucketKey =
        reconOMMetadataManager.getBucketKey(bucketInfo.getVolumeName(),
            bucketInfo.getBucketName());
    reconOMMetadataManager.getBucketTable().put(bucketKey, bucketInfo);

    // Create a new directory and add it to the directory table
    OmDirectoryInfo dirInfo1 = OmDirectoryInfo.newBuilder()
        .setName("dir1")
        .setParentObjectID(1L)
        .setUpdateID(1L)
        .setObjectID(2L)
        .build();
    OmDirectoryInfo dirInfo2 = OmDirectoryInfo.newBuilder()
        .setName("dir2")
        .setParentObjectID(1L)
        .setUpdateID(1L)
        .setObjectID(3L)
        .build();
    String dirKey1 = reconOMMetadataManager.getOzonePathKey(0, 1, 1L, "dir1");
    String dirKey2 = reconOMMetadataManager.getOzonePathKey(0, 1, 2L, "dir2");
    reconOMMetadataManager.getDirectoryTable().put(dirKey1, dirInfo1);
    reconOMMetadataManager.getDirectoryTable().put(dirKey2, dirInfo2);

    OmKeyLocationInfoGroup locationInfoGroup =
        getLocationInfoGroup1();

    // add the multi-block key to Recon's OM
    writeKeyToOm(reconOMMetadataManager,
        FSO_KEY_NAME1,
        BUCKET_NAME,
        VOLUME_NAME,
        FILE_NAME1,
        FILE_ONE_OBJECT_ID,
        PARENT_OBJECT_ID,
        BUCKET_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(locationInfoGroup),
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        KEY_ONE_SIZE);

    // add the multi-block key to Recon's OM
    writeKeyToOm(reconOMMetadataManager,
        FSO_KEY_NAME2,
        BUCKET_NAME,
        VOLUME_NAME,
        FILE_NAME2,
        FILE_TWO_OBJECT_ID,
        PARENT_OBJECT_ID2,
        BUCKET_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(locationInfoGroup),
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        KEY_ONE_SIZE);

    // add the multi-block key to Recon's OM
    writeKeyToOm(reconOMMetadataManager,
        FSO_KEY_NAME3,
        BUCKET_NAME,
        VOLUME_NAME,
        FILE_NAME3,
        FILE_THREE_OBJECT_ID,
        PARENT_OBJECT_ID2,
        BUCKET_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(locationInfoGroup),
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        KEY_ONE_SIZE);

    // add the multi-block key to Recon's OM
    writeKeyToOm(reconOMMetadataManager,
        FSO_KEY_NAME4,
        BUCKET_NAME,
        VOLUME_NAME,
        FILE_NAME4,
        FILE_FOUR_OBJECT_ID,
        PARENT_OBJECT_ID3,
        BUCKET_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(locationInfoGroup),
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        KEY_ONE_SIZE);
  }

  private OmKeyLocationInfoGroup getLocationInfoGroup1() {
    List<OmKeyLocationInfo> locationInfoList = new ArrayList<>();
    BlockID block1 = new BlockID(CONTAINER_ID_1, LOCAL_ID);
    BlockID block2 = new BlockID(CONTAINER_ID_2, LOCAL_ID);
    BlockID block3 = new BlockID(CONTAINER_ID_3, LOCAL_ID);

    OmKeyLocationInfo location1 = new OmKeyLocationInfo.Builder()
        .setBlockID(block1)
        .setLength(1000L)
        .build();
    OmKeyLocationInfo location2 = new OmKeyLocationInfo.Builder()
        .setBlockID(block2)
        .setLength(2000L)
        .build();
    OmKeyLocationInfo location3 = new OmKeyLocationInfo.Builder()
        .setBlockID(block3)
        .setLength(3000L)
        .build();
    locationInfoList.add(location1);
    locationInfoList.add(location2);
    locationInfoList.add(location3);

    return new OmKeyLocationInfoGroup(0L, locationInfoList);
  }

  @Test
  public void testGetKeysForContainer() throws Exception {
    Response response = containerEndpoint.getKeysForContainer(1L, -1, "");

    KeysResponse data = (KeysResponse) response.getEntity();
    Collection<KeyMetadata> keyMetadataList = data.getKeys();

    assertEquals(3, data.getTotalCount());
    assertEquals(2, keyMetadataList.size());

    Iterator<KeyMetadata> iterator = keyMetadataList.iterator();

    KeyMetadata keyMetadata = iterator.next();
    assertEquals("key_one", keyMetadata.getKey());
    assertEquals(1, keyMetadata.getVersions().size());
    assertEquals(1, keyMetadata.getBlockIds().size());
    Map<Long, List<KeyMetadata.ContainerBlockMetadata>> blockIds =
        keyMetadata.getBlockIds();
    assertEquals(101, blockIds.get(0L).iterator().next().getLocalID());

    keyMetadata = iterator.next();
    assertEquals("key_two", keyMetadata.getKey());
    assertEquals(2, keyMetadata.getVersions().size());
    assertThat(keyMetadata.getVersions()).contains(0L, 1L);
    assertEquals(2, keyMetadata.getBlockIds().size());
    blockIds = keyMetadata.getBlockIds();
    assertEquals(103, blockIds.get(0L).iterator().next().getLocalID());
    assertEquals(104, blockIds.get(1L).iterator().next().getLocalID());

    response = containerEndpoint.getKeysForContainer(3L, -1, "");
    data = (KeysResponse) response.getEntity();
    keyMetadataList = data.getKeys();
    assertThat(keyMetadataList).isEmpty();
    assertEquals(0, data.getTotalCount());

    // test if limit works as expected
    response = containerEndpoint.getKeysForContainer(1L, 1, "");
    data = (KeysResponse) response.getEntity();
    keyMetadataList = data.getKeys();
    assertEquals(1, keyMetadataList.size());
    assertEquals(3, data.getTotalCount());

    // Now to check if the ContainerEndpoint also reads the File table
    // Set up test data for FSO keys
    setUpFSOData();
    NSSummaryTaskWithFSO nSSummaryTaskWithFso =
        new NSSummaryTaskWithFSO(reconNamespaceSummaryManager, reconOMMetadataManager, 10, 5, 20, 2000);
    nSSummaryTaskWithFso.reprocessWithFSO(reconOMMetadataManager);
    // Reprocess the container key mapper to ensure the latest mapping is used
    reprocessContainerKeyMapper();
    response = containerEndpoint.getKeysForContainer(20L, -1, "");

    // Ensure that the expected number of keys is returned
    data = (KeysResponse) response.getEntity();
    keyMetadataList = data.getKeys();

    assertEquals(4, data.getTotalCount());
    assertEquals(4, keyMetadataList.size());

    // Retrieve the first key from the list and verify its metadata
    iterator = keyMetadataList.iterator();
    keyMetadata = iterator.next();
    assertEquals(FSO_KEY_NAME1, keyMetadata.getKey());
    assertEquals(1, keyMetadata.getVersions().size());
    assertEquals(1, keyMetadata.getBlockIds().size());
    blockIds = keyMetadata.getBlockIds();
    assertEquals(0, blockIds.get(0L).get(0).getLocalID());

    keyMetadata = iterator.next();
    assertEquals(FSO_KEY_NAME2, keyMetadata.getKey());
    assertEquals(1, keyMetadata.getVersions().size());
    assertEquals(1, keyMetadata.getBlockIds().size());
    blockIds = keyMetadata.getBlockIds();
    assertEquals(0, blockIds.get(0L).get(0).getLocalID());

  }

  @Test
  public void testGetKeysForContainerWithPrevKey() throws Exception {
    // test if prev-key param works as expected
    Response response = containerEndpoint.getKeysForContainer(
        1L, -1, "/sampleVol/bucketOne/key_one");

    KeysResponse data =
        (KeysResponse) response.getEntity();

    assertEquals(3, data.getTotalCount());

    Collection<KeyMetadata> keyMetadataList = data.getKeys();
    assertEquals(1, keyMetadataList.size());

    Iterator<KeyMetadata> iterator = keyMetadataList.iterator();
    KeyMetadata keyMetadata = iterator.next();

    // assert that the returned key metadata is correct
    assertEquals("key_two", keyMetadata.getKey());
    assertEquals(2, keyMetadata.getVersions().size());
    assertEquals(2, keyMetadata.getBlockIds().size());

    // test for an empty prev-key parameter
    response = containerEndpoint.getKeysForContainer(
        1L, -1, StringUtils.EMPTY);
    data = (KeysResponse) response.getEntity();
    keyMetadataList = data.getKeys();

    assertEquals(3, data.getTotalCount());
    assertEquals(2, keyMetadataList.size());
    iterator = keyMetadataList.iterator();
    keyMetadata = iterator.next();
    assertEquals("key_one", keyMetadata.getKey());

    // test for negative cases
    response = containerEndpoint.getKeysForContainer(
        1L, -1, "/sampleVol/bucketOne/invalid_key");
    data = (KeysResponse) response.getEntity();
    keyMetadataList = data.getKeys();
    assertEquals(3, data.getTotalCount());
    assertEquals(0, keyMetadataList.size());

    // test for a container ID that does not exist
    response = containerEndpoint.getKeysForContainer(
        5L, -1, "");
    data = (KeysResponse) response.getEntity();
    keyMetadataList = data.getKeys();
    assertEquals(0, keyMetadataList.size());
    assertEquals(0, data.getTotalCount());

    // Now to check if the ContainerEndpoint also reads the File table
    // Set up test data for FSO keys
    setUpFSOData();
    // Reprocess the container key mapper to ensure the latest mapping is used
    reprocessContainerKeyMapper();
    NSSummaryTaskWithFSO nSSummaryTaskWithFso =
        new NSSummaryTaskWithFSO(reconNamespaceSummaryManager, reconOMMetadataManager, 10, 5, 20, 2000);
    nSSummaryTaskWithFso.reprocessWithFSO(reconOMMetadataManager);
    response = containerEndpoint.getKeysForContainer(20L, -1, "/0/1/2/file7");

    // Ensure that the expected number of keys is returned
    data = (KeysResponse) response.getEntity();
    keyMetadataList = data.getKeys();

    assertEquals(4, data.getTotalCount());
    assertEquals(3, keyMetadataList.size());

    // Retrieve the first key from the list and verify its metadata
    iterator = keyMetadataList.iterator();
    keyMetadata = iterator.next();
    assertEquals(FSO_KEY_NAME2, keyMetadata.getKey());
    assertEquals(1, keyMetadata.getVersions().size());
    assertEquals(1, keyMetadata.getBlockIds().size());
    Map<Long, List<KeyMetadata.ContainerBlockMetadata>> blockIds =
        keyMetadata.getBlockIds();
    assertEquals(0, blockIds.get(0L).get(0).getLocalID());

    keyMetadata = iterator.next();
    assertEquals(FSO_KEY_NAME3, keyMetadata.getKey());
    assertEquals(1, keyMetadata.getVersions().size());
    assertEquals(1, keyMetadata.getBlockIds().size());
    blockIds = keyMetadata.getBlockIds();
    assertEquals(0, blockIds.get(0L).get(0).getLocalID());

  }

  @Test
  public void testGetContainers() throws IOException, TimeoutException {
    putContainerInfos(5);

    Response response = containerEndpoint.getContainers(10, 0L);

    ContainersResponse responseObject =
        (ContainersResponse) response.getEntity();

    ContainersResponse.ContainersResponseData data =
        responseObject.getContainersResponseData();
    assertEquals(5, data.getTotalCount());

    List<ContainerMetadata> containers = new ArrayList<>(data.getContainers());

    Iterator<ContainerMetadata> iterator = containers.iterator();

    ContainerMetadata containerMetadata = iterator.next();
    assertEquals(1L, containerMetadata.getContainerID());
    // Number of keys for CID:1
    assertEquals(5L, containerMetadata.getNumberOfKeys());

    containerMetadata = iterator.next();
    assertEquals(2L, containerMetadata.getContainerID());
    assertEquals(5L, containerMetadata.getNumberOfKeys());

    // test if limit works as expected
    response = containerEndpoint.getContainers(2, 0L);
    responseObject = (ContainersResponse) response.getEntity();
    data = responseObject.getContainersResponseData();
    containers = new ArrayList<>(data.getContainers());
    // The results will be limited to 2 containers only
    assertEquals(2, containers.size());
    assertEquals(2, data.getTotalCount());

    // test if prevKey parameter in containerResponse works as expected for
    // sequential container Ids
    response = containerEndpoint.getContainers(5, 0L);
    responseObject = (ContainersResponse) response.getEntity();
    data = responseObject.getContainersResponseData();
    containers = new ArrayList<>(data.getContainers());
    // The results will be limited to 5 containers only
    // Get the last container ID from the List
    long expectedLastContainerID =
        containers.get(containers.size() - 1).getContainerID();
    // Get the last container ID from the response
    long actualLastContainerID = data.getPrevKey();
    // test if prev-key param works as expected
    assertEquals(expectedLastContainerID, actualLastContainerID);

    // test if prevKey/lastContainerID object in containerResponse works
    // as expected for non-sequential container Ids
    for (int i = 10; i <= 50; i += 10) {
      final ContainerInfo info = newContainerInfo(i);
      reconContainerManager.addNewContainer(
          new ContainerWithPipeline(info, pipeline));
    }

    response = containerEndpoint.getContainers(10, 0L);
    responseObject = (ContainersResponse) response.getEntity();
    data = responseObject.getContainersResponseData();
    containers = new ArrayList<>(data.getContainers());
    // The results will be limited to 10 containers only
    // Get the last container ID from the List
    expectedLastContainerID =
        containers.get(containers.size() - 1).getContainerID();
    // Get the last container ID from the Response
    actualLastContainerID = data.getPrevKey();
    assertEquals(expectedLastContainerID, actualLastContainerID);
  }

  @Test
  public void testGetContainersWithPrevKey()
      throws IOException, TimeoutException {
    putContainerInfos(5);

    // Test the case where prevKey = 2 and limit = 5
    Response response = containerEndpoint.getContainers(5, 2L);

    // Ensure that the response object is not null
    assertNotNull(response);

    ContainersResponse responseObject =
        (ContainersResponse) response.getEntity();

    ContainersResponse.ContainersResponseData data =
        responseObject.getContainersResponseData();
    // Ensure that the total count of containers is 3 as containers having
    // ID's 1,2, will be skipped and the next 3 containers will be returned
    assertEquals(3, data.getTotalCount());

    List<ContainerMetadata> containers = new ArrayList<>(data.getContainers());

    Iterator<ContainerMetadata> iterator = containers.iterator();

    ContainerMetadata containerMetadata = iterator.next();

    // Ensure that the containers list size is 3
    assertEquals(3, containers.size());
    // Ensure that the first container ID is 3
    assertEquals(3L, containerMetadata.getContainerID());

    // test if prevKey/lastContainerID object in containerResponse works as
    // expected when both limit and prevKey parameters are provided to method
    long expectedLastContainerID =
        containers.get(containers.size() - 1).getContainerID();
    long actualLastContainerID = data.getPrevKey();
    assertEquals(expectedLastContainerID, actualLastContainerID);

    // test for negative cases
    response = containerEndpoint.getContainers(-1, 0L);
    responseObject = (ContainersResponse) response.getEntity();
    // Ensure that the response object is null when limit is negative
    assertNull(responseObject);

    response = containerEndpoint.getContainers(10, -1L);
    responseObject = (ContainersResponse) response.getEntity();
    // Ensure that the response object is null when prevKey is negative
    assertNull(responseObject);
  }

  @Test
  public void testGetMissingContainers() throws IOException, TimeoutException {
    Response response = containerEndpoint.getMissingContainers(1000);

    MissingContainersResponse responseObject =
        (MissingContainersResponse) response.getEntity();

    assertEquals(0, responseObject.getTotalCount());
    assertEquals(Collections.EMPTY_LIST, responseObject.getContainers());

    putContainerInfos(5);
    uuid1 = newDatanode("host1", "127.0.0.1");
    uuid2 = newDatanode("host2", "127.0.0.2");
    uuid3 = newDatanode("host3", "127.0.0.3");
    uuid4 = newDatanode("host4", "127.0.0.4");
    createUnhealthyRecords(5, 0, 0, 0, 0);

    Response responseWithLimit = containerEndpoint.getMissingContainers(3);
    MissingContainersResponse responseWithLimitObject
        = (MissingContainersResponse) responseWithLimit.getEntity();
    assertEquals(3, responseWithLimitObject.getTotalCount());
    MissingContainerMetadata containerWithLimit =
        responseWithLimitObject.getContainers().stream().findFirst()
            .orElse(null);
    assertNotNull(containerWithLimit);
    assertTrue(containerWithLimit.getReplicas().stream()
        .map(ContainerHistory::getState)
        .allMatch(s -> s.equals("UNHEALTHY")));
    assertTrue(containerWithLimit.getReplicas().stream()
            .map(ContainerHistory::getDataChecksum)
            .allMatch(s -> s.equals(1234L)));

    Collection<MissingContainerMetadata> recordsWithLimit
        = responseWithLimitObject.getContainers();
    List<MissingContainerMetadata> missingWithLimit
        = new ArrayList<>(recordsWithLimit);
    assertEquals(3, missingWithLimit.size());
    assertEquals(1L, missingWithLimit.get(0).getContainerID());
    assertEquals(2L, missingWithLimit.get(1).getContainerID());
    assertEquals(3L, missingWithLimit.get(2).getContainerID());

    response = containerEndpoint.getMissingContainers(1000);
    responseObject = (MissingContainersResponse) response.getEntity();
    assertEquals(5, responseObject.getTotalCount());
    MissingContainerMetadata container =
        responseObject.getContainers().stream().findFirst().orElse(null);
    assertNotNull(container);

    assertEquals(containerID.getId(), container.getContainerID());
    assertEquals(keyCount, container.getKeys());
    assertEquals(pipelineID.getId(), container.getPipelineID());
    assertEquals(3, container.getReplicas().size());

    Set<String> datanodes = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList("host2", "host3", "host4")));
    List<ContainerHistory> containerReplicas = container.getReplicas();
    containerReplicas.forEach(history -> {
      assertThat(datanodes).contains(history.getDatanodeHost());
    });
  }

  ContainerInfo newContainerInfo(long containerId) {
    return new ContainerInfo.Builder()
        .setContainerID(containerId)
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setState(HddsProtos.LifeCycleState.OPEN)
        .setOwner("owner1")
        .setNumberOfKeys(keyCount)
        .setPipelineID(pipelineID)
        .build();
  }

  void putContainerInfos(int num) throws IOException, TimeoutException {
    for (int i = 1; i <= num; i++) {
      final ContainerInfo info = newContainerInfo(i);
      reconContainerManager.addNewContainer(
          new ContainerWithPipeline(info, pipeline));
    }
  }

  @Test
  public void testUnhealthyContainers() throws IOException, TimeoutException {
    Response response = containerEndpoint.getUnhealthyContainers(1000, 0, 0);

    UnhealthyContainersResponse responseObject =
        (UnhealthyContainersResponse) response.getEntity();

    assertEquals(0, responseObject.getMissingCount());
    assertEquals(0, responseObject.getOverReplicatedCount());
    assertEquals(0, responseObject.getUnderReplicatedCount());
    assertEquals(0, responseObject.getMisReplicatedCount());

    assertEquals(Collections.EMPTY_LIST, responseObject.getContainers());

    putContainerInfos(15);
    uuid1 = newDatanode("host1", "127.0.0.1");
    uuid2 = newDatanode("host2", "127.0.0.2");
    uuid3 = newDatanode("host3", "127.0.0.3");
    uuid4 = newDatanode("host4", "127.0.0.4");
    createUnhealthyRecords(5, 4, 3, 2, 1);

    response = containerEndpoint.getUnhealthyContainers(1000, 0, 0);

    responseObject = (UnhealthyContainersResponse) response.getEntity();
    assertEquals(5, responseObject.getMissingCount());
    assertEquals(4, responseObject.getOverReplicatedCount());
    assertEquals(3, responseObject.getUnderReplicatedCount());
    assertEquals(2, responseObject.getMisReplicatedCount());
    assertEquals(1, responseObject.getReplicaMismatchCount());

    Collection<UnhealthyContainerMetadata> records
        = responseObject.getContainers();
    assertTrue(records.stream()
        .flatMap(containerMetadata -> containerMetadata.getReplicas().stream()
            .map(ContainerHistory::getState))
        .allMatch(s -> s.equals("UNHEALTHY")));
    List<UnhealthyContainerMetadata> missing = records
        .stream()
        .filter(r -> r.getContainerState()
            .equals(UnHealthyContainerStates.MISSING.toString()))
        .collect(Collectors.toList());
    assertEquals(5, missing.size());
    assertEquals(3, missing.get(0).getExpectedReplicaCount());
    assertEquals(0, missing.get(0).getActualReplicaCount());
    assertEquals(3, missing.get(0).getReplicaDeltaCount());
    assertEquals(12345L, missing.get(0).getUnhealthySince());
    assertEquals(1L, missing.get(0).getContainerID());
    assertEquals(keyCount, missing.get(0).getKeys());
    assertEquals(pipelineID.getId(), missing.get(0).getPipelineID());
    assertEquals(3, missing.get(0).getReplicas().size());
    assertNull(missing.get(0).getReason());

    Set<String> datanodes = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList("host2", "host3", "host4")));
    List<ContainerHistory> containerReplicas = missing.get(0).getReplicas();
    containerReplicas.forEach(history -> {
      assertThat(datanodes).contains(history.getDatanodeHost());
    });

    List<UnhealthyContainerMetadata> overRep = records
        .stream()
        .filter(r -> r.getContainerState()
            .equals(UnHealthyContainerStates.OVER_REPLICATED.toString()))
        .collect(Collectors.toList());
    assertEquals(4, overRep.size());
    assertEquals(3, overRep.get(0).getExpectedReplicaCount());
    assertEquals(5, overRep.get(0).getActualReplicaCount());
    assertEquals(-2, overRep.get(0).getReplicaDeltaCount());
    assertEquals(12345L, overRep.get(0).getUnhealthySince());
    assertEquals(6L, overRep.get(0).getContainerID());
    assertNull(overRep.get(0).getReason());

    List<UnhealthyContainerMetadata> underRep = records
        .stream()
        .filter(r -> r.getContainerState()
            .equals(UnHealthyContainerStates.UNDER_REPLICATED.toString()))
        .collect(Collectors.toList());
    assertEquals(3, underRep.size());
    assertEquals(3, underRep.get(0).getExpectedReplicaCount());
    assertEquals(1, underRep.get(0).getActualReplicaCount());
    assertEquals(2, underRep.get(0).getReplicaDeltaCount());
    assertEquals(12345L, underRep.get(0).getUnhealthySince());
    assertEquals(10L, underRep.get(0).getContainerID());
    assertNull(underRep.get(0).getReason());

    List<UnhealthyContainerMetadata> misRep = records
        .stream()
        .filter(r -> r.getContainerState()
            .equals(UnHealthyContainerStates.MIS_REPLICATED.toString()))
        .collect(Collectors.toList());
    assertEquals(2, misRep.size());
    assertEquals(2, misRep.get(0).getExpectedReplicaCount());
    assertEquals(1, misRep.get(0).getActualReplicaCount());
    assertEquals(1, misRep.get(0).getReplicaDeltaCount());
    assertEquals(12345L, misRep.get(0).getUnhealthySince());
    assertEquals(13L, misRep.get(0).getContainerID());
    assertEquals("some reason", misRep.get(0).getReason());

    List<UnhealthyContainerMetadata> replicaMismatch = records
        .stream()
        .filter(r -> r.getContainerState()
            .equals(UnHealthyContainerStates.REPLICA_MISMATCH.toString()))
        .collect(Collectors.toList());
    assertEquals(1, replicaMismatch.size());
    assertEquals(3, replicaMismatch.get(0).getExpectedReplicaCount());
    assertEquals(3, replicaMismatch.get(0).getActualReplicaCount());
    assertEquals(0, replicaMismatch.get(0).getReplicaDeltaCount());
    assertEquals(12345L, replicaMismatch.get(0).getUnhealthySince());
    assertEquals(15L, replicaMismatch.get(0).getContainerID());
    List<ContainerHistory> replicas = replicaMismatch.get(0).getReplicas();
    assertTrue(replicas.stream().anyMatch(checksum -> checksum.getDataChecksum() == 1234L));
    assertTrue(replicas.stream().anyMatch(checksum -> checksum.getDataChecksum() == 2345L));
  }

  @Test
  public void testUnhealthyContainersFilteredResponse()
      throws IOException, TimeoutException {
    String missing = UnHealthyContainerStates.MISSING.toString();
    String emptyMissing = UnHealthyContainerStates.EMPTY_MISSING.toString();
    String negativeSize = UnHealthyContainerStates.NEGATIVE_SIZE.toString(); // For NEGATIVE_SIZE state

    // Initial empty response verification
    Response response = containerEndpoint
        .getUnhealthyContainers(missing, 1000, 0, 0);

    UnhealthyContainersResponse responseObject =
        (UnhealthyContainersResponse) response.getEntity();

    assertEquals(0, responseObject.getMissingCount());
    assertEquals(0, responseObject.getOverReplicatedCount());
    assertEquals(0, responseObject.getUnderReplicatedCount());
    assertEquals(0, responseObject.getMisReplicatedCount());
    assertEquals(Collections.EMPTY_LIST, responseObject.getContainers());

    // Add unhealthy records
    putContainerInfos(5);
    uuid1 = newDatanode("host1", "127.0.0.1");
    uuid2 = newDatanode("host2", "127.0.0.2");
    uuid3 = newDatanode("host3", "127.0.0.3");
    uuid4 = newDatanode("host4", "127.0.0.4");
    createUnhealthyRecords(5, 4, 3, 2, 1);
    createEmptyMissingUnhealthyRecords(2); // For EMPTY_MISSING state
    createNegativeSizeUnhealthyRecords(2); // For NEGATIVE_SIZE state

    // Check for unhealthy containers
    response = containerEndpoint.getUnhealthyContainers(missing, 1000, 0, 0);

    responseObject = (UnhealthyContainersResponse) response.getEntity();

    // Summary should have the count for all unhealthy:
    assertEquals(5, responseObject.getMissingCount());
    assertEquals(4, responseObject.getOverReplicatedCount());
    assertEquals(3, responseObject.getUnderReplicatedCount());
    assertEquals(2, responseObject.getMisReplicatedCount());
    assertEquals(1, responseObject.getReplicaMismatchCount());

    Collection<UnhealthyContainerMetadata> records = responseObject.getContainers();
    assertTrue(records.stream()
        .flatMap(containerMetadata -> containerMetadata.getReplicas().stream()
            .map(ContainerHistory::getState))
        .allMatch(s -> s.equals("UNHEALTHY")));

    // Verify only missing containers are returned
    assertEquals(5, records.size());
    for (UnhealthyContainerMetadata r : records) {
      assertEquals(missing, r.getContainerState());
    }

    // Check for empty missing containers, should return zero
    Response filteredEmptyMissingResponse = containerEndpoint
        .getUnhealthyContainers(emptyMissing, 1000, 0, 0);
    responseObject = (UnhealthyContainersResponse) filteredEmptyMissingResponse.getEntity();
    records = responseObject.getContainers();
    assertEquals(0, records.size());

    // Check for negative size containers, should return zero
    Response filteredNegativeSizeResponse = containerEndpoint
        .getUnhealthyContainers(negativeSize, 1000, 0, 0);
    responseObject = (UnhealthyContainersResponse) filteredNegativeSizeResponse.getEntity();
    records = responseObject.getContainers();
    assertEquals(0, records.size());
  }

  @Test
  public void testUnhealthyContainersInvalidState() {
    WebApplicationException e = assertThrows(WebApplicationException.class,
        () -> containerEndpoint.getUnhealthyContainers("invalid", 1000, 0, 0));
    assertEquals("HTTP 400 Bad Request", e.getMessage());
  }

  @Test
  public void testUnhealthyContainersPaging()
      throws IOException, TimeoutException {

    // Clear any existing unhealthy container records from previous tests
    containerHealthSchemaManager.clearAllUnhealthyContainerRecords();
    // Create containers for all IDs that will be used in unhealthy records
    // createUnhealthyRecords(5, 4, 3, 2) will create records for containers 1-14
    // So we need to create 14 containers instead of just 6
    putContainerInfos(14);  // Changed from 6 to 14
    uuid1 = newDatanode("host1", "127.0.0.1");
    uuid2 = newDatanode("host2", "127.0.0.2");
    uuid3 = newDatanode("host3", "127.0.0.3");
    uuid4 = newDatanode("host4", "127.0.0.4");
    createUnhealthyRecords(5, 4, 3, 2, 0);

    // Get first batch with no pagination (prevStartKey=0, prevLastKey=0)
    UnhealthyContainersResponse firstBatch =
        (UnhealthyContainersResponse) containerEndpoint.getUnhealthyContainers(
            3, 0, 0).getEntity();
    assertTrue(firstBatch.getContainers().stream()
        .flatMap(containerMetadata -> containerMetadata.getReplicas().stream()
            .map(ContainerHistory::getState))
        .allMatch(s -> s.equals("UNHEALTHY")));

    // For pagination, use the last container ID from the first batch as prevLastKey
    long lastContainerIdFromFirstBatch = firstBatch.getContainers().stream()
        .map(i -> i.getContainerID()).max(Long::compareTo).get();

    // Get second batch using correct pagination parameters
    UnhealthyContainersResponse secondBatch =
        (UnhealthyContainersResponse) containerEndpoint.getUnhealthyContainers(
            3, 0, lastContainerIdFromFirstBatch).getEntity();

    ArrayList<UnhealthyContainerMetadata> records
        = new ArrayList<>(firstBatch.getContainers());
    assertEquals(3, records.size());
    assertEquals(1L, records.get(0).getContainerID());
    assertEquals(2L, records.get(1).getContainerID());
    assertEquals(3L, records.get(2).getContainerID());
    records
        = new ArrayList<>(secondBatch.getContainers());
    assertEquals(3, records.size());
    assertEquals(4L, records.get(0).getContainerID());
    assertEquals(5L, records.get(1).getContainerID());
    assertEquals(6L, records.get(2).getContainerID());
  }

  @Test
  public void testGetReplicaHistoryForContainer() throws IOException {
    // Add container history for container id 1
    final UUID u1 = newDatanode("host1", "127.0.0.1");
    final UUID u2 = newDatanode("host2", "127.0.0.2");
    final UUID u3 = newDatanode("host3", "127.0.0.3");
    final UUID u4 = newDatanode("host4", "127.0.0.4");
    reconContainerManager.upsertContainerHistory(1L, u1, 1L, 1L, "OPEN", ContainerChecksums.of(1234L, 0L));
    reconContainerManager.upsertContainerHistory(1L, u2, 2L, 1L, "OPEN", ContainerChecksums.of(1234L, 0L));
    reconContainerManager.upsertContainerHistory(1L, u3, 3L, 1L, "OPEN", ContainerChecksums.of(1234L, 0L));
    reconContainerManager.upsertContainerHistory(1L, u4, 4L, 1L, "OPEN", ContainerChecksums.of(1234L, 0L));

    reconContainerManager.upsertContainerHistory(1L, u1, 5L, 1L, "OPEN", ContainerChecksums.of(1234L, 0L));

    Response response = containerEndpoint.getReplicaHistoryForContainer(1L);
    List<ContainerHistory> histories =
        (List<ContainerHistory>) response.getEntity();
    assertTrue(histories.stream()
        .map(ContainerHistory::getState)
        .allMatch(s -> s.equals("OPEN")));
    assertTrue(histories.stream()
            .map(ContainerHistory::getDataChecksum)
            .allMatch(s -> s.equals(1234L)));
    Set<String> datanodes = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(
            u1.toString(), u2.toString(), u3.toString(), u4.toString())));
    assertEquals(4, histories.size());
    histories.forEach(history -> {
      assertThat(datanodes).contains(history.getDatanodeUuid());
      if (history.getDatanodeUuid().equals(u1.toString())) {
        assertEquals("host1", history.getDatanodeHost());
        assertEquals(1L, history.getFirstSeenTime());
        assertEquals(5L, history.getLastSeenTime());
      }
    });

    // Check getLatestContainerHistory
    List<ContainerHistory> hist1 = reconContainerManager
        .getLatestContainerHistory(1L, 10);
    assertThat(hist1.size()).isLessThanOrEqualTo(10);
    // Descending order by last report timestamp
    for (int i = 0; i < hist1.size() - 1; i++) {
      assertThat(hist1.get(i).getLastSeenTime())
          .isGreaterThanOrEqualTo(hist1.get(i + 1).getLastSeenTime());
    }
  }

  UUID newDatanode(String hostName, String ipAddress) throws IOException {
    final UUID uuid = UUID.randomUUID();
    reconContainerManager.getNodeDB().put(DatanodeID.of(uuid),
        DatanodeDetails.newBuilder()
            .setUuid(uuid)
            .setHostName(hostName)
            .setIpAddress(ipAddress)
            .build());
    return uuid;
  }

  private void createEmptyMissingUnhealthyRecords(int emptyMissing) {
    int cid = 0;
    for (int i = 0; i < emptyMissing; i++) {
      createUnhealthyRecord(++cid, UnHealthyContainerStates.EMPTY_MISSING.toString(),
          3, 3, 0, null, false);
    }
  }

  private void createNegativeSizeUnhealthyRecords(int negativeSize) {
    int cid = 0;
    for (int i = 0; i < negativeSize; i++) {
      createUnhealthyRecord(++cid, UnHealthyContainerStates.NEGATIVE_SIZE.toString(),
          3, 3, 0, null, false); // Added for NEGATIVE_SIZE state
    }
  }

  private void createUnhealthyRecords(int missing, int overRep, int underRep,
                                      int misRep, int dataChecksum) {
    int cid = 0;
    for (int i = 0; i < missing; i++) {
      createUnhealthyRecord(++cid, UnHealthyContainerStates.MISSING.toString(),
          3, 0, 3, null, false);
    }
    for (int i = 0; i < overRep; i++) {
      createUnhealthyRecord(++cid,
          UnHealthyContainerStates.OVER_REPLICATED.toString(),
          3, 5, -2, null, false);
    }
    for (int i = 0; i < underRep; i++) {
      createUnhealthyRecord(++cid,
          UnHealthyContainerStates.UNDER_REPLICATED.toString(),
          3, 1, 2, null, false);
    }
    for (int i = 0; i < misRep; i++) {
      createUnhealthyRecord(++cid,
          UnHealthyContainerStates.MIS_REPLICATED.toString(),
          2, 1, 1, "some reason", false);
    }
    for (int i = 0; i < dataChecksum; i++) {
      createUnhealthyRecord(++cid,
          UnHealthyContainerStates.REPLICA_MISMATCH.toString(),
          3, 3, 0, null, true);
    }
  }

  private void createUnhealthyRecord(int id, String state, int expected,
                                     int actual, int delta, String reason, boolean dataChecksumMismatch) {
    long cID = Integer.toUnsignedLong(id);
    UnhealthyContainers missing = new UnhealthyContainers();
    missing.setContainerId(cID);
    missing.setContainerState(state);
    missing.setInStateSince(12345L);
    missing.setActualReplicaCount(actual);
    missing.setExpectedReplicaCount(expected);
    missing.setReplicaDelta(delta);
    missing.setReason(reason);

    ArrayList<UnhealthyContainers> missingList = new ArrayList<>();
    missingList.add(missing);
    containerHealthSchemaManager.insertUnhealthyContainerRecords(missingList);

    long differentChecksum = dataChecksumMismatch ? 2345L : 1234L;

    reconContainerManager.upsertContainerHistory(cID, uuid1, 1L, 1L,
        "UNHEALTHY", ContainerChecksums.of(differentChecksum, 0L));
    reconContainerManager.upsertContainerHistory(cID, uuid2, 2L, 1L,
        "UNHEALTHY", ContainerChecksums.of(differentChecksum, 0L));
    reconContainerManager.upsertContainerHistory(cID, uuid3, 3L, 1L,
        "UNHEALTHY", ContainerChecksums.of(1234L, 0L));
    reconContainerManager.upsertContainerHistory(cID, uuid4, 4L, 1L,
        "UNHEALTHY", ContainerChecksums.of(1234L, 0L));
  }

  protected ContainerWithPipeline getTestContainer(
      HddsProtos.LifeCycleState state, long containerId)
      throws IOException, TimeoutException {
    ContainerID localContainerID = ContainerID.valueOf(containerId);
    Pipeline localPipeline = getRandomPipeline();
    reconPipelineManager.addPipeline(localPipeline);
    ContainerInfo containerInfo =
        new ContainerInfo.Builder()
            .setContainerID(localContainerID.getId())
            .setNumberOfKeys(10)
            .setPipelineID(localPipeline.getId())
            .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
            .setOwner("test")
            .setState(state)
            .build();
    return new ContainerWithPipeline(containerInfo, localPipeline);
  }

  void assertContainerCount(HddsProtos.LifeCycleState state, int expected) {
    final int computed = containerStateManager.getContainerCount(state);
    assertEquals(expected, computed);
  }

  @Test
  public void testGetSCMDeletedContainers() throws Exception {
    reconContainerManager.addNewContainer(
        getTestContainer(HddsProtos.LifeCycleState.OPEN, 102L));
    reconContainerManager.addNewContainer(
        getTestContainer(HddsProtos.LifeCycleState.OPEN, 103L));

    reconContainerManager.updateContainerState(ContainerID.valueOf(102L),
        HddsProtos.LifeCycleEvent.FINALIZE);
    reconContainerManager.updateContainerState(ContainerID.valueOf(102L),
        HddsProtos.LifeCycleEvent.CLOSE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(102L),
            HddsProtos.LifeCycleEvent.DELETE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(102L),
            HddsProtos.LifeCycleEvent.CLEANUP);
    assertContainerCount(HddsProtos.LifeCycleState.DELETED, 1);

    reconContainerManager.updateContainerState(ContainerID.valueOf(103L),
        HddsProtos.LifeCycleEvent.FINALIZE);
    reconContainerManager.updateContainerState(ContainerID.valueOf(103L),
        HddsProtos.LifeCycleEvent.CLOSE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(103L),
            HddsProtos.LifeCycleEvent.DELETE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(103L),
            HddsProtos.LifeCycleEvent.CLEANUP);
    assertContainerCount(HddsProtos.LifeCycleState.DELETED, 2);

    Response scmDeletedContainers =
        containerEndpoint.getSCMDeletedContainers(2, 0);
    List<DeletedContainerInfo> deletedContainerInfoList =
        (List<DeletedContainerInfo>) scmDeletedContainers.getEntity();
    assertEquals(2, deletedContainerInfoList.size());

    DeletedContainerInfo deletedContainerInfo = deletedContainerInfoList.get(0);
    assertEquals(102, deletedContainerInfo.getContainerID());
    assertEquals("DELETED", deletedContainerInfo.getContainerState());

    deletedContainerInfo = deletedContainerInfoList.get(1);
    assertEquals(103, deletedContainerInfo.getContainerID());
    assertEquals("DELETED", deletedContainerInfo.getContainerState());
  }

  @Test
  public void testGetSCMDeletedContainersLimitParam() throws Exception {
    reconContainerManager.addNewContainer(
        getTestContainer(HddsProtos.LifeCycleState.OPEN, 104L));
    reconContainerManager.addNewContainer(
        getTestContainer(HddsProtos.LifeCycleState.OPEN, 105L));
    reconContainerManager.updateContainerState(ContainerID.valueOf(104L),
        HddsProtos.LifeCycleEvent.FINALIZE);
    reconContainerManager.updateContainerState(ContainerID.valueOf(104L),
        HddsProtos.LifeCycleEvent.CLOSE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(104L),
            HddsProtos.LifeCycleEvent.DELETE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(104L),
            HddsProtos.LifeCycleEvent.CLEANUP);
    assertContainerCount(HddsProtos.LifeCycleState.DELETED, 1);

    reconContainerManager.updateContainerState(ContainerID.valueOf(105L),
        HddsProtos.LifeCycleEvent.FINALIZE);
    reconContainerManager.updateContainerState(ContainerID.valueOf(105L),
        HddsProtos.LifeCycleEvent.CLOSE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(105L),
            HddsProtos.LifeCycleEvent.DELETE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(105L),
            HddsProtos.LifeCycleEvent.CLEANUP);
    assertContainerCount(HddsProtos.LifeCycleState.DELETED, 2);

    Response scmDeletedContainers =
        containerEndpoint.getSCMDeletedContainers(1, 0);
    List<DeletedContainerInfo> deletedContainerInfoList =
        (List<DeletedContainerInfo>) scmDeletedContainers.getEntity();
    assertEquals(1, deletedContainerInfoList.size());

    DeletedContainerInfo deletedContainerInfo = deletedContainerInfoList.get(0);
    assertEquals(104, deletedContainerInfo.getContainerID());
    assertEquals("DELETED", deletedContainerInfo.getContainerState());
  }

  @Test
  public void testGetSCMDeletedContainersPrevKeyParam() throws Exception {
    reconContainerManager.addNewContainer(
        getTestContainer(HddsProtos.LifeCycleState.OPEN, 106L));
    reconContainerManager.addNewContainer(
        getTestContainer(HddsProtos.LifeCycleState.OPEN, 107L));

    reconContainerManager.updateContainerState(ContainerID.valueOf(106L),
        HddsProtos.LifeCycleEvent.FINALIZE);
    reconContainerManager.updateContainerState(ContainerID.valueOf(106L),
        HddsProtos.LifeCycleEvent.CLOSE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(106L),
            HddsProtos.LifeCycleEvent.DELETE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(106L),
            HddsProtos.LifeCycleEvent.CLEANUP);
    assertContainerCount(HddsProtos.LifeCycleState.DELETED, 1);

    reconContainerManager.updateContainerState(ContainerID.valueOf(107L),
        HddsProtos.LifeCycleEvent.FINALIZE);
    reconContainerManager.updateContainerState(ContainerID.valueOf(107L),
        HddsProtos.LifeCycleEvent.CLOSE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(107L),
            HddsProtos.LifeCycleEvent.DELETE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(107L),
            HddsProtos.LifeCycleEvent.CLEANUP);
    assertContainerCount(HddsProtos.LifeCycleState.DELETED, 2);

    Response scmDeletedContainers =
        containerEndpoint.getSCMDeletedContainers(2, 106L);
    List<DeletedContainerInfo> deletedContainerInfoList =
        (List<DeletedContainerInfo>) scmDeletedContainers.getEntity();
    assertEquals(1, deletedContainerInfoList.size());

    DeletedContainerInfo deletedContainerInfo = deletedContainerInfoList.get(0);
    assertEquals(107, deletedContainerInfo.getContainerID());
    assertEquals("DELETED", deletedContainerInfo.getContainerState());
  }

  private void updateContainerStateToDeleted(long containerId)
      throws IOException, InvalidStateTransitionException, TimeoutException {
    reconContainerManager.updateContainerState(ContainerID.valueOf(containerId),
        HddsProtos.LifeCycleEvent.FINALIZE);
    reconContainerManager.updateContainerState(ContainerID.valueOf(containerId),
        HddsProtos.LifeCycleEvent.CLOSE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(containerId),
            HddsProtos.LifeCycleEvent.DELETE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(containerId),
            HddsProtos.LifeCycleEvent.CLEANUP);
  }

  @Test
  public void testGetContainerInsightsNonSCMContainers()
      throws IOException, TimeoutException {
    Map<Long, ContainerMetadata> omContainers =
        reconContainerMetadataManager.getContainers(-1, 0);
    putContainerInfos(2);
    List<ContainerInfo> scmContainers = reconContainerManager.getContainers();
    assertEquals(omContainers.size(), scmContainers.size());
    // delete container Id 1 from SCM
    reconContainerManager.deleteContainer(ContainerID.valueOf(1));
    Response containerInsights =
        containerEndpoint.getContainerMisMatchInsights(10, 0, "SCM");
    Map<String, Object> response =
        (Map<String, Object>) containerInsights.getEntity();

    List<ContainerDiscrepancyInfo> containerDiscrepancyInfoList =
        (List<ContainerDiscrepancyInfo>) response.get(
            "containerDiscrepancyInfo");

    // Check the prevKey is set correct in the response
    long responsePrevKey = (long) response.get("lastKey");
    assertEquals(containerDiscrepancyInfoList.get(
            containerDiscrepancyInfoList.size() - 1).getContainerID(),
        responsePrevKey);

    ContainerDiscrepancyInfo containerDiscrepancyInfo =
        containerDiscrepancyInfoList.get(0);
    assertEquals(1, containerDiscrepancyInfo.getContainerID());
    assertEquals(1, containerDiscrepancyInfoList.size());
    assertEquals("OM", containerDiscrepancyInfo.getExistsAt());
  }

  @Test
  public void testGetContainerInsightsNonSCMContainersWithPrevKey()
      throws Exception {

    // Add 3 more containers to OM making total container in OM to 5
    String[] keys = {"key_three", "key_four", "key_five"};
    String bucket = "bucketOne";
    String volume = "sampleVol";
    BlockID[] blockIDs =
        {new BlockID(3, 103), new BlockID(4, 105), new BlockID(5, 107)};

    for (int i = 0; i < keys.length; i++) {
      List<OmKeyLocationInfoGroup> infoGroups = new ArrayList<>();
      OmKeyLocationInfo omKeyLocationInfo =
          getOmKeyLocationInfo(blockIDs[i], pipeline);
      List<OmKeyLocationInfo> omKeyLocationInfoList =
          Collections.singletonList(omKeyLocationInfo);
      infoGroups.add(new OmKeyLocationInfoGroup(0, omKeyLocationInfoList));
      writeDataToOm(reconOMMetadataManager, keys[i], bucket, volume,
          infoGroups);
    }

    reprocessContainerKeyMapper();

    Map<Long, ContainerMetadata> omContainers =
        reconContainerMetadataManager.getContainers(-1, 0);
    List<ContainerInfo> scmContainers = reconContainerManager.getContainers();
    // There are 5 containers in OM and 0 in SCM
    assertNotEquals(omContainers.size(), scmContainers.size());

    // Set prevKey and limit
    long prevKey = 0;
    int limit = 3;
    List<ContainerDiscrepancyInfo> allContainerDiscrepancyInfoList =
        new ArrayList<>();

    // Check to see if pagination works as expected by reusing the prevKey
    // from the previous response
    do {
      Response containerInsights =
          containerEndpoint.getContainerMisMatchInsights(limit, prevKey, "SCM");
      Map<String, Object> response =
          (Map<String, Object>) containerInsights.getEntity();
      long responsePrevKey = (long) response.get("lastKey");
      List<ContainerDiscrepancyInfo> containerDiscrepancyInfoList =
          (List<ContainerDiscrepancyInfo>) response.get(
              "containerDiscrepancyInfo");

      // Check the ContainerDiscrepancyInfo objects in the response
      allContainerDiscrepancyInfoList.addAll(containerDiscrepancyInfoList);
      boolean allExistAtOM = containerDiscrepancyInfoList.stream()
          .allMatch(info -> "OM".equals(info.getExistsAt()));

      assertTrue(allExistAtOM);

      // Update prevKey for the next iteration
      prevKey = responsePrevKey;
    } while (allContainerDiscrepancyInfoList.size() < omContainers.size());

    // Ensure all containers in OM are included in the response
    assertEquals(omContainers.size(), allContainerDiscrepancyInfoList.size());
  }

  @Test
  public void testGetContainerInsightsNonOMContainers()
      throws IOException, TimeoutException {
    putContainerInfos(2);
    List<ContainerKeyPrefix> deletedContainerKeyList =
        reconContainerMetadataManager.getKeyPrefixesForContainer(2).entrySet()
            .stream().map(entry -> entry.getKey()).collect(
                Collectors.toList());
    deletedContainerKeyList.forEach((ContainerKeyPrefix key) -> {
      try (RDBBatchOperation rdbBatchOperation = RDBBatchOperation.newAtomicOperation()) {
        reconContainerMetadataManager
            .batchDeleteContainerMapping(rdbBatchOperation, key);
        reconContainerMetadataManager.commitBatchOperation(rdbBatchOperation);
      } catch (IOException e) {
        LOG.error("Unable to write Container Key Prefix data in Recon DB.", e);
      }
    });
    Response containerInsights =
        containerEndpoint.getContainerMisMatchInsights(10, 0, "OM");
    Map<String, Object> response =
        (Map<String, Object>) containerInsights.getEntity();
    List<ContainerDiscrepancyInfo> containerDiscrepancyInfoList =
        (List<ContainerDiscrepancyInfo>) response.get(
            "containerDiscrepancyInfo");

    // Check the prevKey is set correct in the response
    long responsePrevKey = (long) response.get("lastKey");
    assertEquals(containerDiscrepancyInfoList.get(
            containerDiscrepancyInfoList.size() - 1).getContainerID(),
        responsePrevKey);

    ContainerDiscrepancyInfo containerDiscrepancyInfo =
        containerDiscrepancyInfoList.get(0);
    assertEquals(2, containerDiscrepancyInfo.getContainerID());
    assertEquals(1, containerDiscrepancyInfoList.size());
    assertEquals("SCM", containerDiscrepancyInfo.getExistsAt());
  }

  @Test
  public void testGetContainerInsightsNonOMContainersWithPrevKey()
      throws IOException, TimeoutException {
    putContainerInfos(5);
    List<ContainerKeyPrefix> deletedContainerKeyList =
        reconContainerMetadataManager.getKeyPrefixesForContainer(2).entrySet()
            .stream().map(entry -> entry.getKey()).collect(Collectors.toList());
    deletedContainerKeyList.forEach((ContainerKeyPrefix key) -> {
      try (RDBBatchOperation rdbBatchOperation = RDBBatchOperation.newAtomicOperation()) {
        reconContainerMetadataManager.batchDeleteContainerMapping(
            rdbBatchOperation, key);
        reconContainerMetadataManager.commitBatchOperation(rdbBatchOperation);
      } catch (IOException e) {
        LOG.error("Unable to write Container Key Prefix data in Recon DB.", e);
      }
    });

    // Set prevKey and limit
    long prevKey = 2;
    int limit = 3;

    Response containerInsights =
        containerEndpoint.getContainerMisMatchInsights(limit, prevKey, "OM");
    Map<String, Object> response =
        (Map<String, Object>) containerInsights.getEntity();
    List<ContainerDiscrepancyInfo> containerDiscrepancyInfoList =
        (List<ContainerDiscrepancyInfo>) response.get(
            "containerDiscrepancyInfo");

    // Check the prevKey is set correct in the response
    long responsePrevKey = (long) response.get("lastKey");
    assertEquals(containerDiscrepancyInfoList.get(
            containerDiscrepancyInfoList.size() - 1).getContainerID(),
        responsePrevKey);

    // Check the first two ContainerDiscrepancyInfo objects in the response
    assertEquals(3, containerDiscrepancyInfoList.size());

    ContainerDiscrepancyInfo containerDiscrepancyInfo1 =
        containerDiscrepancyInfoList.get(0);
    assertEquals(3, containerDiscrepancyInfo1.getContainerID());
    assertEquals("SCM", containerDiscrepancyInfo1.getExistsAt());

    ContainerDiscrepancyInfo containerDiscrepancyInfo2 =
        containerDiscrepancyInfoList.get(1);
    assertEquals(4, containerDiscrepancyInfo2.getContainerID());
    assertEquals("SCM", containerDiscrepancyInfo2.getExistsAt());
  }

  @Test
  public void testContainerMissingFilter()
      throws IOException, TimeoutException {
    // Put 5 containers in SCM with containerID 1 to 5
    putContainerInfos(5);
    // Delete containerID 1 and 2 from SCM so that they are missing in SCM
    // but present in OM
    reconContainerManager.deleteContainer(ContainerID.valueOf(1));
    reconContainerManager.deleteContainer(ContainerID.valueOf(2));

    // There are currently 2 containers in OM with containerID 1 and 2
    Map<Long, ContainerMetadata> omContainers =
        reconContainerMetadataManager.getContainers(-1, 0);
    assertEquals(2, omContainers.size());

    // Set the filter to "OM" to get missing containers in OM
    Response responseOM =
        containerEndpoint.getContainerMisMatchInsights(10, 0, "OM");
    Map<String, Object> responseMapOM =
        (Map<String, Object>) responseOM.getEntity();
    List<ContainerDiscrepancyInfo> containerDiscrepancyInfoListOM =
        (List<ContainerDiscrepancyInfo>)
            responseMapOM.get("containerDiscrepancyInfo");
    assertEquals(3, containerDiscrepancyInfoListOM.size());

    // Set the filter to "SCM" to get missing containers in SCM
    Response responseSCM =
        containerEndpoint.getContainerMisMatchInsights(10, 0, "SCM");
    Map<String, Object> responseMapSCM =
        (Map<String, Object>) responseSCM.getEntity();
    List<ContainerDiscrepancyInfo> containerDiscrepancyInfoListSCM =
        (List<ContainerDiscrepancyInfo>)
            responseMapSCM.get("containerDiscrepancyInfo");
    assertEquals(2, containerDiscrepancyInfoListSCM.size());

    List<Long> missingContainerIdsOM = containerDiscrepancyInfoListOM.stream()
        .map(ContainerDiscrepancyInfo::getContainerID)
        .collect(Collectors.toList());
    // ContainerID 1 and 2 are missing in OM but present in SCM
    assertThat(missingContainerIdsOM).contains(3L);
    assertThat(missingContainerIdsOM).contains(4L);
    assertThat(missingContainerIdsOM).contains(5L);

    List<Long> missingContainerIdsSCM = containerDiscrepancyInfoListSCM.stream()
        .map(ContainerDiscrepancyInfo::getContainerID)
        .collect(Collectors.toList());
    // ContainerID 1 and 2 are missing in SCM but present in OM
    assertThat(missingContainerIdsSCM).contains(1L);
    assertThat(missingContainerIdsSCM).contains(2L);
  }

  @Test
  public void testGetOmContainersDeletedInSCM() throws Exception {
    Map<Long, ContainerMetadata> omContainers =
        reconContainerMetadataManager.getContainers(-1, 0);
    putContainerInfos(2);
    List<ContainerInfo> scmContainers = reconContainerManager.getContainers();
    assertEquals(2, omContainers.size());
    assertEquals(2, scmContainers.size());
    // Update container state of Container Id 1 to CLOSING to CLOSED
    // and then to DELETED
    reconContainerManager.updateContainerState(ContainerID.valueOf(1),
        HddsProtos.LifeCycleEvent.FINALIZE);
    reconContainerManager.updateContainerState(ContainerID.valueOf(1),
        HddsProtos.LifeCycleEvent.CLOSE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(1),
            HddsProtos.LifeCycleEvent.DELETE);
    assertContainerCount(HddsProtos.LifeCycleState.DELETING, 1);

    reconContainerManager
        .updateContainerState(ContainerID.valueOf(1),
            HddsProtos.LifeCycleEvent.CLEANUP);
    assertContainerCount(HddsProtos.LifeCycleState.DELETED, 1);

    List<ContainerInfo> deletedSCMContainers =
        reconContainerManager.getContainers(HddsProtos.LifeCycleState.DELETED);
    assertEquals(1, deletedSCMContainers.size());

    Response omContainersDeletedInSCMResponse =
        containerEndpoint.getOmContainersDeletedInSCM(-1, 0);
    assertNotNull(omContainersDeletedInSCMResponse);

    Map<String, Object> responseMap =
        (Map<String, Object>) omContainersDeletedInSCMResponse.getEntity();

    // Fetch the ContainerDiscrepancyInfo list from the response
    List<ContainerDiscrepancyInfo> containerDiscrepancyInfoList =
        (List<ContainerDiscrepancyInfo>) responseMap.get(
            "containerDiscrepancyInfo");

    // Check the prevKey is set correct in the response
    long responsePrevKey = (long) responseMap.get("lastKey");
    assertEquals(containerDiscrepancyInfoList.get(
            containerDiscrepancyInfoList.size() - 1).getContainerID(),
        responsePrevKey);

    assertEquals(3, containerDiscrepancyInfoList.get(0)
        .getNumberOfKeys());
    assertEquals(1, containerDiscrepancyInfoList.size());
  }

  @Test
  public void testGetOmContainersDeletedInSCMPagination() throws Exception {
    Map<Long, ContainerMetadata> omContainers = reconContainerMetadataManager.getContainers(-1, 0);
    putContainerInfos(2);
    List<ContainerInfo> scmContainers = reconContainerManager.getContainers();
    assertEquals(omContainers.size(), scmContainers.size());
    // Update container state of Container Id 2 to CLOSING to CLOSED
    // and then to DELETED
    updateContainerStateToDeleted(2);

    assertContainerCount(HddsProtos.LifeCycleState.DELETED, 1);

    List<ContainerInfo> deletedSCMContainers = reconContainerManager.getContainers(HddsProtos.LifeCycleState.DELETED);
    assertEquals(1, deletedSCMContainers.size());

    Response omContainersDeletedInSCMResponse =
        containerEndpoint.getOmContainersDeletedInSCM(1, 0);
    assertNotNull(omContainersDeletedInSCMResponse);

    Map<String, Object> responseMap = (Map<String, Object>) omContainersDeletedInSCMResponse.getEntity();

    // Fetch the ContainerDiscrepancyInfo list from the response
    List<ContainerDiscrepancyInfo> containerDiscrepancyInfoList =
        (List<ContainerDiscrepancyInfo>) responseMap.get("containerDiscrepancyInfo");
    assertEquals(1, containerDiscrepancyInfoList.size());
    // Check the prevKey is set correct in the response
    long responsePrevKey = (long) responseMap.get("lastKey");
    assertEquals(containerDiscrepancyInfoList.get(containerDiscrepancyInfoList.size() - 1).getContainerID(),
        responsePrevKey);
    assertEquals(2, responsePrevKey);

    assertEquals(omContainers.get(2L).getNumberOfKeys(), containerDiscrepancyInfoList.get(0).getNumberOfKeys());
    assertEquals(1, containerDiscrepancyInfoList.size());
  }

  @Test
  public void testGetOmContainersDeletedInSCMLimitParam() throws Exception {
    Map<Long, ContainerMetadata> omContainers =
        reconContainerMetadataManager.getContainers(-1, 0);
    putContainerInfos(2);
    List<ContainerInfo> scmContainers = reconContainerManager.getContainers();
    assertEquals(omContainers.size(), scmContainers.size());
    // Update container state of Container Id 1 to CLOSING to CLOSED
    // and then to DELETED
    updateContainerStateToDeleted(1);

    assertContainerCount(HddsProtos.LifeCycleState.DELETED, 1);

    List<ContainerInfo> deletedSCMContainers =
        reconContainerManager.getContainers(HddsProtos.LifeCycleState.DELETED);
    assertEquals(1, deletedSCMContainers.size());

    Response omContainersDeletedInSCMResponse =
        containerEndpoint.getOmContainersDeletedInSCM(1, 0);
    assertNotNull(omContainersDeletedInSCMResponse);

    Map<String, Object> responseMap =
        (Map<String, Object>) omContainersDeletedInSCMResponse.getEntity();

    // Fetch the ContainerDiscrepancyInfo list from the response
    List<ContainerDiscrepancyInfo> containerDiscrepancyInfoList =
        (List<ContainerDiscrepancyInfo>) responseMap.get(
            "containerDiscrepancyInfo");

    // Check the prevKey is set correct in the response
    long responsePrevKey = (long) responseMap.get("lastKey");
    assertEquals(containerDiscrepancyInfoList.get(
            containerDiscrepancyInfoList.size() - 1).getContainerID(),
        responsePrevKey);

    assertEquals(3, containerDiscrepancyInfoList.get(0)
        .getNumberOfKeys());
    assertEquals(1, containerDiscrepancyInfoList.size());
  }

  @Test
  public void testGetOmContainersDeletedInSCMPrevContainerParam()
      throws Exception {
    Map<Long, ContainerMetadata> omContainers =
        reconContainerMetadataManager.getContainers(-1, 0);
    putContainerInfos(2);
    List<ContainerInfo> scmContainers = reconContainerManager.getContainers();
    assertEquals(omContainers.size(), scmContainers.size());
    // Update container state of Container Id 1 to CLOSING to CLOSED
    // and then to DELETED
    updateContainerStateToDeleted(1);
    updateContainerStateToDeleted(2);

    assertContainerCount(HddsProtos.LifeCycleState.DELETED, 2);

    List<ContainerInfo> deletedSCMContainers =
        reconContainerManager.getContainers(HddsProtos.LifeCycleState.DELETED);
    assertEquals(2, deletedSCMContainers.size());

    Response omContainersDeletedInSCMResponse =
        containerEndpoint.getOmContainersDeletedInSCM(2,
            1);

    Map<String, Object> responseMap =
        (Map<String, Object>) omContainersDeletedInSCMResponse.getEntity();

    // Fetch the ContainerDiscrepancyInfo list from the response
    List<ContainerDiscrepancyInfo> containerDiscrepancyInfoList =
        (List<ContainerDiscrepancyInfo>) responseMap.get(
            "containerDiscrepancyInfo");

    // Check the prevKey is set correct in the response
    long responsePrevKey = (long) responseMap.get("lastKey");
    assertEquals(containerDiscrepancyInfoList.get(
            containerDiscrepancyInfoList.size() - 1).getContainerID(),
        responsePrevKey);

    assertEquals(2, containerDiscrepancyInfoList.get(0)
        .getNumberOfKeys());
    assertEquals(1, containerDiscrepancyInfoList.size());
    assertEquals(2, containerDiscrepancyInfoList.get(0).getContainerID());
  }

  /**
   * Helper method that creates duplicate FSO file keys – two keys having the same file
   * name but under different directories. It creates the necessary volume, bucket, and
   * directory entries, and then writes two keys using writeKeyToOm.
   */
  private void setUpDuplicateFSOFileKeys() throws IOException {
    // Ensure the volume exists.
    String volumeKey = reconOMMetadataManager.getVolumeKey(VOLUME_NAME);
    OmVolumeArgs volArgs = OmVolumeArgs.newBuilder()
        .setVolume(VOLUME_NAME)
        .setAdminName("TestUser")
        .setOwnerName("TestUser")
        .setObjectID(VOL_OBJECT_ID)
        .build();
    reconOMMetadataManager.getVolumeTable().put(volumeKey, volArgs);

    // Ensure the bucket exists.
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .setObjectID(BUCKET_OBJECT_ID)
        .build();
    String bucketKey = reconOMMetadataManager.getBucketKey(VOLUME_NAME, BUCKET_NAME);
    reconOMMetadataManager.getBucketTable().put(bucketKey, bucketInfo);

    // Create two directories: "dirA" and "dirB" with unique object IDs.
    // For a top-level directory in a bucket, the parent's object id is the bucket's id.
    OmDirectoryInfo dirA = OmDirectoryInfo.newBuilder()
        .setName("dirA")
        .setParentObjectID(BUCKET_OBJECT_ID)
        .setUpdateID(1L)
        .setObjectID(5L)   // Unique object id for dirA.
        .build();
    OmDirectoryInfo dirB = OmDirectoryInfo.newBuilder()
        .setName("dirB")
        .setParentObjectID(BUCKET_OBJECT_ID)
        .setUpdateID(1L)
        .setObjectID(6L)   // Unique object id for dirB.
        .build();
    // Build DB directory keys. (The third parameter is used to form a unique key.)
    String dirKeyA = reconOMMetadataManager.getOzonePathKey(VOL_OBJECT_ID, BUCKET_OBJECT_ID, 5L, "dirA");
    String dirKeyB = reconOMMetadataManager.getOzonePathKey(VOL_OBJECT_ID, BUCKET_OBJECT_ID, 6L, "dirB");
    reconOMMetadataManager.getDirectoryTable().put(dirKeyA, dirA);
    reconOMMetadataManager.getDirectoryTable().put(dirKeyB, dirB);

    // Use a common OmKeyLocationInfoGroup.
    OmKeyLocationInfoGroup locationInfoGroup = getLocationInfoGroup1();

    // Write two FSO keys with the same file name ("dupFile") but in different directories.
    // The file name stored in OM is the full path (e.g., "dirA/dupFile" vs. "dirB/dupFile").
    writeKeyToOm(reconOMMetadataManager,
        "dupFileKey1",           // internal key name for the first key
        BUCKET_NAME,
        VOLUME_NAME,
        "dupFileKey1",          // full file path for the first key
        100L,                    // object id (example)
        5L,                      // parent's object id for dirA (same as dirA's object id)
        BUCKET_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(locationInfoGroup),
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        KEY_ONE_SIZE);

    writeKeyToOm(reconOMMetadataManager,
        "dupFileKey1",           // internal key name for the second key
        BUCKET_NAME,
        VOLUME_NAME,
        "dupFileKey1",          // full file path for the second key
        100L,
        6L,                      // parent's object id for dirB
        BUCKET_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(locationInfoGroup),
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        KEY_ONE_SIZE);
  }

  /**
   * Test method that sets up two duplicate FSO file keys (same file name but in different directories)
   * and then verifies that the ContainerEndpoint returns two distinct key records.
   */
  @Test
  public void testDuplicateFSOKeysForContainerEndpoint() throws Exception {
    // Set up duplicate FSO file keys.
    setUpDuplicateFSOFileKeys();
    NSSummaryTaskWithFSO nSSummaryTaskWithFso =
        new NSSummaryTaskWithFSO(reconNamespaceSummaryManager,
            reconOMMetadataManager, 10, 5, 20, 2000);
    nSSummaryTaskWithFso.reprocessWithFSO(reconOMMetadataManager);
    // Reprocess the container key mappings so that the new keys are loaded.
    reprocessContainerKeyMapper();

    // Assume that FSO keys are mapped to container ID 20L (as in previous tests for FSO).
    Response response = containerEndpoint.getKeysForContainer(20L, -1, "");
    KeysResponse keysResponse = (KeysResponse) response.getEntity();
    Collection<KeyMetadata> keyMetadataList = keysResponse.getKeys();

    // We expect two distinct keys.
    assertEquals(2, keysResponse.getTotalCount());
    assertEquals(2, keyMetadataList.size());

    for (KeyMetadata km : keyMetadataList) {
      String completePath = km.getCompletePath();
      assertNotNull(completePath);
      // Verify that the complete path reflects either directory "dirA" or "dirB"
      if (completePath.contains("dirA")) {
        assertEquals(VOLUME_NAME + "/" + BUCKET_NAME + "/dirA/dupFileKey1", completePath);
      } else if (completePath.contains("dirB")) {
        assertEquals(VOLUME_NAME + "/" + BUCKET_NAME + "/dirB/dupFileKey1", completePath);
      } else {
        throw new AssertionError("Unexpected complete path: " + completePath);
      }
    }
  }
}
