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

package org.apache.hadoop.ozone.recon.tasks;

import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getMockOzoneManagerServiceProvider;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getOmKeyLocationInfo;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getRandomPipeline;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDataToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeKeyToOm;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Unit test for Container Key mapper task.
 */
public class TestContainerKeyMapperTask {

  @TempDir
  private Path temporaryFolder;

  private ReconContainerMetadataManager reconContainerMetadataManager;
  private OMMetadataManager omMetadataManager;
  private ReconOMMetadataManager reconOMMetadataManager;
  private OzoneConfiguration omConfiguration;

  private static final String FSO_KEY_NAME = "dir1/file7";
  private static final String BUCKET_NAME = "bucket1";
  private static final String VOLUME_NAME = "vol";
  private static final String FILE_NAME = "file7";
  private static final String OTHER_FILE_NAME = "file8";
  private static final String INSERTED_KEY = "keyToBeInserted";
  private static final String DELETED_KEY = "keyToBeDeleted";
  private static final long KEY_ONE_OBJECT_ID = 3L; // 3 bytes
  private static final long BUCKET_ONE_OBJECT_ID = 1L;
  private static final long VOL_OBJECT_ID = 0L;
  private static final long KEY_ONE_SIZE = 500L; // 500 bytes

  @BeforeEach
  public void setUp() throws Exception {
    omMetadataManager = initializeNewOmMetadataManager(
        temporaryFolder.resolve("JunitOmDBDir").toFile());
    OzoneManagerServiceProviderImpl ozoneManagerServiceProvider = getMockOzoneManagerServiceProvider();
    reconOMMetadataManager = getTestReconOmMetadataManager(omMetadataManager,
        temporaryFolder.resolve("JunitOmMetadataDir").toFile());
    omConfiguration = new OzoneConfiguration();

    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder.toFile())
            .withReconSqlDb()
            .withReconOm(reconOMMetadataManager)
            .withOmServiceProvider(ozoneManagerServiceProvider)
            .withContainerDB()
            .build();
    reconContainerMetadataManager =
        reconTestInjector.getInstance(ReconContainerMetadataManager.class);
    
    // Clear shared container count map and reset flags for clean test state
    ContainerKeyMapperHelper.clearSharedContainerCountMap();
    ReconConstants.resetTableTruncatedFlags();
  }

  @Test
  public void testKeyTableReprocess() throws Exception {

    Map<ContainerKeyPrefix, Integer> keyPrefixesForContainer =
        reconContainerMetadataManager.getKeyPrefixesForContainer(1);
    assertThat(keyPrefixesForContainer).isEmpty();

    keyPrefixesForContainer = reconContainerMetadataManager
        .getKeyPrefixesForContainer(2);
    assertThat(keyPrefixesForContainer).isEmpty();

    Pipeline pipeline = getRandomPipeline();

    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();
    BlockID blockID1 = new BlockID(1, 1);
    OmKeyLocationInfo omKeyLocationInfo1 = getOmKeyLocationInfo(blockID1,
        pipeline);

    BlockID blockID2 = new BlockID(2, 1);
    OmKeyLocationInfo omKeyLocationInfo2
        = getOmKeyLocationInfo(blockID2, pipeline);

    omKeyLocationInfoList.add(omKeyLocationInfo1);
    omKeyLocationInfoList.add(omKeyLocationInfo2);

    OmKeyLocationInfoGroup omKeyLocationInfoGroup = new
        OmKeyLocationInfoGroup(0, omKeyLocationInfoList);

    writeDataToOm(reconOMMetadataManager,
        FILE_NAME,
        BUCKET_NAME,
        VOLUME_NAME,
        Collections.singletonList(omKeyLocationInfoGroup));

    ContainerKeyMapperTaskOBS containerKeyMapperTaskOBS =
        new ContainerKeyMapperTaskOBS(reconContainerMetadataManager,
            omConfiguration);
    containerKeyMapperTaskOBS.reprocess(reconOMMetadataManager);

    keyPrefixesForContainer =
        reconContainerMetadataManager.getKeyPrefixesForContainer(1);
    assertEquals(1, keyPrefixesForContainer.size());
    String omKey = omMetadataManager.getOzoneKey(VOLUME_NAME, BUCKET_NAME,
        FILE_NAME);
    ContainerKeyPrefix containerKeyPrefix = ContainerKeyPrefix.get(1,
        omKey, 0);
    assertEquals(1,
        keyPrefixesForContainer.get(containerKeyPrefix).intValue());

    keyPrefixesForContainer =
        reconContainerMetadataManager.getKeyPrefixesForContainer(2);
    assertEquals(1, keyPrefixesForContainer.size());
    containerKeyPrefix = ContainerKeyPrefix.get(2, omKey,
        0);
    assertEquals(1,
        keyPrefixesForContainer.get(containerKeyPrefix).intValue());

    // Test if container key counts are updated
    assertEquals(1, reconContainerMetadataManager.getKeyCountForContainer(1L));
    assertEquals(1, reconContainerMetadataManager.getKeyCountForContainer(2L));
    assertEquals(0, reconContainerMetadataManager.getKeyCountForContainer(3L));

    // Test if container count is updated
    assertEquals(2, reconContainerMetadataManager.getCountForContainers());
  }

  @Test
  public void testFileTableReprocess() throws Exception {
    // Make sure the key prefixes are empty for container 1
    Map<ContainerKeyPrefix, Integer> keyPrefixesForContainer =
        reconContainerMetadataManager.getKeyPrefixesForContainer(1L);
    assertThat(keyPrefixesForContainer).isEmpty();

    // Make sure the key prefixes are empty for container 2
    keyPrefixesForContainer =
        reconContainerMetadataManager.getKeyPrefixesForContainer(2L);
    assertThat(keyPrefixesForContainer).isEmpty();

    // Create a random pipeline and a list of OmKeyLocationInfo objects
    Pipeline pipeline = getRandomPipeline();
    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();
    BlockID blockID1 = new BlockID(1L, 1L);
    OmKeyLocationInfo omKeyLocationInfo1 =
        getOmKeyLocationInfo(blockID1, pipeline);
    BlockID blockID2 = new BlockID(2L, 1L);
    OmKeyLocationInfo omKeyLocationInfo2 =
        getOmKeyLocationInfo(blockID2, pipeline);
    omKeyLocationInfoList.add(omKeyLocationInfo1);
    omKeyLocationInfoList.add(omKeyLocationInfo2);
    OmKeyLocationInfoGroup omKeyLocationInfoGroup =
        new OmKeyLocationInfoGroup(0L, omKeyLocationInfoList);

    // Write the key to OM
    writeKeyToOm(reconOMMetadataManager,
        FSO_KEY_NAME,
        BUCKET_NAME,
        VOLUME_NAME,
        FILE_NAME,
        KEY_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(omKeyLocationInfoGroup),
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        KEY_ONE_SIZE);

    // Reprocess container key mappings
    ContainerKeyMapperTaskFSO containerKeyMapperTaskFSO =
        new ContainerKeyMapperTaskFSO(reconContainerMetadataManager,
            omConfiguration);
    containerKeyMapperTaskFSO.reprocess(reconOMMetadataManager);

    // Check the key prefixes for container 1
    keyPrefixesForContainer =
        reconContainerMetadataManager.getKeyPrefixesForContainer(1L);
    String omKey =
        omMetadataManager.getOzonePathKey(VOL_OBJECT_ID, BUCKET_ONE_OBJECT_ID,
            BUCKET_ONE_OBJECT_ID, FILE_NAME);
    ContainerKeyPrefix containerKeyPrefix =
        ContainerKeyPrefix.get(1L, omKey, 0L);
    assertEquals(1L, keyPrefixesForContainer.size());
    assertEquals(1L,
        keyPrefixesForContainer.get(containerKeyPrefix).intValue());

    // Check the key prefixes for container 2
    keyPrefixesForContainer =
        reconContainerMetadataManager.getKeyPrefixesForContainer(2L);
    containerKeyPrefix = ContainerKeyPrefix.get(2L, omKey, 0L);
    assertEquals(1L, keyPrefixesForContainer.size());
    assertEquals(1L,
        keyPrefixesForContainer.get(containerKeyPrefix).intValue());

    // Check that the container key counts are updated
    assertEquals(1L, reconContainerMetadataManager.getKeyCountForContainer(1L));
    assertEquals(1L, reconContainerMetadataManager.getKeyCountForContainer(2L));
    assertEquals(0L, reconContainerMetadataManager.getKeyCountForContainer(3L));

    // Check that the container count is updated
    assertEquals(2L, reconContainerMetadataManager.getCountForContainers());
  }

  @Test
  public void testKeyTableProcess() throws IOException {
    Map<ContainerKeyPrefix, Integer> keyPrefixesForContainer =
        reconContainerMetadataManager.getKeyPrefixesForContainer(1);
    assertThat(keyPrefixesForContainer).isEmpty();

    keyPrefixesForContainer = reconContainerMetadataManager
        .getKeyPrefixesForContainer(2);
    assertThat(keyPrefixesForContainer).isEmpty();

    Pipeline pipeline = getRandomPipeline();

    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();
    BlockID blockID1 = new BlockID(1, 1);
    OmKeyLocationInfo omKeyLocationInfo1 = getOmKeyLocationInfo(blockID1,
        pipeline);

    BlockID blockID2 = new BlockID(2, 1);
    OmKeyLocationInfo omKeyLocationInfo2
        = getOmKeyLocationInfo(blockID2, pipeline);

    omKeyLocationInfoList.add(omKeyLocationInfo1);
    omKeyLocationInfoList.add(omKeyLocationInfo2);

    OmKeyLocationInfoGroup omKeyLocationInfoGroup = new
        OmKeyLocationInfoGroup(0, omKeyLocationInfoList);

    String bucket = BUCKET_NAME;
    String volume = VOLUME_NAME;
    String key = FILE_NAME;
    String omKey = omMetadataManager.getOzoneKey(volume, bucket, key);
    OmKeyInfo omKeyInfo = buildOmKeyInfo(volume, bucket, key,
        omKeyLocationInfoGroup);

    OMDBUpdateEvent keyEvent1 = new OMDBUpdateEvent.
        OMUpdateEventBuilder<String, OmKeyInfo>()
        .setKey(omKey)
        .setValue(omKeyInfo)
        .setTable(omMetadataManager.getKeyTable(getBucketLayout()).getName())
        .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
        .build();

    BlockID blockID3 = new BlockID(1, 2);
    OmKeyLocationInfo omKeyLocationInfo3 =
        getOmKeyLocationInfo(blockID3, pipeline);

    BlockID blockID4 = new BlockID(3, 1);
    OmKeyLocationInfo omKeyLocationInfo4
        = getOmKeyLocationInfo(blockID4, pipeline);

    omKeyLocationInfoList = new ArrayList<>();
    omKeyLocationInfoList.add(omKeyLocationInfo3);
    omKeyLocationInfoList.add(omKeyLocationInfo4);
    omKeyLocationInfoGroup = new OmKeyLocationInfoGroup(0,
        omKeyLocationInfoList);

    String key2 = DELETED_KEY;
    writeDataToOm(reconOMMetadataManager, key2, bucket, volume, Collections
        .singletonList(omKeyLocationInfoGroup));

    omKey = omMetadataManager.getOzoneKey(volume, bucket, key2);
    OMDBUpdateEvent keyEvent2 = new OMDBUpdateEvent.
        OMUpdateEventBuilder<String, OmKeyInfo>()
        .setKey(omKey)
        .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
        .setValue(omKeyInfo)
        .setTable(omMetadataManager.getKeyTable(getBucketLayout()).getName())
        .build();

    OMUpdateEventBatch omUpdateEventBatch = new OMUpdateEventBatch(
        new ArrayList<>(Arrays.asList(keyEvent1, keyEvent2)), 0L);

    ContainerKeyMapperTaskOBS containerKeyMapperTaskOBS =
        new ContainerKeyMapperTaskOBS(reconContainerMetadataManager,
            omConfiguration);
    containerKeyMapperTaskOBS.reprocess(reconOMMetadataManager);

    keyPrefixesForContainer = reconContainerMetadataManager
        .getKeyPrefixesForContainer(1);
    assertEquals(1, keyPrefixesForContainer.size());

    keyPrefixesForContainer = reconContainerMetadataManager
        .getKeyPrefixesForContainer(2);
    assertThat(keyPrefixesForContainer).isEmpty();

    keyPrefixesForContainer = reconContainerMetadataManager
        .getKeyPrefixesForContainer(3);
    assertEquals(1, keyPrefixesForContainer.size());

    assertEquals(1, reconContainerMetadataManager.getKeyCountForContainer(1L));
    assertEquals(0, reconContainerMetadataManager.getKeyCountForContainer(2L));
    assertEquals(1, reconContainerMetadataManager.getKeyCountForContainer(3L));

    // Process PUT & DELETE event.
    containerKeyMapperTaskOBS.process(omUpdateEventBatch, Collections.emptyMap());

    keyPrefixesForContainer = reconContainerMetadataManager
        .getKeyPrefixesForContainer(1);
    assertEquals(1, keyPrefixesForContainer.size());

    keyPrefixesForContainer = reconContainerMetadataManager
        .getKeyPrefixesForContainer(2);
    assertEquals(1, keyPrefixesForContainer.size());

    keyPrefixesForContainer = reconContainerMetadataManager
        .getKeyPrefixesForContainer(3);
    assertThat(keyPrefixesForContainer).isEmpty();

    assertEquals(1, reconContainerMetadataManager.getKeyCountForContainer(1L));
    assertEquals(1, reconContainerMetadataManager.getKeyCountForContainer(2L));
    assertEquals(0, reconContainerMetadataManager.getKeyCountForContainer(3L));

    // Test if container count is updated
    assertEquals(3, reconContainerMetadataManager.getCountForContainers());
  }

  @Test
  public void testFileTableProcess() throws Exception {
    // Verify that keyPrefixesForContainer is empty for container 1 and 2
    Map<ContainerKeyPrefix, Integer> keyPrefixesForContainer =
        reconContainerMetadataManager.getKeyPrefixesForContainer(1);
    assertThat(keyPrefixesForContainer).isEmpty();

    keyPrefixesForContainer = reconContainerMetadataManager
        .getKeyPrefixesForContainer(2);
    assertThat(keyPrefixesForContainer).isEmpty();

    // Create a random pipeline and a list of OmKeyLocationInfo objects
    Pipeline pipeline = getRandomPipeline();
    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();
    BlockID blockID1 = new BlockID(1L, 1L);
    OmKeyLocationInfo omKeyLocationInfo1 =
        getOmKeyLocationInfo(blockID1, pipeline);
    BlockID blockID2 = new BlockID(2L, 1L);
    OmKeyLocationInfo omKeyLocationInfo2 =
        getOmKeyLocationInfo(blockID2, pipeline);
    omKeyLocationInfoList.add(omKeyLocationInfo1);
    omKeyLocationInfoList.add(omKeyLocationInfo2);
    OmKeyLocationInfoGroup omKeyLocationInfoGroup =
        new OmKeyLocationInfoGroup(0L, omKeyLocationInfoList);

    // Reprocess container key mappings
    ContainerKeyMapperTaskFSO containerKeyMapperTaskFSO =
        new ContainerKeyMapperTaskFSO(reconContainerMetadataManager,
            omConfiguration);

    String bucket = BUCKET_NAME;
    String volume = VOLUME_NAME;
    String key = INSERTED_KEY;
    String omKey = omMetadataManager.getOzoneKey(volume, bucket, key);
    OmKeyInfo omKeyInfo = buildOmKeyInfo(volume, bucket, key,
        omKeyLocationInfoGroup);

    OMDBUpdateEvent keyEvent1 = new OMDBUpdateEvent.
        OMUpdateEventBuilder<String, OmKeyInfo>()
        .setKey(omKey)
        .setValue(omKeyInfo)
        .setTable(
            omMetadataManager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
                .getName())
        .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
        .build();

    String key2 = DELETED_KEY;

    omKey = omMetadataManager.getOzoneKey(volume, bucket, key2);
    OMDBUpdateEvent keyEvent2 = new OMDBUpdateEvent.
        OMUpdateEventBuilder<String, OmKeyInfo>()
        .setKey(omKey)
        .setValue(omKeyInfo)
        .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
        .setTable(
            omMetadataManager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
                .getName())
        .build();

    OMUpdateEventBatch omUpdateEventBatch =
        new OMUpdateEventBatch(new ArrayList<>(Arrays.asList(keyEvent1, keyEvent2)), 0L);

    // Process PUT event for both the keys
    containerKeyMapperTaskFSO.process(omUpdateEventBatch, Collections.emptyMap());

    keyPrefixesForContainer = reconContainerMetadataManager
        .getKeyPrefixesForContainer(1);
    assertEquals(2, keyPrefixesForContainer.size());
    Iterator<ContainerKeyPrefix> iterator =
        keyPrefixesForContainer.keySet().iterator();
    ContainerKeyPrefix firstKeyPrefix = iterator.next();
    ContainerKeyPrefix secondKeyPrefix = iterator.next();

    assertEquals("/" + VOLUME_NAME + "/" + BUCKET_NAME + "/" + DELETED_KEY,
        firstKeyPrefix.getKeyPrefix());
    assertEquals("/" + VOLUME_NAME + "/" + BUCKET_NAME + "/" + INSERTED_KEY,
        secondKeyPrefix.getKeyPrefix());

    omKey = omMetadataManager.getOzoneKey(volume, bucket, key2);
    OMDBUpdateEvent keyEvent3 = new OMDBUpdateEvent.
        OMUpdateEventBuilder<String, OmKeyInfo>()
        .setKey(omKey)
        .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
        .setValue(omKeyInfo)
        .setTable(
            omMetadataManager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
                .getName())
        .build();
    OMUpdateEventBatch omUpdateEventBatch2 =
        new OMUpdateEventBatch(new ArrayList<>(Collections.singletonList(keyEvent3)), 0L);

    // Process DELETE event for key2
    containerKeyMapperTaskFSO.process(omUpdateEventBatch2, Collections.emptyMap());

    keyPrefixesForContainer = reconContainerMetadataManager
        .getKeyPrefixesForContainer(1);
    // The second key is deleted
    assertEquals(1, keyPrefixesForContainer.size());
    iterator = keyPrefixesForContainer.keySet().iterator();
    firstKeyPrefix = iterator.next();
    assertEquals("/" + VOLUME_NAME + "/" + BUCKET_NAME + "/" + INSERTED_KEY,
        firstKeyPrefix.getKeyPrefix());
  }

  @Test
  public void testDuplicateFSOKeysInDifferentDirectories() throws Exception {
    // Ensure container 1 is initially empty.
    Map<ContainerKeyPrefix, Integer> keyPrefixesForContainer =
        reconContainerMetadataManager.getKeyPrefixesForContainer(1L);
    assertThat(keyPrefixesForContainer).isEmpty();

    Pipeline pipeline = getRandomPipeline();
    // Create a common OmKeyLocationInfoGroup for all keys.
    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();
    BlockID blockID = new BlockID(1L, 1L);
    OmKeyLocationInfo omKeyLocationInfo = getOmKeyLocationInfo(blockID, pipeline);
    omKeyLocationInfoList.add(omKeyLocationInfo);
    OmKeyLocationInfoGroup omKeyLocationInfoGroup =
        new OmKeyLocationInfoGroup(0L, omKeyLocationInfoList);

    // Define file names.
    String file1Key = "file1";
    String file2Key = "file2";

    // Define directory (parent) object IDs with shorter values.
    long dir1Id = -101L;
    long dir2Id = -102L;
    long dir3Id = -103L;

    // Write three FSO keys for "file1" with different parent object IDs.
    writeKeyToOm(reconOMMetadataManager,
        file1Key,                // keyName
        BUCKET_NAME,             // bucketName
        VOLUME_NAME,             // volName
        file1Key,                // fileName
        KEY_ONE_OBJECT_ID,       // objectId
        dir1Id,                  // ObjectId for first directory
        BUCKET_ONE_OBJECT_ID,    // bucketObjectId
        VOL_OBJECT_ID,           // volumeObjectId
        Collections.singletonList(omKeyLocationInfoGroup),
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        KEY_ONE_SIZE);

    writeKeyToOm(reconOMMetadataManager,
        file1Key,
        BUCKET_NAME,
        VOLUME_NAME,
        file1Key,
        KEY_ONE_OBJECT_ID,
        dir2Id,            // ObjectId for second directory
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(omKeyLocationInfoGroup),
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        KEY_ONE_SIZE);

    writeKeyToOm(reconOMMetadataManager,
        file1Key,
        BUCKET_NAME,
        VOLUME_NAME,
        file1Key,
        KEY_ONE_OBJECT_ID,
        dir3Id,            // ObjectId for third directory
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(omKeyLocationInfoGroup),
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        KEY_ONE_SIZE);

    // Write three FSO keys for "file2" with different parent object IDs.
    writeKeyToOm(reconOMMetadataManager,
        "fso-file2",
        BUCKET_NAME,
        VOLUME_NAME,
        file2Key,
        KEY_ONE_OBJECT_ID,
        dir1Id,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(omKeyLocationInfoGroup),
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        KEY_ONE_SIZE);

    writeKeyToOm(reconOMMetadataManager,
        "fso-file2",
        BUCKET_NAME,
        VOLUME_NAME,
        file2Key,
        KEY_ONE_OBJECT_ID,
        dir2Id,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(omKeyLocationInfoGroup),
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        KEY_ONE_SIZE);

    writeKeyToOm(reconOMMetadataManager,
        "fso-file2",
        BUCKET_NAME,
        VOLUME_NAME,
        file2Key,
        KEY_ONE_OBJECT_ID,
        dir3Id,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(omKeyLocationInfoGroup),
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        KEY_ONE_SIZE);

    // Reprocess container key mappings.
    ContainerKeyMapperTaskFSO containerKeyMapperTask =
        new ContainerKeyMapperTaskFSO(reconContainerMetadataManager, omConfiguration);
    containerKeyMapperTask.reprocess(reconOMMetadataManager);

    // With our changes using the raw key prefix as the unique identifier,
    // we expect six distinct entries in container 1.
    keyPrefixesForContainer = reconContainerMetadataManager.getKeyPrefixesForContainer(1L);
    assertEquals(6, keyPrefixesForContainer.size());
  }

  @ParameterizedTest
  @EnumSource(value = BucketLayout.class, names = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
  public void testOverwriteMovesKeyToOtherContainers(BucketLayout layout) throws Exception {
    // The key has blocks in containers 1 and 2; another key keeps container 1 in use.
    OmKeyInfo oldKeyInfo = writeKey(layout, FILE_NAME, 1L, 2L);
    writeKey(layout, OTHER_FILE_NAME, 1L);
    ReconOmTask task = getContainerKeyMapperTask(layout);
    task.reprocess(reconOMMetadataManager);

    // The overwrite moves the block in container 1 to container 3 and keeps the block in container 2.
    String dbKey = getDbKey(layout, FILE_NAME);
    OmKeyInfo newKeyInfo = buildOmKeyInfo(VOLUME_NAME, BUCKET_NAME, FILE_NAME, getLocationGroup(2L, 3L));
    OMUpdateEventBatch batch = new OMUpdateEventBatch(new ArrayList<>(Collections.singletonList(
        getUpdateEvent(layout, dbKey, oldKeyInfo, newKeyInfo))), 0L);
    task.process(batch, Collections.emptyMap());

    assertThat(getKeysInContainer(1L)).containsExactly(getDbKey(layout, OTHER_FILE_NAME));
    assertThat(getKeysInContainer(2L)).containsExactly(dbKey);
    assertThat(getKeysInContainer(3L)).containsExactly(dbKey);
    assertEquals(1, reconContainerMetadataManager.getKeyCountForContainer(1L));
    assertEquals(1, reconContainerMetadataManager.getKeyCountForContainer(2L));
    assertEquals(1, reconContainerMetadataManager.getKeyCountForContainer(3L));
    assertEquals(3, reconContainerMetadataManager.getCountForContainers());
  }

  @ParameterizedTest
  @EnumSource(value = BucketLayout.class, names = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
  public void testTwoOverwritesInOneBatch(BucketLayout layout) throws Exception {
    // The key has a block in container 1; another key keeps container 1 in use.
    OmKeyInfo oldKeyInfo = writeKey(layout, FILE_NAME, 1L);
    writeKey(layout, OTHER_FILE_NAME, 1L);
    ReconOmTask task = getContainerKeyMapperTask(layout);
    task.reprocess(reconOMMetadataManager);

    // Two overwrites in one batch move the key from container 1 to container 2, then to container 3.
    String dbKey = getDbKey(layout, FILE_NAME);
    OmKeyInfo firstKeyInfo = buildOmKeyInfo(VOLUME_NAME, BUCKET_NAME, FILE_NAME, getLocationGroup(2L));
    OmKeyInfo secondKeyInfo = buildOmKeyInfo(VOLUME_NAME, BUCKET_NAME, FILE_NAME, getLocationGroup(3L));
    OMUpdateEventBatch batch = new OMUpdateEventBatch(new ArrayList<>(Arrays.asList(
        getUpdateEvent(layout, dbKey, oldKeyInfo, firstKeyInfo),
        getUpdateEvent(layout, dbKey, firstKeyInfo, secondKeyInfo))), 0L);
    task.process(batch, Collections.emptyMap());

    assertThat(getKeysInContainer(1L)).containsExactly(getDbKey(layout, OTHER_FILE_NAME));
    assertThat(getKeysInContainer(2L)).isEmpty();
    assertThat(getKeysInContainer(3L)).containsExactly(dbKey);
    assertEquals(1, reconContainerMetadataManager.getKeyCountForContainer(1L));
    assertEquals(0, reconContainerMetadataManager.getKeyCountForContainer(2L));
    assertEquals(1, reconContainerMetadataManager.getKeyCountForContainer(3L));
  }

  private ReconOmTask getContainerKeyMapperTask(BucketLayout layout) {
    return layout == BucketLayout.FILE_SYSTEM_OPTIMIZED
        ? new ContainerKeyMapperTaskFSO(reconContainerMetadataManager, omConfiguration)
        : new ContainerKeyMapperTaskOBS(reconContainerMetadataManager, omConfiguration);
  }

  /** Returns the OM DB key of a file directly under the bucket. */
  private String getDbKey(BucketLayout layout, String fileName) {
    return layout == BucketLayout.FILE_SYSTEM_OPTIMIZED
        ? omMetadataManager.getOzonePathKey(VOL_OBJECT_ID, BUCKET_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID, fileName)
        : omMetadataManager.getOzoneKey(VOLUME_NAME, BUCKET_NAME, fileName);
  }

  /** Writes a file directly under the bucket, with one block in each of the given containers. */
  private OmKeyInfo writeKey(BucketLayout layout, String fileName, long... containerIds) throws IOException {
    writeKeyToOm(reconOMMetadataManager, fileName, BUCKET_NAME, VOLUME_NAME, fileName, KEY_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID, VOL_OBJECT_ID,
        Collections.singletonList(getLocationGroup(containerIds)), layout, KEY_ONE_SIZE);
    return reconOMMetadataManager.getKeyTable(layout).get(getDbKey(layout, fileName));
  }

  private OmKeyLocationInfoGroup getLocationGroup(long... containerIds) {
    Pipeline pipeline = getRandomPipeline();
    List<OmKeyLocationInfo> locations = new ArrayList<>();
    for (long containerId : containerIds) {
      locations.add(getOmKeyLocationInfo(new BlockID(containerId, 1L), pipeline));
    }
    return new OmKeyLocationInfoGroup(0L, locations);
  }

  private List<String> getKeysInContainer(long containerId) throws IOException {
    return reconContainerMetadataManager.getKeyPrefixesForContainer(containerId).keySet().stream()
        .map(ContainerKeyPrefix::getKeyPrefix).collect(Collectors.toList());
  }

  private OMDBUpdateEvent getUpdateEvent(BucketLayout layout, String dbKey, OmKeyInfo oldKeyInfo,
      OmKeyInfo newKeyInfo) {
    return new OMDBUpdateEvent.OMUpdateEventBuilder<String, OmKeyInfo>()
        .setKey(dbKey)
        .setOldValue(oldKeyInfo)
        .setValue(newKeyInfo)
        .setTable(omMetadataManager.getKeyTable(layout).getName())
        .setAction(OMDBUpdateEvent.OMDBUpdateAction.UPDATE)
        .build();
  }

  private OmKeyInfo buildOmKeyInfo(String volume,
                                   String bucket,
                                   String key,
                                   OmKeyLocationInfoGroup
                                       omKeyLocationInfoGroup) {
    return new OmKeyInfo.Builder()
        .setBucketName(bucket)
        .setVolumeName(volume)
        .setKeyName(key)
        .setReplicationConfig(StandaloneReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.ONE))
        .setOmKeyLocationInfos(Collections.singletonList(
            omKeyLocationInfoGroup))
        .build();
  }

  private BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }
}
