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

package org.apache.hadoop.ozone.recon.tasks;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.impl.ReconNamespaceSummaryManagerImpl;
import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.VOLUME_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_DIR_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_KEY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeKeyToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDeletedDirToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeOpenKeyToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeOpenFileToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDeletedKeysToOm;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.DELETE;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.PUT;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.UPDATE;
import static org.hadoop.ozone.recon.schema.tables.GlobalStatsTable.GLOBAL_STATS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This test class is designed for the OM Table Insight Task. It conducts tests
 * for tables that require both Size and Count, as well as for those that only
 * require Count.
 */
public class TestOmTableInsightTask extends AbstractReconSqlDBTest {
  @TempDir
  private Path temporaryFolder;
  private static GlobalStatsDao globalStatsDao;
  private static OmTableInsightTask omTableInsightTask;
  private static DSLContext dslContext;
  private boolean isSetupDone = false;
  private static ReconOMMetadataManager reconOMMetadataManager;
  private static NSSummaryTaskWithFSO nSSummaryTaskWithFso;
  private static OzoneConfiguration ozoneConfiguration;
  private static ReconNamespaceSummaryManagerImpl reconNamespaceSummaryManager;

  // Object names in FSO-enabled format
  private static final String VOL = "volume1";
  private static final String BUCKET_ONE = "bucket1";
  private static final String BUCKET_TWO = "bucket2";
  private static final String KEY_ONE = "file1";
  private static final String KEY_TWO = "file2";
  private static final String KEY_THREE = "dir1/dir2/file3";
  private static final String FILE_ONE = "file1";
  private static final String FILE_TWO = "file2";
  private static final String FILE_THREE = "file3";
  private static final String DIR_ONE = "dir1";
  private static final String DIR_TWO = "dir2";
  private static final String DIR_THREE = "dir3";


  private static final long VOL_OBJECT_ID = 0L;
  private static final long BUCKET_ONE_OBJECT_ID = 1L;
  private static final long BUCKET_TWO_OBJECT_ID = 2L;
  private static final long KEY_ONE_OBJECT_ID = 3L;
  private static final long DIR_ONE_OBJECT_ID = 14L;
  private static final long KEY_TWO_OBJECT_ID = 5L;
  private static final long DIR_TWO_OBJECT_ID = 17L;
  private static final long KEY_THREE_OBJECT_ID = 8L;
  private static final long DIR_THREE_OBJECT_ID = 10L;

  private static final long KEY_ONE_SIZE = 500L;
  private static final long KEY_TWO_SIZE = 1025L;
  private static final long KEY_THREE_SIZE = 2000L;

  // mock client's path requests
  private static final String TEST_USER = "TestUser";

  @Mock
  private Table<Long, NSSummary> nsSummaryTable;

  public TestOmTableInsightTask() {
    super();
  }

  private void initializeInjector() throws IOException {
    ozoneConfiguration = new OzoneConfiguration();
    reconOMMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(Files.createDirectory(
            temporaryFolder.resolve("JunitOmDBDir")).toFile()),
        Files.createDirectory(temporaryFolder.resolve("NewDir")).toFile());
    globalStatsDao = getDao(GlobalStatsDao.class);

    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder.toFile())
            .withReconSqlDb()
            .withReconOm(reconOMMetadataManager)
            .withContainerDB()
            .build();
    reconNamespaceSummaryManager = reconTestInjector.getInstance(
        ReconNamespaceSummaryManagerImpl.class);

    omTableInsightTask = new OmTableInsightTask(
        globalStatsDao, getConfiguration(), reconOMMetadataManager);
    nSSummaryTaskWithFso = new NSSummaryTaskWithFSO(
        reconNamespaceSummaryManager, reconOMMetadataManager,
        ozoneConfiguration);
    dslContext = getDslContext();
  }

  @BeforeEach
  public void setUp() throws IOException {
    // The following setup runs only once
    if (!isSetupDone) {
      initializeInjector();
      isSetupDone = true;
    }
    MockitoAnnotations.openMocks(this);
    // Truncate table before running each test
    dslContext.truncate(GLOBAL_STATS);
  }

  /**
   * Populate OM-DB with the following structure.
   * volume1
   * |      \
   * bucket1   bucket2
   * /     \       \
   * dir1    dir2     dir3
   * / \        \
   * file1  file2  file3
   *
   * @throws IOException
   */
  private void populateOMDB() throws IOException {

    // Create 2 Buckets bucket1 and bucket2
    OmBucketInfo bucketInfo1 = OmBucketInfo.newBuilder()
        .setVolumeName(VOL)
        .setBucketName(BUCKET_ONE)
        .setObjectID(BUCKET_ONE_OBJECT_ID)
        .build();
    String bucketKey = reconOMMetadataManager.getBucketKey(
        bucketInfo1.getVolumeName(), bucketInfo1.getBucketName());
    reconOMMetadataManager.getBucketTable().put(bucketKey, bucketInfo1);
    OmBucketInfo bucketInfo2 = OmBucketInfo.newBuilder()
        .setVolumeName(VOL)
        .setBucketName(BUCKET_TWO)
        .setObjectID(BUCKET_TWO_OBJECT_ID)
        .build();
    bucketKey = reconOMMetadataManager.getBucketKey(
        bucketInfo2.getVolumeName(), bucketInfo2.getBucketName());
    reconOMMetadataManager.getBucketTable().put(bucketKey, bucketInfo2);

    // Create a single volume named volume1
    String volumeKey = reconOMMetadataManager.getVolumeKey(VOL);
    OmVolumeArgs args =
        OmVolumeArgs.newBuilder()
            .setObjectID(VOL_OBJECT_ID)
            .setVolume(VOL)
            .setAdminName(TEST_USER)
            .setOwnerName(TEST_USER)
            .build();
    reconOMMetadataManager.getVolumeTable().put(volumeKey, args);

    // Generate keys for the File Table
    writeKeyToOm(reconOMMetadataManager,
        KEY_ONE,
        BUCKET_ONE,
        VOL,
        FILE_ONE,
        KEY_ONE_OBJECT_ID,
        DIR_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        KEY_ONE_SIZE,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    writeKeyToOm(reconOMMetadataManager,
        KEY_TWO,
        BUCKET_ONE,
        VOL,
        FILE_TWO,
        KEY_TWO_OBJECT_ID,
        DIR_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        KEY_TWO_SIZE,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    writeKeyToOm(reconOMMetadataManager,
        KEY_THREE,
        BUCKET_ONE,
        VOL,
        FILE_THREE,
        KEY_THREE_OBJECT_ID,
        DIR_TWO_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        KEY_THREE_SIZE,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    // Generate Deleted Directories in OM
    writeDeletedDirToOm(reconOMMetadataManager,
        BUCKET_ONE,
        VOL,
        DIR_ONE,
        BUCKET_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        DIR_ONE_OBJECT_ID);
    writeDeletedDirToOm(reconOMMetadataManager,
        BUCKET_ONE,
        VOL,
        DIR_TWO,
        BUCKET_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        DIR_TWO_OBJECT_ID);
    writeDeletedDirToOm(reconOMMetadataManager,
        BUCKET_TWO,
        VOL,
        DIR_THREE,
        BUCKET_TWO_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID,
        VOL_OBJECT_ID,
        DIR_THREE_OBJECT_ID);
  }

  @Test
  public void testReprocessForDeletedDirectory() throws Exception {
    // Create keys and deleted directories
    populateOMDB();

    // Generate NamespaceSummary for the OM DB
    nSSummaryTaskWithFso.reprocessWithFSO(reconOMMetadataManager);

    Pair<String, Boolean> result =
        omTableInsightTask.reprocess(reconOMMetadataManager);
    assertTrue(result.getRight());
    assertEquals(3, getCountForTable(DELETED_DIR_TABLE));
  }

  @Test
  public void testProcessForDeletedDirectoryTable() throws IOException {
    // Prepare mock data size
    Long expectedSize1 = 1000L;
    Long expectedSize2 = 2000L;
    NSSummary nsSummary1 = new NSSummary();
    NSSummary nsSummary2 = new NSSummary();
    nsSummary1.setSizeOfFiles(expectedSize1);
    nsSummary2.setSizeOfFiles(expectedSize2);
    when(nsSummaryTable.get(1L)).thenReturn(nsSummary1);
    when(nsSummaryTable.get(2L)).thenReturn(nsSummary1);
    when(nsSummaryTable.get(3L)).thenReturn(nsSummary2);
    when(nsSummaryTable.get(4L)).thenReturn(nsSummary2);
    when(nsSummaryTable.get(5L)).thenReturn(nsSummary2);

    /* DB key in DeletedDirectoryTable =>
                  "/volumeId/bucketId/parentId/dirName/dirObjectId" */
    List<String> paths = Arrays.asList(
        "/18/28/22/dir1/1",
        "/18/26/23/dir1/2",
        "/18/20/24/dir1/3",
        "/18/21/25/dir1/4",
        "/18/27/26/dir1/5"
    );

    // Testing PUT events
    // Create 5 OMDBUpdateEvent instances for 5 different deletedDirectory paths
    ArrayList<OMDBUpdateEvent> putEvents = new ArrayList<>();
    for (long i = 0L; i < 5L; i++) {
      putEvents.add(getOMUpdateEvent(paths.get((int) i),
          getOmKeyInfo("vol1", "bucket1", DIR_ONE, (i + 1), false),
          DELETED_DIR_TABLE, PUT, null));
    }
    OMUpdateEventBatch putEventBatch = new OMUpdateEventBatch(putEvents);
    omTableInsightTask.process(putEventBatch);
    assertEquals(5, getCountForTable(DELETED_DIR_TABLE));


    // Testing DELETE events
    // Create 2 OMDBUpdateEvent instances for 2 different deletedDirectory paths
    ArrayList<OMDBUpdateEvent> deleteEvents = new ArrayList<>();
    deleteEvents.add(getOMUpdateEvent(paths.get(0),
        getOmKeyInfo("vol1", "bucket1", DIR_ONE, 1L, false), DELETED_DIR_TABLE,
        DELETE, null));
    deleteEvents.add(getOMUpdateEvent(paths.get(2),
        getOmKeyInfo("vol1", "bucket1", DIR_ONE, 3L, false), DELETED_DIR_TABLE,
        DELETE, null));
    OMUpdateEventBatch deleteEventBatch = new OMUpdateEventBatch(deleteEvents);
    omTableInsightTask.process(deleteEventBatch);
    assertEquals(3, getCountForTable(DELETED_DIR_TABLE));
  }

  @Test
  public void testReprocessForCount() throws Exception {
    OMMetadataManager omMetadataManager = mock(OmMetadataManagerImpl.class);

    // Mock 5 rows in each table and test the count
    for (String tableName : omTableInsightTask.getTaskTables()) {
      TypedTable<String, Object> table = mock(TypedTable.class);
      TypedTable.TypedTableIterator mockIter =
          mock(TypedTable.TypedTableIterator.class);
      when(table.iterator()).thenReturn(mockIter);
      when(omMetadataManager.getTable(tableName)).thenReturn(table);
      when(mockIter.hasNext()).thenReturn(true, true, true, true, true, false);

      TypedTable.TypedKeyValue mockKeyValue =
          mock(TypedTable.TypedKeyValue.class);

      if (tableName.equals(DELETED_TABLE)) {
        RepeatedOmKeyInfo keyInfo = mock(RepeatedOmKeyInfo.class);
        when(keyInfo.getTotalSize()).thenReturn(ImmutablePair.of(100L, 100L));
        when(keyInfo.getOmKeyInfoList()).thenReturn(
            Arrays.asList(mock(OmKeyInfo.class)));
        when(mockKeyValue.getValue()).thenReturn(keyInfo);
      } else {
        when(mockKeyValue.getValue()).thenReturn(mock(OmKeyInfo.class));
      }

      when(mockIter.next()).thenReturn(mockKeyValue);
    }

    Pair<String, Boolean> result =
        omTableInsightTask.reprocess(omMetadataManager);

    assertTrue(result.getRight());
    assertEquals(5L, getCountForTable(KEY_TABLE));
    assertEquals(5L, getCountForTable(VOLUME_TABLE));
    assertEquals(5L, getCountForTable(BUCKET_TABLE));
    assertEquals(5L, getCountForTable(OPEN_KEY_TABLE));
    assertEquals(5L, getCountForTable(DELETED_TABLE));
  }

  @Test
  public void testReprocessForOpenKeyTable() throws Exception {
    // Populate the OpenKeys table in OM DB
    writeOpenKeyToOm(reconOMMetadataManager,
        "key1", "Bucket1", "Volume1", null, 1L);
    writeOpenKeyToOm(reconOMMetadataManager,
        "key1", "Bucket2", "Volume2", null, 2L);
    writeOpenKeyToOm(reconOMMetadataManager,
        "key1", "Bucket3", "Volume3", null, 3L);

    Pair<String, Boolean> result =
        omTableInsightTask.reprocess(reconOMMetadataManager);
    assertTrue(result.getRight());
    assertEquals(3L, getCountForTable(OPEN_KEY_TABLE));
    // Test for both replicated and unreplicated size for OPEN_KEY_TABLE
    assertEquals(6L, getUnReplicatedSizeForTable(OPEN_KEY_TABLE));
    assertEquals(18L, getReplicatedSizeForTable(OPEN_KEY_TABLE));
  }

  @Test
  public void testReprocessForOpenFileTable() throws Exception {
    // Populate the OpenFile table in OM DB
    writeOpenFileToOm(reconOMMetadataManager,
        "file1", "Bucket1", "Volume1", "file1", 1, 0, 1, 1, null, 1L);
    writeOpenFileToOm(reconOMMetadataManager,
        "file2", "Bucket2", "Volume2", "file2", 2, 0, 2, 2, null, 2L);
    writeOpenFileToOm(reconOMMetadataManager,
        "file3", "Bucket3", "Volume3", "file3", 3, 0, 3, 3, null, 3L);

    Pair<String, Boolean> result =
        omTableInsightTask.reprocess(reconOMMetadataManager);
    assertTrue(result.getRight());
    assertEquals(3L, getCountForTable(OPEN_FILE_TABLE));
    // Test for both replicated and unreplicated size for OPEN_FILE_TABLE
    assertEquals(6L, getUnReplicatedSizeForTable(OPEN_FILE_TABLE));
    assertEquals(18L, getReplicatedSizeForTable(OPEN_FILE_TABLE));
  }

  @Test
  public void testReprocessForDeletedTable() throws Exception {
    // Populate the deletedKeys table in OM DB
    // By default the size of each key is set to 100L
    List<String> deletedKeysList1 = Arrays.asList("key1");
    writeDeletedKeysToOm(reconOMMetadataManager,
        deletedKeysList1, "Bucket1", "Volume1");
    List<String> deletedKeysList2 = Arrays.asList("key2", "key2");
    writeDeletedKeysToOm(reconOMMetadataManager,
        deletedKeysList2, "Bucket2", "Volume2");
    List<String> deletedKeysList3 = Arrays.asList("key3", "key3", "key3");
    writeDeletedKeysToOm(reconOMMetadataManager,
        deletedKeysList3, "Bucket3", "Volume3");


    Pair<String, Boolean> result =
        omTableInsightTask.reprocess(reconOMMetadataManager);
    assertTrue(result.getRight());
    assertEquals(6L, getCountForTable(DELETED_TABLE));
    // Test for both replicated and unreplicated size for DELETED_TABLE
    assertEquals(600L, getUnReplicatedSizeForTable(DELETED_TABLE));
    assertEquals(600L, getReplicatedSizeForTable(DELETED_TABLE));
  }

  @Test
  public void testProcessForCount() {
    List<OMDBUpdateEvent> initialEvents = new ArrayList<>();

    // Creating events for each table except the deleted table
    for (String tableName : omTableInsightTask.getTaskTables()) {
      if (tableName.equals(DELETED_TABLE)) {
        continue; // Skipping deleted table as it has a separate test
      }

      // Adding 5 PUT events per table
      for (int i = 0; i < 5; i++) {
        initialEvents.add(
            getOMUpdateEvent("item" + i, mock(OmKeyInfo.class), tableName, PUT,
                null));
      }

      // Adding 1 DELETE event where value is null, indicating non-existence
      // in the database.
      initialEvents.add(
          getOMUpdateEvent("item0", mock(OmKeyInfo.class), tableName, DELETE,
              null));
      // Adding 1 UPDATE event. This should not affect the count.
      initialEvents.add(
          getOMUpdateEvent("item1", mock(OmKeyInfo.class), tableName, UPDATE,
              mock(OmKeyInfo.class)));
    }

    // Processing the initial batch of events
    OMUpdateEventBatch initialBatch = new OMUpdateEventBatch(initialEvents);
    omTableInsightTask.process(initialBatch);

    // Verifying the count in each table
    for (String tableName : omTableInsightTask.getTaskTables()) {
      if (tableName.equals(DELETED_TABLE)) {
        continue;
      }
      assertEquals(4L, getCountForTable(
          tableName)); // 4 items expected after processing (5 puts - 1 delete)
    }

    List<OMDBUpdateEvent> additionalEvents = new ArrayList<>();
    // Simulating new PUT and DELETE events
    for (String tableName : omTableInsightTask.getTaskTables()) {
      if (tableName.equals(DELETED_TABLE)) {
        continue;
      }
      // Adding 1 new PUT event
      additionalEvents.add(
          getOMUpdateEvent("item6", mock(OmKeyInfo.class), tableName, PUT,
              null));
      // Attempting to delete a non-existing item (value: null)
      additionalEvents.add(
          getOMUpdateEvent("item0", null, tableName, DELETE, null));
    }

    // Processing the additional events
    OMUpdateEventBatch additionalBatch =
        new OMUpdateEventBatch(additionalEvents);
    omTableInsightTask.process(additionalBatch);
    // Verifying the final count in each table
    for (String tableName : omTableInsightTask.getTaskTables()) {
      if (tableName.equals(DELETED_TABLE)) {
        continue;
      }
      // 5 items expected after processing the additional events.
      assertEquals(5L, getCountForTable(
          tableName));
    }
  }

  @Test
  public void testProcessForOpenKeyTableAndOpenFileTable() {
    // Prepare mock data size
    Long sizeToBeReturned = 1000L;
    OmKeyInfo omKeyInfo = mock(OmKeyInfo.class);
    when(omKeyInfo.getDataSize()).thenReturn(sizeToBeReturned);
    when(omKeyInfo.getReplicatedSize()).thenReturn(sizeToBeReturned * 3);

    // Test PUT events.
    // Add 5 PUT events for OpenKeyTable and OpenFileTable.
    ArrayList<OMDBUpdateEvent> putEvents = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String table = (i < 5) ? OPEN_KEY_TABLE : OPEN_FILE_TABLE;
      putEvents.add(getOMUpdateEvent("item" + i, omKeyInfo, table, PUT, null));
    }

    OMUpdateEventBatch putEventBatch = new OMUpdateEventBatch(putEvents);
    omTableInsightTask.process(putEventBatch);

    // After 5 PUTs, size should be 5 * 1000 = 5000
    for (String tableName : new ArrayList<>(
        Arrays.asList(OPEN_KEY_TABLE, OPEN_FILE_TABLE))) {
      assertEquals(5000L, getUnReplicatedSizeForTable(tableName));
      assertEquals(15000L, getReplicatedSizeForTable(tableName));
    }

    // Test DELETE events
    ArrayList<OMDBUpdateEvent> deleteEvents = new ArrayList<>();
    // Delete "item0" for OpenKeyTable and OpenFileTable.
    deleteEvents.add(
        getOMUpdateEvent("item0", omKeyInfo, OPEN_KEY_TABLE, DELETE, null));
    deleteEvents.add(
        getOMUpdateEvent("item0", omKeyInfo, OPEN_FILE_TABLE, DELETE, null));

    OMUpdateEventBatch deleteEventBatch = new OMUpdateEventBatch(deleteEvents);
    omTableInsightTask.process(deleteEventBatch);

    // After deleting "item0", size should be 4 * 1000 = 4000
    for (String tableName : new ArrayList<>(
        Arrays.asList(OPEN_KEY_TABLE, OPEN_FILE_TABLE))) {
      assertEquals(4000L, getUnReplicatedSizeForTable(tableName));
      assertEquals(12000L, getReplicatedSizeForTable(tableName));
    }

    // Test UPDATE events
    ArrayList<OMDBUpdateEvent> updateEvents = new ArrayList<>();
    Long newSizeToBeReturned = 2000L;
    for (String tableName : new ArrayList<>(
        Arrays.asList(OPEN_KEY_TABLE, OPEN_FILE_TABLE))) {
      // Update "item1" with a new size
      OmKeyInfo newKeyInfo = mock(OmKeyInfo.class);
      when(newKeyInfo.getDataSize()).thenReturn(newSizeToBeReturned);
      when(newKeyInfo.getReplicatedSize()).thenReturn(newSizeToBeReturned * 3);
      updateEvents.add(
          getOMUpdateEvent("item1", newKeyInfo, tableName, UPDATE, omKeyInfo));
    }

    OMUpdateEventBatch updateEventBatch = new OMUpdateEventBatch(updateEvents);
    omTableInsightTask.process(updateEventBatch);

    // After updating "item1", size should be 4000 - 1000 + 2000 = 5000
    //  presentValue - oldValue + newValue = updatedValue
    for (String tableName : new ArrayList<>(
        Arrays.asList(OPEN_KEY_TABLE, OPEN_FILE_TABLE))) {
      assertEquals(5000L, getUnReplicatedSizeForTable(tableName));
      assertEquals(15000L, getReplicatedSizeForTable(tableName));
    }
  }

  @Test
  public void testProcessForDeletedTable() {
    // Prepare mock data size
    ImmutablePair<Long, Long> sizeToBeReturned =
        new ImmutablePair<>(1000L, 3000L);
    ArrayList<OmKeyInfo> omKeyInfoList = new ArrayList<>();
    // Add 5 OmKeyInfo objects to the list
    for (long i = 0; i < 5; i++) {
      OmKeyInfo omKeyInfo =
          getOmKeyInfo("sampleVol", "non_fso_Bucket", "non_fso_key1", i + 1,
              true);
      // Set properties of OmKeyInfo object if needed
      omKeyInfoList.add(omKeyInfo);
    }
    RepeatedOmKeyInfo repeatedOmKeyInfo = mock(RepeatedOmKeyInfo.class);
    when(repeatedOmKeyInfo.getTotalSize()).thenReturn(sizeToBeReturned);
    when(repeatedOmKeyInfo.getOmKeyInfoList()).thenReturn(omKeyInfoList);

    // Test PUT events
    ArrayList<OMDBUpdateEvent> putEvents = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      putEvents.add(
          getOMUpdateEvent("item" + i, repeatedOmKeyInfo, DELETED_TABLE, PUT,
              null));
    }
    OMUpdateEventBatch putEventBatch = new OMUpdateEventBatch(putEvents);
    omTableInsightTask.process(putEventBatch);
    // Each of the 5 RepeatedOmKeyInfo object has 5 OmKeyInfo obj,
    // so total deleted keys should be 5 * 5 = 25
    assertEquals(25L, getCountForTable(DELETED_TABLE));
    // After 5 PUTs, size should be 5 * 1000 = 5000 for each size-related table
    assertEquals(5000L, getUnReplicatedSizeForTable(DELETED_TABLE));
    assertEquals(15000L, getReplicatedSizeForTable(DELETED_TABLE));


    // Test DELETE events
    ArrayList<OMDBUpdateEvent> deleteEvents = new ArrayList<>();
    // Delete "item0"
    deleteEvents.add(
        getOMUpdateEvent("item0", repeatedOmKeyInfo, DELETED_TABLE, DELETE,
            null));
    OMUpdateEventBatch deleteEventBatch = new OMUpdateEventBatch(deleteEvents);
    omTableInsightTask.process(deleteEventBatch);
    // After deleting "item0" total deleted keys should be 20
    assertEquals(20L, getCountForTable(DELETED_TABLE));
    // After deleting "item0", size should be 4 * 1000 = 4000
    assertEquals(4000L, getUnReplicatedSizeForTable(DELETED_TABLE));
    assertEquals(12000L, getReplicatedSizeForTable(DELETED_TABLE));
  }

  private OMDBUpdateEvent getOMUpdateEvent(
      String name, Object value,
      String table,
      OMDBUpdateEvent.OMDBUpdateAction action,
      Object oldValue) {
    return new OMDBUpdateEvent.OMUpdateEventBuilder()
        .setAction(action)
        .setKey(name)
        .setValue(value)
        .setTable(table)
        .setOldValue(oldValue)
        .build();
  }

  private long getCountForTable(String tableName) {
    String key = OmTableInsightTask.getTableCountKeyFromTable(tableName);
    return globalStatsDao.findById(key).getValue();
  }

  private long getUnReplicatedSizeForTable(String tableName) {
    String key = OmTableInsightTask.getUnReplicatedSizeKeyFromTable(tableName);
    return globalStatsDao.findById(key).getValue();
  }

  private long getReplicatedSizeForTable(String tableName) {
    String key = OmTableInsightTask.getReplicatedSizeKeyFromTable(tableName);
    return globalStatsDao.findById(key).getValue();
  }

  private OmKeyInfo getOmKeyInfo(String volumeName, String bucketName,
                                 String keyName, Long objectID,
                                 boolean isFile) {
    return new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setFile(isFile)
        .setReplicationConfig(StandaloneReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.ONE))
        .setDataSize(100L)
        .setObjectID(objectID)
        .build();
  }
}
