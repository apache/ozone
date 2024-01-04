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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMUpdateEventBuilder;

import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_KEY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.VOLUME_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDeletedKeysToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeOpenKeyToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeOpenFileToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.DELETE;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.PUT;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.UPDATE;
import static org.hadoop.ozone.recon.schema.tables.GlobalStatsTable.GLOBAL_STATS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for Object Count Task.
 */
public class TestOmTableInsightTask extends AbstractReconSqlDBTest {
  @TempDir
  private Path temporaryFolder;
  private GlobalStatsDao globalStatsDao;
  private OmTableInsightTask omTableInsightTask;
  private DSLContext dslContext;
  private boolean isSetupDone = false;
  private ReconOMMetadataManager reconOMMetadataManager;

  private void initializeInjector() throws IOException {
    reconOMMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(Files.createDirectory(
            temporaryFolder.resolve("JunitOmDBDir")).toFile()),
        Files.createDirectory(temporaryFolder.resolve("NewDir")).toFile());
    globalStatsDao = getDao(GlobalStatsDao.class);
    omTableInsightTask = new OmTableInsightTask(
        globalStatsDao, getConfiguration(), reconOMMetadataManager);
    dslContext = getDslContext();
  }

  @BeforeEach
  public void setUp() throws IOException {
    // The following setup runs only once
    if (!isSetupDone) {
      initializeInjector();
      isSetupDone = true;
    }
    // Truncate table before running each test
    dslContext.truncate(GLOBAL_STATS);
  }

  @Test
  public void testReprocessForCount() throws Exception {
    OMMetadataManager omMetadataManager = mock(OmMetadataManagerImpl.class);

    // Mock 5 rows in each table and test the count
    for (String tableName : omTableInsightTask.getTaskTables()) {
      TypedTable<String, Object> table = mock(TypedTable.class);
      TypedTable.TypedTableIterator mockIter = mock(TypedTable
          .TypedTableIterator.class);
      when(table.iterator()).thenReturn(mockIter);
      when(omMetadataManager.getTable(tableName)).thenReturn(table);
      when(mockIter.hasNext())
          .thenReturn(true)
          .thenReturn(true)
          .thenReturn(true)
          .thenReturn(true)
          .thenReturn(true)
          .thenReturn(false);
      TypedTable.TypedKeyValue mockKeyValue =
          mock(TypedTable.TypedKeyValue.class);
      when(mockKeyValue.getValue()).thenReturn(mock(OmKeyInfo.class));
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
    ArrayList<OMDBUpdateEvent> events = new ArrayList<>();
    // Create 5 put, 1 delete and 1 update event for each table
    for (String tableName : omTableInsightTask.getTaskTables()) {
      for (int i = 0; i < 5; i++) {
        events.add(getOMUpdateEvent("item" + i, null, tableName, PUT, null));
      }
      // for delete event, if value is set to null, the counter will not be
      // decremented. This is because the value will be null if item does not
      // exist in the database and there is no need to delete.
      events.add(getOMUpdateEvent("item0", mock(OmKeyInfo.class), tableName,
          DELETE, null));
      events.add(getOMUpdateEvent("item1", null, tableName, UPDATE, null));
    }
    OMUpdateEventBatch omUpdateEventBatch = new OMUpdateEventBatch(events);
    omTableInsightTask.process(omUpdateEventBatch);

    // Verify 4 items in each table. (5 puts - 1 delete + 0 update)
    assertEquals(4L, getCountForTable(KEY_TABLE));
    assertEquals(4L, getCountForTable(VOLUME_TABLE));
    assertEquals(4L, getCountForTable(BUCKET_TABLE));
    assertEquals(4L, getCountForTable(FILE_TABLE));

    // add a new key and simulate delete on non-existing item (value: null)
    ArrayList<OMDBUpdateEvent> newEvents = new ArrayList<>();
    for (String tableName : omTableInsightTask.getTaskTables()) {
      newEvents.add(getOMUpdateEvent("item5", null, tableName, PUT, null));
      // This delete event should be a noop since value is null
      newEvents.add(getOMUpdateEvent("item0", null, tableName, DELETE, null));
    }

    omUpdateEventBatch = new OMUpdateEventBatch(newEvents);
    omTableInsightTask.process(omUpdateEventBatch);

    // Verify 5 items in each table. (1 new put + 0 delete)
    assertEquals(5L, getCountForTable(KEY_TABLE));
    assertEquals(5L, getCountForTable(VOLUME_TABLE));
    assertEquals(5L, getCountForTable(BUCKET_TABLE));
    assertEquals(5L, getCountForTable(FILE_TABLE));
  }

  @Test
  public void testProcessForOpenKeyTableAndOpenFileTable() {
    // Prepare mock data size
    Long sizeToBeReturned = 1000L;
    OmKeyInfo omKeyInfo = mock(OmKeyInfo.class);
    when(omKeyInfo.getDataSize()).thenReturn(sizeToBeReturned);
    when(omKeyInfo.getReplicatedSize()).thenReturn(sizeToBeReturned * 3);

    // Test PUT events
    ArrayList<OMDBUpdateEvent> putEvents = new ArrayList<>();
    for (String tableName : omTableInsightTask.getTablesToCalculateSize()) {
      for (int i = 0; i < 5; i++) {
        putEvents.add(
            getOMUpdateEvent("item" + i, omKeyInfo, tableName, PUT, null));
      }
    }
    OMUpdateEventBatch putEventBatch = new OMUpdateEventBatch(putEvents);
    omTableInsightTask.process(putEventBatch);

    // After 5 PUTs, size should be 5 * 1000 = 5000 for each size-related table
    for (String tableName : omTableInsightTask.getTablesToCalculateSize()) {
      assertEquals(5000L, getUnReplicatedSizeForTable(tableName));
      assertEquals(15000L, getReplicatedSizeForTable(tableName));
    }

    // Test DELETE events
    ArrayList<OMDBUpdateEvent> deleteEvents = new ArrayList<>();
    for (String tableName : omTableInsightTask.getTablesToCalculateSize()) {
      // Delete "item0"
      deleteEvents.add(
          getOMUpdateEvent("item0", omKeyInfo, tableName, DELETE, null));
    }
    OMUpdateEventBatch deleteEventBatch = new OMUpdateEventBatch(deleteEvents);
    omTableInsightTask.process(deleteEventBatch);

    // After deleting "item0", size should be 4 * 1000 = 4000
    for (String tableName : omTableInsightTask.getTablesToCalculateSize()) {
      assertEquals(4000L, getUnReplicatedSizeForTable(tableName));
      assertEquals(12000L, getReplicatedSizeForTable(tableName));
    }

    // Test UPDATE events
    ArrayList<OMDBUpdateEvent> updateEvents = new ArrayList<>();
    Long newSizeToBeReturned = 2000L;
    for (String tableName : omTableInsightTask.getTablesToCalculateSize()) {
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
    for (String tableName : omTableInsightTask.getTablesToCalculateSize()) {
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
    for (int i = 0; i < 5; i++) {
      OmKeyInfo omKeyInfo =
          getOmKeyInfo("sampleVol", "non_fso_Bucket", "non_fso_key1", true);
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


    // Test UPDATE events
    ArrayList<OMDBUpdateEvent> updateEvents = new ArrayList<>();
    // Update "item1" with new sizes
    ImmutablePair<Long, Long> newSizesToBeReturned =
        new ImmutablePair<>(500L, 1500L);
    RepeatedOmKeyInfo newRepeatedOmKeyInfo = mock(RepeatedOmKeyInfo.class);
    when(newRepeatedOmKeyInfo.getTotalSize()).thenReturn(newSizesToBeReturned);
    when(newRepeatedOmKeyInfo.getOmKeyInfoList()).thenReturn(
        omKeyInfoList.subList(1, 5));
    OMUpdateEventBatch updateEventBatch = new OMUpdateEventBatch(updateEvents);
    // For item1, newSize=500 and totalCount of deleted keys should be 4
    updateEvents.add(
        getOMUpdateEvent("item1", newRepeatedOmKeyInfo, DELETED_TABLE, UPDATE,
            repeatedOmKeyInfo));
    omTableInsightTask.process(updateEventBatch);
    // Since one key has been deleted, total deleted keys should be 19
    assertEquals(19L, getCountForTable(DELETED_TABLE));
    // After updating "item1", size should be 4000 - 1000 + 500 = 3500
    //  presentValue - oldValue + newValue = updatedValue
    assertEquals(3500L, getUnReplicatedSizeForTable(DELETED_TABLE));
    assertEquals(10500L, getReplicatedSizeForTable(DELETED_TABLE));
  }


  private OMDBUpdateEvent getOMUpdateEvent(
      String name, Object value,
      String table,
      OMDBUpdateEvent.OMDBUpdateAction action,
      Object oldValue) {
    return new OMUpdateEventBuilder()
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
                                 String keyName, boolean isFile) {
    return new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setFile(isFile)
        .setReplicationConfig(StandaloneReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.ONE))
        .setDataSize(100L)
        .build();
  }
}
