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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMUpdateEventBuilder;

import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.*;
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
public class TestTableInsightTask extends AbstractReconSqlDBTest {

  private GlobalStatsDao globalStatsDao;
  private TableInsightTask tableInsightTask;
  private DSLContext dslContext;
  private boolean isSetupDone = false;
  private ReconOMMetadataManager reconOMMetadataManager;

  private void initializeInjector() throws IOException {
    reconOMMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(temporaryFolder.newFolder()),
        temporaryFolder.newFolder());
    globalStatsDao = getDao(GlobalStatsDao.class);
    tableInsightTask = new TableInsightTask(globalStatsDao, getConfiguration(),
        reconOMMetadataManager);
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
    for (String tableName : tableInsightTask.getTaskTables()) {
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
    }

    Pair<String, Boolean> result =
        tableInsightTask.reprocess(omMetadataManager);
    assertTrue(result.getRight());

    assertEquals(5L, getCountForTable(KEY_TABLE));
    assertEquals(5L, getCountForTable(VOLUME_TABLE));
    assertEquals(5L, getCountForTable(BUCKET_TABLE));
    assertEquals(5L, getCountForTable(OPEN_KEY_TABLE));
    assertEquals(5L, getCountForTable(DELETED_TABLE));
  }


  @Test
  public void testReprocessForSize() throws Exception {
    // Populate the OpenKeys table in OM DB
    writeOpenKeyToOm(reconOMMetadataManager,
        "key1", "Bucket1", "Volume1", null, 1L);
    writeOpenKeyToOm(reconOMMetadataManager,
        "key1", "Bucket2", "Volume2", null, 2L);
    writeOpenKeyToOm(reconOMMetadataManager,
        "key1", "Bucket3", "Volume3", null, 3L);

    // Populate the OpenFile table in OM DB
    writeOpenFileToOm(reconOMMetadataManager,
        "file1", "Bucket1", "Volume1", "file1", 1, 0, 1, 1, null, 1L);
    writeOpenFileToOm(reconOMMetadataManager,
        "file2", "Bucket2", "Volume2", "file2", 2, 0, 2, 2, null, 2L);
    writeOpenFileToOm(reconOMMetadataManager,
        "file3", "Bucket3", "Volume3", "file3", 3, 0, 3, 3, null, 3L);

    Pair<String, Boolean> result =
        tableInsightTask.reprocess(reconOMMetadataManager);
    assertTrue(result.getRight());
    assertEquals(6L, getSizeForTable(OPEN_KEY_TABLE));
    assertEquals(6L, getSizeForTable(OPEN_FILE_TABLE));
  }


  @Test
  public void testProcessForCount() {
    ArrayList<OMDBUpdateEvent> events = new ArrayList<>();
    // Create 5 put, 1 delete and 1 update event for each table
    for (String tableName: tableInsightTask.getTaskTables()) {
      for (int i = 0; i < 5; i++) {
        events.add(getOMUpdateEvent("item" + i, null, tableName, PUT));
      }
      // for delete event, if value is set to null, the counter will not be
      // decremented. This is because the value will be null if item does not
      // exist in the database and there is no need to delete.
      events.add(getOMUpdateEvent("item0", mock(OmKeyInfo.class), tableName,
          DELETE));
      events.add(getOMUpdateEvent("item1", null, tableName, UPDATE));
    }
    OMUpdateEventBatch omUpdateEventBatch = new OMUpdateEventBatch(events);
    tableInsightTask.process(omUpdateEventBatch);

    // Verify 4 items in each table. (5 puts - 1 delete + 0 update)
    assertEquals(4L, getCountForTable(KEY_TABLE));
    assertEquals(4L, getCountForTable(VOLUME_TABLE));
    assertEquals(4L, getCountForTable(BUCKET_TABLE));
    assertEquals(4L, getCountForTable(OPEN_KEY_TABLE));
    assertEquals(4L, getCountForTable(DELETED_TABLE));

    // add a new key and simulate delete on non-existing item (value: null)
    ArrayList<OMDBUpdateEvent> newEvents = new ArrayList<>();
    for (String tableName: tableInsightTask.getTaskTables()) {
      newEvents.add(getOMUpdateEvent("item5", null, tableName, PUT));
      // This delete event should be a noop since value is null
      newEvents.add(getOMUpdateEvent("item0", null, tableName, DELETE));
    }

    omUpdateEventBatch = new OMUpdateEventBatch(newEvents);
    tableInsightTask.process(omUpdateEventBatch);

    // Verify 5 items in each table. (1 new put + 0 delete)
    assertEquals(5L, getCountForTable(KEY_TABLE));
    assertEquals(5L, getCountForTable(VOLUME_TABLE));
    assertEquals(5L, getCountForTable(BUCKET_TABLE));
    assertEquals(5L, getCountForTable(OPEN_KEY_TABLE));
    assertEquals(5L, getCountForTable(DELETED_TABLE));
  }

  @Test
  public void testProcessForSize() {
    // Prepare mock data size
    OmKeyInfo omKeyInfo = mock(OmKeyInfo.class);
    when(omKeyInfo.getDataSize()).thenReturn(1000L);

    // Test PUT events
    ArrayList<OMDBUpdateEvent> putEvents = new ArrayList<>();
    for (String tableName : tableInsightTask.getTablesRequiringSizeCalculation()) {
      for (int i = 0; i < 5; i++) {
        putEvents.add(getOMUpdateEvent("item" + i, omKeyInfo, tableName, PUT));
      }
    }
    OMUpdateEventBatch putEventBatch = new OMUpdateEventBatch(putEvents);
    tableInsightTask.process(putEventBatch);

    // After 5 PUTs, size should be 5 * 1000 = 5000 for each size-related table
    for (String tableName : tableInsightTask.getTablesRequiringSizeCalculation()) {
      assertEquals(5000L, getSizeForTable(tableName));
    }

    // Test DELETE events
    ArrayList<OMDBUpdateEvent> deleteEvents = new ArrayList<>();
    for (String tableName : tableInsightTask.getTablesRequiringSizeCalculation()) {
      // Delete "item0"
      deleteEvents.add(getOMUpdateEvent("item0", omKeyInfo, tableName, DELETE));
    }
    OMUpdateEventBatch deleteEventBatch = new OMUpdateEventBatch(deleteEvents);
    tableInsightTask.process(deleteEventBatch);

    // After deleting "item0", size should be 4 * 1000 = 4000 for each size-related table
    for (String tableName : tableInsightTask.getTablesRequiringSizeCalculation()) {
      assertEquals(4000L, getSizeForTable(tableName));
    }

    // Test UPDATE of DataSize by performing "delete + put" operations
    // Assume that an "update" changes the key data size to 500
    OmKeyInfo updatedOmKeyInfo = mock(OmKeyInfo.class);
    when(updatedOmKeyInfo.getDataSize()).thenReturn(500L);
    ArrayList<OMDBUpdateEvent> updateEvents = new ArrayList<>();
    for (String tableName : tableInsightTask.getTablesRequiringSizeCalculation()) {
      // "Update" "item1" by first deleting it...
      updateEvents.add(getOMUpdateEvent("item1", omKeyInfo, tableName, DELETE));
      // ...and then putting it back with updated size
      updateEvents.add(
          getOMUpdateEvent("item1", updatedOmKeyInfo, tableName, PUT));
    }
    OMUpdateEventBatch updateEventBatch = new OMUpdateEventBatch(updateEvents);
    tableInsightTask.process(updateEventBatch);

    // After UPDATE, size should be (4000 - 1000) + 500 = 3500 for each size-related table
    for (String tableName : tableInsightTask.getTablesRequiringSizeCalculation()) {
      assertEquals(3500L, getSizeForTable(tableName));
    }
  }

  private OMDBUpdateEvent getOMUpdateEvent(String name, Object value,
                                           String table,
                           OMDBUpdateEvent.OMDBUpdateAction action) {
    return new OMUpdateEventBuilder()
        .setAction(action)
        .setKey(name)
        .setValue(value)
        .setTable(table)
        .build();
  }

  private long getCountForTable(String tableName) {
    String key = TableInsightTask.getTableCountKeyFromTable(tableName);
    return globalStatsDao.findById(key).getValue();
  }

  private long getSizeForTable(String tableName) {
    String key = TableInsightTask.getTableSizeKeyFromTable(tableName);
    return globalStatsDao.findById(key).getValue();
  }
}
