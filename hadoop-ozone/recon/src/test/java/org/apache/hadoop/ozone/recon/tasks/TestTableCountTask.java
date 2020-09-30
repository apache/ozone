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

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_KEY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.VOLUME_TABLE;
import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.DELETE;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.PUT;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.UPDATE;
import static org.hadoop.ozone.recon.schema.tables.GlobalStatsTable.GLOBAL_STATS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for Object Count Task.
 */
public class TestTableCountTask extends AbstractReconSqlDBTest {

  private GlobalStatsDao globalStatsDao;
  private TableCountTask tableCountTask;
  private DSLContext dslContext;
  private boolean isSetupDone = false;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private void initializeInjector() throws IOException {
    ReconOMMetadataManager omMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(temporaryFolder.newFolder()),
        temporaryFolder.newFolder());
    globalStatsDao = getDao(GlobalStatsDao.class);
    tableCountTask = new TableCountTask(globalStatsDao, getConfiguration(),
            omMetadataManager);
    dslContext = getDslContext();
  }

  @Before
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
  public void testReprocess() {
    OMMetadataManager omMetadataManager = mock(OmMetadataManagerImpl.class);
    // Mock 5 rows in each table and test the count
    for (String tableName: tableCountTask.getTaskTables()) {
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

    Pair<String, Boolean> result = tableCountTask.reprocess(omMetadataManager);
    assertTrue(result.getRight());

    assertEquals(5L, getCountForTable(KEY_TABLE));
    assertEquals(5L, getCountForTable(VOLUME_TABLE));
    assertEquals(5L, getCountForTable(BUCKET_TABLE));
    assertEquals(5L, getCountForTable(OPEN_KEY_TABLE));
    assertEquals(5L, getCountForTable(DELETED_TABLE));
  }

  @Test
  public void testProcess() {
    ArrayList<OMDBUpdateEvent> events = new ArrayList<>();
    // Create 5 put, 1 delete and 1 update event for each table
    for (String tableName: tableCountTask.getTaskTables()) {
      for (int i=0; i<5; i++) {
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
    tableCountTask.process(omUpdateEventBatch);

    // Verify 4 items in each table. (5 puts - 1 delete + 0 update)
    assertEquals(4L, getCountForTable(KEY_TABLE));
    assertEquals(4L, getCountForTable(VOLUME_TABLE));
    assertEquals(4L, getCountForTable(BUCKET_TABLE));
    assertEquals(4L, getCountForTable(OPEN_KEY_TABLE));
    assertEquals(4L, getCountForTable(DELETED_TABLE));

    // add a new key and simulate delete on non-existing item (value: null)
    ArrayList<OMDBUpdateEvent> newEvents = new ArrayList<>();
    for (String tableName: tableCountTask.getTaskTables()) {
      newEvents.add(getOMUpdateEvent("item5", null, tableName, PUT));
      // This delete event should be a noop since value is null
      newEvents.add(getOMUpdateEvent("item0", null, tableName, DELETE));
    }

    omUpdateEventBatch = new OMUpdateEventBatch(newEvents);
    tableCountTask.process(omUpdateEventBatch);

    // Verify 5 items in each table. (1 new put + 0 delete)
    assertEquals(5L, getCountForTable(KEY_TABLE));
    assertEquals(5L, getCountForTable(VOLUME_TABLE));
    assertEquals(5L, getCountForTable(BUCKET_TABLE));
    assertEquals(5L, getCountForTable(OPEN_KEY_TABLE));
    assertEquals(5L, getCountForTable(DELETED_TABLE));
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
    String key = TableCountTask.getRowKeyFromTable(tableName);
    return globalStatsDao.findById(key).getValue();
  }
}
