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
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMUpdateEventBuilder;
import org.hadoop.ozone.recon.schema.StatsSchemaDefinition;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.VOLUME_TABLE;
import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.apache.hadoop.ozone.recon.ReconConstants.BUCKET_COUNT_KEY;
import static org.apache.hadoop.ozone.recon.ReconConstants.DELETED_KEY_COUNT_KEY;
import static org.apache.hadoop.ozone.recon.ReconConstants.OPEN_KEY_COUNT_KEY;
import static org.apache.hadoop.ozone.recon.ReconConstants.VOLUME_COUNT_KEY;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.DELETE;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.PUT;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.UPDATE;
import static org.hadoop.ozone.recon.schema.tables.GlobalStatsTable.GLOBAL_STATS;
import static org.apache.hadoop.ozone.recon.ReconConstants.KEY_COUNT_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for Object Count Task.
 */
public class TestObjectCountTask extends AbstractReconSqlDBTest {

  private GlobalStatsDao globalStatsDao;
  private ObjectCountTask objectCountTask;

  @Before
  public void setUp() {
    globalStatsDao = getDao(GlobalStatsDao.class);
    StatsSchemaDefinition statsSchemaDefinition =
        getSchemaDefinition(StatsSchemaDefinition.class);
    objectCountTask =
        new ObjectCountTask(globalStatsDao, getConfiguration());
    DSLContext dslContext = statsSchemaDefinition.getDSLContext();
    // Truncate table before running each test
    dslContext.truncate(GLOBAL_STATS);
  }

  @Test
  public void testReprocess() {
    OMMetadataManager omMetadataManager = mock(OmMetadataManagerImpl.class);
    TypedTable<String, OmKeyInfo> keyTable = mock(TypedTable.class);
    TypedTable<String, OmBucketInfo> bucketTable = mock(TypedTable.class);
    TypedTable<String, OmVolumeArgs> volumeTable = mock(TypedTable.class);
    TypedTable<String, OmKeyInfo> openKeyTable = mock(TypedTable.class);
    TypedTable<String, RepeatedOmKeyInfo> deletedkeyTable =
        mock(TypedTable.class);

    TypedTable.TypedTableIterator mockKeyIter = mock(TypedTable
        .TypedTableIterator.class);
    TypedTable.TypedTableIterator mockBucketIter = mock(TypedTable
        .TypedTableIterator.class);
    TypedTable.TypedTableIterator mockVolumeIter = mock(TypedTable
        .TypedTableIterator.class);
    TypedTable.TypedTableIterator mockOpenKeyIter = mock(TypedTable
        .TypedTableIterator.class);
    TypedTable.TypedTableIterator mockDeletedKeyIter = mock(TypedTable
        .TypedTableIterator.class);

    when(keyTable.iterator()).thenReturn(mockKeyIter);
    when(bucketTable.iterator()).thenReturn(mockBucketIter);
    when(volumeTable.iterator()).thenReturn(mockVolumeIter);
    when(openKeyTable.iterator()).thenReturn(mockOpenKeyIter);
    when(deletedkeyTable.iterator()).thenReturn(mockDeletedKeyIter);

    when(omMetadataManager.getKeyTable()).thenReturn(keyTable);
    when(omMetadataManager.getBucketTable()).thenReturn(bucketTable);
    when(omMetadataManager.getVolumeTable()).thenReturn(volumeTable);
    when(omMetadataManager.getOpenKeyTable()).thenReturn(openKeyTable);
    when(omMetadataManager.getDeletedTable()).thenReturn(deletedkeyTable);

    // mock for 3 keys in the key table
    when(mockKeyIter.hasNext())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);

    Pair<String, Boolean> result =
        objectCountTask.reprocess(omMetadataManager);
    assertTrue(result.getRight());

    assertEquals(3L,
        globalStatsDao.findById(KEY_COUNT_KEY).getValue().longValue());

    // add mocks for 4 volumes, 2 buckets, 5 open keys and 1 deleted key
    when(mockVolumeIter.hasNext())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);
    when(mockBucketIter.hasNext())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);
    when(mockOpenKeyIter.hasNext())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);
    when(mockDeletedKeyIter.hasNext())
        .thenReturn(true)
        .thenReturn(false);

    result = objectCountTask.reprocess(omMetadataManager);
    assertTrue(result.getRight());

    assertEquals(0L,
        globalStatsDao.findById(KEY_COUNT_KEY).getValue().longValue());
    assertEquals(4L,
        globalStatsDao.findById(VOLUME_COUNT_KEY).getValue().longValue());
    assertEquals(2L,
        globalStatsDao.findById(BUCKET_COUNT_KEY).getValue().longValue());
    assertEquals(5L,
        globalStatsDao.findById(OPEN_KEY_COUNT_KEY).getValue().longValue());
    assertEquals(1L,
        globalStatsDao.findById(DELETED_KEY_COUNT_KEY).getValue().longValue());
  }

  @Test
  public void testProcess() {
    ArrayList<OMDBUpdateEvent> events = new ArrayList<>();
    // Create 1 volume, 2 buckets and write 4 keys.
    OmVolumeArgs volume1 = mock(OmVolumeArgs.class);
    events.add(getOMUpdateEvent("volume1", volume1, VOLUME_TABLE, PUT));

    OmBucketInfo bucket1 = mock(OmBucketInfo.class);
    events.add(getOMUpdateEvent("bucket1", bucket1, BUCKET_TABLE, PUT));

    OmBucketInfo bucket2 = mock(OmBucketInfo.class);
    events.add(getOMUpdateEvent("bucket2", bucket2, BUCKET_TABLE, PUT));

    OmKeyInfo toBeDeletedKey = mock(OmKeyInfo.class);
    events.add(getOMUpdateEvent("deletedKey", toBeDeletedKey, KEY_TABLE, PUT));

    OmKeyInfo toBeUpdatedKey = mock(OmKeyInfo.class);
    events.add(getOMUpdateEvent("updatedKey", toBeUpdatedKey, KEY_TABLE, PUT));

    OmKeyInfo key3 = mock(OmKeyInfo.class);
    events.add(getOMUpdateEvent("key3", key3, KEY_TABLE, PUT));

    OmKeyInfo key4 = mock(OmKeyInfo.class);
    events.add(getOMUpdateEvent("key4", key4, KEY_TABLE, PUT));

    OMUpdateEventBatch omUpdateEventBatch = new OMUpdateEventBatch(events);
    objectCountTask.process(omUpdateEventBatch);

    // Verify 1 volume.
    assertEquals(1L,
        globalStatsDao.findById(VOLUME_COUNT_KEY).getValue().longValue());

    // Verify 2 Buckets.
    assertEquals(2L,
        globalStatsDao.findById(BUCKET_COUNT_KEY).getValue().longValue());

    // Verify 4 keys are in key count.
    assertEquals(4L,
        globalStatsDao.findById(KEY_COUNT_KEY).getValue().longValue());

    ArrayList<OMDBUpdateEvent> newEvents = new ArrayList<>();

    // Add 2 new keys, delete 1 key and update 1 key
    OmKeyInfo key5 = mock(OmKeyInfo.class);
    newEvents.add(getOMUpdateEvent("key5", key5, KEY_TABLE, PUT));

    OmKeyInfo key6 = mock(OmKeyInfo.class);
    newEvents.add(getOMUpdateEvent("key6", key6, KEY_TABLE, PUT));

    newEvents.add(getOMUpdateEvent("deletedKey", toBeDeletedKey, KEY_TABLE,
        DELETE));

    newEvents.add(getOMUpdateEvent("updatedKey", toBeUpdatedKey,
        KEY_TABLE, UPDATE));

    // Add 2 new volumes, delete 1 volume and update 1 volume
    OmVolumeArgs volume2 = mock(OmVolumeArgs.class);
    OmVolumeArgs volume3 = mock(OmVolumeArgs.class);

    newEvents.add(getOMUpdateEvent("volume2", volume2, VOLUME_TABLE, PUT));
    newEvents.add(getOMUpdateEvent("volume3", volume3, VOLUME_TABLE, PUT));
    newEvents.add(getOMUpdateEvent("volume1", volume1, VOLUME_TABLE, DELETE));
    newEvents.add(getOMUpdateEvent("volume2", volume2, VOLUME_TABLE, UPDATE));

    // Add 2 new buckets, delete 1 bucket and update 1 bucket
    OmBucketInfo bucket3 = mock(OmBucketInfo.class);
    OmBucketInfo bucket4 = mock(OmBucketInfo.class);

    newEvents.add(getOMUpdateEvent("bucket3", bucket3, BUCKET_TABLE, PUT));
    newEvents.add(getOMUpdateEvent("bucket4", bucket4, BUCKET_TABLE, PUT));
    newEvents.add(getOMUpdateEvent("bucket1", bucket1, BUCKET_TABLE, DELETE));
    newEvents.add(getOMUpdateEvent("bucket2", bucket2, BUCKET_TABLE, UPDATE));

    omUpdateEventBatch = new OMUpdateEventBatch(newEvents);
    objectCountTask.process(omUpdateEventBatch);

    // Verify 2 volumes.
    assertEquals(2L,
        globalStatsDao.findById(VOLUME_COUNT_KEY).getValue().longValue());

    // Verify 3 Buckets.
    assertEquals(3L,
        globalStatsDao.findById(BUCKET_COUNT_KEY).getValue().longValue());

    // Verify 5 keys are in key count.
    assertEquals(5L,
        globalStatsDao.findById(KEY_COUNT_KEY).getValue().longValue());
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
}
