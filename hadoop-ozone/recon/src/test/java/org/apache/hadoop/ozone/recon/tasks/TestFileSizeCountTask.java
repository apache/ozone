/**
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
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.apache.hadoop.ozone.recon.persistence.AbstractSqlDatabaseTest;
import org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMUpdateEventBuilder;
import org.hadoop.ozone.recon.schema.UtilizationSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.FileCountBySizeDao;
import org.jooq.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;

import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.DELETE;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.PUT;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.UPDATE;
import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for File Size Count Task.
 */
public class TestFileSizeCountTask extends AbstractSqlDatabaseTest {

  private FileCountBySizeDao fileCountBySizeDao;

  @Before
  public void setUp() throws SQLException {
    UtilizationSchemaDefinition schemaDefinition =
        getInjector().getInstance(UtilizationSchemaDefinition.class);
    schemaDefinition.initializeSchema();
    Configuration sqlConfiguration =
        getInjector().getInstance((Configuration.class));
    fileCountBySizeDao = new FileCountBySizeDao(sqlConfiguration);
  }

  @Test
  public void testCalculateBinIndex() {
    FileSizeCountTask fileSizeCountTask = mock(FileSizeCountTask.class);

    when(fileSizeCountTask.getMaxFileSizeUpperBound()).
        thenReturn(1125899906842624L);    // 1 PB
    when(fileSizeCountTask.getOneKB()).thenReturn(1024L);
    when(fileSizeCountTask.getMaxBinSize()).thenReturn(42);
    when(fileSizeCountTask.calculateBinIndex(anyLong())).thenCallRealMethod();
    when(fileSizeCountTask.nextClosestPowerIndexOfTwo(
        anyLong())).thenCallRealMethod();

    long fileSize = 1024L;            // 1 KB
    int binIndex = fileSizeCountTask.calculateBinIndex(fileSize);
    assertEquals(1, binIndex);

    fileSize = 1023L;                // 1KB - 1B
    binIndex = fileSizeCountTask.calculateBinIndex(fileSize);
    assertEquals(0, binIndex);

    fileSize = 562949953421312L;      // 512 TB
    binIndex = fileSizeCountTask.calculateBinIndex(fileSize);
    assertEquals(40, binIndex);

    fileSize = 562949953421313L;      // (512 TB + 1B)
    binIndex = fileSizeCountTask.calculateBinIndex(fileSize);
    assertEquals(40, binIndex);

    fileSize = 562949953421311L;      // (512 TB - 1B)
    binIndex = fileSizeCountTask.calculateBinIndex(fileSize);
    assertEquals(39, binIndex);

    fileSize = 1125899906842624L;      // 1 PB - last (extra) bin
    binIndex = fileSizeCountTask.calculateBinIndex(fileSize);
    assertEquals(41, binIndex);

    fileSize = 100000L;
    binIndex = fileSizeCountTask.calculateBinIndex(fileSize);
    assertEquals(7, binIndex);

    fileSize = 1125899906842623L;      // (1 PB - 1B)
    binIndex = fileSizeCountTask.calculateBinIndex(fileSize);
    assertEquals(40, binIndex);

    fileSize = 1125899906842624L * 4;      // 4 PB - last extra bin
    binIndex = fileSizeCountTask.calculateBinIndex(fileSize);
    assertEquals(41, binIndex);

    fileSize = Long.MAX_VALUE;        // extra bin
    binIndex = fileSizeCountTask.calculateBinIndex(fileSize);
    assertEquals(41, binIndex);
  }

  @Test
  public void testReprocess() throws IOException {
    OmKeyInfo omKeyInfo1 = mock(OmKeyInfo.class);
    given(omKeyInfo1.getKeyName()).willReturn("key1");
    given(omKeyInfo1.getDataSize()).willReturn(1000L);

    OMMetadataManager omMetadataManager = mock(OmMetadataManagerImpl.class);
    TypedTable<String, OmKeyInfo> keyTable = mock(TypedTable.class);

    TypedTable.TypedTableIterator mockKeyIter = mock(TypedTable
        .TypedTableIterator.class);
    TypedTable.TypedKeyValue mockKeyValue = mock(
        TypedTable.TypedKeyValue.class);

    when(keyTable.iterator()).thenReturn(mockKeyIter);
    when(omMetadataManager.getKeyTable()).thenReturn(keyTable);
    when(mockKeyIter.hasNext()).thenReturn(true).thenReturn(false);
    when(mockKeyIter.next()).thenReturn(mockKeyValue);
    when(mockKeyValue.getValue()).thenReturn(omKeyInfo1);

    FileSizeCountTask fileSizeCountTask =
        new FileSizeCountTask(fileCountBySizeDao);
    Pair<String, Boolean> result =
        fileSizeCountTask.reprocess(omMetadataManager);
    assertTrue(result.getRight());

    long[] upperBoundCount = fileSizeCountTask.getUpperBoundCount();
    assertEquals(1, upperBoundCount[0]);
    for (int i = 1; i < upperBoundCount.length; i++) {
      assertEquals(0, upperBoundCount[i]);
    }
  }

  @Test
  public void testProcess() {
    FileSizeCountTask fileSizeCountTask =
        new FileSizeCountTask(fileCountBySizeDao);

    // Write 2 keys.
    OmKeyInfo toBeDeletedKey = mock(OmKeyInfo.class);
    given(toBeDeletedKey.getKeyName()).willReturn("deletedKey");
    given(toBeDeletedKey.getDataSize()).willReturn(2000L); // Bin 1
    OMDBUpdateEvent event = new OMUpdateEventBuilder()
        .setAction(PUT)
        .setKey("deletedKey")
        .setValue(toBeDeletedKey)
        .build();

    OmKeyInfo toBeUpdatedKey = mock(OmKeyInfo.class);
    given(toBeUpdatedKey.getKeyName()).willReturn("updatedKey");
    given(toBeUpdatedKey.getDataSize()).willReturn(10000L); // Bin 4
    OMDBUpdateEvent event2 = new OMUpdateEventBuilder()
        .setAction(PUT)
        .setKey("updatedKey")
        .setValue(toBeUpdatedKey)
        .build();

    OMUpdateEventBatch omUpdateEventBatch =
        new OMUpdateEventBatch(Arrays.asList(event, event2));
    fileSizeCountTask.process(omUpdateEventBatch);

    // Verify 2 keys are in correct bins.
    long[] upperBoundCount = fileSizeCountTask.getUpperBoundCount();
    assertEquals(1, upperBoundCount[4]); // updatedKey
    assertEquals(1, upperBoundCount[1]); // deletedKey

    // Add new key.
    OmKeyInfo newKey = mock(OmKeyInfo.class);
    given(newKey.getKeyName()).willReturn("newKey");
    given(newKey.getDataSize()).willReturn(1000L); // Bin 0
    OMDBUpdateEvent putEvent = new OMUpdateEventBuilder()
        .setAction(PUT)
        .setKey("newKey")
        .setValue(newKey)
        .build();

    // Update existing key.
    OmKeyInfo updatedKey = mock(OmKeyInfo.class);
    given(updatedKey.getKeyName()).willReturn("updatedKey");
    given(updatedKey.getDataSize()).willReturn(50000L); // Bin 6
    OMDBUpdateEvent updateEvent = new OMUpdateEventBuilder()
        .setAction(UPDATE)
        .setKey("updatedKey")
        .setValue(updatedKey)
        .setOldValue(toBeUpdatedKey)
        .build();

    // Delete another existing key.
    OMDBUpdateEvent deleteEvent = new OMUpdateEventBuilder()
        .setAction(DELETE)
        .setKey("deletedKey")
        .setValue(toBeDeletedKey)
        .build();

    omUpdateEventBatch = new OMUpdateEventBatch(
        Arrays.asList(updateEvent, putEvent, deleteEvent));
    fileSizeCountTask.process(omUpdateEventBatch);

    upperBoundCount = fileSizeCountTask.getUpperBoundCount();
    assertEquals(1, upperBoundCount[0]); // newKey
    assertEquals(0, upperBoundCount[1]); // deletedKey
    assertEquals(0, upperBoundCount[4]); // updatedKey old
    assertEquals(1, upperBoundCount[6]); // updatedKey new
  }
}
