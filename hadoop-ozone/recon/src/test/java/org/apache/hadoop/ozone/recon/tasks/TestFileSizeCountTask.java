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
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMUpdateEventBuilder;
import org.hadoop.ozone.recon.schema.UtilizationSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.FileCountBySizeDao;
import org.hadoop.ozone.recon.schema.tables.pojos.FileCountBySize;
import org.jooq.DSLContext;
import org.jooq.Record3;
import org.junit.Before;
import org.junit.Test;
import org.mockito.AdditionalAnswers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.DELETE;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.PUT;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.UPDATE;
import static org.hadoop.ozone.recon.schema.tables.FileCountBySizeTable.FILE_COUNT_BY_SIZE;
import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for File Size Count Task.
 */
public class TestFileSizeCountTask extends AbstractReconSqlDBTest {

  private FileCountBySizeDao fileCountBySizeDao;
  private FileSizeCountTask fileSizeCountTask;
  private DSLContext dslContext;

  @Before
  public void setUp() {
    fileCountBySizeDao = getDao(FileCountBySizeDao.class);
    UtilizationSchemaDefinition utilizationSchemaDefinition =
        getSchemaDefinition(UtilizationSchemaDefinition.class);
    fileSizeCountTask =
        new FileSizeCountTask(fileCountBySizeDao, utilizationSchemaDefinition);
    dslContext = utilizationSchemaDefinition.getDSLContext();
    // Truncate table before running each test
    dslContext.truncate(FILE_COUNT_BY_SIZE);
  }

  @Test
  public void testReprocess() throws IOException {
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
    given(omKeyInfo3.getKeyName()).willReturn("key3");
    given(omKeyInfo3.getVolumeName()).willReturn("vol1");
    given(omKeyInfo3.getBucketName()).willReturn("bucket1");
    given(omKeyInfo3.getDataSize()).willReturn(1125899906842624L * 4); // 4PB

    OMMetadataManager omMetadataManager = mock(OmMetadataManagerImpl.class);
    TypedTable<String, OmKeyInfo> keyTable = mock(TypedTable.class);

    TypedTable.TypedTableIterator mockKeyIter = mock(TypedTable
        .TypedTableIterator.class);
    TypedTable.TypedKeyValue mockKeyValue = mock(
        TypedTable.TypedKeyValue.class);

    when(keyTable.iterator()).thenReturn(mockKeyIter);
    when(omMetadataManager.getKeyTable()).thenReturn(keyTable);
    when(mockKeyIter.hasNext())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);
    when(mockKeyIter.next()).thenReturn(mockKeyValue);
    when(mockKeyValue.getValue())
        .thenReturn(omKeyInfo1)
        .thenReturn(omKeyInfo2)
        .thenReturn(omKeyInfo3);

    // Reprocess could be called from table having existing entries. Adding
    // an entry to simulate that.
    fileCountBySizeDao.insert(
        new FileCountBySize("vol1", "bucket1", 1024L, 10L));

    Pair<String, Boolean> result =
        fileSizeCountTask.reprocess(omMetadataManager);
    assertTrue(result.getRight());

    assertEquals(3, fileCountBySizeDao.count());
    Record3<String, String, Long> recordToFind = dslContext
        .newRecord(FILE_COUNT_BY_SIZE.VOLUME,
        FILE_COUNT_BY_SIZE.BUCKET,
        FILE_COUNT_BY_SIZE.FILE_SIZE)
        .value1("vol1")
        .value2("bucket1")
        .value3(1024L);
    assertEquals(1L,
        fileCountBySizeDao.findById(recordToFind).getCount().longValue());
    // file size upper bound for 100000L is 131072L (next highest power of 2)
    recordToFind.value3(131072L);
    assertEquals(1L,
        fileCountBySizeDao.findById(recordToFind).getCount().longValue());
    // file size upper bound for 4PB is Long.MAX_VALUE
    recordToFind.value3(Long.MAX_VALUE);
    assertEquals(1L,
        fileCountBySizeDao.findById(recordToFind).getCount().longValue());
  }

  @Test
  public void testProcess() {
    // Write 2 keys.
    OmKeyInfo toBeDeletedKey = mock(OmKeyInfo.class);
    given(toBeDeletedKey.getVolumeName()).willReturn("vol1");
    given(toBeDeletedKey.getBucketName()).willReturn("bucket1");
    given(toBeDeletedKey.getKeyName()).willReturn("deletedKey");
    given(toBeDeletedKey.getDataSize()).willReturn(2000L); // Bin 1
    OMDBUpdateEvent event = new OMUpdateEventBuilder()
        .setAction(PUT)
        .setKey("deletedKey")
        .setValue(toBeDeletedKey)
        .setTable(OmMetadataManagerImpl.KEY_TABLE)
        .build();

    OmKeyInfo toBeUpdatedKey = mock(OmKeyInfo.class);
    given(toBeUpdatedKey.getVolumeName()).willReturn("vol1");
    given(toBeUpdatedKey.getBucketName()).willReturn("bucket1");
    given(toBeUpdatedKey.getKeyName()).willReturn("updatedKey");
    given(toBeUpdatedKey.getDataSize()).willReturn(10000L); // Bin 4
    OMDBUpdateEvent event2 = new OMUpdateEventBuilder()
        .setAction(PUT)
        .setKey("updatedKey")
        .setValue(toBeUpdatedKey)
        .setTable(OmMetadataManagerImpl.KEY_TABLE)
        .build();

    OMUpdateEventBatch omUpdateEventBatch =
        new OMUpdateEventBatch(Arrays.asList(event, event2));
    fileSizeCountTask.process(omUpdateEventBatch);

    // Verify 2 keys are in correct bins.
    assertEquals(2, fileCountBySizeDao.count());
    Record3<String, String, Long> recordToFind = dslContext
        .newRecord(FILE_COUNT_BY_SIZE.VOLUME,
            FILE_COUNT_BY_SIZE.BUCKET,
            FILE_COUNT_BY_SIZE.FILE_SIZE)
        .value1("vol1")
        .value2("bucket1")
        .value3(2048L);
    assertEquals(1L,
        fileCountBySizeDao.findById(recordToFind).getCount().longValue());
    // file size upper bound for 10000L is 16384L (next highest power of 2)
    recordToFind.value3(16384L);
    assertEquals(1L,
        fileCountBySizeDao.findById(recordToFind).getCount().longValue());

    // Add new key.
    OmKeyInfo newKey = mock(OmKeyInfo.class);
    given(newKey.getVolumeName()).willReturn("vol1");
    given(newKey.getBucketName()).willReturn("bucket1");
    given(newKey.getKeyName()).willReturn("newKey");
    given(newKey.getDataSize()).willReturn(1000L); // Bin 0
    OMDBUpdateEvent putEvent = new OMUpdateEventBuilder()
        .setAction(PUT)
        .setKey("newKey")
        .setValue(newKey)
        .setTable(OmMetadataManagerImpl.KEY_TABLE)
        .build();

    // Update existing key.
    OmKeyInfo updatedKey = mock(OmKeyInfo.class);
    given(updatedKey.getVolumeName()).willReturn("vol1");
    given(updatedKey.getBucketName()).willReturn("bucket1");
    given(updatedKey.getKeyName()).willReturn("updatedKey");
    given(updatedKey.getDataSize()).willReturn(50000L); // Bin 6
    OMDBUpdateEvent updateEvent = new OMUpdateEventBuilder()
        .setAction(UPDATE)
        .setKey("updatedKey")
        .setValue(updatedKey)
        .setOldValue(toBeUpdatedKey)
        .setTable(OmMetadataManagerImpl.KEY_TABLE)
        .build();

    // Delete another existing key.
    OMDBUpdateEvent deleteEvent = new OMUpdateEventBuilder()
        .setAction(DELETE)
        .setKey("deletedKey")
        .setValue(toBeDeletedKey)
        .setTable(OmMetadataManagerImpl.KEY_TABLE)
        .build();

    omUpdateEventBatch = new OMUpdateEventBatch(
        Arrays.asList(updateEvent, putEvent, deleteEvent));
    fileSizeCountTask.process(omUpdateEventBatch);

    assertEquals(4, fileCountBySizeDao.count());
    recordToFind.value3(1024L);
    assertEquals(1, fileCountBySizeDao.findById(recordToFind)
        .getCount().longValue());
    recordToFind.value3(2048L);
    assertEquals(0, fileCountBySizeDao.findById(recordToFind)
        .getCount().longValue());
    recordToFind.value3(16384L);
    assertEquals(0, fileCountBySizeDao.findById(recordToFind)
        .getCount().longValue());
    recordToFind.value3(65536L);
    assertEquals(1, fileCountBySizeDao.findById(recordToFind)
        .getCount().longValue());
  }

  @Test
  public void testReprocessAtScale() throws IOException {
    // generate mocks for 2 volumes, 500 buckets each volume
    // and 42 keys in each bucket.
    List<OmKeyInfo> omKeyInfoList = new ArrayList<>();
    List<Boolean> hasNextAnswer = new ArrayList<>();
    for (int volIndex = 1; volIndex <= 2; volIndex++) {
      for (int bktIndex = 1; bktIndex <= 500; bktIndex++) {
        for (int keyIndex = 1; keyIndex <= 42; keyIndex++) {
          OmKeyInfo omKeyInfo = mock(OmKeyInfo.class);
          given(omKeyInfo.getKeyName()).willReturn("key" + keyIndex);
          given(omKeyInfo.getVolumeName()).willReturn("vol" + volIndex);
          given(omKeyInfo.getBucketName()).willReturn("bucket" + bktIndex);
          // Place keys in each bin
          long fileSize = (long)Math.pow(2, keyIndex + 9) - 1L;
          given(omKeyInfo.getDataSize()).willReturn(fileSize);
          omKeyInfoList.add(omKeyInfo);
          hasNextAnswer.add(true);
        }
      }
    }
    hasNextAnswer.add(false);

    OMMetadataManager omMetadataManager = mock(OmMetadataManagerImpl.class);
    TypedTable<String, OmKeyInfo> keyTable = mock(TypedTable.class);

    TypedTable.TypedTableIterator mockKeyIter = mock(TypedTable
        .TypedTableIterator.class);
    TypedTable.TypedKeyValue mockKeyValue = mock(
        TypedTable.TypedKeyValue.class);

    when(keyTable.iterator()).thenReturn(mockKeyIter);
    when(omMetadataManager.getKeyTable()).thenReturn(keyTable);
    when(mockKeyIter.hasNext())
        .thenAnswer(AdditionalAnswers.returnsElementsOf(hasNextAnswer));
    when(mockKeyIter.next()).thenReturn(mockKeyValue);
    when(mockKeyValue.getValue())
        .thenAnswer(AdditionalAnswers.returnsElementsOf(omKeyInfoList));

    Pair<String, Boolean> result =
        fileSizeCountTask.reprocess(omMetadataManager);
    assertTrue(result.getRight());

    // 2 volumes * 500 buckets * 42 bins = 42000 rows
    assertEquals(42000, fileCountBySizeDao.count());
    Record3<String, String, Long> recordToFind = dslContext
        .newRecord(FILE_COUNT_BY_SIZE.VOLUME,
            FILE_COUNT_BY_SIZE.BUCKET,
            FILE_COUNT_BY_SIZE.FILE_SIZE)
        .value1("vol1")
        .value2("bucket1")
        .value3(1024L);
    assertEquals(1L,
        fileCountBySizeDao.findById(recordToFind).getCount().longValue());
    // file size upper bound for 100000L is 131072L (next highest power of 2)
    recordToFind.value1("vol1");
    recordToFind.value3(131072L);
    assertEquals(1L,
        fileCountBySizeDao.findById(recordToFind).getCount().longValue());
    recordToFind.value2("bucket500");
    recordToFind.value3(Long.MAX_VALUE);
    assertEquals(1L,
        fileCountBySizeDao.findById(recordToFind).getCount().longValue());
  }

  @Test
  public void testProcessAtScale() {
    // Write 10000 keys.
    List<OMDBUpdateEvent> omDbEventList = new ArrayList<>();
    List<OmKeyInfo> omKeyInfoList = new ArrayList<>();
    for (int volIndex = 1; volIndex <= 10; volIndex++) {
      for (int bktIndex = 1; bktIndex <= 100; bktIndex++) {
        for (int keyIndex = 1; keyIndex <= 10; keyIndex++) {
          OmKeyInfo omKeyInfo = mock(OmKeyInfo.class);
          given(omKeyInfo.getKeyName()).willReturn("key" + keyIndex);
          given(omKeyInfo.getVolumeName()).willReturn("vol" + volIndex);
          given(omKeyInfo.getBucketName()).willReturn("bucket" + bktIndex);
          // Place keys in each bin
          long fileSize = (long)Math.pow(2, keyIndex + 9) - 1L;
          given(omKeyInfo.getDataSize()).willReturn(fileSize);
          omKeyInfoList.add(omKeyInfo);
          omDbEventList.add(new OMUpdateEventBuilder()
              .setAction(PUT)
              .setKey("key" + keyIndex)
              .setValue(omKeyInfo)
              .setTable(OmMetadataManagerImpl.KEY_TABLE)
              .build());
        }
      }
    }

    OMUpdateEventBatch omUpdateEventBatch =
        new OMUpdateEventBatch(omDbEventList);
    fileSizeCountTask.process(omUpdateEventBatch);

    // Verify 2 keys are in correct bins.
    assertEquals(10000, fileCountBySizeDao.count());
    Record3<String, String, Long> recordToFind = dslContext
        .newRecord(FILE_COUNT_BY_SIZE.VOLUME,
            FILE_COUNT_BY_SIZE.BUCKET,
            FILE_COUNT_BY_SIZE.FILE_SIZE)
        .value1("vol1")
        .value2("bucket1")
        .value3(2048L);
    assertEquals(1L,
        fileCountBySizeDao.findById(recordToFind).getCount().longValue());
    recordToFind.value1("vol10");
    recordToFind.value2("bucket100");
    // file size upper bound for 10000L is 16384L (next highest power of 2)
    recordToFind.value3(16384L);
    assertEquals(1L,
        fileCountBySizeDao.findById(recordToFind).getCount().longValue());

    // Process 500 deletes and 500 updates
    omDbEventList = new ArrayList<>();
    for (int volIndex = 1; volIndex <= 1; volIndex++) {
      for (int bktIndex = 1; bktIndex <= 100; bktIndex++) {
        for (int keyIndex = 1; keyIndex <= 10; keyIndex++) {
          OmKeyInfo omKeyInfo = mock(OmKeyInfo.class);
          given(omKeyInfo.getKeyName()).willReturn("key" + keyIndex);
          given(omKeyInfo.getVolumeName()).willReturn("vol" + volIndex);
          given(omKeyInfo.getBucketName()).willReturn("bucket" + bktIndex);
          if (keyIndex <= 5) {
            long fileSize = (long)Math.pow(2, keyIndex + 9) - 1L;
            given(omKeyInfo.getDataSize()).willReturn(fileSize);
            omDbEventList.add(new OMUpdateEventBuilder()
                .setAction(DELETE)
                .setKey("key" + keyIndex)
                .setValue(omKeyInfo)
                .setTable(OmMetadataManagerImpl.KEY_TABLE)
                .build());
          } else {
            // update all the files with keyIndex > 5 to filesize 1023L
            // so that they get into first bin
            given(omKeyInfo.getDataSize()).willReturn(1023L);
            omDbEventList.add(new OMUpdateEventBuilder()
                .setAction(UPDATE)
                .setKey("key" + keyIndex)
                .setValue(omKeyInfo)
                .setTable(OmMetadataManagerImpl.KEY_TABLE)
                .setOldValue(
                    omKeyInfoList.get((volIndex * bktIndex) + keyIndex))
                .build());
          }
        }
      }
    }

    omUpdateEventBatch = new OMUpdateEventBatch(omDbEventList);
    fileSizeCountTask.process(omUpdateEventBatch);

    assertEquals(10000, fileCountBySizeDao.count());
    recordToFind = dslContext
        .newRecord(FILE_COUNT_BY_SIZE.VOLUME,
            FILE_COUNT_BY_SIZE.BUCKET,
            FILE_COUNT_BY_SIZE.FILE_SIZE)
        .value1("vol1")
        .value2("bucket1")
        .value3(1024L);
    // The update events on keys 6-10 should now put them under first bin 1024L
    assertEquals(5, fileCountBySizeDao.findById(recordToFind)
        .getCount().longValue());
    recordToFind.value2("bucket100");
    assertEquals(5, fileCountBySizeDao.findById(recordToFind)
        .getCount().longValue());
    recordToFind.value3(2048L);
    assertEquals(0, fileCountBySizeDao.findById(recordToFind)
        .getCount().longValue());
    // Volumes 2 - 10 should not be affected by this process
    recordToFind.value1("vol2");
    assertEquals(1, fileCountBySizeDao.findById(recordToFind)
        .getCount().longValue());
  }
}
