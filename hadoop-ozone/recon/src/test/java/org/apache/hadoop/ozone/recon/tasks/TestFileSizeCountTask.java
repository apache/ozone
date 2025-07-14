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

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.DELETE;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.PUT;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.UPDATE;
import static org.apache.ozone.recon.schema.generated.tables.FileCountBySizeTable.FILE_COUNT_BY_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.returnsElementsOf;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMUpdateEventBuilder;
import org.apache.ozone.recon.schema.UtilizationSchemaDefinition;
import org.apache.ozone.recon.schema.generated.tables.daos.FileCountBySizeDao;
import org.apache.ozone.recon.schema.generated.tables.pojos.FileCountBySize;
import org.jooq.DSLContext;
import org.jooq.Record3;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit test for File Size Count Task.
 */
public class TestFileSizeCountTask extends AbstractReconSqlDBTest {

  private FileCountBySizeDao fileCountBySizeDao;
  private FileSizeCountTaskOBS fileSizeCountTaskOBS;
  private FileSizeCountTaskFSO fileSizeCountTaskFSO;
  private DSLContext dslContext;
  private UtilizationSchemaDefinition utilizationSchemaDefinition;

  public TestFileSizeCountTask() {
    super();
  }

  @BeforeEach
  public void setUp() {
    fileCountBySizeDao = getDao(FileCountBySizeDao.class);
    utilizationSchemaDefinition = getSchemaDefinition(UtilizationSchemaDefinition.class);
    // Create separate task instances.
    fileSizeCountTaskOBS = new FileSizeCountTaskOBS(fileCountBySizeDao, utilizationSchemaDefinition);
    fileSizeCountTaskFSO = new FileSizeCountTaskFSO(fileCountBySizeDao, utilizationSchemaDefinition);
    dslContext = utilizationSchemaDefinition.getDSLContext();
    // Truncate table before each test.
    dslContext.truncate(FILE_COUNT_BY_SIZE);
  }

  @Test
  public void testReprocess() throws IOException {
    // Create three sample OmKeyInfo objects.
    OmKeyInfo[] omKeyInfos = new OmKeyInfo[3];
    String[] keyNames = {"key1", "key2", "key3"};
    String[] volumeNames = {"vol1", "vol1", "vol1"};
    String[] bucketNames = {"bucket1", "bucket1", "bucket1"};
    // Use sizes so that each falls into a distinct bin:
    //  - 1000L   falls into first bin (upper bound 1024L)
    //  - 100000L falls into second bin (upper bound 131072L)
    //  - 4PB (i.e. 1125899906842624L * 4) falls into the highest bin (upper bound Long.MAX_VALUE)
    Long[] dataSizes = {1000L, 100000L, 1125899906842624L * 4};

    for (int i = 0; i < 3; i++) {
      omKeyInfos[i] = mock(OmKeyInfo.class);
      given(omKeyInfos[i].getKeyName()).willReturn(keyNames[i]);
      given(omKeyInfos[i].getVolumeName()).willReturn(volumeNames[i]);
      given(omKeyInfos[i].getBucketName()).willReturn(bucketNames[i]);
      given(omKeyInfos[i].getDataSize()).willReturn(dataSizes[i]);
    }

    // Prepare the OMMetadataManager mock.
    OMMetadataManager omMetadataManager = mock(OmMetadataManagerImpl.class);

    // Configure the OBS (OBJECT_STORE) endpoint.
    TypedTable<String, OmKeyInfo> keyTableOBS = mock(TypedTable.class);
    // Note: Even though legacy and OBS share the same underlying table, we simulate OBS here.
    when(omMetadataManager.getKeyTable(eq(BucketLayout.OBJECT_STORE)))
        .thenReturn(keyTableOBS);
    TypedTable.TypedTableIterator mockIterOBS = mock(TypedTable.TypedTableIterator.class);
    when(keyTableOBS.iterator()).thenReturn(mockIterOBS);
    // Simulate three keys then end.
    when(mockIterOBS.hasNext()).thenReturn(true, true, true, false);
    final Table.KeyValue mockKeyValueOBS = mock(Table.KeyValue.class);
    when(mockIterOBS.next()).thenReturn(mockKeyValueOBS);
    when(mockKeyValueOBS.getValue()).thenReturn(omKeyInfos[0], omKeyInfos[1], omKeyInfos[2]);

    // Configure the FSO (FILE_SYSTEM_OPTIMIZED) endpoint.
    TypedTable<String, OmKeyInfo> keyTableFSO = mock(TypedTable.class);
    when(omMetadataManager.getKeyTable(eq(BucketLayout.FILE_SYSTEM_OPTIMIZED)))
        .thenReturn(keyTableFSO);
    TypedTable.TypedTableIterator mockIterFSO = mock(TypedTable.TypedTableIterator.class);
    when(keyTableFSO.iterator()).thenReturn(mockIterFSO);
    when(mockIterFSO.hasNext()).thenReturn(true, true, true, false);
    final Table.KeyValue mockKeyValueFSO = mock(Table.KeyValue.class);
    when(mockIterFSO.next()).thenReturn(mockKeyValueFSO);
    when(mockKeyValueFSO.getValue()).thenReturn(omKeyInfos[0], omKeyInfos[1], omKeyInfos[2]);

    // Simulate a preexisting entry in the DB.
    // (This record will be removed by the first task via table truncation.)
    fileCountBySizeDao.insert(new FileCountBySize("vol1", "bucket1", 1024L, 10L));

    // Call reprocess on both tasks.
    ReconOmTask.TaskResult resultOBS = fileSizeCountTaskOBS.reprocess(omMetadataManager);
    ReconOmTask.TaskResult resultFSO = fileSizeCountTaskFSO.reprocess(omMetadataManager);

    // Verify that both tasks reported success.
    assertTrue(resultOBS.isTaskSuccess(), "OBS reprocess should return true");
    assertTrue(resultFSO.isTaskSuccess(), "FSO reprocess should return true");

    // After processing, there should be 3 rows (one per bin).
    assertEquals(3, fileCountBySizeDao.count(), "Expected 3 rows in the DB");

    // Now verify the counts in each bin.
    // Because each task processes the same 3 keys and each key contributes a count of 1,
    // the final count per bin should be 2 (1 from OBS + 1 from FSO).

    // Verify bin for key size 1000L -> upper bound 1024L.
    Record3<String, String, Long> recordToFind = dslContext.newRecord(
            FILE_COUNT_BY_SIZE.VOLUME,
            FILE_COUNT_BY_SIZE.BUCKET,
            FILE_COUNT_BY_SIZE.FILE_SIZE)
        .value1("vol1")
        .value2("bucket1")
        .value3(1024L);
    assertEquals(2L, fileCountBySizeDao.findById(recordToFind).getCount().longValue(),
        "Expected bin 1024 to have count 2");

    // Verify bin for key size 100000L -> upper bound 131072L.
    recordToFind.value3(131072L);
    assertEquals(2L, fileCountBySizeDao.findById(recordToFind).getCount().longValue(),
        "Expected bin 131072 to have count 2");

    // Verify bin for key size 4PB -> upper bound Long.MAX_VALUE.
    recordToFind.value3(Long.MAX_VALUE);
    assertEquals(2L, fileCountBySizeDao.findById(recordToFind).getCount().longValue(),
        "Expected bin Long.MAX_VALUE to have count 2");
  }

  @Test
  public void testProcess() {
    // First batch: Write 2 keys.
    OmKeyInfo toBeDeletedKey = mock(OmKeyInfo.class);
    given(toBeDeletedKey.getVolumeName()).willReturn("vol1");
    given(toBeDeletedKey.getBucketName()).willReturn("bucket1");
    given(toBeDeletedKey.getKeyName()).willReturn("deletedKey");
    given(toBeDeletedKey.getDataSize()).willReturn(2000L); // Falls in bin with upper bound 2048L
    OMDBUpdateEvent event = new OMUpdateEventBuilder()
        .setAction(PUT)
        .setKey("deletedKey")
        .setValue(toBeDeletedKey)
        .setTable(KEY_TABLE)
        .build();

    OmKeyInfo toBeUpdatedKey = mock(OmKeyInfo.class);
    given(toBeUpdatedKey.getVolumeName()).willReturn("vol1");
    given(toBeUpdatedKey.getBucketName()).willReturn("bucket1");
    given(toBeUpdatedKey.getKeyName()).willReturn("updatedKey");
    given(toBeUpdatedKey.getDataSize()).willReturn(10000L); // Falls in bin with upper bound 16384L
    OMDBUpdateEvent event2 = new OMUpdateEventBuilder()
        .setAction(PUT)
        .setKey("updatedKey")
        .setValue(toBeUpdatedKey)
        .setTable(FILE_TABLE)
        .build();

    OMUpdateEventBatch omUpdateEventBatch =
        new OMUpdateEventBatch(Arrays.asList(event, event2), 0L);

    // Process the same batch on both endpoints.
    fileSizeCountTaskOBS.process(omUpdateEventBatch, Collections.emptyMap());
    fileSizeCountTaskFSO.process(omUpdateEventBatch, Collections.emptyMap());

    // After processing the first batch:
    // Since each endpoint processes the same events, the counts are doubled.
    // Expected: 2 rows (bins 2048 and 16384) with counts 2 each.
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

    // Second batch: Process update events.
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
        .setTable(KEY_TABLE)
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
        .setTable(KEY_TABLE)
        .build();

    // Delete another existing key.
    OMDBUpdateEvent deleteEvent = new OMUpdateEventBuilder()
        .setAction(DELETE)
        .setKey("deletedKey")
        .setValue(toBeDeletedKey)
        .setTable(FILE_TABLE)
        .build();

    omUpdateEventBatch = new OMUpdateEventBatch(
        Arrays.asList(updateEvent, putEvent, deleteEvent), 0L);
    fileSizeCountTaskOBS.process(omUpdateEventBatch, Collections.emptyMap());
    fileSizeCountTaskFSO.process(omUpdateEventBatch, Collections.emptyMap());

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
    // generate mocks for 2 volumes, 500 buckets each volume, and 42 keys in each bucket.
    List<OmKeyInfo> omKeyInfoList = new ArrayList<>();
    List<Boolean> hasNextAnswer = new ArrayList<>();
    for (int volIndex = 1; volIndex <= 2; volIndex++) {
      for (int bktIndex = 1; bktIndex <= 500; bktIndex++) {
        for (int keyIndex = 1; keyIndex <= 42; keyIndex++) {
          OmKeyInfo omKeyInfo = mock(OmKeyInfo.class);
          given(omKeyInfo.getKeyName()).willReturn("key" + keyIndex);
          given(omKeyInfo.getVolumeName()).willReturn("vol" + volIndex);
          given(omKeyInfo.getBucketName()).willReturn("bucket" + bktIndex);
          // Each key's fileSize = 2^(keyIndex+9) - 1, so that it falls into its respective bin.
          long fileSize = (long) Math.pow(2, keyIndex + 9) - 1L;
          given(omKeyInfo.getDataSize()).willReturn(fileSize);
          omKeyInfoList.add(omKeyInfo);
          hasNextAnswer.add(true);
        }
      }
    }
    hasNextAnswer.add(false);

    OMMetadataManager omMetadataManager = mock(OmMetadataManagerImpl.class);
    // Create two mock key tables: one for OBS (using LEGACY in this test) and one for FSO.
    TypedTable<String, OmKeyInfo> keyTableLegacy = mock(TypedTable.class);
    TypedTable<String, OmKeyInfo> keyTableFso = mock(TypedTable.class);

    TypedTable.TypedTableIterator mockKeyIterLegacy = mock(TypedTable.TypedTableIterator.class);
    TypedTable.TypedTableIterator mockKeyIterFso = mock(TypedTable.TypedTableIterator.class);
    final Table.KeyValue mockKeyValueLegacy = mock(Table.KeyValue.class);
    final Table.KeyValue mockKeyValueFso = mock(Table.KeyValue.class);

    when(keyTableLegacy.iterator()).thenReturn(mockKeyIterLegacy);
    when(keyTableFso.iterator()).thenReturn(mockKeyIterFso);

    // In this test, assume OBS task uses BucketLayout.LEGACY and FSO uses FILE_SYSTEM_OPTIMIZED.
    when(omMetadataManager.getKeyTable(BucketLayout.OBJECT_STORE)).thenReturn(keyTableLegacy);
    when(omMetadataManager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)).thenReturn(keyTableFso);

    when(mockKeyIterLegacy.hasNext()).thenAnswer(returnsElementsOf(hasNextAnswer));
    when(mockKeyIterFso.hasNext()).thenAnswer(returnsElementsOf(hasNextAnswer));
    when(mockKeyIterLegacy.next()).thenReturn(mockKeyValueLegacy);
    when(mockKeyIterFso.next()).thenReturn(mockKeyValueFso);

    when(mockKeyValueLegacy.getValue()).thenAnswer(returnsElementsOf(omKeyInfoList));
    when(mockKeyValueFso.getValue()).thenAnswer(returnsElementsOf(omKeyInfoList));

    // Call reprocess on both endpoints.
    ReconOmTask.TaskResult resultOBS = fileSizeCountTaskOBS.reprocess(omMetadataManager);
    ReconOmTask.TaskResult resultFSO = fileSizeCountTaskFSO.reprocess(omMetadataManager);
    assertTrue(resultOBS.isTaskSuccess());
    assertTrue(resultFSO.isTaskSuccess());

    // 2 volumes * 500 buckets * 42 bins = 42000 rows
    assertEquals(42000, fileCountBySizeDao.count());

    // Verify counts for a few representative bins.
    // For volume "vol1", bucket "bucket1", the first bin (upper bound 1024L) should have a count of 2.
    Record3<String, String, Long> recordToFind = dslContext.newRecord(
            FILE_COUNT_BY_SIZE.VOLUME,
            FILE_COUNT_BY_SIZE.BUCKET,
            FILE_COUNT_BY_SIZE.FILE_SIZE)
        .value1("vol1")
        .value2("bucket1")
        .value3(1024L);
    assertEquals(2L,
        fileCountBySizeDao.findById(recordToFind).getCount().longValue());

    // For volume "vol1", bucket "bucket1", the bin with upper bound 131072L should have a count of 2.
    recordToFind.value3(131072L);
    assertEquals(2L,
        fileCountBySizeDao.findById(recordToFind).getCount().longValue());

    // For volume "vol1", bucket "bucket500", the highest bin (upper bound Long.MAX_VALUE) should have a count of 2.
    recordToFind.value2("bucket500");
    recordToFind.value3(Long.MAX_VALUE);
    assertEquals(2L,
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
          // All the keys ending with even will be stored in KEY-TABLE
          if (keyIndex % 2 == 0) {
            omDbEventList.add(new OMUpdateEventBuilder()
                .setAction(PUT)
                .setKey("key" + keyIndex)
                .setValue(omKeyInfo)
                .setTable(KEY_TABLE)
                .build());
          } else {
            // All the keys ending with odd will be stored in FILE-TABLE
            omDbEventList.add(new OMUpdateEventBuilder()
                .setAction(PUT)
                .setKey("key" + keyIndex)
                .setValue(omKeyInfo)
                .setTable(FILE_TABLE)
                .build());
          }
        }
      }
    }

    OMUpdateEventBatch omUpdateEventBatch = new OMUpdateEventBatch(omDbEventList, 0L);
    // Process the same batch on both endpoints.
    fileSizeCountTaskOBS.process(omUpdateEventBatch, Collections.emptyMap());
    fileSizeCountTaskFSO.process(omUpdateEventBatch, Collections.emptyMap());

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
            if (keyIndex % 2 == 0) {
              omDbEventList.add(new OMUpdateEventBuilder()
                  .setAction(DELETE)
                  .setKey("key" + keyIndex)
                  .setValue(omKeyInfo)
                  .setTable(KEY_TABLE)
                  .build());
            } else {
              omDbEventList.add(new OMUpdateEventBuilder()
                  .setAction(DELETE)
                  .setKey("key" + keyIndex)
                  .setValue(omKeyInfo)
                  .setTable(FILE_TABLE)
                  .build());
            }
          } else {
            // update all the files with keyIndex > 5 to filesize 1023L
            // so that they get into first bin
            given(omKeyInfo.getDataSize()).willReturn(1023L);
            if (keyIndex % 2 == 0) {
              omDbEventList.add(new OMUpdateEventBuilder()
                  .setAction(UPDATE)
                  .setKey("key" + keyIndex)
                  .setValue(omKeyInfo)
                  .setTable(KEY_TABLE)
                  .setOldValue(
                      omKeyInfoList.get((volIndex * bktIndex) + keyIndex))
                  .build());
            } else {
              omDbEventList.add(new OMUpdateEventBuilder()
                  .setAction(UPDATE)
                  .setKey("key" + keyIndex)
                  .setValue(omKeyInfo)
                  .setTable(FILE_TABLE)
                  .setOldValue(
                      omKeyInfoList.get((volIndex * bktIndex) + keyIndex))
                  .build());
            }
          }
        }
      }
    }

    omUpdateEventBatch = new OMUpdateEventBatch(omDbEventList, 0L);
    fileSizeCountTaskOBS.process(omUpdateEventBatch, Collections.emptyMap());
    fileSizeCountTaskFSO.process(omUpdateEventBatch, Collections.emptyMap());

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

  @Test
  public void testTruncateTableExceptionPropagation() {
    // Mock DSLContext and FileCountBySizeDao
    DSLContext mockDslContext = mock(DSLContext.class);
    FileCountBySizeDao mockDao = mock(FileCountBySizeDao.class);

    // Mock schema definition and ensure it returns our mocked DSLContext
    UtilizationSchemaDefinition mockSchema = mock(UtilizationSchemaDefinition.class);
    when(mockSchema.getDSLContext()).thenReturn(mockDslContext);

    // Mock delete operation to throw an exception
    when(mockDslContext.delete(FILE_COUNT_BY_SIZE))
        .thenThrow(new RuntimeException("Simulated DB failure"));

    // Create instances of FileSizeCountTaskOBS and FileSizeCountTaskFSO using mocks
    fileSizeCountTaskOBS = new FileSizeCountTaskOBS(mockDao, mockSchema);
    fileSizeCountTaskFSO = new FileSizeCountTaskFSO(mockDao, mockSchema);

    // Mock OMMetadataManager
    OMMetadataManager omMetadataManager = mock(OmMetadataManagerImpl.class);

    // Verify that an exception is thrown from reprocess() for both tasks.
    assertThrows(RuntimeException.class, () -> fileSizeCountTaskOBS.reprocess(omMetadataManager),
        "Expected reprocess to propagate exception but it didn't.");

    assertThrows(RuntimeException.class, () -> fileSizeCountTaskFSO.reprocess(omMetadataManager),
        "Expected reprocess to propagate exception but it didn't.");
  }

}
