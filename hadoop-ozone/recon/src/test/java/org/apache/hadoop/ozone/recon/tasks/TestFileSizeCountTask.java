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
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.DELETE;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.PUT;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.UPDATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.returnsElementsOf;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconFileMetadataManager;
import org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMUpdateEventBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit test for File Size Count Task.
 */
public class TestFileSizeCountTask {

  @TempDir
  private static Path temporaryFolder;
  private static ReconFileMetadataManager reconFileMetadataManager;
  private FileSizeCountTaskOBS fileSizeCountTaskOBS;
  private FileSizeCountTaskFSO fileSizeCountTaskFSO;

  @BeforeAll
  public static void setupOnce() throws Exception {
    ReconOMMetadataManager reconOMMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(Files.createDirectory(
            temporaryFolder.resolve("JunitOmDBDir")).toFile()),
        Files.createDirectory(temporaryFolder.resolve("NewDir")).toFile());
    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder.toFile())
            .withReconSqlDb()
            .withReconOm(reconOMMetadataManager)
            .withContainerDB()
            .build();
    reconFileMetadataManager = reconTestInjector.getInstance(ReconFileMetadataManager.class);
  }

  @BeforeEach
  public void setUp() throws IOException {
    // Reset the truncated flag to ensure clean state for each test
    ReconConstants.FILE_SIZE_COUNT_TABLE_TRUNCATED.set(false);
    
    OzoneConfiguration configuration = new OzoneConfiguration();
    // Create separate task instances.
    fileSizeCountTaskOBS = new FileSizeCountTaskOBS(reconFileMetadataManager, configuration);
    fileSizeCountTaskFSO = new FileSizeCountTaskFSO(reconFileMetadataManager, configuration);
    // Clear RocksDB table before each test.
    try (TableIterator<FileSizeCountKey, ? extends Table.KeyValue<FileSizeCountKey, Long>> iterator = 
         reconFileMetadataManager.getFileCountTable().iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<FileSizeCountKey, Long> keyValue = iterator.next();
        reconFileMetadataManager.getFileCountTable().delete(keyValue.getKey());
      }
    } catch (Exception e) {
      // Ignore cleanup errors
    }
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
    when(keyTableOBS.getName()).thenReturn("keyTable");  // Mock table name for parallelization
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
    when(keyTableFSO.getName()).thenReturn("fileTable");  // Mock table name for parallelization
    TypedTable.TypedTableIterator mockIterFSO = mock(TypedTable.TypedTableIterator.class);
    when(keyTableFSO.iterator()).thenReturn(mockIterFSO);
    when(mockIterFSO.hasNext()).thenReturn(true, true, true, false);
    final Table.KeyValue mockKeyValueFSO = mock(Table.KeyValue.class);
    when(mockIterFSO.next()).thenReturn(mockKeyValueFSO);
    when(mockKeyValueFSO.getValue()).thenReturn(omKeyInfos[0], omKeyInfos[1], omKeyInfos[2]);


    // Call reprocess on both tasks.
    ReconOmTask.TaskResult resultOBS = fileSizeCountTaskOBS.reprocess(omMetadataManager);
    ReconOmTask.TaskResult resultFSO = fileSizeCountTaskFSO.reprocess(omMetadataManager);

    // Verify that both tasks reported success.
    assertTrue(resultOBS.isTaskSuccess(), "OBS reprocess should return true");
    assertTrue(resultFSO.isTaskSuccess(), "FSO reprocess should return true");

    // Verify RocksDB results.
    // After processing, the data should be in RocksDB.
    // Because each task processes the same 3 keys and each key contributes a count of 1,
    // the final count per bin should be 2 (1 from OBS + 1 from FSO).
    // Check bin for key size 1000L -> upper bound 1024L.
    FileSizeCountKey rocksKey1 = new FileSizeCountKey("vol1", "bucket1", 1024L);
    Long rocksCount1 = reconFileMetadataManager.getFileSizeCount(rocksKey1);
    assertNotNull(rocksCount1, "Expected RocksDB bin 1024 to exist");
    assertEquals(2L, rocksCount1.longValue(), "Expected RocksDB bin 1024 to have count 2");

    // Check bin for key size 100000L -> upper bound 131072L.
    FileSizeCountKey rocksKey2 = new FileSizeCountKey("vol1", "bucket1", 131072L);
    Long rocksCount2 = reconFileMetadataManager.getFileSizeCount(rocksKey2);
    assertNotNull(rocksCount2, "Expected RocksDB bin 131072 to exist");
    assertEquals(2L, rocksCount2.longValue(), "Expected RocksDB bin 131072 to have count 2");

    // Check bin for key size 4PB -> upper bound Long.MAX_VALUE.
    FileSizeCountKey rocksKey3 = new FileSizeCountKey("vol1", "bucket1", Long.MAX_VALUE);
    Long rocksCount3 = reconFileMetadataManager.getFileSizeCount(rocksKey3);
    assertNotNull(rocksCount3, "Expected RocksDB bin Long.MAX_VALUE to exist");
    assertEquals(2L, rocksCount3.longValue(), "Expected RocksDB bin Long.MAX_VALUE to have count 2");
  }

  @Test
  public void testProcess() throws IOException {
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
    // Since each endpoint processes the same events, the counts are additive.
    // Expected: 2 bins (2048 and 16384) with count 1 each.
    
    // Verify RocksDB bin for data size 2000L -> upper bound 2048L
    FileSizeCountKey key2048 = new FileSizeCountKey("vol1", "bucket1", 2048L);
    Long count2048 = reconFileMetadataManager.getFileSizeCount(key2048);
    assertNotNull(count2048, "Expected RocksDB bin 2048 to exist");
    assertEquals(1L, count2048.longValue(), "Expected RocksDB bin 2048 to have count 1");
    
    // Verify RocksDB bin for data size 10000L -> upper bound 16384L
    FileSizeCountKey key16384 = new FileSizeCountKey("vol1", "bucket1", 16384L);
    Long count16384 = reconFileMetadataManager.getFileSizeCount(key16384);
    assertNotNull(count16384, "Expected RocksDB bin 16384 to exist");
    assertEquals(1L, count16384.longValue(), "Expected RocksDB bin 16384 to have count 1");

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

    // After the second batch (updates):
    // New key (1000L) -> bin 1024L: count = 1
    // Deleted key (2000L) -> bin 2048L: count = 0 (deleted)
    // Updated key: old (10000L) -> bin 16384L: count = 0, new (50000L) -> bin 65536L: count = 1
    
    // Verify RocksDB bin for new key data size 1000L -> upper bound 1024L
    FileSizeCountKey key1024 = new FileSizeCountKey("vol1", "bucket1", 1024L);
    Long count1024 = reconFileMetadataManager.getFileSizeCount(key1024);
    assertNotNull(count1024, "Expected RocksDB bin 1024 to exist");
    assertEquals(1L, count1024.longValue(), "Expected RocksDB bin 1024 to have count 1");
    
    // Verify RocksDB bin 2048L should be 0 (deleted key)
    count2048 = reconFileMetadataManager.getFileSizeCount(key2048);
    // Count might be null or 0 after deletion
    Long expected2048 = count2048 != null ? count2048 : 0L;
    assertEquals(0L, expected2048.longValue(), "Expected RocksDB bin 2048 to have count 0");
    
    // Verify RocksDB bin 16384L should be 0 (updated key moved out)
    count16384 = reconFileMetadataManager.getFileSizeCount(key16384);
    Long expected16384 = count16384 != null ? count16384 : 0L;
    assertEquals(0L, expected16384.longValue(), "Expected RocksDB bin 16384 to have count 0");
    
    // Verify RocksDB bin for updated key data size 50000L -> upper bound 65536L
    FileSizeCountKey key65536 = new FileSizeCountKey("vol1", "bucket1", 65536L);
    Long count65536 = reconFileMetadataManager.getFileSizeCount(key65536);
    assertNotNull(count65536, "Expected RocksDB bin 65536 to exist");
    assertEquals(1L, count65536.longValue(), "Expected RocksDB bin 65536 to have count 1");
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

    // Verify RocksDB data for scale test
    // 2 volumes * 500 buckets * 42 unique keys each = large dataset processed
    // Since both OBS and FSO tasks process the same data, counts should be doubled
    
    // Verify counts for a few representative bins in RocksDB.
    // For volume "vol1", bucket "bucket1", the first bin (upper bound 1024L) should have a count of 2.
    FileSizeCountKey key1 = new FileSizeCountKey("vol1", "bucket1", 1024L);
    Long count1 = reconFileMetadataManager.getFileSizeCount(key1);
    assertNotNull(count1, "Expected RocksDB bin vol1/bucket1/1024 to exist");
    assertEquals(2L, count1.longValue(), "Expected RocksDB bin vol1/bucket1/1024 to have count 2");

    // For volume "vol1", bucket "bucket1", the bin with upper bound 131072L should have a count of 2.
    FileSizeCountKey key2 = new FileSizeCountKey("vol1", "bucket1", 131072L);
    Long count2 = reconFileMetadataManager.getFileSizeCount(key2);
    assertNotNull(count2, "Expected RocksDB bin vol1/bucket1/131072 to exist");
    assertEquals(2L, count2.longValue(), "Expected RocksDB bin vol1/bucket1/131072 to have count 2");

    // For volume "vol1", bucket "bucket500", the highest bin (upper bound Long.MAX_VALUE) should have a count of 2.
    FileSizeCountKey key3 = new FileSizeCountKey("vol1", "bucket500", Long.MAX_VALUE);
    Long count3 = reconFileMetadataManager.getFileSizeCount(key3);
    assertNotNull(count3, "Expected RocksDB bin vol1/bucket500/Long.MAX_VALUE to exist");
    assertEquals(2L, count3.longValue(), "Expected RocksDB bin vol1/bucket500/Long.MAX_VALUE to have count 2");
  }

  @Test
  public void testProcessAtScale() throws IOException {
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

    // Verify initial data is correctly stored in RocksDB bins.
    // 10 volumes * 100 buckets * 10 keys = 10000 total records processed
    
    // Verify RocksDB bin for key data size 1000L -> upper bound 2048L
    FileSizeCountKey initialKey1 = new FileSizeCountKey("vol1", "bucket1", 2048L);
    Long initialCount1 = reconFileMetadataManager.getFileSizeCount(initialKey1);
    assertNotNull(initialCount1, "Expected RocksDB bin vol1/bucket1/2048 to exist");
    assertEquals(1L, initialCount1.longValue(), "Expected RocksDB bin vol1/bucket1/2048 to have count 1");
    
    // Verify RocksDB bin for key data size 10000L -> upper bound 16384L
    FileSizeCountKey initialKey2 = new FileSizeCountKey("vol10", "bucket100", 16384L);
    Long initialCount2 = reconFileMetadataManager.getFileSizeCount(initialKey2);
    assertNotNull(initialCount2, "Expected RocksDB bin vol10/bucket100/16384 to exist");
    assertEquals(1L, initialCount2.longValue(), "Expected RocksDB bin vol10/bucket100/16384 to have count 1");

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

    // Verify RocksDB data after updates and deletes
    // The update events on keys 6-10 should now put them under first bin 1024L (data size 100L)
    FileSizeCountKey updateKey1 = new FileSizeCountKey("vol1", "bucket1", 1024L);
    Long updateCount1 = reconFileMetadataManager.getFileSizeCount(updateKey1);
    assertNotNull(updateCount1, "Expected RocksDB bin vol1/bucket1/1024 to exist after updates");
    assertEquals(5L, updateCount1.longValue(), "Expected RocksDB bin vol1/bucket1/1024 to have count 5 after updates");

    FileSizeCountKey updateKey2 = new FileSizeCountKey("vol1", "bucket100", 1024L);
    Long updateCount2 = reconFileMetadataManager.getFileSizeCount(updateKey2);
    assertNotNull(updateCount2, "Expected RocksDB bin vol1/bucket100/1024 to exist after updates");
    assertEquals(5L, updateCount2.longValue(),
        "Expected RocksDB bin vol1/bucket100/1024 to have count 5 after updates");

    // Verify bin 2048L should be 0 after deletions
    FileSizeCountKey deleteKey = new FileSizeCountKey("vol1", "bucket100", 2048L);
    Long deleteCount = reconFileMetadataManager.getFileSizeCount(deleteKey);
    // Count should be null or 0 after deletion
    Long expectedDeleteCount = deleteCount != null ? deleteCount : 0L;
    assertEquals(0L, expectedDeleteCount.longValue(),
        "Expected RocksDB bin vol1/bucket100/2048 to have count 0 after deletions");

    // Volumes 2 - 10 should not be affected by this process
    FileSizeCountKey unaffectedKey = new FileSizeCountKey("vol2", "bucket1", 2048L);
    Long unaffectedCount = reconFileMetadataManager.getFileSizeCount(unaffectedKey);
    assertNotNull(unaffectedCount, "Expected RocksDB bin vol2/bucket1/2048 to remain unaffected");
    assertEquals(1L, unaffectedCount.longValue(),
        "Expected RocksDB bin vol2/bucket1/2048 to have count 1 (unaffected)");
  }
}
