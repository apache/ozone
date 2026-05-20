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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * Test for NSSummaryTaskWithFSO.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestNSSummaryTaskWithFSO extends AbstractNSSummaryTaskTest {

  // Answer Sets
  private static Set<Long> bucketOneAns = new HashSet<>();
  private static Set<Long> bucketTwoAns = new HashSet<>();
  private static Set<Long> dirOneAns = new HashSet<>();
  private NSSummaryTaskWithFSO nSSummaryTaskWithFso;

  private static BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  @BeforeAll
  void setUp(@TempDir File tmpDir) throws Exception {
    commonSetup(tmpDir,
        new OMConfigParameter(true,
          false,
          getBucketLayout(),
          3,
          true,
          true,
          false));
    long threshold = getOzoneConfiguration().getLong(OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD, 3);
    nSSummaryTaskWithFso = new NSSummaryTaskWithFSO(
        getReconNamespaceSummaryManager(),
        getReconOMMetadataManager(),
        threshold, 5, 20, 2000);
  }



  /**
   * Nested class for testing NSSummaryTaskWithFSO reprocess.
   */
  @Nested
  public class TestReprocess {

    private NSSummary nsSummaryForBucket1;
    private NSSummary nsSummaryForBucket2;

    @BeforeEach
    public void setUp() throws IOException {
      // write a NSSummary prior to reprocess
      // verify it got cleaned up after.
      List<NSSummary> result =
          commonSetUpTestReprocess(() -> nSSummaryTaskWithFso.reprocessWithFSO(getReconOMMetadataManager()),
              BUCKET_ONE_OBJECT_ID, BUCKET_TWO_OBJECT_ID);
      nsSummaryForBucket1 = result.get(0);
      nsSummaryForBucket2 = result.get(1);
    }

    @Test
    public void testReprocessNSSummaryNull() throws IOException {
      assertNull(getReconNamespaceSummaryManager().getNSSummary(-1L));
    }

    @Test
    public void testReprocessGetFiles() {
      assertEquals(2, nsSummaryForBucket1.getNumOfFiles());
      assertEquals(2, nsSummaryForBucket2.getNumOfFiles());

      assertEquals(KEY_ONE_SIZE + KEY_THREE_SIZE, nsSummaryForBucket1.getSizeOfFiles());
      assertEquals(KEY_TWO_OLD_SIZE + KEY_FOUR_SIZE,
          nsSummaryForBucket2.getSizeOfFiles());
    }

    @Test
    public void testReprocessFileBucketSize() {
      int[] fileDistBucket1 = nsSummaryForBucket1.getFileSizeBucket();
      int[] fileDistBucket2 = nsSummaryForBucket2.getFileSizeBucket();
      assertEquals(ReconConstants.NUM_OF_FILE_SIZE_BINS,
          fileDistBucket1.length);
      assertEquals(ReconConstants.NUM_OF_FILE_SIZE_BINS,
          fileDistBucket2.length);

      assertEquals(1, fileDistBucket1[0]);
      for (int i = 1; i < ReconConstants.NUM_OF_FILE_SIZE_BINS; ++i) {
        assertEquals(0, fileDistBucket1[i]);
      }
      assertEquals(1, fileDistBucket2[1]);
      assertEquals(1, fileDistBucket2[2]);
      for (int i = 0; i < ReconConstants.NUM_OF_FILE_SIZE_BINS; ++i) {
        if (i == 1 || i == 2) {
          continue;
        }
        assertEquals(0, fileDistBucket2[i]);
      }
    }

    @Test
    public void testReprocessBucketDirs() {
      // Bucket one has one dir, bucket two has none.
      Set<Long> childDirBucketOne = nsSummaryForBucket1.getChildDir();
      Set<Long> childDirBucketTwo = nsSummaryForBucket2.getChildDir();
      assertEquals(1, childDirBucketOne.size());
      bucketOneAns.clear();
      bucketOneAns.add(DIR_ONE_OBJECT_ID);
      assertEquals(bucketOneAns, childDirBucketOne);
      assertEquals(0, childDirBucketTwo.size());
    }

    @Test
    public void testReprocessDirsUnderDir() throws Exception {

      // Dir 1 has two dir: dir2 and dir3.
      NSSummary nsSummaryInDir1 = getReconNamespaceSummaryManager()
          .getNSSummary(DIR_ONE_OBJECT_ID);
      assertNotNull(nsSummaryInDir1);
      Set<Long> childDirForDirOne = nsSummaryInDir1.getChildDir();
      assertEquals(2, childDirForDirOne.size());
      dirOneAns.clear();
      dirOneAns.add(DIR_TWO_OBJECT_ID);
      dirOneAns.add(DIR_THREE_OBJECT_ID);
      assertEquals(dirOneAns, childDirForDirOne);

      NSSummary nsSummaryInDir2 = getReconNamespaceSummaryManager()
          .getNSSummary(DIR_TWO_OBJECT_ID);
      assertEquals(1, nsSummaryInDir2.getNumOfFiles());
      assertEquals(KEY_THREE_SIZE, nsSummaryInDir2.getSizeOfFiles());

      int[] fileDistForDir2 = nsSummaryInDir2.getFileSizeBucket();
      assertEquals(ReconConstants.NUM_OF_FILE_SIZE_BINS,
          fileDistForDir2.length);
      assertEquals(1, fileDistForDir2[fileDistForDir2.length - 1]);
      for (int i = 0; i < ReconConstants.NUM_OF_FILE_SIZE_BINS - 1; ++i) {
        assertEquals(0, fileDistForDir2[i]);
      }
      assertEquals(0, nsSummaryInDir2.getChildDir().size());

      // bucket should have empty dirName
      assertEquals(0, nsSummaryForBucket1.getDirName().length());
      assertEquals(0, nsSummaryForBucket2.getDirName().length());
      // check dirName is correctly written
      assertEquals(DIR_ONE, nsSummaryInDir1.getDirName());
      assertEquals(DIR_TWO, nsSummaryInDir2.getDirName());
    }

    @Test
    public void testDirectoryParentIdAssignment() throws Exception {
      // Trigger reprocess to simulate reading from OM DB and processing into NSSummary.
      nSSummaryTaskWithFso.reprocessWithFSO(getReconOMMetadataManager());

      // Fetch NSSummary for DIR_ONE and verify its parent ID matches BUCKET_ONE_OBJECT_ID.
      NSSummary nsSummaryDirOne =
          getReconNamespaceSummaryManager().getNSSummary(DIR_ONE_OBJECT_ID);
      assertNotNull(nsSummaryDirOne,
          "NSSummary for DIR_ONE should not be null.");
      assertEquals(BUCKET_ONE_OBJECT_ID, nsSummaryDirOne.getParentId(),
          "DIR_ONE's parent ID should match BUCKET_ONE_OBJECT_ID.");

      // Fetch NSSummary for DIR_TWO and verify its parent ID matches DIR_ONE_OBJECT_ID.
      NSSummary nsSummaryDirTwo =
          getReconNamespaceSummaryManager().getNSSummary(DIR_TWO_OBJECT_ID);
      assertNotNull(nsSummaryDirTwo,
          "NSSummary for DIR_TWO should not be null.");
      assertEquals(DIR_ONE_OBJECT_ID, nsSummaryDirTwo.getParentId(),
          "DIR_TWO's parent ID should match DIR_ONE_OBJECT_ID.");

      // Fetch NSSummary for DIR_THREE and verify its parent ID matches DIR_ONE_OBJECT_ID.
      NSSummary nsSummaryDirThree =
          getReconNamespaceSummaryManager().getNSSummary(DIR_THREE_OBJECT_ID);
      assertNotNull(nsSummaryDirThree,
          "NSSummary for DIR_THREE should not be null.");
      assertEquals(DIR_ONE_OBJECT_ID, nsSummaryDirThree.getParentId(),
          "DIR_THREE's parent ID should match DIR_ONE_OBJECT_ID.");
    }

  }

  /**
   * Nested class for testing NSSummaryTaskWithFSO process.
   */
  @Nested
  public class TestProcess {

    private Pair<Integer, Boolean> result;

    @BeforeEach
    public void setUp() throws IOException {
      getReconNamespaceSummaryManager().clearNSSummaryTable();
      nSSummaryTaskWithFso.reprocessWithFSO(getReconOMMetadataManager());
      result = nSSummaryTaskWithFso.processWithFSO(processEventBatch(), 0);
    }

    private OMUpdateEventBatch processEventBatch() throws IOException {
      // Events for keyTable change:
      // put file5 under bucket 2
      String omPutKey = BUCKET_TWO_OBJECT_ID + OM_KEY_PREFIX + FILE_FIVE;
      OmKeyInfo omPutKeyInfo = buildOmKeyInfo(VOL, BUCKET_TWO, KEY_FIVE,
          FILE_FIVE, KEY_FIVE_OBJECT_ID, BUCKET_TWO_OBJECT_ID, KEY_FIVE_SIZE);
      OMDBUpdateEvent keyEvent1 = new OMDBUpdateEvent.
                                          OMUpdateEventBuilder<String, OmKeyInfo>()
                                      .setKey(omPutKey)
                                      .setValue(omPutKeyInfo)
                                      .setTable(getOmMetadataManager().getKeyTable(getBucketLayout())
                                                    .getName())
                                      .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
                                      .build();

      // delete file 1 under bucket 1
      String omDeleteKey = BUCKET_ONE_OBJECT_ID + OM_KEY_PREFIX + FILE_ONE;
      OmKeyInfo omDeleteInfo = buildOmKeyInfo(
          VOL, BUCKET_ONE, KEY_ONE, FILE_ONE,
          KEY_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID);
      OMDBUpdateEvent keyEvent2 = new OMDBUpdateEvent.
                                          OMUpdateEventBuilder<String, OmKeyInfo>()
                                      .setKey(omDeleteKey)
                                      .setValue(omDeleteInfo)
                                      .setTable(getOmMetadataManager().getKeyTable(getBucketLayout())
                                                    .getName())
                                      .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
                                      .build();

      // update file 2's size under bucket 2
      String omUpdateKey = BUCKET_TWO_OBJECT_ID + OM_KEY_PREFIX + FILE_TWO;
      OmKeyInfo omOldInfo = buildOmKeyInfo(
          VOL, BUCKET_TWO, KEY_TWO, FILE_TWO,
          KEY_TWO_OBJECT_ID, BUCKET_TWO_OBJECT_ID, KEY_TWO_OLD_SIZE);
      OmKeyInfo omUpdateInfo = buildOmKeyInfo(
          VOL, BUCKET_TWO, KEY_TWO, FILE_TWO,
          KEY_TWO_OBJECT_ID, BUCKET_TWO_OBJECT_ID, KEY_TWO_UPDATE_SIZE);
      OMDBUpdateEvent keyEvent3 = new OMDBUpdateEvent.
                                          OMUpdateEventBuilder<String, OmKeyInfo>()
                                      .setKey(omUpdateKey)
                                      .setValue(omUpdateInfo)
                                      .setOldValue(omOldInfo)
                                      .setTable(getOmMetadataManager().getKeyTable(getBucketLayout())
                                                    .getName())
                                      .setAction(OMDBUpdateEvent.OMDBUpdateAction.UPDATE)
                                      .build();

      // Events for DirectoryTable change:
      // add dir 4 under bucket 1
      String omDirPutKey1 = BUCKET_ONE_OBJECT_ID + OM_KEY_PREFIX + DIR_FOUR;
      OmDirectoryInfo omDirPutValue1 = buildOmDirInfo(DIR_FOUR,
          DIR_FOUR_OBJECT_ID, BUCKET_ONE_OBJECT_ID);
      OMDBUpdateEvent keyEvent4 = new OMDBUpdateEvent.
                                          OMUpdateEventBuilder<String, OmDirectoryInfo>()
                                      .setKey(omDirPutKey1)
                                      .setValue(omDirPutValue1)
                                      .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
                                      .setTable(getOmMetadataManager().getDirectoryTable().getName())
                                      .build();

      // add dir 5 under bucket 2
      String omDirPutKey2 = BUCKET_TWO_OBJECT_ID + OM_KEY_PREFIX + DIR_FIVE;
      OmDirectoryInfo omDirPutValue2 = buildOmDirInfo(DIR_FIVE,
          DIR_FIVE_OBJECT_ID, BUCKET_TWO_OBJECT_ID);
      OMDBUpdateEvent keyEvent5 = new OMDBUpdateEvent.
                                          OMUpdateEventBuilder<String, OmDirectoryInfo>()
                                      .setKey(omDirPutKey2)
                                      .setValue(omDirPutValue2)
                                      .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
                                      .setTable(getOmMetadataManager().getDirectoryTable().getName())
                                      .build();

      // delete dir 3 under dir 1
      String omDirDeleteKey = DIR_ONE_OBJECT_ID + OM_KEY_PREFIX + DIR_THREE;
      OmDirectoryInfo omDirDeleteValue = buildOmDirInfo(DIR_THREE,
          DIR_THREE_OBJECT_ID, DIR_ONE_OBJECT_ID);
      OMDBUpdateEvent keyEvent6 = new OMDBUpdateEvent.
                                          OMUpdateEventBuilder<String, OmDirectoryInfo>()
                                      .setKey(omDirDeleteKey)
                                      .setValue(omDirDeleteValue)
                                      .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
                                      .setTable(getOmMetadataManager().getDirectoryTable().getName())
                                      .build();

      // rename dir1
      String omDirUpdateKey = BUCKET_ONE_OBJECT_ID + OM_KEY_PREFIX + DIR_ONE;
      OmDirectoryInfo omDirOldValue = buildOmDirInfo(DIR_ONE,
          DIR_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID);
      OmDirectoryInfo omDirUpdateValue = buildOmDirInfo(DIR_ONE_RENAME,
          DIR_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID);
      OMDBUpdateEvent keyEvent7 = new OMDBUpdateEvent.
                                          OMUpdateEventBuilder<String, OmDirectoryInfo>()
                                      .setKey(omDirUpdateKey)
                                      .setValue(omDirUpdateValue)
                                      .setOldValue(omDirOldValue)
                                      .setAction(OMDBUpdateEvent.OMDBUpdateAction.UPDATE)
                                      .setTable(getOmMetadataManager().getDirectoryTable().getName())
                                      .build();

      return new OMUpdateEventBatch(Arrays.asList(
          keyEvent1, keyEvent2, keyEvent3, keyEvent4, keyEvent5,
          keyEvent6, keyEvent7
      ), 0L);
    }

    @Test
    public void testProcessUpdateFileSize() throws IOException {
      NSSummary nsSummaryForBucket1 =
          getReconNamespaceSummaryManager().getNSSummary(BUCKET_ONE_OBJECT_ID);

      assertNotNull(nsSummaryForBucket1);
      assertEquals(1, nsSummaryForBucket1.getNumOfFiles());

      Set<Long> childDirBucket1 = nsSummaryForBucket1.getChildDir();
      // after put dir4, bucket1 now has two child dirs: dir1 and dir4
      assertEquals(2, childDirBucket1.size());
      bucketOneAns.clear();
      bucketOneAns.add(DIR_ONE_OBJECT_ID);
      bucketOneAns.add(DIR_FOUR_OBJECT_ID);
      assertEquals(bucketOneAns, childDirBucket1);
    }

    @Test
    public void testProcessBucket() throws IOException {
      NSSummary nsSummaryForBucket2 =
          getReconNamespaceSummaryManager().getNSSummary(BUCKET_TWO_OBJECT_ID);
      // file 5 is added under bucket 2, so bucket 2 has 3 keys now
      // file 2 is updated with new datasize,
      // so file size dist for bucket 2 should be updated
      assertNotNull(nsSummaryForBucket2);
      assertEquals(3, nsSummaryForBucket2.getNumOfFiles());
      // key 4 + key 5 + updated key 2
      assertEquals(KEY_FOUR_SIZE + KEY_FIVE_SIZE
          + KEY_TWO_UPDATE_SIZE, nsSummaryForBucket2.getSizeOfFiles());

      int[] fileSizeDist = nsSummaryForBucket2.getFileSizeBucket();
      assertEquals(ReconConstants.NUM_OF_FILE_SIZE_BINS,
          fileSizeDist.length);
      // 1023L and 100L
      assertEquals(2, fileSizeDist[0]);
      // 2050L
      assertEquals(1, fileSizeDist[2]);
      for (int i = 0; i < ReconConstants.NUM_OF_FILE_SIZE_BINS; ++i) {
        if (i == 0 || i == 2) {
          continue;
        }
        assertEquals(0, fileSizeDist[i]);
      }

      // after put dir5, bucket 2 now has one dir
      Set<Long> childDirBucket2 = nsSummaryForBucket2.getChildDir();
      assertEquals(1, childDirBucket2.size());
      bucketTwoAns.add(DIR_FIVE_OBJECT_ID);
      assertEquals(bucketTwoAns, childDirBucket2);
    }

    @Test
    public void testProcessDirDeleteRename() throws IOException {
      // after delete dir 3, dir 1 now has only one dir: dir2
      NSSummary nsSummaryForDir1 = getReconNamespaceSummaryManager()
          .getNSSummary(DIR_ONE_OBJECT_ID);
      assertNotNull(nsSummaryForDir1);
      Set<Long> childDirForDir1 = nsSummaryForDir1.getChildDir();
      assertEquals(1, childDirForDir1.size());
      dirOneAns.clear();
      dirOneAns.add(DIR_TWO_OBJECT_ID);
      assertEquals(dirOneAns, childDirForDir1);

      // after renaming dir1, check its new name
      assertEquals(DIR_ONE_RENAME, nsSummaryForDir1.getDirName());
    }

    @Test
    public void testParentIdAfterProcessEventBatch() throws IOException {

      // Verify the parent ID of DIR_FOUR after it's added under BUCKET_ONE.
      NSSummary nsSummaryDirFour =
          getReconNamespaceSummaryManager().getNSSummary(DIR_FOUR_OBJECT_ID);
      assertNotNull(nsSummaryDirFour,
          "NSSummary for DIR_FOUR should not be null.");
      assertEquals(BUCKET_ONE_OBJECT_ID, nsSummaryDirFour.getParentId(),
          "DIR_FOUR's parent ID should match BUCKET_ONE_OBJECT_ID.");

      // Verify the parent ID of DIR_FIVE after it's added under BUCKET_TWO.
      NSSummary nsSummaryDirFive =
          getReconNamespaceSummaryManager().getNSSummary(DIR_FIVE_OBJECT_ID);
      assertNotNull(nsSummaryDirFive,
          "NSSummary for DIR_FIVE should not be null.");
      assertEquals(BUCKET_TWO_OBJECT_ID, nsSummaryDirFive.getParentId(),
          "DIR_FIVE's parent ID should match BUCKET_TWO_OBJECT_ID.");
    }

    @Test
    void testProcessWithFSOFlushAfterThresholdAndSuccess() throws IOException {
      // Call the method under test

      // Assertions
      Assertions.assertNotNull(result, "Result should not be null");
      // Why seekPos should be 7 ? because we have threshold value for flush is set as 3,
      // and we have total 7 events, so nsSummaryMap will be flushed in 2 batches and
      // during second batch flush, eventCounter will be 6, then last event7 alone will
      // be flushed out of loop as remaining event. At every batch flush based on threshold,
      // seekPos is set as equal to eventCounter + 1, so  seekPos will be 7.
      Assertions.assertEquals(7, result.getLeft(), "seekPos should be 7");
      Assertions.assertTrue(result.getRight(), "The processing should fail due to flush failure");
    }

    @Test
    void testProcessWithFSOFlushAfterThresholdAndFailureOfLastElement()
        throws NoSuchFieldException, IllegalAccessException {
      // Assume the NamespaceSummaryTaskWithFSO object is already created
      NSSummaryTaskWithFSO task = mock(NSSummaryTaskWithFSO.class);

      // Set the value of nsSummaryFlushToDBMaxThreshold to 3 using reflection
      Field thresholdField = NSSummaryTaskWithFSO.class.getDeclaredField("nsSummaryFlushToDBMaxThreshold");
      thresholdField.setAccessible(true);
      thresholdField.set(task, 3);

      ReconNamespaceSummaryManager mockReconNamespaceSummaryManager = mock(ReconNamespaceSummaryManager.class);
      Field managerField = NSSummaryTaskDbEventHandler.class.getDeclaredField("reconNamespaceSummaryManager");
      managerField.setAccessible(true);
      managerField.set(task, mockReconNamespaceSummaryManager);

      // Mock the OMUpdateEventBatch and its iterator
      OMUpdateEventBatch events = mock(OMUpdateEventBatch.class);
      Iterator<OMDBUpdateEvent> mockIterator = mock(Iterator.class);

      Mockito.when(events.getIterator()).thenReturn(mockIterator);

      // Mock OMDBUpdateEvent objects and their behavior
      OMDBUpdateEvent<String, OmKeyInfo> event1 = mock(OMDBUpdateEvent.class);
      OMDBUpdateEvent<String, OmKeyInfo> event2 = mock(OMDBUpdateEvent.class);
      OMDBUpdateEvent<String, OmKeyInfo> event3 = mock(OMDBUpdateEvent.class);
      OMDBUpdateEvent<String, OmKeyInfo> event4 = mock(OMDBUpdateEvent.class);

      // Mock getAction() for each event
      Mockito.when(event1.getAction()).thenReturn(OMDBUpdateEvent.OMDBUpdateAction.PUT);
      Mockito.when(event2.getAction()).thenReturn(OMDBUpdateEvent.OMDBUpdateAction.PUT);
      Mockito.when(event3.getAction()).thenReturn(OMDBUpdateEvent.OMDBUpdateAction.PUT);
      Mockito.when(event4.getAction()).thenReturn(OMDBUpdateEvent.OMDBUpdateAction.PUT);

      OmKeyInfo keyInfo1 = new OmKeyInfo.Builder().setParentObjectID(1).setObjectID(2).setKeyName("key1")
          .setBucketName("bucket1").setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
          .setDataSize(1024).setVolumeName("volume1").build();
      OmKeyInfo keyInfo2 = new OmKeyInfo.Builder().setParentObjectID(1).setObjectID(3).setKeyName("key2")
          .setBucketName("bucket1").setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
          .setDataSize(1024).setVolumeName("volume1").build();
      OmKeyInfo keyInfo3 = new OmKeyInfo.Builder().setParentObjectID(1).setObjectID(3).setKeyName("key2")
          .setBucketName("bucket1").setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
          .setDataSize(1024).setVolumeName("volume1").build();
      OmKeyInfo keyInfo4 = new OmKeyInfo.Builder().setParentObjectID(1).setObjectID(3).setKeyName("key2")
          .setBucketName("bucket1").setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
          .setDataSize(1024).setVolumeName("volume1").build();
      Mockito.when(event1.getValue()).thenReturn(keyInfo1);
      Mockito.when(event2.getValue()).thenReturn(keyInfo2);
      Mockito.when(event3.getValue()).thenReturn(keyInfo3);
      Mockito.when(event4.getValue()).thenReturn(keyInfo4);

      // Mock getTable() to return valid table name
      Mockito.when(event1.getTable()).thenReturn(FILE_TABLE);
      Mockito.when(event2.getTable()).thenReturn(FILE_TABLE);
      Mockito.when(event3.getTable()).thenReturn(FILE_TABLE);
      Mockito.when(event4.getTable()).thenReturn(FILE_TABLE);

      // Mock iterator to return the events
      Mockito.when(mockIterator.hasNext()).thenReturn(true, true, true, true, false);
      Mockito.when(mockIterator.next()).thenReturn(event1, event2, event3, event4);

      // Mock the flushAndCommitUpdatedNSToDB method to fail on the last flush
      NSSummaryTaskWithFSO taskSpy = Mockito.spy(task);
      Mockito.doReturn(true).doReturn(true).doReturn(false).when(taskSpy)
          .flushAndCommitUpdatedNSToDB(Mockito.anyMap(), Mockito.anyCollection());

      // Call the method under test
      Pair<Integer, Boolean> result1 = taskSpy.processWithFSO(events, 0);

      // Assertions
      Assertions.assertNotNull(result1, "Result should not be null");
      Assertions.assertEquals(0, result1.getLeft(), "seekPos should be 4");

      // Verify interactions
      Mockito.verify(mockIterator, Mockito.times(3)).next();
      Mockito.verify(taskSpy, Mockito.times(1)).flushAndCommitUpdatedNSToDB(Mockito.anyMap(), Mockito.anyCollection());
    }
  }
}
