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

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test for NSSummaryTask. Creates one bucket of each layout (FSO, Legacy, OBS)
 * and exercises reprocess, parallel process(), and bucket-cache invalidation.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestNSSummaryTask extends AbstractNSSummaryTaskTest {

  private NSSummaryTask nSSummaryTask;

  @BeforeAll
  void setUp(@TempDir File tmpDir) throws Exception {
    commonSetup(tmpDir,
        new OMConfigParameter(false,
            false,
            null, 0,
            false,
            true,
            false));

    nSSummaryTask = new NSSummaryTask(
        getReconNamespaceSummaryManager(),
        getReconOMMetadataManager(),
        getOmConfiguration()
    );
  }

  @Test
  public void testTaskInstancesReuseSharedProcessExecutor() throws Exception {
    NSSummaryTask anotherTask = new NSSummaryTask(
        getReconNamespaceSummaryManager(),
        getReconOMMetadataManager(),
        getOmConfiguration());

    assertSame(getSubTaskExecutor(nSSummaryTask),
        getSubTaskExecutor(anotherTask));
  }

  private Object getSubTaskExecutor(NSSummaryTask task) throws Exception {
    Field executorField = NSSummaryTask.class.getDeclaredField(
        "SUB_TASK_EXECUTOR");
    executorField.setAccessible(true);
    return executorField.get(task);
  }

  /**
   * Nested class for testing NSSummaryTaskWithLegacy reprocess.
   */
  @Nested
  public class TestReprocess {

    private NSSummary nsSummaryForBucket1;
    private NSSummary nsSummaryForBucket2;
    private NSSummary nsSummaryForBucket3;

    @BeforeEach
    public void setUp() throws Exception {
      List<NSSummary> result = commonSetUpTestReprocess(
          () -> nSSummaryTask.reprocess(getReconOMMetadataManager()),
          BUCKET_ONE_OBJECT_ID,
          BUCKET_TWO_OBJECT_ID,
          BUCKET_THREE_OBJECT_ID);
      nsSummaryForBucket1 = result.get(0);
      nsSummaryForBucket2 = result.get(1);
      nsSummaryForBucket3 = result.get(2);
    }

    @Test
    public void testReprocessNSSummaryNull() throws IOException {
      assertNull(getReconNamespaceSummaryManager().getNSSummary(-1L));
    }

    @Test
    public void testReprocessGetFiles() {
      assertEquals(1, nsSummaryForBucket1.getNumOfFiles());
      assertEquals(1, nsSummaryForBucket2.getNumOfFiles());
      assertEquals(1, nsSummaryForBucket3.getNumOfFiles());

      assertEquals(KEY_ONE_SIZE, nsSummaryForBucket1.getSizeOfFiles());
      assertEquals(KEY_TWO_SIZE, nsSummaryForBucket2.getSizeOfFiles());
      assertEquals(KEY_THREE_SIZE, nsSummaryForBucket3.getSizeOfFiles());
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
      for (int i = 0; i < ReconConstants.NUM_OF_FILE_SIZE_BINS; ++i) {
        if (i == 1) {
          continue;
        }
        assertEquals(0, fileDistBucket2[i]);
      }
    }

  }

  /**
   * Nested class for testing NSSummaryTaskWithLegacy process.
   */
  @Nested
  public class TestProcess {

    private NSSummary nsSummaryForBucket1;
    private NSSummary nsSummaryForBucket2;
    private NSSummary nsSummaryForBucket3;
    private ReconOmTask.TaskResult processResult;

    @BeforeEach
    public void setUp() throws IOException {
      nSSummaryTask.reprocess(getReconOMMetadataManager());
      // Exercise process() across all three bucket layouts in a single batch
      // so the parallel sub-task dispatch is covered end-to-end.
      processResult = nSSummaryTask.process(processEventBatch(), Collections.emptyMap());

      nsSummaryForBucket1 =
          getReconNamespaceSummaryManager().getNSSummary(BUCKET_ONE_OBJECT_ID);
      assertNotNull(nsSummaryForBucket1);
      nsSummaryForBucket2 =
          getReconNamespaceSummaryManager().getNSSummary(BUCKET_TWO_OBJECT_ID);
      assertNotNull(nsSummaryForBucket2);
      nsSummaryForBucket3 =
          getReconNamespaceSummaryManager().getNSSummary(BUCKET_THREE_OBJECT_ID);
      assertNotNull(nsSummaryForBucket3);
    }

    private OMUpdateEventBatch processEventBatch() throws IOException {
      // PUT file5 under bucket 2 (Legacy)
      String omPutKey =
          OM_KEY_PREFIX + VOL +
              OM_KEY_PREFIX + BUCKET_TWO +
              OM_KEY_PREFIX + FILE_FIVE;
      OmKeyInfo omPutKeyInfo = buildOmKeyInfo(VOL, BUCKET_TWO, KEY_FIVE,
          FILE_FIVE, KEY_FIVE_OBJECT_ID, BUCKET_TWO_OBJECT_ID, KEY_FIVE_SIZE);
      OMDBUpdateEvent keyEvent1 = new OMDBUpdateEvent.
                                          OMUpdateEventBuilder<String, OmKeyInfo>()
                                      .setKey(omPutKey)
                                      .setValue(omPutKeyInfo)
                                      .setTable(getOmMetadataManager().getKeyTable(getLegacyBucketLayout())
                                                    .getName())
                                      .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
                                      .build();

      // DELETE file1 under bucket 1 (FSO)
      String omDeleteKey = BUCKET_ONE_OBJECT_ID + OM_KEY_PREFIX + FILE_ONE;
      OmKeyInfo omDeleteInfo = buildOmKeyInfo(
          VOL, BUCKET_ONE, KEY_ONE, FILE_ONE,
          KEY_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID);
      OMDBUpdateEvent keyEvent2 = new OMDBUpdateEvent.
                                          OMUpdateEventBuilder<String, OmKeyInfo>()
                                      .setKey(omDeleteKey)
                                      .setValue(omDeleteInfo)
                                      .setTable(getOmMetadataManager().getKeyTable(getFSOBucketLayout())
                                                    .getName())
                                      .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
                                      .build();

      // PUT file4 under bucket 3 (OBS) — exercises the OBS sub-task path so a
      // regression in the OBS branch (e.g. missed events) is caught.
      String omObsPutKey =
          OM_KEY_PREFIX + VOL +
              OM_KEY_PREFIX + BUCKET_THREE +
              OM_KEY_PREFIX + KEY_FOUR;
      OmKeyInfo omObsPutKeyInfo = buildOmKeyInfo(VOL, BUCKET_THREE, KEY_FOUR,
          KEY_FOUR, KEY_FOUR_OBJECT_ID, BUCKET_THREE_OBJECT_ID, KEY_FOUR_SIZE);
      OMDBUpdateEvent keyEvent3 = new OMDBUpdateEvent.
                                          OMUpdateEventBuilder<String, OmKeyInfo>()
                                      .setKey(omObsPutKey)
                                      .setValue(omObsPutKeyInfo)
                                      .setTable(getOmMetadataManager().getKeyTable(getOBSBucketLayout())
                                                    .getName())
                                      .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
                                      .build();

      return new OMUpdateEventBatch(Arrays.asList(keyEvent1, keyEvent2, keyEvent3), 0L);
    }

    @Test
    public void testProcessUpdateFileSize() throws IOException {
      // file 1 is gone, so bucket 1 is empty now
      assertNotNull(nsSummaryForBucket1);
      assertEquals(0, nsSummaryForBucket1.getNumOfFiles());

      Set<Long> childDirBucket1 = nsSummaryForBucket1.getChildDir();
      assertEquals(0, childDirBucket1.size());
    }

    @Test
    public void testProcessBucket() throws IOException {
      // file 5 is added under bucket 2, so bucket 2 has 2 keys now
      assertNotNull(nsSummaryForBucket2);
      assertEquals(2, nsSummaryForBucket2.getNumOfFiles());
      // key 2 + key 5
      assertEquals(KEY_TWO_SIZE + KEY_FIVE_SIZE,
          nsSummaryForBucket2.getSizeOfFiles());

      int[] fileSizeDist = nsSummaryForBucket2.getFileSizeBucket();
      assertEquals(ReconConstants.NUM_OF_FILE_SIZE_BINS,
          fileSizeDist.length);
      // 1025L
      assertEquals(1, fileSizeDist[0]);
      // 2050L
      assertEquals(1, fileSizeDist[1]);
      for (int i = 2; i < ReconConstants.NUM_OF_FILE_SIZE_BINS; ++i) {
        assertEquals(0, fileSizeDist[i]);
      }
    }

    @Test
    public void testProcessObsBucket() {
      // bucket 3 (OBS) had file3 from reprocess; the batch added file4.
      assertEquals(2, nsSummaryForBucket3.getNumOfFiles());
      assertEquals(KEY_THREE_SIZE + KEY_FOUR_SIZE,
          nsSummaryForBucket3.getSizeOfFiles());
    }

    @Test
    public void testProcessTaskResult() {
      // Sub-task seek positions must be reported for all three layouts so the
      // dispatcher can resume each sub-task independently on retry.
      assertNotNull(processResult);
      assertTrue(processResult.isTaskSuccess());
      assertNotNull(processResult.getSubTaskSeekPositions().get(NSSummaryTask.BucketType.FSO.name()));
      assertNotNull(processResult.getSubTaskSeekPositions().get(NSSummaryTask.BucketType.LEGACY.name()));
      assertNotNull(processResult.getSubTaskSeekPositions().get(NSSummaryTask.BucketType.OBS.name()));
    }
  }

  /**
   * Exercises parallel FSO / Legacy / OBS sub-task dispatch beyond the happy
   * path: interleaved multi-mutation batches, per-layout filtering in a shared
   * event stream, and independent seek positions per sub-task on retry.
   */
  @Nested
  public class TestProcessParallelMixedBatch {

    @BeforeEach
    public void reprocessBaseline() throws IOException {
      nSSummaryTask.reprocess(getReconOMMetadataManager());
    }

    @Test
    public void testInterleavedBatchAppliesAllLayoutMutations() throws IOException {
      // Deliberately scramble layout order: OBS, Legacy, FSO, OBS, Legacy.
      ReconOmTask.TaskResult result = nSSummaryTask.process(
          new OMUpdateEventBatch(Arrays.asList(
              obsDeleteEvent(KEY_THREE, KEY_THREE_OBJECT_ID, KEY_THREE_SIZE),
              legacyPutEvent(KEY_FIVE, KEY_FIVE_OBJECT_ID, KEY_FIVE_SIZE),
              fsoDeleteEvent(KEY_ONE, KEY_ONE_OBJECT_ID, KEY_ONE_SIZE),
              obsPutEvent(KEY_FOUR, KEY_FOUR_OBJECT_ID, KEY_FOUR_SIZE),
              legacyUpdateEvent(KEY_TWO, KEY_TWO_OBJECT_ID, KEY_TWO_SIZE,
                  KEY_TWO_UPDATE_SIZE)),
              0L),
          Collections.emptyMap());

      assertTrue(result.isTaskSuccess());

      NSSummary fsoBucket =
          getReconNamespaceSummaryManager().getNSSummary(BUCKET_ONE_OBJECT_ID);
      assertNotNull(fsoBucket);
      assertEquals(0, fsoBucket.getNumOfFiles());
      assertEquals(0, fsoBucket.getSizeOfFiles());

      NSSummary legacyBucket =
          getReconNamespaceSummaryManager().getNSSummary(BUCKET_TWO_OBJECT_ID);
      assertNotNull(legacyBucket);
      assertEquals(2, legacyBucket.getNumOfFiles());
      assertEquals(KEY_TWO_UPDATE_SIZE + KEY_FIVE_SIZE,
          legacyBucket.getSizeOfFiles());

      NSSummary obsBucket =
          getReconNamespaceSummaryManager().getNSSummary(BUCKET_THREE_OBJECT_ID);
      assertNotNull(obsBucket);
      assertEquals(1, obsBucket.getNumOfFiles());
      assertEquals(KEY_FOUR_SIZE, obsBucket.getSizeOfFiles());
    }

    @Test
    public void testLayoutFilteringInMixedBatch() throws IOException {
      // OBS-only keyTable events in a batch that still runs all three sub-tasks
      // in parallel. Legacy and FSO must leave their buckets at the reprocess
      // baseline while OBS applies delete+put.
      ReconOmTask.TaskResult result = nSSummaryTask.process(
          new OMUpdateEventBatch(Arrays.asList(
              obsDeleteEvent(KEY_THREE, KEY_THREE_OBJECT_ID, KEY_THREE_SIZE),
              obsPutEvent(KEY_FOUR, KEY_FOUR_OBJECT_ID, KEY_FOUR_SIZE)),
              0L),
          Collections.emptyMap());

      assertTrue(result.isTaskSuccess());

      assertEquals(1, getReconNamespaceSummaryManager()
          .getNSSummary(BUCKET_ONE_OBJECT_ID).getNumOfFiles());
      assertEquals(KEY_ONE_SIZE, getReconNamespaceSummaryManager()
          .getNSSummary(BUCKET_ONE_OBJECT_ID).getSizeOfFiles());
      assertEquals(1, getReconNamespaceSummaryManager()
          .getNSSummary(BUCKET_TWO_OBJECT_ID).getNumOfFiles());
      assertEquals(KEY_TWO_SIZE, getReconNamespaceSummaryManager()
          .getNSSummary(BUCKET_TWO_OBJECT_ID).getSizeOfFiles());
      assertEquals(1, getReconNamespaceSummaryManager()
          .getNSSummary(BUCKET_THREE_OBJECT_ID).getNumOfFiles());
      assertEquals(KEY_FOUR_SIZE, getReconNamespaceSummaryManager()
          .getNSSummary(BUCKET_THREE_OBJECT_ID).getSizeOfFiles());
    }

    @Test
    public void testIndependentSubTaskSeekPositions() throws IOException {
      // Shared stream (5 events): OBS delete, Legacy put, FSO delete, OBS put,
      // Legacy update. Legacy seek=3 skips the first three stream positions
      // (including its own put at position 2) while FSO/OBS start at 0.
      OMUpdateEventBatch batch = new OMUpdateEventBatch(Arrays.asList(
          obsDeleteEvent(KEY_THREE, KEY_THREE_OBJECT_ID, KEY_THREE_SIZE),
          legacyPutEvent(KEY_FIVE, KEY_FIVE_OBJECT_ID, KEY_FIVE_SIZE),
          fsoDeleteEvent(KEY_ONE, KEY_ONE_OBJECT_ID, KEY_ONE_SIZE),
          obsPutEvent(KEY_FOUR, KEY_FOUR_OBJECT_ID, KEY_FOUR_SIZE),
          legacyUpdateEvent(KEY_TWO, KEY_TWO_OBJECT_ID, KEY_TWO_SIZE,
              KEY_TWO_UPDATE_SIZE)),
          0L);

      Map<String, Integer> legacySeekOnly = new HashMap<>();
      legacySeekOnly.put(NSSummaryTask.BucketType.LEGACY.name(), 3);

      ReconOmTask.TaskResult result =
          nSSummaryTask.process(batch, legacySeekOnly);

      assertTrue(result.isTaskSuccess());
      // Each sub-task reports its resume position independently. Legacy was
      // told to skip the first three stream indices; FSO/OBS consumed from 0.
      assertNotNull(result.getSubTaskSeekPositions());
      assertEquals(0, result.getSubTaskSeekPositions()
          .get(NSSummaryTask.BucketType.FSO.name()).intValue());
      assertEquals(3, result.getSubTaskSeekPositions()
          .get(NSSummaryTask.BucketType.LEGACY.name()).intValue());
      assertEquals(0, result.getSubTaskSeekPositions()
          .get(NSSummaryTask.BucketType.OBS.name()).intValue());

      // FSO/OBS processed from the start; Legacy only picked up the update.
      assertEquals(0, getReconNamespaceSummaryManager()
          .getNSSummary(BUCKET_ONE_OBJECT_ID).getNumOfFiles());
      assertEquals(1, getReconNamespaceSummaryManager()
          .getNSSummary(BUCKET_TWO_OBJECT_ID).getNumOfFiles());
      assertEquals(KEY_TWO_UPDATE_SIZE, getReconNamespaceSummaryManager()
          .getNSSummary(BUCKET_TWO_OBJECT_ID).getSizeOfFiles());
      assertEquals(1, getReconNamespaceSummaryManager()
          .getNSSummary(BUCKET_THREE_OBJECT_ID).getNumOfFiles());
      assertEquals(KEY_FOUR_SIZE, getReconNamespaceSummaryManager()
          .getNSSummary(BUCKET_THREE_OBJECT_ID).getSizeOfFiles());
    }

    @Test
    public void testLegacySeekZeroAppliesAllLegacyEvents() throws IOException {
      // Contrast with testIndependentSubTaskSeekPositions: without a Legacy
      // seek offset, the interleaved batch applies both Legacy mutations.
      OMUpdateEventBatch batch = new OMUpdateEventBatch(Arrays.asList(
          obsDeleteEvent(KEY_THREE, KEY_THREE_OBJECT_ID, KEY_THREE_SIZE),
          legacyPutEvent(KEY_FIVE, KEY_FIVE_OBJECT_ID, KEY_FIVE_SIZE),
          fsoDeleteEvent(KEY_ONE, KEY_ONE_OBJECT_ID, KEY_ONE_SIZE),
          obsPutEvent(KEY_FOUR, KEY_FOUR_OBJECT_ID, KEY_FOUR_SIZE),
          legacyUpdateEvent(KEY_TWO, KEY_TWO_OBJECT_ID, KEY_TWO_SIZE,
              KEY_TWO_UPDATE_SIZE)),
          0L);

      assertTrue(nSSummaryTask.process(batch, Collections.emptyMap()).isTaskSuccess());
      assertEquals(2, getReconNamespaceSummaryManager()
          .getNSSummary(BUCKET_TWO_OBJECT_ID).getNumOfFiles());
      assertEquals(KEY_TWO_UPDATE_SIZE + KEY_FIVE_SIZE,
          getReconNamespaceSummaryManager()
              .getNSSummary(BUCKET_TWO_OBJECT_ID).getSizeOfFiles());
    }

    @Test
    public void testSequentialMixedBatches() throws IOException {
      ReconOmTask.TaskResult batch1 = nSSummaryTask.process(
          new OMUpdateEventBatch(Collections.singletonList(
              fsoDeleteEvent(KEY_ONE, KEY_ONE_OBJECT_ID, KEY_ONE_SIZE)), 0L),
          Collections.emptyMap());
      assertTrue(batch1.isTaskSuccess());

      ReconOmTask.TaskResult batch2 = nSSummaryTask.process(
          new OMUpdateEventBatch(Arrays.asList(
              legacyPutEvent(KEY_FIVE, KEY_FIVE_OBJECT_ID, KEY_FIVE_SIZE),
              obsPutEvent(KEY_FOUR, KEY_FOUR_OBJECT_ID, KEY_FOUR_SIZE)),
              0L),
          Collections.emptyMap());
      assertTrue(batch2.isTaskSuccess());

      assertEquals(0, getReconNamespaceSummaryManager()
          .getNSSummary(BUCKET_ONE_OBJECT_ID).getNumOfFiles());
      assertEquals(2, getReconNamespaceSummaryManager()
          .getNSSummary(BUCKET_TWO_OBJECT_ID).getNumOfFiles());
      assertEquals(2, getReconNamespaceSummaryManager()
          .getNSSummary(BUCKET_THREE_OBJECT_ID).getNumOfFiles());
      assertEquals(KEY_THREE_SIZE + KEY_FOUR_SIZE, getReconNamespaceSummaryManager()
          .getNSSummary(BUCKET_THREE_OBJECT_ID).getSizeOfFiles());
    }

    private OMDBUpdateEvent fsoDeleteEvent(String fileName, long objectId,
                                           long dataSize) {
      String key = BUCKET_ONE_OBJECT_ID + OM_KEY_PREFIX + fileName;
      OmKeyInfo keyInfo = buildOmKeyInfo(
          VOL, BUCKET_ONE, fileName, fileName, objectId, BUCKET_ONE_OBJECT_ID,
          dataSize);
      return new OMDBUpdateEvent.OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(key)
          .setValue(keyInfo)
          .setTable(getOmMetadataManager().getKeyTable(getFSOBucketLayout()).getName())
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
          .build();
    }

    private OMDBUpdateEvent legacyPutEvent(String keyName, long objectId,
                                           long dataSize) {
      String key = OM_KEY_PREFIX + VOL + OM_KEY_PREFIX + BUCKET_TWO
          + OM_KEY_PREFIX + keyName;
      OmKeyInfo keyInfo = buildOmKeyInfo(
          VOL, BUCKET_TWO, keyName, keyName, objectId, BUCKET_TWO_OBJECT_ID,
          dataSize);
      return new OMDBUpdateEvent.OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(key)
          .setValue(keyInfo)
          .setTable(getOmMetadataManager().getKeyTable(getLegacyBucketLayout()).getName())
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
          .build();
    }

    private OMDBUpdateEvent legacyUpdateEvent(String keyName, long objectId,
                                              long oldSize, long newSize) {
      String key = OM_KEY_PREFIX + VOL + OM_KEY_PREFIX + BUCKET_TWO
          + OM_KEY_PREFIX + keyName;
      OmKeyInfo oldInfo = buildOmKeyInfo(
          VOL, BUCKET_TWO, keyName, keyName, objectId, BUCKET_TWO_OBJECT_ID,
          oldSize);
      OmKeyInfo newInfo = buildOmKeyInfo(
          VOL, BUCKET_TWO, keyName, keyName, objectId, BUCKET_TWO_OBJECT_ID,
          newSize);
      return new OMDBUpdateEvent.OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(key)
          .setValue(newInfo)
          .setOldValue(oldInfo)
          .setTable(getOmMetadataManager().getKeyTable(getLegacyBucketLayout()).getName())
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.UPDATE)
          .build();
    }

    private OMDBUpdateEvent obsPutEvent(String keyName, long objectId,
                                        long dataSize) {
      String key = OM_KEY_PREFIX + VOL + OM_KEY_PREFIX + BUCKET_THREE
          + OM_KEY_PREFIX + keyName;
      OmKeyInfo keyInfo = buildOmKeyInfo(
          VOL, BUCKET_THREE, keyName, keyName, objectId, BUCKET_THREE_OBJECT_ID,
          dataSize);
      return new OMDBUpdateEvent.OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(key)
          .setValue(keyInfo)
          .setTable(getOmMetadataManager().getKeyTable(getOBSBucketLayout()).getName())
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
          .build();
    }

    private OMDBUpdateEvent obsDeleteEvent(String keyName, long objectId,
                                           long dataSize) {
      String key = OM_KEY_PREFIX + VOL + OM_KEY_PREFIX + BUCKET_THREE
          + OM_KEY_PREFIX + keyName;
      OmKeyInfo keyInfo = buildOmKeyInfo(
          VOL, BUCKET_THREE, keyName, keyName, objectId, BUCKET_THREE_OBJECT_ID,
          dataSize);
      return new OMDBUpdateEvent.OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(key)
          .setValue(keyInfo)
          .setTable(getOmMetadataManager().getKeyTable(getOBSBucketLayout()).getName())
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
          .build();
    }
  }

  /**
   * Regression test for the field-level OmBucketInfo cache in
   * {@link NSSummaryTaskDbEventHandler}. A bucket deleted and recreated under
   * the same volume/bucket name gets a new objectID but reuses the same DB key.
   * The cached entry must be invalidated when the bucketTable event is observed,
   * otherwise key events for the recreated bucket would be attributed to the
   * stale (old) bucket objectID.
   */
  @Nested
  public class TestProcessBucketRecreate {

    private static final String RECREATE_BUCKET = "bucketrecreate";
    private static final long OLD_BUCKET_OBJECT_ID = 100L;
    private static final long NEW_BUCKET_OBJECT_ID = 200L;
    private static final long FIRST_KEY_OBJECT_ID = 101L;
    private static final long SECOND_KEY_OBJECT_ID = 201L;
    private static final long RECREATE_KEY_SIZE = 1024L;

    @Test
    public void testBucketCacheInvalidatedOnRecreate() throws IOException {
      ReconOMMetadataManager reconMetadataMgr = getReconOMMetadataManager();
      String bucketKey = reconMetadataMgr.getBucketKey(VOL, RECREATE_BUCKET);

      // The bucket initially exists as an OBS bucket with the old objectID.
      reconMetadataMgr.getBucketTable()
          .put(bucketKey, buildObsBucketInfo(OLD_BUCKET_OBJECT_ID));

      // A fresh task starts with an empty bucket cache and exercises the full
      // parallel sub-task dispatch.
      NSSummaryTask task = new NSSummaryTask(getReconNamespaceSummaryManager(),
          reconMetadataMgr, getOmConfiguration());

      // Batch 1: a key lands in the original bucket. This warms the OBS
      // sub-task's bucket cache with the old objectID.
      OMDBUpdateEvent firstKeyPut =
          obsKeyPutEvent(bucketKey, KEY_ONE, FIRST_KEY_OBJECT_ID, OLD_BUCKET_OBJECT_ID);
      task.process(new OMUpdateEventBatch(
          Collections.singletonList(firstKeyPut), 0L), Collections.emptyMap());

      NSSummary oldBucketSummary =
          getReconNamespaceSummaryManager().getNSSummary(OLD_BUCKET_OBJECT_ID);
      assertNotNull(oldBucketSummary);
      assertEquals(1, oldBucketSummary.getNumOfFiles());
      assertEquals(RECREATE_KEY_SIZE, oldBucketSummary.getSizeOfFiles());

      // The bucket is deleted and recreated under the same name with a new
      // objectID (same DB key, different identity). The manual bucketTable put
      // below simulates OM DB state after the recreate: synthetic events in this
      // harness do not apply bucketTable writes to RocksDB (unlike production,
      // where Recon commits the OM batch before tasks run).
      reconMetadataMgr.getBucketTable()
          .put(bucketKey, buildObsBucketInfo(NEW_BUCKET_OBJECT_ID));

      // Batch 2 mirrors the real recreate sequence: the old bucket is deleted,
      // a new bucket is created under the same name, then a key lands in it.
      // The bucketTable delete event must invalidate the stale cache entry so
      // the key event re-reads the recreated bucket's objectID.
      OMDBUpdateEvent bucketDelete = new OMDBUpdateEvent
          .OMUpdateEventBuilder<String, OmBucketInfo>()
          .setKey(bucketKey)
          .setValue(buildObsBucketInfo(OLD_BUCKET_OBJECT_ID))
          .setTable(reconMetadataMgr.getBucketTable().getName())
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
          .build();
      OMDBUpdateEvent bucketPut = new OMDBUpdateEvent
          .OMUpdateEventBuilder<String, OmBucketInfo>()
          .setKey(bucketKey)
          .setValue(buildObsBucketInfo(NEW_BUCKET_OBJECT_ID))
          .setTable(reconMetadataMgr.getBucketTable().getName())
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
          .build();
      OMDBUpdateEvent secondKeyPut =
          obsKeyPutEvent(bucketKey, KEY_TWO, SECOND_KEY_OBJECT_ID, NEW_BUCKET_OBJECT_ID);
      task.process(new OMUpdateEventBatch(
          Arrays.asList(bucketDelete, bucketPut, secondKeyPut), 0L), Collections.emptyMap());

      // The new key must be attributed to the recreated bucket's objectID...
      NSSummary newBucketSummary =
          getReconNamespaceSummaryManager().getNSSummary(NEW_BUCKET_OBJECT_ID);
      assertNotNull(newBucketSummary);
      assertEquals(1, newBucketSummary.getNumOfFiles());
      assertEquals(RECREATE_KEY_SIZE, newBucketSummary.getSizeOfFiles());

      // ...and the stale (old) bucket must not have absorbed it.
      NSSummary staleBucketSummary =
          getReconNamespaceSummaryManager().getNSSummary(OLD_BUCKET_OBJECT_ID);
      assertEquals(1, staleBucketSummary.getNumOfFiles());
      assertEquals(RECREATE_KEY_SIZE, staleBucketSummary.getSizeOfFiles());
    }

    private OmBucketInfo buildObsBucketInfo(long objectId) {
      return OmBucketInfo.newBuilder()
          .setVolumeName(VOL)
          .setBucketName(RECREATE_BUCKET)
          .setObjectID(objectId)
          .setBucketLayout(getOBSBucketLayout())
          .build();
    }

    private OMDBUpdateEvent obsKeyPutEvent(String bucketKey, String keyName,
                                           long keyObjectId, long parentObjectId) {
      String omKey = bucketKey + OM_KEY_PREFIX + keyName;
      OmKeyInfo keyInfo = buildOmKeyInfo(VOL, RECREATE_BUCKET, keyName, keyName,
          keyObjectId, parentObjectId, RECREATE_KEY_SIZE);
      return new OMDBUpdateEvent.OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(omKey)
          .setValue(keyInfo)
          .setTable(getReconOMMetadataManager().getKeyTable(getOBSBucketLayout()).getName())
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
          .build();
    }
  }
}
