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
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test for NSSummaryTaskWithLegacy.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestNSSummaryTaskWithLegacy extends AbstractNSSummaryTaskTest {

  private NSSummaryTaskWithLegacy nSSummaryTaskWithLegacy;
  // Answer Sets
  private static Set<Long> bucketOneAns = new HashSet<>();
  private static Set<Long> bucketTwoAns = new HashSet<>();
  private static Set<Long> dirOneAns = new HashSet<>();

  @BeforeAll
  void setUp(@TempDir File tmpDir) throws Exception {
    commonSetup(tmpDir,
          new OMConfigParameter(false,
          false,
          getBucketLayout(),
          10,
          false,
          true,
          true));
    long threshold = getOmConfiguration().getLong(OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD, 10);
    nSSummaryTaskWithLegacy =
        new NSSummaryTaskWithLegacy(getReconNamespaceSummaryManager(), getReconOMMetadataManager(),
            getOmConfiguration(), threshold);
  }

  /**
   * Nested class for testing NSSummaryTaskWithLegacy reprocess.
   */
  @Nested
  public class TestReprocess {

    private NSSummary nsSummaryForBucket1;
    private NSSummary nsSummaryForBucket2;

    @BeforeEach
    public void setUp() throws IOException {
      List<NSSummary> result =
          commonSetUpTestReprocess(() -> nSSummaryTaskWithLegacy.reprocessWithLegacy(getReconOMMetadataManager()),
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
      assertEquals(DIR_ONE + OM_KEY_PREFIX + DIR_TWO,
          nsSummaryInDir2.getDirName());
    }
  }

  /**
   * Nested class for testing NSSummaryTaskWithLegacy process.
   */
  @Nested
  public class TestProcess {

    private NSSummary nsSummaryForBucket1;
    private NSSummary nsSummaryForBucket2;

    @BeforeEach
    public void setUp() throws IOException {
      getReconNamespaceSummaryManager().clearNSSummaryTable();
      nSSummaryTaskWithLegacy.reprocessWithLegacy(getReconOMMetadataManager());
      nSSummaryTaskWithLegacy.processWithLegacy(processEventBatch(), 0);

      nsSummaryForBucket1 =
          getReconNamespaceSummaryManager().getNSSummary(BUCKET_ONE_OBJECT_ID);
      assertNotNull(nsSummaryForBucket1);
      nsSummaryForBucket2 =
          getReconNamespaceSummaryManager().getNSSummary(BUCKET_TWO_OBJECT_ID);
      assertNotNull(nsSummaryForBucket2);
    }

    private OMUpdateEventBatch processEventBatch() throws IOException {
      // put file5 under bucket 2
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
                                      .setTable(getOmMetadataManager().getKeyTable(getBucketLayout())
                                                    .getName())
                                      .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
                                      .build();

      // delete file 1 under bucket 1
      String omDeleteKey =
          OM_KEY_PREFIX + VOL +
              OM_KEY_PREFIX + BUCKET_ONE +
              OM_KEY_PREFIX + FILE_ONE;
      OmKeyInfo omDeleteInfo = buildOmKeyInfo(
          VOL, BUCKET_ONE, KEY_ONE,
          FILE_ONE, KEY_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID);
      OMDBUpdateEvent keyEvent2 = new OMDBUpdateEvent.
                                          OMUpdateEventBuilder<String, OmKeyInfo>()
                                      .setKey(omDeleteKey)
                                      .setValue(omDeleteInfo)
                                      .setTable(getOmMetadataManager().getKeyTable(getBucketLayout())
                                                    .getName())
                                      .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
                                      .build();

      // update file 2's size under bucket 2
      String omUpdateKey =
          OM_KEY_PREFIX + VOL +
              OM_KEY_PREFIX + BUCKET_TWO +
              OM_KEY_PREFIX + FILE_TWO;
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

      // add dir 4 under bucket 1
      String omDirPutKey1 =
          OM_KEY_PREFIX + VOL +
              OM_KEY_PREFIX + BUCKET_ONE +
              OM_KEY_PREFIX + DIR_FOUR + OM_KEY_PREFIX;
      OmKeyInfo omDirPutValue1 = buildOmDirKeyInfo(VOL, BUCKET_ONE,
          (DIR_FOUR + OM_KEY_PREFIX), DIR_FOUR,
          DIR_FOUR_OBJECT_ID);
      OMDBUpdateEvent keyEvent4 = new OMDBUpdateEvent.
                                          OMUpdateEventBuilder<String, OmKeyInfo>()
                                      .setKey(omDirPutKey1)
                                      .setValue(omDirPutValue1)
                                      .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
                                      .setTable(getOmMetadataManager().getKeyTable(getBucketLayout()).getName())
                                      .build();

      // add dir 5 under bucket 2
      String omDirPutKey2 =
          OM_KEY_PREFIX + VOL +
              OM_KEY_PREFIX + BUCKET_TWO +
              OM_KEY_PREFIX + DIR_FIVE + OM_KEY_PREFIX;
      OmKeyInfo omDirPutValue2 = buildOmDirKeyInfo(VOL, BUCKET_TWO,
          (DIR_FIVE + OM_KEY_PREFIX), DIR_FIVE,
          DIR_FIVE_OBJECT_ID);
      OMDBUpdateEvent keyEvent5 = new OMDBUpdateEvent.
                                          OMUpdateEventBuilder<String, OmKeyInfo>()
                                      .setKey(omDirPutKey2)
                                      .setValue(omDirPutValue2)
                                      .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
                                      .setTable(getOmMetadataManager().getKeyTable(getBucketLayout()).getName())
                                      .build();

      // delete dir 3 under dir 1
      String omDirDeleteKey =
          OM_KEY_PREFIX + VOL +
              OM_KEY_PREFIX + BUCKET_ONE +
              OM_KEY_PREFIX + DIR_ONE +
              OM_KEY_PREFIX + DIR_THREE + OM_KEY_PREFIX;
      OmKeyInfo omDirDeleteValue = buildOmKeyInfo(VOL, BUCKET_ONE,
          (DIR_ONE + OM_KEY_PREFIX + DIR_THREE + OM_KEY_PREFIX),
          DIR_THREE, DIR_THREE_OBJECT_ID, DIR_ONE_OBJECT_ID);
      OMDBUpdateEvent keyEvent6 = new OMDBUpdateEvent.
                                          OMUpdateEventBuilder<String, OmKeyInfo>()
                                      .setKey(omDirDeleteKey)
                                      .setValue(omDirDeleteValue)
                                      .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
                                      .setTable(getOmMetadataManager().getKeyTable(getBucketLayout()).getName())
                                      .build();

      // rename dir1
      String omDirUpdateKey =
          OM_KEY_PREFIX + VOL +
              OM_KEY_PREFIX + BUCKET_ONE +
              OM_KEY_PREFIX + DIR_ONE + OM_KEY_PREFIX;
      OmKeyInfo omDirOldValue = buildOmDirKeyInfo(VOL, BUCKET_ONE,
          (DIR_ONE + OM_KEY_PREFIX), DIR_ONE,
          DIR_ONE_OBJECT_ID);
      OmKeyInfo omDirUpdateValue = buildOmDirKeyInfo(VOL, BUCKET_ONE,
          (DIR_ONE_RENAME + OM_KEY_PREFIX), DIR_ONE_RENAME,
          DIR_ONE_OBJECT_ID);
      OMDBUpdateEvent keyEvent7 = new OMDBUpdateEvent.
                                          OMUpdateEventBuilder<String, OmKeyInfo>()
                                      .setKey(omDirUpdateKey)
                                      .setValue(omDirUpdateValue)
                                      .setOldValue(omDirOldValue)
                                      .setAction(OMDBUpdateEvent.OMDBUpdateAction.UPDATE)
                                      .setTable(getOmMetadataManager().getKeyTable(getBucketLayout()).getName())
                                      .build();

      return new OMUpdateEventBatch(Arrays.asList(
          keyEvent1, keyEvent2, keyEvent3, keyEvent4, keyEvent5,
          keyEvent6, keyEvent7
      ), 0L);
    }

    @Test
    public void testProcessUpdateFileSize() throws IOException {
      // file 1 is gone, so bucket 1 is empty now
      assertNotNull(nsSummaryForBucket1);
      assertEquals(2, nsSummaryForBucket1.getNumOfFiles());

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
  }

  private static BucketLayout getBucketLayout() {
    return BucketLayout.LEGACY;
  }
}
