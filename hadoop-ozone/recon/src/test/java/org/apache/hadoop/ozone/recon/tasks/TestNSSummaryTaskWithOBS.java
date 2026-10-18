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
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
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
 * Unit test for NSSummaryTaskWithOBS.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestNSSummaryTaskWithOBS extends AbstractNSSummaryTaskTest {

  private NSSummaryTaskWithOBS nSSummaryTaskWithOBS;

  @BeforeAll
  void setUp(@TempDir File tmpDir) throws Exception {
    commonSetup(tmpDir, new OMConfigParameter(true,
        false, getBucketLayout(),
        OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD_DEFAULT,
        false,
        true,
        false));
    long threshold = getOmConfiguration().getLong(
        OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD,
        OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD_DEFAULT);
    nSSummaryTaskWithOBS = new NSSummaryTaskWithOBS(getReconNamespaceSummaryManager(),
        getReconOMMetadataManager(), threshold, 5, 20, 2000);
  }

  /**
   * Nested class for testing NSSummaryTaskWithOBS reprocess.
   */
  @Nested
  class TestReprocess {

    private NSSummary nsSummaryForBucket1;
    private NSSummary nsSummaryForBucket2;

    @BeforeEach
    public void setUp() throws IOException {
      List<NSSummary> result = commonSetUpTestReprocess(
          () -> nSSummaryTaskWithOBS.reprocessWithOBS(getReconOMMetadataManager()),
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
      assertEquals(3, nsSummaryForBucket1.getNumOfFiles());
      assertEquals(2, nsSummaryForBucket2.getNumOfFiles());

      assertEquals(KEY_ONE_SIZE + KEY_TWO_OLD_SIZE + KEY_THREE_SIZE,
          nsSummaryForBucket1.getSizeOfFiles());
      assertEquals(KEY_FOUR_SIZE + KEY_FIVE_SIZE,
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

      // Check for 1's and 0's in fileDistBucket1
      int[] expectedIndexes1 = {0, 1, 40};
      for (int index = 0; index < fileDistBucket1.length; index++) {
        if (contains(expectedIndexes1, index)) {
          assertEquals(1, fileDistBucket1[index]);
        } else {
          assertEquals(0, fileDistBucket1[index]);
        }
      }

      // Check for 1's and 0's in fileDistBucket2
      int[] expectedIndexes2 = {0, 2};
      for (int index = 0; index < fileDistBucket2.length; index++) {
        if (contains(expectedIndexes2, index)) {
          assertEquals(1, fileDistBucket2[index]);
        } else {
          assertEquals(0, fileDistBucket2[index]);
        }
      }
    }

  }

  /**
   * Nested class for testing NSSummaryTaskWithOBS process.
   */
  @Nested
  public class TestProcess {

    private NSSummary nsSummaryForBucket1;
    private NSSummary nsSummaryForBucket2;

    @BeforeEach
    public void setUp() throws IOException {
      // reinit Recon RocksDB's namespace CF.
      getReconNamespaceSummaryManager().clearNSSummaryTable();
      nSSummaryTaskWithOBS.reprocessWithOBS(getReconOMMetadataManager());
      nSSummaryTaskWithOBS.processWithOBS(processEventBatch(), 0);

      nsSummaryForBucket1 =
          getReconNamespaceSummaryManager().getNSSummary(BUCKET_ONE_OBJECT_ID);
      assertNotNull(nsSummaryForBucket1);
      nsSummaryForBucket2 =
          getReconNamespaceSummaryManager().getNSSummary(BUCKET_TWO_OBJECT_ID);
      assertNotNull(nsSummaryForBucket2);
    }

    private OMUpdateEventBatch processEventBatch() throws IOException {
      // Test PUT Event.
      // PUT Key6 in Bucket2.
      String omPutKey =
          OM_KEY_PREFIX + VOL
              + OM_KEY_PREFIX + BUCKET_TWO +
              OM_KEY_PREFIX + KEY_SIX;
      OmKeyInfo omPutKeyInfo = buildOmKeyInfo(VOL, BUCKET_TWO, KEY_SIX,
          KEY_SIX, KEY_SIX_OBJECT_ID, BUCKET_TWO_OBJECT_ID, KEY_SIX_SIZE);
      OMDBUpdateEvent keyEvent1 = new OMDBUpdateEvent.
                                          OMUpdateEventBuilder<String, OmKeyInfo>()
                                      .setKey(omPutKey)
                                      .setValue(omPutKeyInfo)
                                      .setTable(getOmMetadataManager().getKeyTable(getBucketLayout())
                                                    .getName())
                                      .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
                                      .build();
      // PUT Key7 in Bucket1.
      omPutKey =
          OM_KEY_PREFIX + VOL
              + OM_KEY_PREFIX + BUCKET_ONE +
              OM_KEY_PREFIX + KEY_SEVEN;
      omPutKeyInfo = buildOmKeyInfo(VOL, BUCKET_ONE, KEY_SEVEN,
          KEY_SEVEN, KEY_SEVEN_OBJECT_ID, BUCKET_ONE_OBJECT_ID, KEY_SEVEN_SIZE);
      OMDBUpdateEvent keyEvent2 = new OMDBUpdateEvent.
                                          OMUpdateEventBuilder<String, OmKeyInfo>()
                                      .setKey(omPutKey)
                                      .setValue(omPutKeyInfo)
                                      .setTable(getOmMetadataManager().getKeyTable(getBucketLayout())
                                                    .getName())
                                      .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
                                      .build();

      // Test DELETE Event.
      // Delete Key1 in Bucket1.
      String omDeleteKey =
          OM_KEY_PREFIX + VOL
              + OM_KEY_PREFIX + BUCKET_ONE +
              OM_KEY_PREFIX + KEY_ONE;
      OmKeyInfo omDeleteKeyInfo = buildOmKeyInfo(VOL, BUCKET_ONE, KEY_ONE,
          KEY_ONE, KEY_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID, KEY_ONE_SIZE);
      OMDBUpdateEvent keyEvent3 = new OMDBUpdateEvent.
                                          OMUpdateEventBuilder<String, OmKeyInfo>()
                                      .setKey(omDeleteKey)
                                      .setTable(getOmMetadataManager().getKeyTable(getBucketLayout())
                                                    .getName())
                                      .setValue(omDeleteKeyInfo)
                                      .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
                                      .build();

      // Test UPDATE Event.
      // Resize Key2 in Bucket1.
      String omResizeKey =
          OM_KEY_PREFIX + VOL
              + OM_KEY_PREFIX + BUCKET_ONE +
              OM_KEY_PREFIX + KEY_TWO;
      OmKeyInfo oldOmResizeKeyInfo =
          buildOmKeyInfo(VOL, BUCKET_ONE, KEY_TWO, KEY_TWO, KEY_TWO_OBJECT_ID,
              BUCKET_ONE_OBJECT_ID, KEY_TWO_OLD_SIZE);
      OmKeyInfo newOmResizeKeyInfo =
          buildOmKeyInfo(VOL, BUCKET_ONE, KEY_TWO, KEY_TWO, KEY_TWO_OBJECT_ID,
              BUCKET_ONE_OBJECT_ID, KEY_TWO_OLD_SIZE + 100);
      OMDBUpdateEvent keyEvent4 = new OMDBUpdateEvent.
                                          OMUpdateEventBuilder<String, OmKeyInfo>()
                                      .setKey(omResizeKey)
                                      .setOldValue(oldOmResizeKeyInfo)
                                      .setValue(newOmResizeKeyInfo)
                                      .setTable(getOmMetadataManager().getKeyTable(getBucketLayout())
                                                    .getName())
                                      .setAction(OMDBUpdateEvent.OMDBUpdateAction.UPDATE)
                                      .build();

      return new OMUpdateEventBatch(
          Arrays.asList(keyEvent1, keyEvent2, keyEvent3, keyEvent4), 0L);
    }

    @Test
    public void testProcessForCount() throws IOException {
      assertNotNull(nsSummaryForBucket1);
      assertEquals(3, nsSummaryForBucket1.getNumOfFiles());
      assertNotNull(nsSummaryForBucket2);
      assertEquals(3, nsSummaryForBucket2.getNumOfFiles());

      Set<Long> childDirBucket1 = nsSummaryForBucket1.getChildDir();
      assertEquals(0, childDirBucket1.size());
      Set<Long> childDirBucket2 = nsSummaryForBucket2.getChildDir();
      assertEquals(0, childDirBucket2.size());
    }

    @Test
    public void testProcessForSize() throws IOException {
      assertNotNull(nsSummaryForBucket1);
      assertEquals(
          KEY_THREE_SIZE + KEY_SEVEN_SIZE + KEY_TWO_OLD_SIZE + 100,
          nsSummaryForBucket1.getSizeOfFiles());
      assertNotNull(nsSummaryForBucket2);
      assertEquals(KEY_FOUR_SIZE + KEY_FIVE_SIZE + KEY_SIX_SIZE,
          nsSummaryForBucket2.getSizeOfFiles());
    }

    @Test
    public void testProcessFileBucketSize() {
      int[] fileDistBucket1 = nsSummaryForBucket1.getFileSizeBucket();
      int[] fileDistBucket2 = nsSummaryForBucket2.getFileSizeBucket();
      assertEquals(ReconConstants.NUM_OF_FILE_SIZE_BINS,
          fileDistBucket1.length);
      assertEquals(ReconConstants.NUM_OF_FILE_SIZE_BINS,
          fileDistBucket2.length);

      // Check for 1's and 0's in fileDistBucket1
      int[] expectedIndexes1 = {1, 3, 40};
      for (int index = 0; index < fileDistBucket1.length; index++) {
        if (contains(expectedIndexes1, index)) {
          assertEquals(1, fileDistBucket1[index]);
        } else {
          assertEquals(0, fileDistBucket1[index]);
        }
      }

      // Check for 1's and 0's in fileDistBucket2
      int[] expectedIndexes2 = {0, 2, 3};
      for (int index = 0; index < fileDistBucket2.length; index++) {
        if (contains(expectedIndexes2, index)) {
          assertEquals(1, fileDistBucket2[index]);
        } else {
          assertEquals(0, fileDistBucket2[index]);
        }
      }
    }

  }

  private static BucketLayout getBucketLayout() {
    return BucketLayout.OBJECT_STORE;
  }
}
