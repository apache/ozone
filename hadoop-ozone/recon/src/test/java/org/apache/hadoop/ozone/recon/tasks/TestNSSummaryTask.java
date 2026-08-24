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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
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
 * Test for NSSummaryTask. Create one bucket of each layout
 * and test process and reprocess. Currently, there is no
 * support for OBS buckets. Check that the NSSummary
 * for the OBS bucket is null.
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

    @BeforeEach
    public void setUp() throws IOException {
      nSSummaryTask.reprocess(getReconOMMetadataManager());
      nSSummaryTask.process(processEventBatch(), Collections.emptyMap());

      nsSummaryForBucket1 =
          getReconNamespaceSummaryManager().getNSSummary(BUCKET_ONE_OBJECT_ID);
      assertNotNull(nsSummaryForBucket1);
      nsSummaryForBucket2 =
          getReconNamespaceSummaryManager().getNSSummary(BUCKET_TWO_OBJECT_ID);
      assertNotNull(nsSummaryForBucket2);
      NSSummary nsSummaryForBucket3 = getReconNamespaceSummaryManager().getNSSummary(BUCKET_THREE_OBJECT_ID);
      assertNotNull(nsSummaryForBucket3);
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
                                      .setTable(getOmMetadataManager().getKeyTable(getLegacyBucketLayout())
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
                                      .setTable(getOmMetadataManager().getKeyTable(getFSOBucketLayout())
                                                    .getName())
                                      .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
                                      .build();

      return new OMUpdateEventBatch(Arrays.asList(keyEvent1, keyEvent2), 0L);
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
  }
}
