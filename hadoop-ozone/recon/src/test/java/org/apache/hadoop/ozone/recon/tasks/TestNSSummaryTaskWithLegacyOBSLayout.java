/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.recon.tasks;

import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeKeyToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getMockOzoneManagerServiceProviderWithFSO;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Test for NSSummaryTaskWithLegacy focusing on the OBS (Object Store) layout.
 */
public final class TestNSSummaryTaskWithLegacyOBSLayout {

  private static ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private static ReconOMMetadataManager reconOMMetadataManager;
  private static OzoneConfiguration ozoneConfiguration;
  private static NSSummaryTaskWithLegacy nSSummaryTaskWithLegacy;

  private static OMMetadataManager omMetadataManager;
  private static OzoneConfiguration omConfiguration;

  // Object names
  private static final String VOL = "vol";
  private static final String BUCKET_ONE = "bucket1";
  private static final String BUCKET_TWO = "bucket2";
  private static final String KEY_ONE = "key1";
  private static final String KEY_TWO = "key2";
  private static final String KEY_THREE = "dir1/dir2/key3";
  private static final String KEY_FOUR = "key4///////////";
  private static final String KEY_FIVE = "//////////";
  private static final String KEY_SIX = "key6";
  private static final String KEY_SEVEN = "/////key7";

  private static final String TEST_USER = "TestUser";

  private static final long PARENT_OBJECT_ID_ZERO = 0L;
  private static final long VOL_OBJECT_ID = 0L;
  private static final long BUCKET_ONE_OBJECT_ID = 1L;
  private static final long BUCKET_TWO_OBJECT_ID = 2L;
  private static final long KEY_ONE_OBJECT_ID = 3L;
  private static final long KEY_TWO_OBJECT_ID = 5L;
  private static final long KEY_FOUR_OBJECT_ID = 6L;
  private static final long KEY_THREE_OBJECT_ID = 8L;
  private static final long KEY_FIVE_OBJECT_ID = 9L;
  private static final long KEY_SIX_OBJECT_ID = 10L;
  private static final long KEY_SEVEN_OBJECT_ID = 11L;


  private static final long KEY_ONE_SIZE = 500L;
  private static final long KEY_TWO_OLD_SIZE = 1025L;
  private static final long KEY_TWO_UPDATE_SIZE = 1023L;
  private static final long KEY_THREE_SIZE =
      ReconConstants.MAX_FILE_SIZE_UPPER_BOUND - 100L;
  private static final long KEY_FOUR_SIZE = 2050L;
  private static final long KEY_FIVE_SIZE = 100L;
  private static final long KEY_SIX_SIZE = 6000L;
  private static final long KEY_SEVEN_SIZE = 7000L;

  private TestNSSummaryTaskWithLegacyOBSLayout() {
  }

  @BeforeAll
  public static void setUp(@TempDir File tmpDir) throws Exception {
    initializeNewOmMetadataManager(new File(tmpDir, "om"));
    OzoneManagerServiceProviderImpl ozoneManagerServiceProvider =
        getMockOzoneManagerServiceProviderWithFSO();
    reconOMMetadataManager = getTestReconOmMetadataManager(omMetadataManager,
        new File(tmpDir, "recon"));
    ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS,
        false);

    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(tmpDir)
            .withReconOm(reconOMMetadataManager)
            .withOmServiceProvider(ozoneManagerServiceProvider)
            .withReconSqlDb()
            .withContainerDB()
            .build();
    reconNamespaceSummaryManager =
        reconTestInjector.getInstance(ReconNamespaceSummaryManager.class);

    NSSummary nonExistentSummary =
        reconNamespaceSummaryManager.getNSSummary(BUCKET_ONE_OBJECT_ID);
    assertNull(nonExistentSummary);

    populateOMDB();

    nSSummaryTaskWithLegacy = new NSSummaryTaskWithLegacy(
        reconNamespaceSummaryManager,
        reconOMMetadataManager, ozoneConfiguration);
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
      // write a NSSummary prior to reprocess
      // verify it got cleaned up after.
      NSSummary staleNSSummary = new NSSummary();
      RDBBatchOperation rdbBatchOperation = new RDBBatchOperation();
      reconNamespaceSummaryManager.batchStoreNSSummaries(rdbBatchOperation, -1L,
          staleNSSummary);
      reconNamespaceSummaryManager.commitBatchOperation(rdbBatchOperation);

      // Verify commit
      assertNotNull(reconNamespaceSummaryManager.getNSSummary(-1L));

      // reinit Recon RocksDB's namespace CF.
      reconNamespaceSummaryManager.clearNSSummaryTable();

      nSSummaryTaskWithLegacy.reprocessWithLegacy(reconOMMetadataManager);
      assertNull(reconNamespaceSummaryManager.getNSSummary(-1L));

      nsSummaryForBucket1 =
          reconNamespaceSummaryManager.getNSSummary(BUCKET_ONE_OBJECT_ID);
      nsSummaryForBucket2 =
          reconNamespaceSummaryManager.getNSSummary(BUCKET_TWO_OBJECT_ID);
      assertNotNull(nsSummaryForBucket1);
      assertNotNull(nsSummaryForBucket2);
    }

    @Test
    public void testReprocessNSSummaryNull() throws IOException {
      assertNull(reconNamespaceSummaryManager.getNSSummary(-1L));
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
   * Nested class for testing NSSummaryTaskWithLegacy process.
   */
  @Nested
  public class TestProcess {

    private NSSummary nsSummaryForBucket1;
    private NSSummary nsSummaryForBucket2;

    private OMDBUpdateEvent keyEvent1;
    private OMDBUpdateEvent keyEvent2;
    private OMDBUpdateEvent keyEvent3;
    private OMDBUpdateEvent keyEvent4;

    @BeforeEach
    public void setUp() throws IOException {
      // reinit Recon RocksDB's namespace CF.
      reconNamespaceSummaryManager.clearNSSummaryTable();
      nSSummaryTaskWithLegacy.reprocessWithLegacy(reconOMMetadataManager);
      nSSummaryTaskWithLegacy.processWithLegacy(processEventBatch());

      nsSummaryForBucket1 =
          reconNamespaceSummaryManager.getNSSummary(BUCKET_ONE_OBJECT_ID);
      assertNotNull(nsSummaryForBucket1);
      nsSummaryForBucket2 =
          reconNamespaceSummaryManager.getNSSummary(BUCKET_TWO_OBJECT_ID);
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
      keyEvent1 = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(omPutKey)
          .setValue(omPutKeyInfo)
          .setTable(omMetadataManager.getKeyTable(getBucketLayout())
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
      keyEvent2 = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(omPutKey)
          .setValue(omPutKeyInfo)
          .setTable(omMetadataManager.getKeyTable(getBucketLayout())
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
      keyEvent3 = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(omDeleteKey)
          .setTable(omMetadataManager.getKeyTable(getBucketLayout())
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
      keyEvent4 = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(omResizeKey)
          .setOldValue(oldOmResizeKeyInfo)
          .setValue(newOmResizeKeyInfo)
          .setTable(omMetadataManager.getKeyTable(getBucketLayout())
              .getName())
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.UPDATE)
          .build();

      return new OMUpdateEventBatch(
          Arrays.asList(keyEvent1, keyEvent2, keyEvent3, keyEvent4));
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

  /**
   * Populate OMDB with the following configs.
   *         vol
   *      /       \
   * bucket1     bucket2
   * /    \   \        \  \
   * key1  key2   key3   key4 key5
   *
   * @throws IOException
   */
  private static void populateOMDB() throws IOException {
    writeKeyToOm(reconOMMetadataManager,
        KEY_ONE,
        BUCKET_ONE,
        VOL,
        KEY_ONE,
        KEY_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        KEY_ONE_SIZE,
        getBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
        KEY_TWO,
        BUCKET_ONE,
        VOL,
        KEY_TWO,
        KEY_TWO_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        KEY_TWO_OLD_SIZE,
        getBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
        KEY_THREE,
        BUCKET_ONE,
        VOL,
        KEY_THREE,
        KEY_THREE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        KEY_THREE_SIZE,
        getBucketLayout());

    writeKeyToOm(reconOMMetadataManager,
        KEY_FOUR,
        BUCKET_TWO,
        VOL,
        KEY_FOUR,
        KEY_FOUR_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID,
        VOL_OBJECT_ID,
        KEY_FOUR_SIZE,
        getBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
        KEY_FIVE,
        BUCKET_TWO,
        VOL,
        KEY_FIVE,
        KEY_FIVE_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_TWO_OBJECT_ID,
        VOL_OBJECT_ID,
        KEY_FIVE_SIZE,
        getBucketLayout());
  }

  /**
   * Create a new OM Metadata manager instance with one user, one vol, and two
   * buckets.
   *
   * @throws IOException ioEx
   */
  private static void initializeNewOmMetadataManager(
      File omDbDir)
      throws IOException {
    omConfiguration = new OzoneConfiguration();
    omConfiguration.set(OZONE_OM_DB_DIRS,
        omDbDir.getAbsolutePath());
    omConfiguration.set(OMConfigKeys
        .OZONE_OM_ENABLE_FILESYSTEM_PATHS, "true");
    omMetadataManager = new OmMetadataManagerImpl(
        omConfiguration, null);

    String volumeKey = omMetadataManager.getVolumeKey(VOL);
    OmVolumeArgs args =
        OmVolumeArgs.newBuilder()
            .setObjectID(VOL_OBJECT_ID)
            .setVolume(VOL)
            .setAdminName(TEST_USER)
            .setOwnerName(TEST_USER)
            .build();
    omMetadataManager.getVolumeTable().put(volumeKey, args);

    OmBucketInfo bucketInfo1 = OmBucketInfo.newBuilder()
        .setVolumeName(VOL)
        .setBucketName(BUCKET_ONE)
        .setObjectID(BUCKET_ONE_OBJECT_ID)
        .setBucketLayout(getBucketLayout())
        .build();

    OmBucketInfo bucketInfo2 = OmBucketInfo.newBuilder()
        .setVolumeName(VOL)
        .setBucketName(BUCKET_TWO)
        .setObjectID(BUCKET_TWO_OBJECT_ID)
        .setBucketLayout(getBucketLayout())
        .build();

    String bucketKey = omMetadataManager.getBucketKey(
        bucketInfo1.getVolumeName(), bucketInfo1.getBucketName());
    String bucketKey2 = omMetadataManager.getBucketKey(
        bucketInfo2.getVolumeName(), bucketInfo2.getBucketName());

    omMetadataManager.getBucketTable().put(bucketKey, bucketInfo1);
    omMetadataManager.getBucketTable().put(bucketKey2, bucketInfo2);
  }

  /**
   * Build a key info for put/update action.
   *
   * @param volume         volume name
   * @param bucket         bucket name
   * @param key            key name
   * @param fileName       file name
   * @param objectID       object ID
   * @param parentObjectId parent object ID
   * @param dataSize       file size
   * @return the KeyInfo
   */
  private static OmKeyInfo buildOmKeyInfo(String volume,
                                          String bucket,
                                          String key,
                                          String fileName,
                                          long objectID,
                                          long parentObjectId,
                                          long dataSize) {
    return new OmKeyInfo.Builder()
        .setBucketName(bucket)
        .setVolumeName(volume)
        .setKeyName(key)
        .setFileName(fileName)
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.ONE))
        .setObjectID(objectID)
        .setParentObjectID(parentObjectId)
        .setDataSize(dataSize)
        .build();
  }

  // Helper method to check if an array contains a specific value
  private boolean contains(int[] arr, int value) {
    for (int num : arr) {
      if (num == value) {
        return true;
      }
    }
    return false;
  }

  private static BucketLayout getBucketLayout() {
    return BucketLayout.LEGACY;
  }
}
