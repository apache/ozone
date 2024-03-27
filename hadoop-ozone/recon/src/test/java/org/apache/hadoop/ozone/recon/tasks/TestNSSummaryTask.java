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
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getMockOzoneManagerServiceProvider;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test for NSSummaryTask. Create one bucket of each layout
 * and test process and reprocess. Currently, there is no
 * support for OBS buckets. Check that the NSSummary
 * for the OBS bucket is null.
 */
public final class TestNSSummaryTask {

  private static ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private static OMMetadataManager omMetadataManager;
  private static ReconOMMetadataManager reconOMMetadataManager;
  private static NSSummaryTask nSSummaryTask;
  private static OzoneConfiguration omConfiguration;

  // Object names
  private static final String VOL = "vol";
  private static final String BUCKET_ONE = "bucket1";
  private static final String BUCKET_TWO = "bucket2";
  private static final String BUCKET_THREE = "bucket3";
  private static final String KEY_ONE = "file1";
  private static final String KEY_TWO = "file2";
  private static final String KEY_THREE = "file3";
  private static final String KEY_FIVE = "file5";
  private static final String FILE_ONE = "file1";
  private static final String FILE_TWO = "file2";
  private static final String FILE_THREE = "file3";
  private static final String FILE_FIVE = "file5";

  private static final String TEST_USER = "TestUser";

  private static final long PARENT_OBJECT_ID_ZERO = 0L;
  private static final long VOL_OBJECT_ID = 0L;
  private static final long BUCKET_ONE_OBJECT_ID = 1L;
  private static final long BUCKET_TWO_OBJECT_ID = 2L;
  private static final long BUCKET_THREE_OBJECT_ID = 4L;
  private static final long KEY_ONE_OBJECT_ID = 3L;
  private static final long KEY_TWO_OBJECT_ID = 5L;
  private static final long KEY_THREE_OBJECT_ID = 8L;
  private static final long KEY_FIVE_OBJECT_ID = 9L;

  private static final long KEY_ONE_SIZE = 500L;
  private static final long KEY_TWO_SIZE = 1025L;
  private static final long KEY_THREE_SIZE =
      ReconConstants.MAX_FILE_SIZE_UPPER_BOUND - 100L;
  private static final long KEY_FIVE_SIZE = 100L;

  private TestNSSummaryTask() {
  }

  @BeforeAll
  public static void setUp(@TempDir File tmpDir) throws Exception {
    initializeNewOmMetadataManager(new File(tmpDir, "om"));
    OzoneManagerServiceProviderImpl ozoneManagerServiceProvider =
        getMockOzoneManagerServiceProvider();
    reconOMMetadataManager = getTestReconOmMetadataManager(omMetadataManager,
        new File(tmpDir, "recon"));

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

    nSSummaryTask = new NSSummaryTask(reconNamespaceSummaryManager,
        reconOMMetadataManager, omConfiguration);
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
      // write a NSSummary prior to reprocess
      // verify it got cleaned up after.
      NSSummary staleNSSummary = new NSSummary();
      RDBBatchOperation rdbBatchOperation = new RDBBatchOperation();
      reconNamespaceSummaryManager.batchStoreNSSummaries(rdbBatchOperation, -1L,
          staleNSSummary);
      reconNamespaceSummaryManager.commitBatchOperation(rdbBatchOperation);

      // Verify commit
      assertNotNull(reconNamespaceSummaryManager.getNSSummary(-1L));

      nSSummaryTask.reprocess(reconOMMetadataManager);
      assertNull(reconNamespaceSummaryManager.getNSSummary(-1L));

      nsSummaryForBucket1 =
          reconNamespaceSummaryManager.getNSSummary(BUCKET_ONE_OBJECT_ID);
      nsSummaryForBucket2 =
          reconNamespaceSummaryManager.getNSSummary(BUCKET_TWO_OBJECT_ID);
      nsSummaryForBucket3 =
      reconNamespaceSummaryManager.getNSSummary(BUCKET_THREE_OBJECT_ID);
      assertNotNull(nsSummaryForBucket1);
      assertNotNull(nsSummaryForBucket2);
      assertNotNull(nsSummaryForBucket3);
    }

    @Test
    public void testReprocessNSSummaryNull() throws IOException {
      assertNull(reconNamespaceSummaryManager.getNSSummary(-1L));
    }

    @Test
    public void testReprocessGetFiles() {
      assertEquals(1, nsSummaryForBucket1.getNumOfFiles());
      assertEquals(1, nsSummaryForBucket2.getNumOfFiles());

      assertEquals(KEY_ONE_SIZE, nsSummaryForBucket1.getSizeOfFiles());
      assertEquals(KEY_TWO_SIZE, nsSummaryForBucket2.getSizeOfFiles());
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

    private OMDBUpdateEvent keyEvent1;
    private OMDBUpdateEvent keyEvent2;

    @BeforeEach
    public void setUp() throws IOException {
      nSSummaryTask.reprocess(reconOMMetadataManager);
      nSSummaryTask.process(processEventBatch());

      nsSummaryForBucket1 =
          reconNamespaceSummaryManager.getNSSummary(BUCKET_ONE_OBJECT_ID);
      assertNotNull(nsSummaryForBucket1);
      nsSummaryForBucket2 =
          reconNamespaceSummaryManager.getNSSummary(BUCKET_TWO_OBJECT_ID);
      assertNotNull(nsSummaryForBucket2);
      nsSummaryForBucket3 =
          reconNamespaceSummaryManager.getNSSummary(BUCKET_THREE_OBJECT_ID);
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
      keyEvent1 = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(omPutKey)
          .setValue(omPutKeyInfo)
          .setTable(omMetadataManager.getKeyTable(getLegacyBucketLayout())
              .getName())
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
          .build();

      // delete file 1 under bucket 1
      String omDeleteKey = BUCKET_ONE_OBJECT_ID + OM_KEY_PREFIX + FILE_ONE;
      OmKeyInfo omDeleteInfo = buildOmKeyInfo(
          VOL, BUCKET_ONE, KEY_ONE, FILE_ONE,
          KEY_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID);
      keyEvent2 = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(omDeleteKey)
          .setValue(omDeleteInfo)
          .setTable(omMetadataManager.getKeyTable(getFSOBucketLayout())
              .getName())
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
          .build();

      return new OMUpdateEventBatch(Arrays.asList(keyEvent1, keyEvent2));
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

  /**
   * Build a key info for put/update action.
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

  /**
   * Build a key info for delete action.
   * @param volume         volume name
   * @param bucket         bucket name
   * @param key            key name
   * @param fileName       file name
   * @param objectID       object ID
   * @param parentObjectId parent object ID
   * @return the KeyInfo
   */
  private static OmKeyInfo buildOmKeyInfo(String volume,
                                          String bucket,
                                          String key,
                                          String fileName,
                                          long objectID,
                                          long parentObjectId) {
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
        .build();
  }

  /**
   * Populate OMDB with the following configs.
   *             vol
   *      /       \       \
   * bucket1   bucket2    bucket3
   *    /        /        /
   * file1    file2     file3
   *
   * @throws IOException
   */
  private static void populateOMDB() throws IOException {
    // Bucket1 FSO layout
    writeKeyToOm(reconOMMetadataManager,
        KEY_ONE,
        BUCKET_ONE,
        VOL,
        FILE_ONE,
        KEY_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        KEY_ONE_SIZE,
        getFSOBucketLayout());

    // Bucket2 Legacy layout
    writeKeyToOm(reconOMMetadataManager,
        KEY_TWO,
        BUCKET_TWO,
        VOL,
        FILE_TWO,
        KEY_TWO_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_TWO_OBJECT_ID,
        VOL_OBJECT_ID,
          KEY_TWO_SIZE,
        getLegacyBucketLayout());

    // Bucket3 OBS layout
    writeKeyToOm(reconOMMetadataManager,
        KEY_THREE,
        BUCKET_THREE,
        VOL,
        FILE_THREE,
        KEY_THREE_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_THREE_OBJECT_ID,
        VOL_OBJECT_ID,
        KEY_THREE_SIZE,
        getOBSBucketLayout());
  }

  /**
   * Create a new OM Metadata manager instance with one user, one vol, and two
   * buckets. Bucket1 will have FSO layout, bucket2 will have Legacy layout
   * and bucket3 will have OBS layout.
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
        .setBucketLayout(getFSOBucketLayout())
        .build();

    OmBucketInfo bucketInfo2 = OmBucketInfo.newBuilder()
        .setVolumeName(VOL)
        .setBucketName(BUCKET_TWO)
        .setObjectID(BUCKET_TWO_OBJECT_ID)
        .setBucketLayout(getLegacyBucketLayout())
        .build();

    OmBucketInfo bucketInfo3 = OmBucketInfo.newBuilder()
        .setVolumeName(VOL)
        .setBucketName(BUCKET_THREE)
        .setObjectID(BUCKET_THREE_OBJECT_ID)
        .setBucketLayout(getOBSBucketLayout())
        .build();

    String bucketKey = omMetadataManager.getBucketKey(
        bucketInfo1.getVolumeName(), bucketInfo1.getBucketName());
    String bucketKey2 = omMetadataManager.getBucketKey(
        bucketInfo2.getVolumeName(), bucketInfo2.getBucketName());
    String bucketKey3 = omMetadataManager.getBucketKey(
        bucketInfo3.getVolumeName(), bucketInfo3.getBucketName());

    omMetadataManager.getBucketTable().put(bucketKey, bucketInfo1);
    omMetadataManager.getBucketTable().put(bucketKey2, bucketInfo2);
    omMetadataManager.getBucketTable().put(bucketKey3, bucketInfo3);
  }

  private static BucketLayout getFSOBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  private static BucketLayout getLegacyBucketLayout() {
    return BucketLayout.LEGACY;
  }

  private static BucketLayout getOBSBucketLayout() {
    return BucketLayout.OBJECT_STORE;
  }
}
