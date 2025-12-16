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
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getMockOzoneManagerServiceProvider;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getMockOzoneManagerServiceProviderWithFSO;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDirToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeKeyToOm;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;

/**
 * Abstract Class created to handle common objects and methods.
 */
public abstract class AbstractNSSummaryTaskTest {
  // User and Volume Constants
  protected static final String TEST_USER = "TestUser";
  protected static final String VOL = "vol";
  protected static final long PARENT_OBJECT_ID_ZERO = 0L;
  protected static final long VOL_OBJECT_ID = 0L;
  // Bucket Constants
  protected static final String BUCKET_ONE = "bucket1";
  protected static final String BUCKET_TWO = "bucket2";
  protected static final String BUCKET_THREE = "bucket3";
  protected static final long BUCKET_ONE_OBJECT_ID = 1L;
  protected static final long BUCKET_TWO_OBJECT_ID = 2L;
  protected static final long BUCKET_THREE_OBJECT_ID = 4L;
  // File/Key Constants
  protected static final String KEY_ONE = "file1";
  protected static final String KEY_TWO = "file2";
  protected static final String KEY_THREE = "file3";
  protected static final String KEY_FOUR = "file4";
  protected static final String KEY_FIVE = "file5";
  protected static final String KEY_SIX = "key6";
  protected static final String KEY_SEVEN = "/////key7";
  protected static final String KEY_THREE_1 = "dir1/dir2/file3";
  protected static final String FILE_ONE = "file1";
  protected static final String FILE_TWO = "file2";
  protected static final String FILE_THREE = "file3";
  protected static final String FILE_FOUR = "file4";
  protected static final String FILE_FIVE = "file5";
  protected static final long KEY_ONE_OBJECT_ID = 3L;
  protected static final long KEY_TWO_OBJECT_ID = 5L;
  protected static final long KEY_THREE_OBJECT_ID = 8L;
  protected static final long KEY_FOUR_OBJECT_ID = 6L;
  protected static final long KEY_FIVE_OBJECT_ID = 9L;
  protected static final long KEY_SIX_OBJECT_ID = 10L;
  protected static final long KEY_SEVEN_OBJECT_ID = 11L;
  protected static final long KEY_ONE_SIZE = 500L;
  protected static final long KEY_TWO_SIZE = 1025L;
  protected static final long KEY_TWO_OLD_SIZE = 1025L;
  protected static final long KEY_TWO_UPDATE_SIZE = 1023L;
  protected static final long KEY_THREE_SIZE = ReconConstants.MAX_FILE_SIZE_UPPER_BOUND - 100L;
  protected static final long KEY_FOUR_SIZE = 2050L;
  protected static final long KEY_FIVE_SIZE = 100L;
  protected static final long KEY_SIX_SIZE = 6000L;
  protected static final long KEY_SEVEN_SIZE = 7000L;
  // Directory Constants
  protected static final String DIR_ONE = "dir1";
  protected static final String DIR_ONE_RENAME = "dir1_new";
  protected static final String DIR_TWO = "dir2";
  protected static final String DIR_THREE = "dir3";
  protected static final String DIR_FOUR = "dir4";
  protected static final String DIR_FIVE = "dir5";
  protected static final long DIR_ONE_OBJECT_ID = 4L;
  protected static final long DIR_TWO_OBJECT_ID = 7L;
  protected static final long DIR_THREE_OBJECT_ID = 10L;
  protected static final long DIR_FOUR_OBJECT_ID = 11L;
  protected static final long DIR_FIVE_OBJECT_ID = 12L;

  private OzoneConfiguration ozoneConfiguration;
  private OzoneConfiguration omConfiguration;
  private OMMetadataManager omMetadataManager;
  private ReconOMMetadataManager reconOMMetadataManager;
  private ReconNamespaceSummaryManager reconNamespaceSummaryManager;

  // Helper Methods
  protected void commonSetup(File tmpDir, OMConfigParameter configParameter) throws Exception {

    if (configParameter.overrideConfig) {
      setOzoneConfiguration(new OzoneConfiguration());
      getOzoneConfiguration().setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, configParameter.enableFSPaths);
      getOzoneConfiguration().setLong(OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD, configParameter.flushThreshold);
    }

    initializeNewOmMetadataManager(new File(tmpDir, "om"), configParameter.layout);
    OzoneManagerServiceProviderImpl ozoneManagerServiceProvider =
        configParameter.isFSO ? getMockOzoneManagerServiceProviderWithFSO()
            : getMockOzoneManagerServiceProvider();

    setReconOMMetadataManager(getTestReconOmMetadataManager(getOmMetadataManager(), new File(tmpDir, "recon")));
    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(tmpDir)
            .withReconOm(getReconOMMetadataManager())
            .withOmServiceProvider(ozoneManagerServiceProvider)
            .withReconSqlDb()
            .withContainerDB()
            .build();

    setReconNamespaceSummaryManager(reconTestInjector.getInstance(ReconNamespaceSummaryManager.class));
    assertNull(getReconNamespaceSummaryManager().getNSSummary(BUCKET_ONE_OBJECT_ID));

    if (!configParameter.legacyPopulate && !configParameter.isFSO && !configParameter.isOBS) {
      populateOMDBCommon();
    } else if (configParameter.isOBS) {
      populateOMDBOBS(configParameter.layout);
    } else if (configParameter.legacyPopulate) {
      populateOMDB(configParameter.layout, true);
    } else {
      populateOMDB(configParameter.layout, false);
    }
  }

  public List<NSSummary> commonSetUpTestReprocess(Runnable reprocessTask, long... bucketObjectIds) throws IOException {

    List<NSSummary> result = new ArrayList<>();
    NSSummary staleNSSummary = new NSSummary();
    RDBBatchOperation rdbBatchOperation = new RDBBatchOperation();
    getReconNamespaceSummaryManager().batchStoreNSSummaries(rdbBatchOperation, -1L, staleNSSummary);
    getReconNamespaceSummaryManager().commitBatchOperation(rdbBatchOperation);

    // Verify commit
    assertNotNull(getReconNamespaceSummaryManager().getNSSummary(-1L));
    // Reinitialize Recon RocksDB's namespace CF.
    getReconNamespaceSummaryManager().clearNSSummaryTable();
    // Run specific reprocess task
    reprocessTask.run();
    // Verify cleanup
    assertNull(getReconNamespaceSummaryManager().getNSSummary(-1L));
    // Assign and verify NSSummaries for buckets
    if (bucketObjectIds.length > 0) {
      NSSummary nsSummaryForBucket1 = getReconNamespaceSummaryManager().getNSSummary(bucketObjectIds[0]);
      assertNotNull(nsSummaryForBucket1);
      result.add(nsSummaryForBucket1);
    }
    if (bucketObjectIds.length > 1) {
      NSSummary nsSummaryForBucket2 = getReconNamespaceSummaryManager().getNSSummary(bucketObjectIds[1]);
      assertNotNull(nsSummaryForBucket2);
      result.add(nsSummaryForBucket2);
    }
    if (bucketObjectIds.length > 2) {
      NSSummary nsSummaryForBucket3 = getReconNamespaceSummaryManager().getNSSummary(bucketObjectIds[2]);
      assertNotNull(nsSummaryForBucket3);
      result.add(nsSummaryForBucket3);
    }
    return result;
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
  protected static OmKeyInfo buildOmKeyInfo(String volume, String bucket, String key, String fileName,
                                            long objectID, long parentObjectId, long dataSize) {
    return new OmKeyInfo.Builder()
        .setBucketName(bucket)
        .setVolumeName(volume)
        .setKeyName(key)
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE))
        .setObjectID(objectID)
        .setParentObjectID(parentObjectId)
        .setDataSize(dataSize)
        .build();
  }

  /**
   * Build a key info for delete action.
   *
   * @param volume         volume name
   * @param bucket         bucket name
   * @param key            key name
   * @param fileName       file name
   * @param objectID       object ID
   * @param parentObjectId parent object ID
   * @return the KeyInfo
   */
  protected static OmKeyInfo buildOmKeyInfo(String volume, String bucket, String key, String fileName,
                                            long objectID, long parentObjectId) {
    return new OmKeyInfo.Builder()
        .setBucketName(bucket)
        .setVolumeName(volume)
        .setKeyName(key)
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE))
        .setObjectID(objectID)
        .setParentObjectID(parentObjectId)
        .build();
  }

  protected static OmDirectoryInfo buildOmDirInfo(String dirName, long objectId, long parentObjectId) {
    return OmDirectoryInfo.newBuilder()
        .setName(dirName)
        .setObjectID(objectId)
        .setParentObjectID(parentObjectId)
        .build();
  }

  /**
   * Build a directory as key info for put/update action.
   * We don't need to set size.
   * @param volume volume name
   * @param bucket bucket name
   * @param key key name
   * @param fileName file name
   * @param objectID object ID
   * @return the KeyInfo
   */
  protected static OmKeyInfo buildOmDirKeyInfo(String volume,
                                               String bucket,
                                               String key,
                                               String fileName,
                                               long objectID) {
    return new OmKeyInfo.Builder()
        .setBucketName(bucket)
        .setVolumeName(volume)
        .setKeyName(key)
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.ONE))
        .setObjectID(objectID)
        .build();
  }

  protected static BucketLayout getFSOBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  protected static BucketLayout getLegacyBucketLayout() {
    return BucketLayout.LEGACY;
  }

  protected static BucketLayout getOBSBucketLayout() {
    return BucketLayout.OBJECT_STORE;
  }

  protected void initializeNewOmMetadataManager(File omDbDir, BucketLayout layout) throws IOException {

    if (layout == null) {
      initializeNewOmMetadataManager(omDbDir);
      return;
    }

    if (layout == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
      omMetadataManager = OMMetadataManagerTestUtils.initializeNewOmMetadataManager(omDbDir);
      return;
    }

    omConfiguration = new OzoneConfiguration();
    omConfiguration.set(OZONE_OM_DB_DIRS, omDbDir.getAbsolutePath());
    omConfiguration.set(OmConfig.Keys.ENABLE_FILESYSTEM_PATHS, "true");
    omConfiguration.set(OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD, "10");

    omMetadataManager = new OmMetadataManagerImpl(omConfiguration, null);

    String volumeKey = omMetadataManager.getVolumeKey(VOL);
    OmVolumeArgs args = OmVolumeArgs.newBuilder()
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
        .setBucketLayout(layout)
        .build();

    OmBucketInfo bucketInfo2 = OmBucketInfo.newBuilder()
        .setVolumeName(VOL)
        .setBucketName(BUCKET_TWO)
        .setObjectID(BUCKET_TWO_OBJECT_ID)
        .setBucketLayout(layout)
        .build();

    String bucketKey1 = omMetadataManager.getBucketKey(VOL, BUCKET_ONE);
    String bucketKey2 = omMetadataManager.getBucketKey(VOL, BUCKET_TWO);
    omMetadataManager.getBucketTable().put(bucketKey1, bucketInfo1);
    omMetadataManager.getBucketTable().put(bucketKey2, bucketInfo2);
  }

  // Helper method to check if an array contains a specific value
  protected boolean contains(int[] arr, int value) {
    for (int num : arr) {
      if (num == value) {
        return true;
      }
    }
    return false;
  }

  private void initializeNewOmMetadataManager(
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

  protected void populateOMDB(BucketLayout layout, boolean isLegacy) throws IOException {
    if (layout == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
      populateOMDBFSO();
    } else if (layout == BucketLayout.LEGACY) {
      populateOMDBLegacy();
    } else if (isLegacy && layout == BucketLayout.OBJECT_STORE) {
      populateOMDBOBS(getLegacyBucketLayout());
    } else {
      populateOMDBOBS(getOBSBucketLayout());
    }
  }

  /**
   * Populate OMDB with the following configs.
   *                 vol
   *           /      |       \
   *      bucket1   bucket2    bucket3
   *         /        |         \
   *       file1    file2     file3
   *
   * @throws IOException
   */
  protected void populateOMDBCommon() throws IOException {
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
   * Populate OMDB with the following configs.
   *              vol
   *            /     \
   *        bucket1   bucket2
   *        /    \      /    \
   *     file1  dir1  file2  file4
   *            /   \
   *         dir2   dir3
   *          /
   *        file3
   *
   * @throws IOException
   */
  private void populateOMDBFSO() throws IOException {
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
    writeKeyToOm(reconOMMetadataManager,
        KEY_TWO,
        BUCKET_TWO,
        VOL,
        FILE_TWO,
        KEY_TWO_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID,
        VOL_OBJECT_ID,
        KEY_TWO_OLD_SIZE,
        getFSOBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
        KEY_THREE,
        BUCKET_ONE,
        VOL,
        FILE_THREE,
        KEY_THREE_OBJECT_ID,
        DIR_TWO_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        KEY_THREE_SIZE,
        getFSOBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
        KEY_FOUR,
        BUCKET_TWO,
        VOL,
        FILE_FOUR,
        KEY_FOUR_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID,
        VOL_OBJECT_ID,
        KEY_FOUR_SIZE,
        getFSOBucketLayout());
    writeDirToOm(reconOMMetadataManager, DIR_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID, DIR_ONE);
    writeDirToOm(reconOMMetadataManager, DIR_TWO_OBJECT_ID,
        DIR_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID, DIR_TWO);
    writeDirToOm(reconOMMetadataManager, DIR_THREE_OBJECT_ID,
        DIR_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID, DIR_THREE);
  }

   /**
   * Populate OMDB with the following configs.
   *              vol
   *            /     \
   *        bucket1   bucket2
   *        /    \      /    \
   *     file1  dir1  file2  file4
   *            /   \
   *         dir2   dir3
   *          /
   *        file3
   *
   * @throws IOException
   */
  private void populateOMDBLegacy() throws IOException {
    writeKeyToOm(reconOMMetadataManager,
        KEY_ONE,
        BUCKET_ONE,
        VOL,
        FILE_ONE,
        KEY_ONE_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        KEY_ONE_SIZE,
        getLegacyBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
        KEY_TWO,
        BUCKET_TWO,
        VOL,
        FILE_TWO,
        KEY_TWO_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_TWO_OBJECT_ID,
        VOL_OBJECT_ID,
        KEY_TWO_OLD_SIZE,
        getLegacyBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
        KEY_THREE_1,
        BUCKET_ONE,
        VOL,
        FILE_THREE,
        KEY_THREE_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        KEY_THREE_SIZE,
        getLegacyBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
        KEY_FOUR,
        BUCKET_TWO,
        VOL,
        FILE_FOUR,
        KEY_FOUR_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_TWO_OBJECT_ID,
        VOL_OBJECT_ID,
        KEY_FOUR_SIZE,
        getLegacyBucketLayout());

    writeDirToOm(reconOMMetadataManager,
        (DIR_ONE + OM_KEY_PREFIX),
        BUCKET_ONE,
        VOL,
        DIR_ONE,
        DIR_ONE_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        getLegacyBucketLayout());
    writeDirToOm(reconOMMetadataManager,
        (DIR_ONE + OM_KEY_PREFIX +
            DIR_TWO + OM_KEY_PREFIX),
        BUCKET_ONE,
        VOL,
        DIR_TWO,
        DIR_TWO_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        getLegacyBucketLayout());
    writeDirToOm(reconOMMetadataManager,
        (DIR_ONE + OM_KEY_PREFIX +
            DIR_THREE + OM_KEY_PREFIX),
        BUCKET_ONE,
        VOL,
        DIR_THREE,
        DIR_THREE_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        getLegacyBucketLayout());
  }

   /**
   * Populate OMDB with the following configs.
   *                 vol
   *              /       \
   *          bucket1     bucket2
   *        /    \   \        \  \
   *     key1  key2   key3   key4 key5
   *
   * @throws IOException
   */
  protected void populateOMDBOBS(BucketLayout layout) throws IOException {
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
        layout);
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
        layout);
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
        layout);

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
        layout);
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
        layout);
  }

  public OzoneConfiguration getOzoneConfiguration() {
    return ozoneConfiguration;
  }

  public void setOzoneConfiguration(OzoneConfiguration ozoneConfiguration) {
    this.ozoneConfiguration = ozoneConfiguration;
  }

  public OzoneConfiguration getOmConfiguration() {
    return omConfiguration;
  }

  public void setOmConfiguration(OzoneConfiguration omConfiguration) {
    this.omConfiguration = omConfiguration;
  }

  public OMMetadataManager getOmMetadataManager() {
    return omMetadataManager;
  }

  public void setOmMetadataManager(OMMetadataManager omMetadataManager) {
    this.omMetadataManager = omMetadataManager;
  }

  public ReconOMMetadataManager getReconOMMetadataManager() {
    return reconOMMetadataManager;
  }

  public void setReconOMMetadataManager(ReconOMMetadataManager reconOMMetadataManager) {
    this.reconOMMetadataManager = reconOMMetadataManager;
  }

  public ReconNamespaceSummaryManager getReconNamespaceSummaryManager() {
    return reconNamespaceSummaryManager;
  }

  public void setReconNamespaceSummaryManager(ReconNamespaceSummaryManager reconNamespaceSummaryManager) {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
  }

  /**
   * Class for storing configuration to identify layout.
   */
  public static class OMConfigParameter {

    private final boolean isFSO;
    private final boolean isOBS;
    private final BucketLayout layout;
    private final long flushThreshold;
    private final boolean overrideConfig;
    private final boolean enableFSPaths;
    private final boolean legacyPopulate;

    public OMConfigParameter(boolean isFSO,
                             boolean isOBS,
                             BucketLayout layout,
                             long flushThreshold,
                             boolean overrideConfig,
                             boolean enableFSPaths,
                             boolean legacyPopulate) {
      this.isFSO = isFSO;
      this.isOBS = isOBS;
      this.layout = layout;
      this.flushThreshold = flushThreshold;
      this.overrideConfig = overrideConfig;
      this.enableFSPaths = enableFSPaths;
      this.legacyPopulate = legacyPopulate;
    }
  }
}
