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

package org.apache.hadoop.ozone.recon.api;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.apache.hadoop.ozone.om.helpers.QuotaUtil.getReplicatedSize;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getMockOzoneManagerServiceProvider;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.setConfiguration;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeKeyToOm;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.BucketObjectDBInfo;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.QuotaUsageResponse;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.api.types.VolumeObjectDBInfo;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.NSSummaryTaskWithLegacy;
import org.apache.hadoop.ozone.recon.tasks.NSSummaryTaskWithOBS;

/**
 * Flat fixture with OBS buckets (bucket1, bucket2 under vol) and Legacy buckets
 * with filesystem paths disabled (bucket3, bucket4 under vol2), so both behave
 * as flat object stores:
 * <pre>
 * └── vol
 *     ├── bucket1 (OBS)   : file1, ////file2, file3///
 *     └── bucket2 (OBS)   : file4, _//////
 * └── vol2
 *     ├── bucket3 (Legacy): file8, //////, ///__file10
 *     └── bucket4 (Legacy): ////file11
 * </pre>
 * Every key is written with RATIS/THREE replication.
 */
public class FlatNSSummaryScenario extends NSSummaryTestScenario {

  // object names
  private static final String VOL = "vol";
  private static final String VOL_TWO = "vol2";
  private static final String BUCKET_ONE = "bucket1";
  private static final String BUCKET_TWO = "bucket2";
  private static final String BUCKET_THREE = "bucket3";
  private static final String BUCKET_FOUR = "bucket4";
  private static final String KEY_ONE = "file1";
  private static final String KEY_TWO = "////file2";
  private static final String KEY_THREE = "file3///";
  private static final String KEY_FOUR = "file4";
  private static final String KEY_FIVE = "_//////";
  private static final String KEY_EIGHT = "file8";
  private static final String KEY_NINE = "//////";
  private static final String KEY_TEN = "///__file10";
  private static final String KEY_ELEVEN = "////file11";
  private static final String MULTI_BLOCK_FILE = KEY_THREE;

  // object IDs
  private static final long VOL_OBJECT_ID = 0L;
  private static final long VOL_TWO_OBJECT_ID = 14L;
  private static final long BUCKET_ONE_OBJECT_ID = 1L;
  private static final long BUCKET_TWO_OBJECT_ID = 2L;
  private static final long BUCKET_THREE_OBJECT_ID = 15L;
  private static final long BUCKET_FOUR_OBJECT_ID = 16L;
  private static final long KEY_ONE_OBJECT_ID = 3L;
  private static final long KEY_TWO_OBJECT_ID = 5L;
  private static final long KEY_THREE_OBJECT_ID = 8L;
  private static final long KEY_FOUR_OBJECT_ID = 6L;
  private static final long KEY_FIVE_OBJECT_ID = 9L;
  private static final long KEY_EIGHT_OBJECT_ID = 17L;
  private static final long KEY_NINE_OBJECT_ID = 19L;
  private static final long KEY_TEN_OBJECT_ID = 20L;
  private static final long KEY_ELEVEN_OBJECT_ID = 21L;
  private static final long MULTI_BLOCK_KEY_OBJECT_ID = 13L;

  // data size in bytes (unreplicated)
  private static final long FILE_ONE_SIZE = 500L; // bin 0
  private static final long FILE_TWO_SIZE = OzoneConsts.KB + 1; // bin 1
  private static final long FILE_THREE_SIZE = 4 * OzoneConsts.KB + 1; // bin 3
  private static final long FILE_FOUR_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  private static final long FILE_FIVE_SIZE = 100L; // bin 0
  private static final long FILE_EIGHT_SIZE = OzoneConsts.KB + 1; // bin 1
  private static final long FILE_NINE_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  private static final long FILE_TEN_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  private static final long FILE_ELEVEN_SIZE = OzoneConsts.KB + 1; // bin 1

  // replicated sizes (RATIS/THREE => 3x)
  private static final long FILE1_REPLICA = repl(FILE_ONE_SIZE);
  private static final long FILE2_REPLICA = repl(FILE_TWO_SIZE);
  private static final long FILE3_REPLICA = repl(FILE_THREE_SIZE);
  private static final long FILE4_REPLICA = repl(FILE_FOUR_SIZE);
  private static final long FILE5_REPLICA = repl(FILE_FIVE_SIZE);
  private static final long FILE8_REPLICA = repl(FILE_EIGHT_SIZE);
  private static final long FILE9_REPLICA = repl(FILE_NINE_SIZE);
  private static final long FILE10_REPLICA = repl(FILE_TEN_SIZE);
  private static final long FILE11_REPLICA = repl(FILE_ELEVEN_SIZE);

  private static final long REPLICA_UNDER_ROOT =
      FILE1_REPLICA + FILE2_REPLICA + FILE3_REPLICA + FILE4_REPLICA
      + FILE5_REPLICA + FILE8_REPLICA + FILE9_REPLICA + FILE10_REPLICA
      + FILE11_REPLICA;
  private static final long REPLICA_UNDER_VOL =
      FILE1_REPLICA + FILE2_REPLICA + FILE3_REPLICA + FILE4_REPLICA
      + FILE5_REPLICA;
  private static final long REPLICA_UNDER_BUCKET1 =
      FILE1_REPLICA + FILE2_REPLICA + FILE3_REPLICA;
  private static final long REPLICA_UNDER_BUCKET3 =
      FILE8_REPLICA + FILE9_REPLICA + FILE10_REPLICA;
  private static final long REPLICA_UNDER_KEY = FILE4_REPLICA;

  // quota in bytes
  private static final long ROOT_QUOTA = 2 * (2 * OzoneConsts.MB);
  private static final long VOL_QUOTA = 2 * OzoneConsts.MB;
  private static final long VOL_TWO_QUOTA = 2 * OzoneConsts.MB;
  private static final long BUCKET_ONE_QUOTA = OzoneConsts.MB;
  private static final long BUCKET_TWO_QUOTA = OzoneConsts.MB;
  private static final long BUCKET_THREE_QUOTA = OzoneConsts.MB;
  private static final long BUCKET_FOUR_QUOTA = OzoneConsts.MB;

  // request paths
  private static final String VOL_PATH = "/vol";
  private static final String VOL_TWO_PATH = "/vol2";
  private static final String BUCKET_ONE_PATH = "/vol/bucket1";
  private static final String BUCKET_TWO_PATH = "/vol/bucket2";
  private static final String BUCKET_THREE_PATH = "/vol2/bucket3";
  private static final String BUCKET_FOUR_PATH = "/vol2/bucket4";
  private static final String KEY_ONE_PATH = "/vol/bucket1/" + KEY_ONE;
  private static final String KEY_TWO_PATH = "/vol/bucket1/" + KEY_TWO;
  private static final String KEY_FIVE_PATH = "/vol/bucket2/" + KEY_FIVE;
  private static final String KEY_EIGHT_PATH = "/vol2/bucket3/" + KEY_EIGHT;
  private static final String KEY_ELEVEN_PATH = "/vol2/bucket4/" + KEY_ELEVEN;
  private static final String MULTI_BLOCK_KEY_PATH = "/vol/bucket1/" + KEY_THREE;

  // unreplicated (namespace) data sizes
  private static final long ROOT_DATA_SIZE = REPLICA_UNDER_ROOT;
  private static final long VOL_DATA_SIZE = FILE_ONE_SIZE + FILE_TWO_SIZE
      + FILE_THREE_SIZE + FILE_FOUR_SIZE + FILE_FIVE_SIZE;
  private static final long VOL_TWO_DATA_SIZE =
      FILE_EIGHT_SIZE + FILE_NINE_SIZE + FILE_TEN_SIZE + FILE_ELEVEN_SIZE;
  private static final long BUCKET_ONE_DATA_SIZE =
      FILE_ONE_SIZE + FILE_TWO_SIZE + FILE_THREE_SIZE;
  private static final long BUCKET_TWO_DATA_SIZE =
      FILE_FOUR_SIZE + FILE_FIVE_SIZE;
  private static final long BUCKET_THREE_DATA_SIZE =
      FILE_EIGHT_SIZE + FILE_NINE_SIZE + FILE_TEN_SIZE;
  private static final long BUCKET_FOUR_DATA_SIZE = FILE_ELEVEN_SIZE;

  public FlatNSSummaryScenario() {
    super("OBS_AND_LEGACY");
  }

  private static long repl(long size) {
    return getReplicatedSize(size, RatisReplicationConfig.getInstance(THREE));
  }

  private static BucketLayout obs() {
    return BucketLayout.OBJECT_STORE;
  }

  private static BucketLayout legacy() {
    return BucketLayout.LEGACY;
  }

  @Override
  boolean isFlatLayout() {
    return true;
  }

  @Override
  long rootQuota() {
    return ROOT_QUOTA;
  }

  @Override
  long rootDataSize() {
    return ROOT_DATA_SIZE;
  }

  @Override
  OzoneConfiguration newConfiguration() {
    OzoneConfiguration conf = new OzoneConfiguration();
    // Legacy buckets behave like OBS buckets with filesystem paths disabled.
    conf.set(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, "false");
    return conf;
  }

  @Override
  OzoneManagerServiceProviderImpl mockOmServiceProvider() throws IOException {
    return getMockOzoneManagerServiceProvider();
  }

  @Override
  OMMetadataManager initializeOmMetadataManager(File omDbDir,
      OzoneConfiguration conf) throws IOException {
    conf.set(OZONE_OM_DB_DIRS, omDbDir.getAbsolutePath());
    conf.set(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, "false");
    // Make the Recon metadata manager (and thus the endpoint's bucket-handler
    // selection) observe filesystem-paths=false, so Legacy buckets are treated
    // as flat object stores. Required because getTestReconOmMetadataManager
    // reads a shared static configuration that a prior scenario may have set.
    setConfiguration(conf);
    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(conf, null);

    putVolume(omMetadataManager, VOL, VOL_OBJECT_ID, VOL_QUOTA);
    putVolume(omMetadataManager, VOL_TWO, VOL_TWO_OBJECT_ID, VOL_TWO_QUOTA);
    putBucket(omMetadataManager, VOL, BUCKET_ONE, BUCKET_ONE_OBJECT_ID,
        BUCKET_ONE_QUOTA, obs());
    putBucket(omMetadataManager, VOL, BUCKET_TWO, BUCKET_TWO_OBJECT_ID,
        BUCKET_TWO_QUOTA, obs());
    putBucket(omMetadataManager, VOL_TWO, BUCKET_THREE, BUCKET_THREE_OBJECT_ID,
        BUCKET_THREE_QUOTA, legacy());
    putBucket(omMetadataManager, VOL_TWO, BUCKET_FOUR, BUCKET_FOUR_OBJECT_ID,
        BUCKET_FOUR_QUOTA, legacy());
    return omMetadataManager;
  }

  private void putVolume(OMMetadataManager omMetadataManager, String volume,
      long objectId, long quota) throws IOException {
    omMetadataManager.getVolumeTable().put(
        omMetadataManager.getVolumeKey(volume),
        OmVolumeArgs.newBuilder()
            .setObjectID(objectId)
            .setVolume(volume)
            .setAdminName(TEST_USER)
            .setOwnerName(TEST_USER)
            .setQuotaInBytes(quota)
            .build());
  }

  private void putBucket(OMMetadataManager omMetadataManager, String volume,
      String bucket, long objectId, long quota, BucketLayout layout)
      throws IOException {
    omMetadataManager.getBucketTable().put(
        omMetadataManager.getBucketKey(volume, bucket),
        OmBucketInfo.newBuilder()
            .setVolumeName(volume)
            .setBucketName(bucket)
            .setObjectID(objectId)
            .setQuotaInBytes(quota)
            .setBucketLayout(layout)
            .build());
  }

  @Override
  void populateAndReprocess(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager recon, OzoneConfiguration conf) throws Exception {
    OmKeyLocationInfoGroup group1 =
        NSSummaryEndpointTestBase.getLocationInfoGroup1();
    OmKeyLocationInfoGroup group2 =
        NSSummaryEndpointTestBase.getLocationInfoGroup2();

    writeReplicatedKey(recon, KEY_ONE, BUCKET_ONE, VOL, KEY_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID, VOL_OBJECT_ID, group1, FILE_ONE_SIZE, obs());
    writeReplicatedKey(recon, KEY_TWO, BUCKET_ONE, VOL, KEY_TWO_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID, VOL_OBJECT_ID, group2, FILE_TWO_SIZE, obs());
    writeReplicatedKey(recon, KEY_THREE, BUCKET_ONE, VOL, KEY_THREE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID, VOL_OBJECT_ID, group1, FILE_THREE_SIZE, obs());
    writeReplicatedKey(recon, KEY_FOUR, BUCKET_TWO, VOL, KEY_FOUR_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID, VOL_OBJECT_ID, group2, FILE_FOUR_SIZE, obs());
    writeReplicatedKey(recon, KEY_FIVE, BUCKET_TWO, VOL, KEY_FIVE_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID, VOL_OBJECT_ID, group1, FILE_FIVE_SIZE, obs());
    writeReplicatedKey(recon, KEY_EIGHT, BUCKET_THREE, VOL_TWO,
        KEY_EIGHT_OBJECT_ID, BUCKET_THREE_OBJECT_ID, VOL_TWO_OBJECT_ID, group2,
        FILE_EIGHT_SIZE, legacy());
    writeReplicatedKey(recon, KEY_NINE, BUCKET_THREE, VOL_TWO,
        KEY_NINE_OBJECT_ID, BUCKET_THREE_OBJECT_ID, VOL_TWO_OBJECT_ID, group1,
        FILE_NINE_SIZE, legacy());
    writeReplicatedKey(recon, KEY_TEN, BUCKET_THREE, VOL_TWO, KEY_TEN_OBJECT_ID,
        BUCKET_THREE_OBJECT_ID, VOL_TWO_OBJECT_ID, group2, FILE_TEN_SIZE,
        legacy());
    writeReplicatedKey(recon, KEY_ELEVEN, BUCKET_FOUR, VOL_TWO,
        KEY_ELEVEN_OBJECT_ID, BUCKET_FOUR_OBJECT_ID, VOL_TWO_OBJECT_ID, group1,
        FILE_ELEVEN_SIZE, legacy());

    new NSSummaryTaskWithOBS(reconNamespaceSummaryManager, recon, 10, 5, 20,
        2000).reprocessWithOBS(recon);
    new NSSummaryTaskWithLegacy(reconNamespaceSummaryManager, recon, conf, 10)
        .reprocessWithLegacy(recon);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private void writeReplicatedKey(ReconOMMetadataManager recon, String key,
      String bucket, String vol, long keyObjectId, long bucketObjectId,
      long volObjectId, OmKeyLocationInfoGroup locationGroup, long dataSize,
      BucketLayout layout) throws IOException {
    writeKeyToOm(recon, key, bucket, vol, key, keyObjectId,
        bucketObjectId, bucketObjectId, volObjectId,
        Collections.singletonList(locationGroup), layout, dataSize,
        RatisReplicationConfig.getInstance(THREE));
  }

  @Override
  void writeMultiBlockKey(ReconOMMetadataManager recon) throws IOException {
    writeReplicatedKey(recon, MULTI_BLOCK_FILE, BUCKET_ONE, VOL,
        MULTI_BLOCK_KEY_OBJECT_ID, BUCKET_ONE_OBJECT_ID, VOL_OBJECT_ID,
        NSSummaryEndpointTestBase.getLocationInfoGroup1(), FILE_THREE_SIZE,
        obs());
  }

  @Override
  String multiBlockKeyPath() {
    return MULTI_BLOCK_KEY_PATH;
  }

  @Override
  long multiBlockKeyReplicatedSize() {
    return FILE3_REPLICA;
  }

  // ---------------------------------------------------------------------------
  // Assertions common to every layout
  // ---------------------------------------------------------------------------

  @Override
  void assertBasicInfoRoot(NSSummaryEndpoint endpoint,
      ReconOMMetadataManager recon) throws Exception {
    NamespaceSummaryResponse root = (NamespaceSummaryResponse)
        endpoint.getBasicInfo(ROOT_PATH).getEntity();
    assertEquals(EntityType.ROOT, root.getEntityType());
    assertEquals(2, root.getCountStats().getNumVolume());
    assertEquals(4, root.getCountStats().getNumBucket());
    assertEquals(9, root.getCountStats().getNumTotalKey());
  }

  @Override
  void assertBasicInfoVolume(NSSummaryEndpoint endpoint) throws Exception {
    NamespaceSummaryResponse vol = (NamespaceSummaryResponse)
        endpoint.getBasicInfo(VOL_PATH).getEntity();
    assertEquals(EntityType.VOLUME, vol.getEntityType());
    assertEquals(2, vol.getCountStats().getNumBucket());
    assertEquals(5, vol.getCountStats().getNumTotalKey());
    assertEquals(TEST_USER,
        ((VolumeObjectDBInfo) vol.getObjectDBInfo()).getAdmin());
    assertEquals(TEST_USER,
        ((VolumeObjectDBInfo) vol.getObjectDBInfo()).getOwner());
    assertEquals(VOL, vol.getObjectDBInfo().getName());
    assertEquals(2097152, vol.getObjectDBInfo().getQuotaInBytes());
    assertEquals(-1, vol.getObjectDBInfo().getQuotaInNamespace());
  }

  @Override
  void assertBasicInfoBucketOne(NSSummaryEndpoint endpoint) throws Exception {
    assertObsBucket(endpoint, BUCKET_ONE_PATH, BUCKET_ONE, VOL, 3);
  }

  @Override
  void assertBasicInfoBucketTwo(NSSummaryEndpoint endpoint) throws Exception {
    assertObsBucket(endpoint, BUCKET_TWO_PATH, BUCKET_TWO, VOL, 2);
  }

  void assertBasicInfoVolTwo(NSSummaryEndpoint endpoint) throws Exception {
    NamespaceSummaryResponse vol = (NamespaceSummaryResponse)
        endpoint.getBasicInfo(VOL_TWO_PATH).getEntity();
    assertEquals(EntityType.VOLUME, vol.getEntityType());
    assertEquals(2, vol.getCountStats().getNumBucket());
    assertEquals(4, vol.getCountStats().getNumTotalKey());
    assertEquals(TEST_USER,
        ((VolumeObjectDBInfo) vol.getObjectDBInfo()).getAdmin());
    assertEquals(TEST_USER,
        ((VolumeObjectDBInfo) vol.getObjectDBInfo()).getOwner());
    assertEquals(VOL_TWO, vol.getObjectDBInfo().getName());
    assertEquals(2097152, vol.getObjectDBInfo().getQuotaInBytes());
    assertEquals(-1, vol.getObjectDBInfo().getQuotaInNamespace());
  }

  void assertBasicInfoBucketThree(NSSummaryEndpoint endpoint) throws Exception {
    assertLegacyBucket(endpoint, BUCKET_THREE_PATH, BUCKET_THREE, VOL_TWO, 3);
  }

  void assertBasicInfoBucketFour(NSSummaryEndpoint endpoint) throws Exception {
    assertLegacyBucket(endpoint, BUCKET_FOUR_PATH, BUCKET_FOUR, VOL_TWO, 1);
  }

  private void assertObsBucket(NSSummaryEndpoint endpoint, String path,
      String bucketName, String volName, int numKeys) throws Exception {
    assertBucket(endpoint, path, bucketName, volName, numKeys, obs());
  }

  private void assertLegacyBucket(NSSummaryEndpoint endpoint, String path,
      String bucketName, String volName, int numKeys) throws Exception {
    assertBucket(endpoint, path, bucketName, volName, numKeys, legacy());
  }

  private void assertBucket(NSSummaryEndpoint endpoint, String path,
      String bucketName, String volName, int numKeys, BucketLayout layout)
      throws Exception {
    NamespaceSummaryResponse bucket = (NamespaceSummaryResponse)
        endpoint.getBasicInfo(path).getEntity();
    assertEquals(EntityType.BUCKET, bucket.getEntityType());
    assertEquals(numKeys, bucket.getCountStats().getNumTotalKey());
    assertEquals(volName,
        ((BucketObjectDBInfo) bucket.getObjectDBInfo()).getVolumeName());
    assertEquals(StorageType.DISK,
        ((BucketObjectDBInfo) bucket.getObjectDBInfo()).getStorageType());
    assertEquals(layout,
        ((BucketObjectDBInfo) bucket.getObjectDBInfo()).getBucketLayout());
    assertEquals(bucketName,
        ((BucketObjectDBInfo) bucket.getObjectDBInfo()).getName());
  }

  @Override
  void assertDiskUsageRoot(NSSummaryEndpoint endpoint) throws Exception {
    DUResponse duRootRes =
        NSSummaryEndpointTestBase.getDiskUsage(endpoint, ROOT_PATH, false);
    assertEquals(2, duRootRes.getCount());
    List<DUResponse.DiskUsage> duData = duRootRes.getDuData();
    Collections.sort(duData,
        Comparator.comparing(DUResponse.DiskUsage::getSubpath));
    assertEquals(VOL_PATH, duData.get(0).getSubpath());
    assertEquals(VOL_TWO_PATH, duData.get(1).getSubpath());
    assertEquals(VOL_DATA_SIZE, duData.get(0).getSize());
    assertEquals(VOL_TWO_DATA_SIZE, duData.get(1).getSize());
  }

  @Override
  void assertDiskUsageVolume(NSSummaryEndpoint endpoint) throws Exception {
    DUResponse duVolRes =
        NSSummaryEndpointTestBase.getDiskUsage(endpoint, VOL_PATH, false);
    assertEquals(2, duVolRes.getCount());
    List<DUResponse.DiskUsage> duData = duVolRes.getDuData();
    Collections.sort(duData,
        Comparator.comparing(DUResponse.DiskUsage::getSubpath));
    assertEquals(BUCKET_ONE_PATH, duData.get(0).getSubpath());
    assertEquals(BUCKET_TWO_PATH, duData.get(1).getSubpath());
    assertEquals(BUCKET_ONE_DATA_SIZE, duData.get(0).getSize());
    assertEquals(BUCKET_TWO_DATA_SIZE, duData.get(1).getSize());
  }

  void assertDiskUsageVolTwo(NSSummaryEndpoint endpoint) throws Exception {
    DUResponse duVolRes =
        NSSummaryEndpointTestBase.getDiskUsage(endpoint, VOL_TWO_PATH, false);
    assertEquals(2, duVolRes.getCount());
    List<DUResponse.DiskUsage> duData = duVolRes.getDuData();
    Collections.sort(duData,
        Comparator.comparing(DUResponse.DiskUsage::getSubpath));
    assertEquals(BUCKET_THREE_PATH, duData.get(0).getSubpath());
    assertEquals(BUCKET_FOUR_PATH, duData.get(1).getSubpath());
    assertEquals(VOL_TWO_DATA_SIZE, duVolRes.getSize());
  }

  /** OBS/Legacy buckets have no sub-paths unless files are explicitly listed. */
  void assertDiskUsageBucketOne(NSSummaryEndpoint endpoint) throws Exception {
    assertFlatBucketDiskUsage(endpoint, BUCKET_ONE_PATH, 3, BUCKET_ONE_DATA_SIZE);
  }

  void assertDiskUsageBucketTwo(NSSummaryEndpoint endpoint) throws Exception {
    assertFlatBucketDiskUsage(endpoint, BUCKET_TWO_PATH, 2, BUCKET_TWO_DATA_SIZE);
  }

  void assertDiskUsageBucketThree(NSSummaryEndpoint endpoint) throws Exception {
    assertFlatBucketDiskUsage(endpoint, BUCKET_THREE_PATH, 3,
        BUCKET_THREE_DATA_SIZE);
  }

  private void assertFlatBucketDiskUsage(NSSummaryEndpoint endpoint, String path,
      int numFiles, long dataSize) throws Exception {
    DUResponse duBucketResponse =
        NSSummaryEndpointTestBase.getDiskUsage(endpoint, path, false);
    assertEquals(0, duBucketResponse.getCount());
    DUResponse withFiles = (DUResponse) endpoint
        .getDiskUsage(path, true, false, false).getEntity();
    assertEquals(numFiles, withFiles.getCount());
    assertEquals(dataSize, duBucketResponse.getSize());
  }

  @Override
  void assertDiskUsageKey(NSSummaryEndpoint endpoint) throws Exception {
    DUResponse duKeyResponse =
        NSSummaryEndpointTestBase.getDiskUsage(endpoint, KEY_PATH, false);
    assertEquals(0, duKeyResponse.getCount());
    assertEquals(FILE_FOUR_SIZE, duKeyResponse.getSize());
  }

  void assertDiskUsageKeys(NSSummaryEndpoint endpoint) throws Exception {
    assertKeySize(endpoint, KEY_ONE_PATH, FILE_ONE_SIZE);
    assertKeySize(endpoint, KEY_TWO_PATH, FILE_TWO_SIZE);
    assertKeySize(endpoint, KEY_FIVE_PATH, FILE_FIVE_SIZE);
    assertKeySize(endpoint, KEY_EIGHT_PATH, FILE_EIGHT_SIZE);
    assertKeySize(endpoint, KEY_ELEVEN_PATH, FILE_ELEVEN_SIZE);
  }

  private void assertKeySize(NSSummaryEndpoint endpoint, String path,
      long expectedSize) throws Exception {
    DUResponse duKeyResponse =
        NSSummaryEndpointTestBase.getDiskUsage(endpoint, path, false);
    assertEquals(0, duKeyResponse.getCount());
    assertEquals(expectedSize, duKeyResponse.getSize());
  }

  @Override
  void assertQuotaUsage(NSSummaryEndpoint endpoint) throws Exception {
    QuotaUsageResponse quRootRes = (QuotaUsageResponse)
        endpoint.getQuotaUsage(ROOT_PATH).getEntity();
    assertEquals(ROOT_QUOTA, quRootRes.getQuota());
    assertEquals(ROOT_DATA_SIZE, quRootRes.getQuotaUsed());

    assertQuota(endpoint, VOL_PATH, VOL_QUOTA, VOL_DATA_SIZE);
    assertQuota(endpoint, BUCKET_ONE_PATH, BUCKET_ONE_QUOTA,
        BUCKET_ONE_DATA_SIZE);
    assertQuota(endpoint, BUCKET_TWO_PATH, BUCKET_TWO_QUOTA,
        BUCKET_TWO_DATA_SIZE);
    assertQuota(endpoint, BUCKET_THREE_PATH, BUCKET_THREE_QUOTA,
        BUCKET_THREE_DATA_SIZE);
    assertQuota(endpoint, BUCKET_FOUR_PATH, BUCKET_FOUR_QUOTA,
        BUCKET_FOUR_DATA_SIZE);

    QuotaUsageResponse naKey = (QuotaUsageResponse)
        endpoint.getQuotaUsage(KEY_PATH).getEntity();
    assertEquals(ResponseStatus.TYPE_NOT_APPLICABLE, naKey.getResponseCode());

    QuotaUsageResponse invalid = (QuotaUsageResponse)
        endpoint.getQuotaUsage(INVALID_PATH).getEntity();
    assertEquals(ResponseStatus.PATH_NOT_FOUND, invalid.getResponseCode());
  }

  private void assertQuota(NSSummaryEndpoint endpoint, String path, long quota,
      long used) throws IOException {
    QuotaUsageResponse res =
        (QuotaUsageResponse) endpoint.getQuotaUsage(path).getEntity();
    assertEquals(quota, res.getQuota());
    assertEquals(used, res.getQuotaUsed());
  }

  @Override
  void assertFileSizeDist(NSSummaryEndpoint endpoint) throws Exception {
    NSSummaryEndpointTestBase.checkFileSizeDist(endpoint, ROOT_PATH, 2, 3, 3, 1);
    NSSummaryEndpointTestBase.checkFileSizeDist(endpoint, VOL_PATH, 2, 1, 1, 1);
    NSSummaryEndpointTestBase.checkFileSizeDist(
        endpoint, BUCKET_ONE_PATH, 1, 1, 0, 1);
  }

  @Override
  void assertDataSizeUnderRootWithReplication(NSSummaryEndpoint endpoint)
      throws IOException {
    DUResponse res =
        NSSummaryEndpointTestBase.getDiskUsage(endpoint, ROOT_PATH, true);
    assertEquals(ResponseStatus.OK, res.getStatus());
    assertEquals(REPLICA_UNDER_ROOT, res.getSizeWithReplica());
    assertEquals(REPLICA_UNDER_VOL, res.getDuData().get(0).getSizeWithReplica());
  }

  @Override
  void assertDataSizeUnderVolWithReplication(NSSummaryEndpoint endpoint)
      throws IOException {
    DUResponse res =
        NSSummaryEndpointTestBase.getDiskUsage(endpoint, VOL_PATH, true);
    assertEquals(ResponseStatus.OK, res.getStatus());
    assertEquals(REPLICA_UNDER_VOL, res.getSizeWithReplica());
    assertEquals(REPLICA_UNDER_BUCKET1,
        res.getDuData().get(0).getSizeWithReplica());
  }

  void assertDataSizeUnderBucketOneWithReplication(NSSummaryEndpoint endpoint)
      throws IOException {
    DUResponse res =
        NSSummaryEndpointTestBase.getDiskUsage(endpoint, BUCKET_ONE_PATH, true);
    assertEquals(ResponseStatus.OK, res.getStatus());
    assertEquals(REPLICA_UNDER_BUCKET1, res.getSizeWithReplica());
  }

  void assertDataSizeUnderBucketThreeWithReplication(NSSummaryEndpoint endpoint)
      throws IOException {
    DUResponse res = NSSummaryEndpointTestBase.getDiskUsage(
        endpoint, BUCKET_THREE_PATH, true);
    assertEquals(ResponseStatus.OK, res.getStatus());
    assertEquals(REPLICA_UNDER_BUCKET3, res.getSizeWithReplica());
  }

  @Override
  void assertDataSizeUnderKeyWithReplication(NSSummaryEndpoint endpoint)
      throws IOException {
    DUResponse res =
        NSSummaryEndpointTestBase.getDiskUsage(endpoint, KEY_PATH, true);
    assertEquals(ResponseStatus.OK, res.getStatus());
    assertEquals(REPLICA_UNDER_KEY, res.getSizeWithReplica());
  }

  void assertNormalizePathUptoBucket() {
    assertEquals("/", OmUtils.normalizePathUptoBucket(null));
    assertEquals("/", OmUtils.normalizePathUptoBucket(""));
    assertEquals("volume1/bucket1/key1/key2",
        OmUtils.normalizePathUptoBucket("///volume1/bucket1/key1/key2"));
    assertEquals("volume1/bucket1",
        OmUtils.normalizePathUptoBucket("volume1/bucket1"));
    assertEquals("volume1/bucket1/key1/key2",
        OmUtils.normalizePathUptoBucket("volume1/bucket1/key1/key2"));
    assertEquals("volume1/bucket1/key1//key2",
        OmUtils.normalizePathUptoBucket("volume1/bucket1/key1//key2"));
    assertEquals("volume/bucket/key$%#1/./////////key$%#2",
        OmUtils.normalizePathUptoBucket("volume/bucket/key$%#1/./////////key$%#2"));
  }

  @Override
  void verifyConstructFullPath(ReconOMMetadataManager recon,
      ReconNamespaceSummaryManager reconNamespaceSummaryManager)
      throws IOException {
    assertFullPath(reconNamespaceSummaryManager, KEY_TWO, VOL, BUCKET_ONE,
        KEY_TWO_OBJECT_ID, "vol/bucket1/" + KEY_TWO);
    assertFullPath(reconNamespaceSummaryManager, KEY_FIVE, VOL, BUCKET_TWO,
        KEY_FIVE_OBJECT_ID, "vol/bucket2/" + KEY_FIVE);
    assertFullPath(reconNamespaceSummaryManager, KEY_EIGHT, VOL_TWO,
        BUCKET_THREE, KEY_EIGHT_OBJECT_ID, "vol2/bucket3/" + KEY_EIGHT);
    assertFullPath(reconNamespaceSummaryManager, KEY_ELEVEN, VOL_TWO,
        BUCKET_FOUR, KEY_ELEVEN_OBJECT_ID, "vol2/bucket4/" + KEY_ELEVEN);
  }

  private void assertFullPath(ReconNamespaceSummaryManager nsMgr,
      String keyName, String vol, String bucket, long objectId,
      String expected) throws IOException {
    OmKeyInfo keyInfo = new OmKeyInfo.Builder()
        .setKeyName(keyName)
        .setVolumeName(vol)
        .setBucketName(bucket)
        .setObjectID(objectId)
        .build();
    assertEquals(expected, ReconUtils.constructFullPath(keyInfo, nsMgr));
  }
}
