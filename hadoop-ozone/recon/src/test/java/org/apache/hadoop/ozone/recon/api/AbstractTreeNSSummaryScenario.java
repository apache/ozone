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

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.apache.hadoop.ozone.om.helpers.QuotaUtil.getReplicatedSize;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeKeyToOm;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.api.types.BucketObjectDBInfo;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.QuotaUsageResponse;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.api.types.VolumeObjectDBInfo;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

/**
 * Shared tree fixture for the FSO and Legacy (filesystem-paths enabled)
 * scenarios. The namespace is:
 * <pre>
 *                vol
 *             /       \
 *        bucket1      bucket2
 *        /    \         /    \
 *     file1    dir1    file4  file5
 *           /   \   \   \
 *        dir2  dir3 dir4 file7
 *         /     \      \
 *       file2   file3  file6
 *  ----------------------------------------
 *                  vol2
 *              /         \
 *      bucket3          bucket4
 *      /      \           /
 *   file8     dir5      file11
 *            /    \
 *        file9    file10
 *  ----------------------------------------
 *                    vol3
 *                     |
 *                 bucket5
 *                 /      \
 *             file12     dir6
 *                     /    \
 *                 file13    dir7
 *                          /
 *                       file14
 * </pre>
 *
 * <p>Every key is written with RATIS/THREE replication so that
 * {@code sizeWithReplica} (3x) is genuinely distinct from the unreplicated
 * {@code size}. FSO and Legacy differ only in the OM DB key scheme, exposed
 * through the {@code writeDir}/{@code writeKey}/{@code writeReplicatedKey}
 * hooks; all constants and assertions are shared here.
 */
public abstract class AbstractTreeNSSummaryScenario extends NSSummaryTestScenario {

  // object names
  static final String VOL = "vol";
  static final String VOL_TWO = "vol2";
  static final String VOL_THREE = "vol3";
  static final String BUCKET_ONE = "bucket1";
  static final String BUCKET_TWO = "bucket2";
  static final String BUCKET_THREE = "bucket3";
  static final String BUCKET_FOUR = "bucket4";
  static final String BUCKET_FIVE = "bucket5";
  static final String KEY_ONE = "file1";
  static final String KEY_TWO = "dir1/dir2/file2";
  static final String KEY_THREE = "dir1/dir3/file3";
  static final String KEY_FOUR = "file4";
  static final String KEY_FIVE = "file5";
  static final String KEY_SIX = "dir1/dir4/file6";
  static final String KEY_SEVEN = "dir1/file7";
  static final String KEY_EIGHT = "file8";
  static final String KEY_NINE = "dir5/file9";
  static final String KEY_TEN = "dir5/file10";
  static final String KEY_ELEVEN = "file11";
  static final String KEY_TWELVE = "file12";
  static final String KEY_THIRTEEN = "dir6/file13";
  static final String KEY_FOURTEEN = "dir6/dir7/file14";
  static final String FILE_ONE = "file1";
  static final String FILE_TWO = "file2";
  static final String FILE_THREE = "file3";
  static final String FILE_FOUR = "file4";
  static final String FILE_FIVE = "file5";
  static final String FILE_SIX = "file6";
  static final String FILE_SEVEN = "file7";
  static final String FILE_EIGHT = "file8";
  static final String FILE_NINE = "file9";
  static final String FILE_TEN = "file10";
  static final String FILE_ELEVEN = "file11";
  static final String FILE_TWELVE = "file12";
  static final String FILE_THIRTEEN = "file13";
  static final String FILE_FOURTEEN = "file14";
  static final String DIR_ONE = "dir1";
  static final String DIR_TWO = "dir2";
  static final String DIR_THREE = "dir3";
  static final String DIR_FOUR = "dir4";
  static final String DIR_FIVE = "dir5";
  static final String DIR_SIX = "dir6";
  static final String DIR_SEVEN = "dir7";

  // object IDs
  static final long VOL_OBJECT_ID = 0L;
  static final long BUCKET_ONE_OBJECT_ID = 1L;
  static final long BUCKET_TWO_OBJECT_ID = 2L;
  static final long KEY_ONE_OBJECT_ID = 3L;
  static final long DIR_ONE_OBJECT_ID = 4L;
  static final long KEY_TWO_OBJECT_ID = 5L;
  static final long KEY_FOUR_OBJECT_ID = 6L;
  static final long DIR_TWO_OBJECT_ID = 7L;
  static final long KEY_THREE_OBJECT_ID = 8L;
  static final long KEY_FIVE_OBJECT_ID = 9L;
  static final long KEY_SIX_OBJECT_ID = 10L;
  static final long DIR_THREE_OBJECT_ID = 11L;
  static final long DIR_FOUR_OBJECT_ID = 12L;
  static final long KEY_SEVEN_OBJECT_ID = 13L;
  static final long VOL_TWO_OBJECT_ID = 14L;
  static final long BUCKET_THREE_OBJECT_ID = 15L;
  static final long BUCKET_FOUR_OBJECT_ID = 16L;
  static final long KEY_EIGHT_OBJECT_ID = 17L;
  static final long DIR_FIVE_OBJECT_ID = 18L;
  static final long KEY_NINE_OBJECT_ID = 19L;
  static final long KEY_TEN_OBJECT_ID = 20L;
  static final long KEY_ELEVEN_OBJECT_ID = 21L;
  static final long VOL_THREE_OBJECT_ID = 22L;
  static final long DIR_SIX_OBJECT_ID = 23L;
  static final long DIR_SEVEN_OBJECT_ID = 24L;
  static final long FILE_TWELVE_OBJECT_ID = 25L;
  static final long FILE_THIRTEEN_OBJECT_ID = 26L;
  static final long FILE_FOURTEEN_OBJECT_ID = 27L;
  static final long BUCKET_FIVE_OBJECT_ID = 28L;

  // data size in bytes (unreplicated)
  static final long KEY_ONE_SIZE = 500L; // bin 0
  static final long KEY_TWO_SIZE = OzoneConsts.KB + 1; // bin 1
  static final long KEY_THREE_SIZE = 4 * OzoneConsts.KB + 1; // bin 3
  static final long KEY_FOUR_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  static final long KEY_FIVE_SIZE = 100L; // bin 0
  static final long KEY_SIX_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  static final long KEY_SEVEN_SIZE = 4 * OzoneConsts.KB + 1;
  static final long KEY_EIGHT_SIZE = OzoneConsts.KB + 1; // bin 1
  static final long KEY_NINE_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  static final long KEY_TEN_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  static final long KEY_ELEVEN_SIZE = OzoneConsts.KB + 1; // bin 1
  static final long KEY_TWELVE_SIZE = OzoneConsts.KB;
  static final long KEY_THIRTEEN_SIZE = OzoneConsts.KB;
  static final long KEY_FOURTEEN_SIZE = OzoneConsts.KB;

  // replicated sizes (RATIS/THREE => 3x the unreplicated size)
  static final long FILE1_SIZE_WITH_REPLICA = repl(KEY_ONE_SIZE);
  static final long FILE2_SIZE_WITH_REPLICA = repl(KEY_TWO_SIZE);
  static final long FILE3_SIZE_WITH_REPLICA = repl(KEY_THREE_SIZE);
  static final long FILE4_SIZE_WITH_REPLICA = repl(KEY_FOUR_SIZE);
  static final long FILE5_SIZE_WITH_REPLICA = repl(KEY_FIVE_SIZE);
  static final long FILE6_SIZE_WITH_REPLICA = repl(KEY_SIX_SIZE);
  static final long FILE7_SIZE_WITH_REPLICA = repl(KEY_SEVEN_SIZE);
  static final long FILE8_SIZE_WITH_REPLICA = repl(KEY_EIGHT_SIZE);
  static final long FILE9_SIZE_WITH_REPLICA = repl(KEY_NINE_SIZE);
  static final long FILE10_SIZE_WITH_REPLICA = repl(KEY_TEN_SIZE);
  static final long FILE11_SIZE_WITH_REPLICA = repl(KEY_ELEVEN_SIZE);
  static final long FILE12_SIZE_WITH_REPLICA = repl(KEY_TWELVE_SIZE);
  static final long FILE13_SIZE_WITH_REPLICA = repl(KEY_THIRTEEN_SIZE);
  static final long FILE14_SIZE_WITH_REPLICA = repl(KEY_FOURTEEN_SIZE);

  static final long REPLICA_UNDER_ROOT =
      FILE1_SIZE_WITH_REPLICA + FILE2_SIZE_WITH_REPLICA + FILE3_SIZE_WITH_REPLICA
      + FILE4_SIZE_WITH_REPLICA + FILE5_SIZE_WITH_REPLICA + FILE6_SIZE_WITH_REPLICA
      + FILE7_SIZE_WITH_REPLICA + FILE8_SIZE_WITH_REPLICA + FILE9_SIZE_WITH_REPLICA
      + FILE10_SIZE_WITH_REPLICA + FILE11_SIZE_WITH_REPLICA + FILE12_SIZE_WITH_REPLICA
      + FILE13_SIZE_WITH_REPLICA + FILE14_SIZE_WITH_REPLICA;
  static final long REPLICA_UNDER_VOL =
      FILE1_SIZE_WITH_REPLICA + FILE2_SIZE_WITH_REPLICA + FILE3_SIZE_WITH_REPLICA
      + FILE4_SIZE_WITH_REPLICA + FILE5_SIZE_WITH_REPLICA + FILE6_SIZE_WITH_REPLICA
      + FILE7_SIZE_WITH_REPLICA;
  static final long REPLICA_UNDER_VOL_TWO =
      FILE8_SIZE_WITH_REPLICA + FILE9_SIZE_WITH_REPLICA + FILE10_SIZE_WITH_REPLICA
      + FILE11_SIZE_WITH_REPLICA;
  static final long REPLICA_UNDER_VOL_THREE =
      FILE12_SIZE_WITH_REPLICA + FILE13_SIZE_WITH_REPLICA + FILE14_SIZE_WITH_REPLICA;
  static final long REPLICA_UNDER_BUCKET1 =
      FILE1_SIZE_WITH_REPLICA + FILE2_SIZE_WITH_REPLICA + FILE3_SIZE_WITH_REPLICA
      + FILE6_SIZE_WITH_REPLICA + FILE7_SIZE_WITH_REPLICA;
  static final long REPLICA_UNDER_BUCKET2 =
      FILE4_SIZE_WITH_REPLICA + FILE5_SIZE_WITH_REPLICA;
  static final long REPLICA_UNDER_DIR1 =
      FILE2_SIZE_WITH_REPLICA + FILE3_SIZE_WITH_REPLICA + FILE6_SIZE_WITH_REPLICA
      + FILE7_SIZE_WITH_REPLICA;
  static final long REPLICA_UNDER_DIR2 = FILE2_SIZE_WITH_REPLICA;
  static final long REPLICA_UNDER_KEY = FILE4_SIZE_WITH_REPLICA;
  static final long REPLICA_UNDER_DIR6 =
      FILE13_SIZE_WITH_REPLICA + FILE14_SIZE_WITH_REPLICA;

  // quota in bytes
  static final long ROOT_QUOTA = 2 * (2 * OzoneConsts.MB);
  static final long VOL_QUOTA = 2 * OzoneConsts.MB;
  static final long VOL_TWO_QUOTA = 2 * OzoneConsts.MB;
  static final long VOL_THREE_QUOTA = 2 * OzoneConsts.MB;
  static final long BUCKET_ONE_QUOTA = OzoneConsts.MB;
  static final long BUCKET_TWO_QUOTA = OzoneConsts.MB;
  static final long BUCKET_THREE_QUOTA = OzoneConsts.MB;
  static final long BUCKET_FOUR_QUOTA = OzoneConsts.MB;
  static final long BUCKET_FIVE_QUOTA = OzoneConsts.MB;

  // request paths
  static final String VOL_PATH = "/vol";
  static final String VOL_TWO_PATH = "/vol2";
  static final String VOL_THREE_PATH = "/vol3";
  static final String BUCKET_ONE_PATH = "/vol/bucket1";
  static final String BUCKET_TWO_PATH = "/vol/bucket2";
  static final String DIR_ONE_PATH = "/vol/bucket1/dir1";
  static final String DIR_TWO_PATH = "/vol/bucket1/dir1/dir2";
  static final String DIR_THREE_PATH = "/vol/bucket1/dir1/dir3";
  static final String DIR_FOUR_PATH = "/vol/bucket1/dir1/dir4";
  static final String MULTI_BLOCK_KEY_PATH = "/vol/bucket1/dir1/file7";

  // unreplicated (namespace) data sizes used by non-replica DU and quota
  static final long ROOT_DATA_SIZE = REPLICA_UNDER_ROOT;
  static final long VOL_DATA_SIZE = KEY_ONE_SIZE + KEY_TWO_SIZE + KEY_THREE_SIZE
      + KEY_FOUR_SIZE + KEY_FIVE_SIZE + KEY_SIX_SIZE + KEY_SEVEN_SIZE;
  static final long VOL_TWO_DATA_SIZE =
      KEY_EIGHT_SIZE + KEY_NINE_SIZE + KEY_TEN_SIZE + KEY_ELEVEN_SIZE;
  static final long BUCKET_ONE_DATA_SIZE = KEY_ONE_SIZE + KEY_TWO_SIZE
      + KEY_THREE_SIZE + KEY_SIX_SIZE + KEY_SEVEN_SIZE;
  static final long BUCKET_TWO_DATA_SIZE = KEY_FOUR_SIZE + KEY_FIVE_SIZE;
  static final long DIR_ONE_DATA_SIZE =
      KEY_TWO_SIZE + KEY_THREE_SIZE + KEY_SIX_SIZE + KEY_SEVEN_SIZE;

  protected AbstractTreeNSSummaryScenario(String displayName) {
    super(displayName);
  }

  private static long repl(long size) {
    return getReplicatedSize(size, RatisReplicationConfig.getInstance(THREE));
  }

  abstract BucketLayout getBucketLayout();

  /** FSO leaves this alone; Legacy enables filesystem paths. */
  abstract void configureOmConfiguration(OzoneConfiguration conf);

  abstract void reprocess(ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager reconOMMetadataManager, OzoneConfiguration conf)
      throws IOException;

  /**
   * Write a directory. {@code legacyPath} and {@code fsoParentObjectId} let the
   * FSO (directory table, real parent) and Legacy (key table, path-based)
   * schemes coexist behind one call site.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  abstract void writeDir(ReconOMMetadataManager recon, String dirName,
      String legacyPath, long dirObjectId, long fsoParentObjectId,
      String bucketName, String volName, long bucketObjectId, long volObjectId)
      throws IOException;

  @SuppressWarnings("checkstyle:ParameterNumber")
  abstract void writeReplicatedKey(ReconOMMetadataManager recon, String key,
      String bucket, String vol, String fileName, long keyObjectId,
      long fsoParentObjectId, long bucketObjectId, long volObjectId,
      OmKeyLocationInfoGroup locationGroup, long dataSize) throws IOException;

  @Override
  boolean hasDirectories() {
    return true;
  }

  @Override
  boolean hasVolumeThree() {
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
  OMMetadataManager initializeOmMetadataManager(File omDbDir,
      OzoneConfiguration conf) throws IOException {
    conf.set(OZONE_OM_DB_DIRS, omDbDir.getAbsolutePath());
    configureOmConfiguration(conf);
    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(conf, null);

    putVolume(omMetadataManager, VOL, VOL_OBJECT_ID, VOL_QUOTA);
    putVolume(omMetadataManager, VOL_TWO, VOL_TWO_OBJECT_ID, VOL_TWO_QUOTA);
    putVolume(omMetadataManager, VOL_THREE, VOL_THREE_OBJECT_ID, VOL_THREE_QUOTA);

    putBucket(omMetadataManager, VOL, BUCKET_ONE, BUCKET_ONE_OBJECT_ID,
        BUCKET_ONE_QUOTA);
    putBucket(omMetadataManager, VOL, BUCKET_TWO, BUCKET_TWO_OBJECT_ID,
        BUCKET_TWO_QUOTA);
    putBucket(omMetadataManager, VOL_TWO, BUCKET_THREE, BUCKET_THREE_OBJECT_ID,
        BUCKET_THREE_QUOTA);
    putBucket(omMetadataManager, VOL_TWO, BUCKET_FOUR, BUCKET_FOUR_OBJECT_ID,
        BUCKET_FOUR_QUOTA);
    putBucket(omMetadataManager, VOL_THREE, BUCKET_FIVE, BUCKET_FIVE_OBJECT_ID,
        BUCKET_FIVE_QUOTA);
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
      String bucket, long objectId, long quota) throws IOException {
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setObjectID(objectId)
        .setQuotaInBytes(quota)
        .setBucketLayout(getBucketLayout())
        .build();
    omMetadataManager.getBucketTable().put(
        omMetadataManager.getBucketKey(volume, bucket), bucketInfo);
  }

  @Override
  void populateAndReprocess(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager recon, OzoneConfiguration conf) throws Exception {
    writeDirectories(recon);
    writeVolumeThree(recon);
    writeReplicatedKeys(recon);
    reprocess(reconNamespaceSummaryManager, recon, conf);
  }

  private void writeDirectories(ReconOMMetadataManager recon)
      throws IOException {
    String p = OzoneConsts.OM_KEY_PREFIX;
    writeDir(recon, DIR_ONE, DIR_ONE + p, DIR_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID, BUCKET_ONE, VOL, BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID);
    writeDir(recon, DIR_TWO, DIR_ONE + p + DIR_TWO + p, DIR_TWO_OBJECT_ID,
        DIR_ONE_OBJECT_ID, BUCKET_ONE, VOL, BUCKET_ONE_OBJECT_ID, VOL_OBJECT_ID);
    writeDir(recon, DIR_THREE, DIR_ONE + p + DIR_THREE + p, DIR_THREE_OBJECT_ID,
        DIR_ONE_OBJECT_ID, BUCKET_ONE, VOL, BUCKET_ONE_OBJECT_ID, VOL_OBJECT_ID);
    writeDir(recon, DIR_FOUR, DIR_ONE + p + DIR_FOUR + p, DIR_FOUR_OBJECT_ID,
        DIR_ONE_OBJECT_ID, BUCKET_ONE, VOL, BUCKET_ONE_OBJECT_ID, VOL_OBJECT_ID);
    writeDir(recon, DIR_FIVE, DIR_FIVE + p, DIR_FIVE_OBJECT_ID,
        BUCKET_THREE_OBJECT_ID, BUCKET_THREE, VOL_TWO, BUCKET_THREE_OBJECT_ID,
        VOL_TWO_OBJECT_ID);
  }

  private void writeVolumeThree(ReconOMMetadataManager recon)
      throws IOException {
    String p = OzoneConsts.OM_KEY_PREFIX;
    writeDir(recon, DIR_SIX, DIR_SIX + p, DIR_SIX_OBJECT_ID,
        BUCKET_FIVE_OBJECT_ID, BUCKET_FIVE, VOL_THREE, BUCKET_FIVE_OBJECT_ID,
        VOL_THREE_OBJECT_ID);
    writeDir(recon, DIR_SEVEN, DIR_SIX + p + DIR_SEVEN + p, DIR_SEVEN_OBJECT_ID,
        DIR_SIX_OBJECT_ID, BUCKET_FIVE, VOL_THREE, BUCKET_FIVE_OBJECT_ID,
        VOL_THREE_OBJECT_ID);
    // vol3 keys use the same (object-id parented) call for FSO and Legacy.
    writeRatisKey(recon, KEY_TWELVE, BUCKET_FIVE, VOL_THREE, FILE_TWELVE,
        FILE_TWELVE_OBJECT_ID, BUCKET_FIVE_OBJECT_ID, BUCKET_FIVE_OBJECT_ID,
        VOL_THREE_OBJECT_ID, KEY_TWELVE_SIZE);
    writeRatisKey(recon, KEY_THIRTEEN, BUCKET_FIVE, VOL_THREE, FILE_THIRTEEN,
        FILE_THIRTEEN_OBJECT_ID, DIR_SIX_OBJECT_ID, BUCKET_FIVE_OBJECT_ID,
        VOL_THREE_OBJECT_ID, KEY_THIRTEEN_SIZE);
    writeRatisKey(recon, KEY_FOURTEEN, BUCKET_FIVE, VOL_THREE, FILE_FOURTEEN,
        FILE_FOURTEEN_OBJECT_ID, DIR_SEVEN_OBJECT_ID, BUCKET_FIVE_OBJECT_ID,
        VOL_THREE_OBJECT_ID, KEY_FOURTEEN_SIZE);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private void writeRatisKey(ReconOMMetadataManager recon, String key,
      String bucket, String vol, String fileName, long objectId,
      long parentObjectId, long bucketObjectId, long volObjectId, long dataSize)
      throws IOException {
    writeKeyToOm(recon, key, bucket, vol, fileName, objectId, parentObjectId,
        bucketObjectId, volObjectId, dataSize, getBucketLayout(),
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
        org.apache.hadoop.util.Time.now(), true);
  }

  private void writeReplicatedKeys(ReconOMMetadataManager recon)
      throws IOException {
    OmKeyLocationInfoGroup group1 =
        NSSummaryEndpointTestBase.getLocationInfoGroup1();
    OmKeyLocationInfoGroup group2 =
        NSSummaryEndpointTestBase.getLocationInfoGroup2();
    writeReplicatedKey(recon, KEY_ONE, BUCKET_ONE, VOL, FILE_ONE,
        KEY_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID, group1, KEY_ONE_SIZE);
    writeReplicatedKey(recon, KEY_TWO, BUCKET_ONE, VOL, FILE_TWO,
        KEY_TWO_OBJECT_ID, DIR_TWO_OBJECT_ID, BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID, group2, KEY_TWO_SIZE);
    writeReplicatedKey(recon, KEY_THREE, BUCKET_ONE, VOL, FILE_THREE,
        KEY_THREE_OBJECT_ID, DIR_THREE_OBJECT_ID, BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID, group1, KEY_THREE_SIZE);
    writeReplicatedKey(recon, KEY_FOUR, BUCKET_TWO, VOL, FILE_FOUR,
        KEY_FOUR_OBJECT_ID, BUCKET_TWO_OBJECT_ID, BUCKET_TWO_OBJECT_ID,
        VOL_OBJECT_ID, group2, KEY_FOUR_SIZE);
    writeReplicatedKey(recon, KEY_FIVE, BUCKET_TWO, VOL, FILE_FIVE,
        KEY_FIVE_OBJECT_ID, BUCKET_TWO_OBJECT_ID, BUCKET_TWO_OBJECT_ID,
        VOL_OBJECT_ID, group1, KEY_FIVE_SIZE);
    writeReplicatedKey(recon, KEY_SIX, BUCKET_ONE, VOL, FILE_SIX,
        KEY_SIX_OBJECT_ID, DIR_FOUR_OBJECT_ID, BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID, group2, KEY_SIX_SIZE);
    writeReplicatedKey(recon, KEY_SEVEN, BUCKET_ONE, VOL, FILE_SEVEN,
        KEY_SEVEN_OBJECT_ID, DIR_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID, group1, KEY_SEVEN_SIZE);
    writeReplicatedKey(recon, KEY_EIGHT, BUCKET_THREE, VOL_TWO, FILE_EIGHT,
        KEY_EIGHT_OBJECT_ID, BUCKET_THREE_OBJECT_ID, BUCKET_THREE_OBJECT_ID,
        VOL_TWO_OBJECT_ID, group2, KEY_EIGHT_SIZE);
    writeReplicatedKey(recon, KEY_NINE, BUCKET_THREE, VOL_TWO, FILE_NINE,
        KEY_NINE_OBJECT_ID, DIR_FIVE_OBJECT_ID, BUCKET_THREE_OBJECT_ID,
        VOL_TWO_OBJECT_ID, group1, KEY_NINE_SIZE);
    writeReplicatedKey(recon, KEY_TEN, BUCKET_THREE, VOL_TWO, FILE_TEN,
        KEY_TEN_OBJECT_ID, DIR_FIVE_OBJECT_ID, BUCKET_THREE_OBJECT_ID,
        VOL_TWO_OBJECT_ID, group2, KEY_TEN_SIZE);
    writeReplicatedKey(recon, KEY_ELEVEN, BUCKET_FOUR, VOL_TWO, FILE_ELEVEN,
        KEY_ELEVEN_OBJECT_ID, BUCKET_FOUR_OBJECT_ID, BUCKET_FOUR_OBJECT_ID,
        VOL_TWO_OBJECT_ID, group1, KEY_ELEVEN_SIZE);
  }

  @Override
  void writeMultiBlockKey(ReconOMMetadataManager recon) throws IOException {
    writeReplicatedKey(recon, KEY_SEVEN, BUCKET_ONE, VOL, FILE_SEVEN,
        KEY_SEVEN_OBJECT_ID, DIR_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID, NSSummaryEndpointTestBase.getLocationInfoGroup1(),
        KEY_SEVEN_SIZE);
  }

  @Override
  String multiBlockKeyPath() {
    return MULTI_BLOCK_KEY_PATH;
  }

  @Override
  long multiBlockKeyReplicatedSize() {
    return FILE7_SIZE_WITH_REPLICA;
  }

  // ---------------------------------------------------------------------------
  // Assertions
  // ---------------------------------------------------------------------------

  @Override
  void assertBasicInfoRoot(NSSummaryEndpoint endpoint,
      ReconOMMetadataManager recon) throws Exception {
    String username = "myuser";
    OmPrefixInfo omPrefixInfo = OmPrefixInfo.newBuilder()
        .setName(ROOT_PATH)
        .setObjectID(10)
        .setUpdateID(100)
        .setAcls(singletonList(OzoneAcl.of(USER, username, ACCESS, WRITE)))
        .addAllMetadata(singletonMap("key", "value"))
        .build();
    recon.getPrefixTable().put(OzoneConsts.OM_KEY_PREFIX, omPrefixInfo);

    NamespaceSummaryResponse root = (NamespaceSummaryResponse)
        endpoint.getBasicInfo(ROOT_PATH).getEntity();
    assertEquals(EntityType.ROOT, root.getEntityType());
    assertEquals(3, root.getCountStats().getNumVolume());
    assertEquals(5, root.getCountStats().getNumBucket());
    assertEquals(7, root.getCountStats().getNumTotalDir());
    assertEquals(14, root.getCountStats().getNumTotalKey());
    assertEquals("USER", root.getObjectDBInfo().getAcls().get(0).getType());
    assertEquals("WRITE",
        root.getObjectDBInfo().getAcls().get(0).getAclList().get(0));
    assertEquals(username, root.getObjectDBInfo().getAcls().get(0).getName());
    assertEquals("value", root.getObjectDBInfo().getMetadata().get("key"));
    assertEquals("ACCESS", root.getObjectDBInfo().getAcls().get(0).getScope());
  }

  @Override
  void assertBasicInfoVolume(NSSummaryEndpoint endpoint) throws Exception {
    NamespaceSummaryResponse vol = (NamespaceSummaryResponse)
        endpoint.getBasicInfo(VOL_PATH).getEntity();
    assertEquals(EntityType.VOLUME, vol.getEntityType());
    assertEquals(2, vol.getCountStats().getNumBucket());
    assertEquals(4, vol.getCountStats().getNumTotalDir());
    assertEquals(7, vol.getCountStats().getNumTotalKey());
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
    NamespaceSummaryResponse bucket = (NamespaceSummaryResponse)
        endpoint.getBasicInfo(BUCKET_ONE_PATH).getEntity();
    assertEquals(EntityType.BUCKET, bucket.getEntityType());
    assertEquals(4, bucket.getCountStats().getNumTotalDir());
    assertEquals(5, bucket.getCountStats().getNumTotalKey());
    assertEquals(VOL,
        ((BucketObjectDBInfo) bucket.getObjectDBInfo()).getVolumeName());
    assertEquals(StorageType.DISK,
        ((BucketObjectDBInfo) bucket.getObjectDBInfo()).getStorageType());
    assertEquals(getBucketLayout(),
        ((BucketObjectDBInfo) bucket.getObjectDBInfo()).getBucketLayout());
    assertEquals(BUCKET_ONE,
        ((BucketObjectDBInfo) bucket.getObjectDBInfo()).getName());
  }

  @Override
  void assertBasicInfoBucketTwo(NSSummaryEndpoint endpoint) throws Exception {
    NamespaceSummaryResponse bucket = (NamespaceSummaryResponse)
        endpoint.getBasicInfo(BUCKET_TWO_PATH).getEntity();
    assertEquals(EntityType.BUCKET, bucket.getEntityType());
    assertEquals(0, bucket.getCountStats().getNumTotalDir());
    assertEquals(2, bucket.getCountStats().getNumTotalKey());
    assertEquals(VOL,
        ((BucketObjectDBInfo) bucket.getObjectDBInfo()).getVolumeName());
    assertEquals(StorageType.DISK,
        ((BucketObjectDBInfo) bucket.getObjectDBInfo()).getStorageType());
    assertEquals(getBucketLayout(),
        ((BucketObjectDBInfo) bucket.getObjectDBInfo()).getBucketLayout());
    assertEquals(BUCKET_TWO,
        ((BucketObjectDBInfo) bucket.getObjectDBInfo()).getName());
  }

  /** Directory basic info: identical for FSO and Legacy. */
  void assertBasicInfoDir(NSSummaryEndpoint endpoint) throws Exception {
    NamespaceSummaryResponse dir = (NamespaceSummaryResponse)
        endpoint.getBasicInfo(DIR_ONE_PATH).getEntity();
    assertEquals(EntityType.DIRECTORY, dir.getEntityType());
    assertEquals(3, dir.getCountStats().getNumTotalDir());
    assertEquals(4, dir.getCountStats().getNumTotalKey());
    assertEquals(DIR_ONE, dir.getObjectDBInfo().getName());
    assertEquals(0, dir.getObjectDBInfo().getMetadata().size());
    assertEquals(0, dir.getObjectDBInfo().getQuotaInBytes());
    assertEquals(0, dir.getObjectDBInfo().getQuotaInNamespace());
    assertEquals(0, dir.getObjectDBInfo().getUsedNamespace());
  }

  @Override
  void assertDiskUsageRoot(NSSummaryEndpoint endpoint) throws Exception {
    DUResponse duRootRes =
        NSSummaryEndpointTestBase.getDiskUsage(endpoint, ROOT_PATH, false);
    assertEquals(3, duRootRes.getCount());
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

  /** Bucket-level DU (tree-only): bucket1 -> dir1. */
  void assertDiskUsageBucket(NSSummaryEndpoint endpoint) throws Exception {
    DUResponse duBucketRes =
        NSSummaryEndpointTestBase.getDiskUsage(endpoint, BUCKET_ONE_PATH, false);
    assertEquals(1, duBucketRes.getCount());
    DUResponse.DiskUsage duDir1 = duBucketRes.getDuData().get(0);
    assertEquals(DIR_ONE_PATH, duDir1.getSubpath());
    assertEquals(DIR_ONE_DATA_SIZE, duDir1.getSize());
  }

  /** Directory-level DU (tree-only): dir1 -> dir2, dir3, dir4. */
  void assertDiskUsageDir(NSSummaryEndpoint endpoint) throws Exception {
    DUResponse duDirRes =
        NSSummaryEndpointTestBase.getDiskUsage(endpoint, DIR_ONE_PATH, false);
    assertEquals(3, duDirRes.getCount());
    List<DUResponse.DiskUsage> duSubDir = duDirRes.getDuData();
    Collections.sort(duSubDir,
        Comparator.comparing(DUResponse.DiskUsage::getSubpath));
    assertEquals(DIR_TWO_PATH, duSubDir.get(0).getSubpath());
    assertEquals(KEY_TWO_SIZE, duSubDir.get(0).getSize());
    assertEquals(DIR_THREE_PATH, duSubDir.get(1).getSubpath());
    assertEquals(KEY_THREE_SIZE, duSubDir.get(1).getSize());
    assertEquals(DIR_FOUR_PATH, duSubDir.get(2).getSubpath());
    assertEquals(KEY_SIX_SIZE, duSubDir.get(2).getSize());
  }

  @Override
  void assertDiskUsageKey(NSSummaryEndpoint endpoint) throws Exception {
    DUResponse keyObj =
        NSSummaryEndpointTestBase.getDiskUsage(endpoint, KEY_PATH, false);
    assertEquals(0, keyObj.getCount());
    assertEquals(KEY_FOUR_SIZE, keyObj.getSize());
  }

  @Override
  void assertQuotaUsage(NSSummaryEndpoint endpoint) throws Exception {
    QuotaUsageResponse quRootRes = (QuotaUsageResponse)
        endpoint.getQuotaUsage(ROOT_PATH).getEntity();
    assertEquals(ROOT_QUOTA, quRootRes.getQuota());
    assertEquals(ROOT_DATA_SIZE, quRootRes.getQuotaUsed());

    QuotaUsageResponse quVolRes = (QuotaUsageResponse)
        endpoint.getQuotaUsage(VOL_PATH).getEntity();
    assertEquals(VOL_QUOTA, quVolRes.getQuota());
    assertEquals(VOL_DATA_SIZE, quVolRes.getQuotaUsed());

    QuotaUsageResponse quBucketRes = (QuotaUsageResponse)
        endpoint.getQuotaUsage(BUCKET_ONE_PATH).getEntity();
    assertEquals(BUCKET_ONE_QUOTA, quBucketRes.getQuota());
    assertEquals(BUCKET_ONE_DATA_SIZE, quBucketRes.getQuotaUsed());

    QuotaUsageResponse quBucketRes2 = (QuotaUsageResponse)
        endpoint.getQuotaUsage(BUCKET_TWO_PATH).getEntity();
    assertEquals(BUCKET_TWO_QUOTA, quBucketRes2.getQuota());
    assertEquals(BUCKET_TWO_DATA_SIZE, quBucketRes2.getQuotaUsed());

    QuotaUsageResponse naDir = (QuotaUsageResponse)
        endpoint.getQuotaUsage(DIR_ONE_PATH).getEntity();
    assertEquals(ResponseStatus.TYPE_NOT_APPLICABLE, naDir.getResponseCode());

    QuotaUsageResponse naKey = (QuotaUsageResponse)
        endpoint.getQuotaUsage(KEY_PATH).getEntity();
    assertEquals(ResponseStatus.TYPE_NOT_APPLICABLE, naKey.getResponseCode());

    QuotaUsageResponse invalid = (QuotaUsageResponse)
        endpoint.getQuotaUsage(INVALID_PATH).getEntity();
    assertEquals(ResponseStatus.PATH_NOT_FOUND, invalid.getResponseCode());
  }

  @Override
  void assertFileSizeDist(NSSummaryEndpoint endpoint) throws Exception {
    NSSummaryEndpointTestBase.checkFileSizeDist(endpoint, ROOT_PATH, 5, 3, 4, 2);
    NSSummaryEndpointTestBase.checkFileSizeDist(endpoint, VOL_PATH, 2, 1, 2, 2);
    NSSummaryEndpointTestBase.checkFileSizeDist(
        endpoint, BUCKET_ONE_PATH, 1, 1, 1, 2);
    NSSummaryEndpointTestBase.checkFileSizeDist(
        endpoint, DIR_ONE_PATH, 0, 1, 1, 2);
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

  /** Bucket-level replicated DU (tree-only). */
  void assertDataSizeUnderBucketWithReplication(NSSummaryEndpoint endpoint)
      throws IOException {
    DUResponse res =
        NSSummaryEndpointTestBase.getDiskUsage(endpoint, BUCKET_ONE_PATH, true);
    assertEquals(ResponseStatus.OK, res.getStatus());
    assertEquals(REPLICA_UNDER_BUCKET1, res.getSizeWithReplica());
    assertEquals(REPLICA_UNDER_DIR1,
        res.getDuData().get(0).getSizeWithReplica());
  }

  /** Directory-level replicated DU (tree-only): dir1 -> dir2. */
  void assertDataSizeUnderDirWithReplication(NSSummaryEndpoint endpoint)
      throws IOException {
    DUResponse res =
        NSSummaryEndpointTestBase.getDiskUsage(endpoint, DIR_ONE_PATH, true);
    assertEquals(ResponseStatus.OK, res.getStatus());
    assertEquals(REPLICA_UNDER_DIR1, res.getSizeWithReplica());
    assertEquals(REPLICA_UNDER_DIR2,
        res.getDuData().get(0).getSizeWithReplica());
  }

  @Override
  void assertDataSizeUnderKeyWithReplication(NSSummaryEndpoint endpoint)
      throws IOException {
    DUResponse res =
        NSSummaryEndpointTestBase.getDiskUsage(endpoint, KEY_PATH, true);
    assertEquals(ResponseStatus.OK, res.getStatus());
    assertEquals(REPLICA_UNDER_KEY, res.getSizeWithReplica());
  }

  /**
   * Replicated size propagates from files up through the directory hierarchy
   * (tree-only). With RATIS/THREE keys, each level's replicated size is 3x its
   * unreplicated size.
   */
  void assertReplicatedSizePropagation(NSSummaryEndpoint endpoint)
      throws IOException {
    assertEquals(FILE2_SIZE_WITH_REPLICA, replicaSize(endpoint, DIR_TWO_PATH + "/file2"));
    assertEquals(FILE3_SIZE_WITH_REPLICA, replicaSize(endpoint, DIR_THREE_PATH + "/file3"));
    assertEquals(FILE6_SIZE_WITH_REPLICA, replicaSize(endpoint, DIR_FOUR_PATH + "/file6"));
    assertEquals(FILE7_SIZE_WITH_REPLICA, replicaSize(endpoint, DIR_ONE_PATH + "/file7"));

    assertEquals(FILE2_SIZE_WITH_REPLICA, replicaSize(endpoint, DIR_TWO_PATH));
    assertEquals(FILE3_SIZE_WITH_REPLICA, replicaSize(endpoint, DIR_THREE_PATH));
    assertEquals(FILE6_SIZE_WITH_REPLICA, replicaSize(endpoint, DIR_FOUR_PATH));
    assertEquals(REPLICA_UNDER_DIR1, replicaSize(endpoint, DIR_ONE_PATH));
    assertEquals(REPLICA_UNDER_BUCKET1, replicaSize(endpoint, BUCKET_ONE_PATH));
    assertEquals(REPLICA_UNDER_BUCKET2, replicaSize(endpoint, BUCKET_TWO_PATH));
    assertEquals(REPLICA_UNDER_VOL, replicaSize(endpoint, VOL_PATH));
    assertEquals(REPLICA_UNDER_ROOT, replicaSize(endpoint, ROOT_PATH));
  }

  private long replicaSize(NSSummaryEndpoint endpoint, String path)
      throws IOException {
    return NSSummaryEndpointTestBase.getDiskUsage(endpoint, path, true)
        .getSizeWithReplica();
  }

  /** vol3 RATIS DU (tree-only): asserts both the raw and 3x replicated size. */
  void assertRatisReplicationUnderVolumeThree(NSSummaryEndpoint endpoint)
      throws IOException {
    DUResponse res =
        NSSummaryEndpointTestBase.getDiskUsage(endpoint, VOL_THREE_PATH, true);
    assertEquals(ResponseStatus.OK, res.getStatus());
    assertEquals(KEY_TWELVE_SIZE + KEY_THIRTEEN_SIZE + KEY_FOURTEEN_SIZE,
        res.getSize());
    assertEquals(REPLICA_UNDER_VOL_THREE, res.getSizeWithReplica());
  }

  void assertRatisReplicationUnderBucketFive(NSSummaryEndpoint endpoint)
      throws IOException {
    DUResponse res = NSSummaryEndpointTestBase.getDiskUsage(
        endpoint, VOL_THREE_PATH + "/" + BUCKET_FIVE, true);
    assertEquals(ResponseStatus.OK, res.getStatus());
    assertEquals(KEY_TWELVE_SIZE + KEY_THIRTEEN_SIZE + KEY_FOURTEEN_SIZE,
        res.getSize());
    assertEquals(REPLICA_UNDER_VOL_THREE, res.getSizeWithReplica());
  }

  void assertRatisReplicationUnderDirSix(NSSummaryEndpoint endpoint)
      throws IOException {
    DUResponse res = NSSummaryEndpointTestBase.getDiskUsage(
        endpoint, VOL_THREE_PATH + "/" + BUCKET_FIVE + "/" + DIR_SIX, true);
    assertEquals(ResponseStatus.OK, res.getStatus());
    assertEquals(KEY_THIRTEEN_SIZE + KEY_FOURTEEN_SIZE, res.getSize());
    assertEquals(REPLICA_UNDER_DIR6, res.getSizeWithReplica());
  }
}
