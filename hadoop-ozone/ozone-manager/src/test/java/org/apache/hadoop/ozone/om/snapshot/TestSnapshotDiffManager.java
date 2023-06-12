/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.snapshot;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.SnapshotDiffJob;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.util.ClosableIterator;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.DELIMITER;

/**
 * Tests for {@link SnapshotDiffManager}.
 */
public class TestSnapshotDiffManager {

  private static final String VOLUME = "vol";
  private static final String BUCKET = "bucket";

  private static File metaDir;
  private static OzoneManager ozoneManager;
  private static OMMetadataManager omMetadataManager;
  private static SnapshotDiffManager snapshotDiffManager;
  private static PersistentMap<String, SnapshotDiffJob> snapDiffJobTable;

  @BeforeAll
  public static void init() throws AuthenticationException,
      IOException, RocksDBException {
    metaDir = GenericTestUtils.getRandomizedTestDir();
    if (!metaDir.exists()) {
      Assertions.assertTrue(metaDir.mkdirs());
    }
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        metaDir.getAbsolutePath());

    OmTestManagers omTestManagers = new OmTestManagers(conf);
    ozoneManager = omTestManagers.getOzoneManager();
    omMetadataManager = omTestManagers.getMetadataManager();

    snapshotDiffManager = ozoneManager
        .getOmSnapshotManager().getSnapshotDiffManager();
    snapDiffJobTable = snapshotDiffManager.getSnapDiffJobTable();

    createVolumeAndBucket();
  }

  @AfterAll
  public static void cleanUp() {
    FileUtil.fullyDelete(metaDir);
  }

  /**
   * Clear the SnapshotDiffTable before every test run.
   */
  @BeforeEach
  public void setUp() {
    ClosableIterator<Map.Entry<String, SnapshotDiffJob>>
        iterator = snapDiffJobTable.iterator();
    while (iterator.hasNext()) {
      String key = iterator.next().getKey();
      snapDiffJobTable.remove(key);
    }
  }

  private static Stream<Arguments> listSnapshotDiffJobsScenarios() {
    return Stream.of(
        Arguments.of("queued", false, false),
        Arguments.of("done", false, false),
        Arguments.of("in_progress", false, true),
        Arguments.of("queued", true, true),
        Arguments.of("done", true, true),
        Arguments.of("in_progress", true, true),
        Arguments.of("invalid", true, true),
        Arguments.of("", true, true)
    );
  }

  @ParameterizedTest
  @MethodSource("listSnapshotDiffJobsScenarios")
  public void testListSnapshotDiffJobs(String jobStatus,
                                       boolean listAll,
                                       boolean containsJob)
      throws IOException {
    String fromSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);
    String toSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);
    String fromSnapshotId = UUID.randomUUID().toString();
    String toSnapshotId = UUID.randomUUID().toString();
    String diffJobKey = fromSnapshotId + DELIMITER + toSnapshotId;

    setUpKeysAndSnapshots(fromSnapshotName, toSnapshotName,
        fromSnapshotId, toSnapshotId);

    SnapshotDiffJob diffJob = snapDiffJobTable.get(diffJobKey);
    Assertions.assertNull(diffJob);

    // There are no jobs in the table, therefore
    // the response list should be empty.
    List<SnapshotDiffJob> jobList = snapshotDiffManager
        .getSnapshotDiffJobList(VOLUME, BUCKET, jobStatus, listAll);
    Assertions.assertTrue(jobList.isEmpty());

    SnapshotDiffResponse snapshotDiffResponse = snapshotDiffManager
        .getSnapshotDiffReport(VOLUME, BUCKET,
            fromSnapshotName, toSnapshotName,
            0, 0, false);

    Assertions.assertEquals(JobStatus.IN_PROGRESS,
        snapshotDiffResponse.getJobStatus());

    diffJob = snapDiffJobTable.get(diffJobKey);
    Assertions.assertNotNull(diffJob);
    Assertions.assertEquals(JobStatus.IN_PROGRESS,
        diffJob.getStatus());

    jobList = snapshotDiffManager
        .getSnapshotDiffJobList(VOLUME, BUCKET, jobStatus, listAll);

    // When listAll is true, jobStatus is ignored.
    // If the job is IN_PROGRESS or listAll is used,
    // there should be a response.
    // Otherwise, response list should be empty.
    if (containsJob) {
      Assertions.assertTrue(jobList.contains(diffJob));
    } else {
      Assertions.assertTrue(jobList.isEmpty());
    }
  }

  @Test
  public void testListSnapDiffWithInvalidStatus() throws IOException {
    String fromSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);
    String toSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);
    String fromSnapshotId = UUID.randomUUID().toString();
    String toSnapshotId = UUID.randomUUID().toString();

    setUpKeysAndSnapshots(fromSnapshotName, toSnapshotName,
        fromSnapshotId, toSnapshotId);

    snapshotDiffManager.getSnapshotDiffReport(VOLUME, BUCKET,
        fromSnapshotName, toSnapshotName, 0, 0, false);

    // Invalid status, without listAll true, results in an exception.
    Assertions.assertThrows(IOException.class, () -> snapshotDiffManager
        .getSnapshotDiffJobList(VOLUME, BUCKET, "invalid", false));
  }

  private void setUpKeysAndSnapshots(String fromSnapshotName,
                                     String toSnapshotName,
                                     String fromSnapshotId,
                                     String toSnapshotId)
      throws IOException {
    // Get IDs.
    long volumeId = omMetadataManager
        .getVolumeId(VOLUME);
    long bucketId = omMetadataManager
        .getBucketId(VOLUME, BUCKET);

    // Create 5 keys.
    for (int i = 0; i < 5; i++) {
      OmKeyInfo omKeyInfo = createOmKeyInfo(bucketId);
      String tableKey = omMetadataManager.getOzonePathKey(volumeId,
          bucketId, bucketId, omKeyInfo.getFileName());
      omMetadataManager.getFileTable()
          .addCacheEntry(new CacheKey<>(tableKey),
              CacheValue.get(1, omKeyInfo));
      omMetadataManager.getFileTable().put(tableKey, omKeyInfo);
    }

    // Create 1st snapshot and put it in SnapshotTable.
    SnapshotInfo fromSnapshotInfo = SnapshotInfo
        .newInstance(VOLUME, BUCKET,
            fromSnapshotName, fromSnapshotId,
            System.currentTimeMillis());
    fromSnapshotInfo.setSnapshotStatus(SnapshotInfo
        .SnapshotStatus.SNAPSHOT_ACTIVE);

    String fromSnapKey = fromSnapshotInfo.getTableKey();

    OmSnapshot omSnapshotFrom = new OmSnapshot(
        ozoneManager.getKeyManager(), ozoneManager.getPrefixManager(),
        ozoneManager, VOLUME, BUCKET, fromSnapshotName);

    ozoneManager.getOmSnapshotManager().getSnapshotCache()
        .put(fromSnapKey, omSnapshotFrom);

    omMetadataManager.getSnapshotInfoTable()
        .addCacheEntry(new CacheKey<>(fromSnapKey),
            CacheValue.get(1, fromSnapshotInfo));
    omMetadataManager
        .getSnapshotInfoTable().put(fromSnapKey, fromSnapshotInfo);

    // Create 20 keys.
    for (int i = 0; i < 20; i++) {
      OmKeyInfo omKeyInfo = createOmKeyInfo(bucketId);
      String tableKey = omMetadataManager.getOzonePathKey(volumeId,
          bucketId, bucketId, omKeyInfo.getFileName());
      omMetadataManager.getFileTable()
          .addCacheEntry(new CacheKey<>(tableKey),
              CacheValue.get(1, omKeyInfo));
      omMetadataManager.getFileTable().put(tableKey, omKeyInfo);
    }

    // Create 2nd snapshot and put it in SnapshotTable.
    SnapshotInfo toSnapshotInfo = SnapshotInfo
        .newInstance(VOLUME, BUCKET,
            toSnapshotName, toSnapshotId,
            System.currentTimeMillis());
    fromSnapshotInfo.setSnapshotStatus(SnapshotInfo
        .SnapshotStatus.SNAPSHOT_ACTIVE);

    String toSnapKey = toSnapshotInfo.getTableKey();

    OmSnapshot omSnapshotTo = new OmSnapshot(
        ozoneManager.getKeyManager(), ozoneManager.getPrefixManager(),
        ozoneManager, VOLUME, BUCKET, toSnapshotName);

    ozoneManager.getOmSnapshotManager().getSnapshotCache()
        .put(toSnapKey, omSnapshotTo);

    omMetadataManager.getSnapshotInfoTable()
        .addCacheEntry(new CacheKey<>(toSnapKey),
            CacheValue.get(1, toSnapshotInfo));
    omMetadataManager
        .getSnapshotInfoTable().put(toSnapKey, toSnapshotInfo);
  }

  private static void createVolumeAndBucket() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    // Create volume and put it in VolumeTable.
    OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(VOLUME)
        .setAdminName(ugi.getShortUserName())
        .setOwnerName(ugi.getShortUserName())
        .build();
    String volumeKey = omMetadataManager
        .getVolumeKey(VOLUME);
    omMetadataManager.getVolumeTable()
        .addCacheEntry(new CacheKey<>(volumeKey),
            CacheValue.get(1, volumeArgs));
    omMetadataManager.getVolumeTable()
        .put(volumeKey, volumeArgs);

    // Create bucket and put it in BucketTable.
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .setOwner(ugi.getShortUserName())
        .build();
    String bucketKey = omMetadataManager
        .getBucketKey(VOLUME, BUCKET);

    omMetadataManager.getBucketTable()
        .addCacheEntry(new CacheKey<>(bucketKey),
            CacheValue.get(1, bucketInfo));
    omMetadataManager.getBucketTable()
        .put(bucketKey, bucketInfo);
  }

  private OmKeyInfo createOmKeyInfo(long parentObjectId) {
    String keyName = "key-" + RandomStringUtils.randomNumeric(5);
    long objectId = ThreadLocalRandom.current().nextLong(100);

    return new OmKeyInfo.Builder()
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setKeyName(keyName)
        .setFileName(keyName)
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(ONE))
        .setObjectID(objectId)
        .setParentObjectID(parentObjectId)
        .setDataSize(500L)
        .setCreationTime(System.currentTimeMillis())
        .build();
  }
}
