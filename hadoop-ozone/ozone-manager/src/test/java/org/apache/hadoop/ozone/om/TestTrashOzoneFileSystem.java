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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.fs.FileSystem.TRASH_PREFIX;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_LISTING_PAGE_SIZE_MAX;
import static org.apache.hadoop.ozone.om.ratis.utils.ProtocolMessageMetricsTestUtils.getRequestCount;
import static org.apache.hadoop.ozone.om.ratis.utils.ProtocolMessageMetricsTestUtils.getRequestTime;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.DBConfigFromFile;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.security.SecurityUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for {@link TrashOzoneFileSystem}.
 */
class TestTrashOzoneFileSystem {

  private static final int PAGE_SIZE = 5;
  private static final int TRASH_ROOT_COUNT = 12;

  private final AtomicInteger objectId = new AtomicInteger();

  private OmTestManagers newOmTestManagers(File testDir) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    System.setProperty(DBConfigFromFile.CONFIG_DIR, "/");
    ServerUtils.setOzoneMetaDirPath(conf, testDir.toString());
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200, TimeUnit.MILLISECONDS);
    conf.setQuietMode(false);
    conf.setInt(OZONE_FS_LISTING_PAGE_SIZE_MAX, PAGE_SIZE);
    return new OmTestManagers(conf);
  }

  @ParameterizedTest
  @ValueSource(strings = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
  void testGetTrashRootsBeyondPageSize(BucketLayout bucketLayout,
      @TempDir File testDir) throws Exception {
    OmTestManagers omTestManagers = newOmTestManagers(testDir);
    try {
      OzoneManager om = omTestManagers.getOzoneManager();
      OzoneManagerProtocol writeClient = omTestManagers.getWriteClient();
      final String volumeName = "vol-" + objectId.incrementAndGet();
      final String bucketName = "bucket-" + objectId.incrementAndGet();
      createVolumeAndBucket(omTestManagers, volumeName, bucketName, bucketLayout,
          writeClient);
      // Create more trash roots than the configured listing page size so that a
      // single listStatus call cannot return all of them.
      for (int i = 0; i < TRASH_ROOT_COUNT; i++) {
        createDirectory(writeClient, volumeName, bucketName,
            TRASH_PREFIX + "/user" + i);
      }

      try (FileSystem fs = SecurityUtil.doAsLoginUser(
          (PrivilegedExceptionAction<FileSystem>)
              () -> new TrashOzoneFileSystem(om))) {
        Collection<FileStatus> trashRoots = fs.getTrashRoots(true);
        assertEquals(TRASH_ROOT_COUNT, trashRoots.size());
      }
    } finally {
      omTestManagers.stop();
    }
  }

  /**
   * The Trash emptier renames and deletes trash directories in FSO buckets through the
   * TrashOzoneFileSystem, submitting internal {@code RenameKey} and {@code DeleteKey} requests.
   * Both should be counted in the OmClientProtocol per-type metrics just like a client request.
   */
  @Test
  void testRenameAndDeleteInFsoBucketIncrementMetrics(@TempDir File testDir) throws Exception {
    OmTestManagers omTestManagers = newOmTestManagers(testDir);
    try {
      OzoneManager om = omTestManagers.getOzoneManager();
      OzoneManagerProtocol writeClient = omTestManagers.getWriteClient();
      final String volumeName = "vol-" + objectId.incrementAndGet();
      final String bucketName = "bucket-" + objectId.incrementAndGet();
      createVolumeAndBucket(omTestManagers, volumeName, bucketName,
          BucketLayout.FILE_SYSTEM_OPTIMIZED, writeClient);
      createDirectory(writeClient, volumeName, bucketName, TRASH_PREFIX + "/user1");

      Path src = trashPath(volumeName, bucketName, "user1");
      Path dst = trashPath(volumeName, bucketName, "user2");
      try (FileSystem fs = SecurityUtil.doAsLoginUser(
          (PrivilegedExceptionAction<FileSystem>) () -> new TrashOzoneFileSystem(om))) {
        // This OM is freshly created, so no RenameKey call has been recorded yet.
        assertEquals(0, getRequestCount(om.getOmClientProtocolMetrics(), Type.RenameKey));
        assertEquals(0, getRequestTime(om.getOmClientProtocolMetrics(), Type.RenameKey));
        fs.rename(src, dst);
        assertThat(getRequestCount(om.getOmClientProtocolMetrics(), Type.RenameKey)).isGreaterThan(0L);
        assertThat(getRequestTime(om.getOmClientProtocolMetrics(), Type.RenameKey)).isGreaterThan(0L);

        // Likewise no DeleteKey call has been recorded yet.
        assertEquals(0, getRequestCount(om.getOmClientProtocolMetrics(), Type.DeleteKey));
        assertEquals(0, getRequestTime(om.getOmClientProtocolMetrics(), Type.DeleteKey));
        fs.delete(dst, true);
        assertThat(getRequestCount(om.getOmClientProtocolMetrics(), Type.DeleteKey)).isGreaterThan(0L);
        assertThat(getRequestTime(om.getOmClientProtocolMetrics(), Type.DeleteKey)).isGreaterThan(0L);
      }
    } finally {
      omTestManagers.stop();
    }
  }

  /**
   * The Trash emptier deletes trash contents in non-FSO buckets one key at a time through the
   * TrashOzoneFileSystem, submitting an internal {@code DeleteKeys} request per key. These should
   * be counted in the OmClientProtocol per-type metrics just like a client request.
   */
  @Test
  void deleteInObjectStoreBucketIncrementsDeleteKeysMetric(@TempDir File testDir) throws Exception {
    OmTestManagers omTestManagers = newOmTestManagers(testDir);
    try {
      OzoneManager om = omTestManagers.getOzoneManager();
      OzoneManagerProtocol writeClient = omTestManagers.getWriteClient();
      final String volumeName = "vol-" + objectId.incrementAndGet();
      final String bucketName = "bucket-" + objectId.incrementAndGet();
      createVolumeAndBucket(omTestManagers, volumeName, bucketName,
          BucketLayout.OBJECT_STORE, writeClient);
      createDirectory(writeClient, volumeName, bucketName, TRASH_PREFIX + "/user1");

      Path trashRoot = new Path("/" + volumeName + "/" + bucketName + "/" + TRASH_PREFIX);
      try (FileSystem fs = SecurityUtil.doAsLoginUser(
          (PrivilegedExceptionAction<FileSystem>) () -> new TrashOzoneFileSystem(om))) {
        // This OM is freshly created, so no DeleteKeys call has been recorded yet.
        assertEquals(0, getRequestCount(om.getOmClientProtocolMetrics(), Type.DeleteKeys));
        assertEquals(0, getRequestTime(om.getOmClientProtocolMetrics(), Type.DeleteKeys));
        fs.delete(trashRoot, true);
        assertThat(getRequestCount(om.getOmClientProtocolMetrics(), Type.DeleteKeys)).isGreaterThan(0L);
        assertThat(getRequestTime(om.getOmClientProtocolMetrics(), Type.DeleteKeys)).isGreaterThan(0L);
      }
    } finally {
      omTestManagers.stop();
    }
  }

  private static Path trashPath(String volumeName, String bucketName, String userDir) {
    return new Path("/" + volumeName + "/" + bucketName + "/" + TRASH_PREFIX + "/" + userDir);
  }

  private void createVolumeAndBucket(OmTestManagers omTestManagers,
      String volumeName, String bucketName, BucketLayout bucketLayout,
      OzoneManagerProtocol writeClient) throws IOException {
    OMRequestTestUtils.addVolumeToOM(omTestManagers.getMetadataManager(),
        OmVolumeArgs.newBuilder()
            .setOwnerName("o")
            .setAdminName("a")
            .setVolume(volumeName)
            .setObjectID(objectId.incrementAndGet())
            .build());
    OMRequestTestUtils.addBucketToOM(omTestManagers.getMetadataManager(),
        OmBucketInfo.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setBucketLayout(bucketLayout)
            .setOwner("o")
            .setObjectID(objectId.incrementAndGet())
            .build());
    writeClient.createDirectory(new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(TRASH_PREFIX)
        .setOwnerName("test")
        .build());
  }

  private void createDirectory(OzoneManagerProtocol writeClient,
      String volumeName, String bucketName, String dirName) throws IOException {
    OmKeyArgs keyArg = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(dirName)
        .setOwnerName("test")
        .build();
    writeClient.createDirectory(keyArg);
  }
}
