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

package org.apache.hadoop.ozone.om.snapshot;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.hdds.utils.NativeConstants.ROCKS_TOOLS_NATIVE_PROPERTY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_COMPACTION_DAG_PRUNE_DAEMON_RUN_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_CACHE_CLEANUP_SERVICE_RUN_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_DISABLE_NATIVE_LIBS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_UPGRADE_FINALIZATION_CHECK_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.DONE;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.IN_PROGRESS;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.QUEUED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.hdds.utils.db.ManagedRawSSTFileReader;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OMUpgradeTestUtils;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * End-to-end coverage for optimized snapshot diff (native libs enabled).
 */
@EnabledIfSystemProperty(named = ROCKS_TOOLS_NATIVE_PROPERTY, matches = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestOptimizedSnapshotDiff {

  private static final int PAGE_SIZE = 5000;

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private ObjectStore store;
  private final AtomicInteger counter = new AtomicInteger();

  @BeforeAll
  void initCluster() throws Exception {
    assumeTrue(ManagedRawSSTFileReader.tryLoadLibrary());
    var conf = new org.apache.hadoop.hdds.conf.OzoneConfiguration();
    conf.setBoolean(OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF, false);
    conf.setBoolean(OZONE_OM_SNAPSHOT_DIFF_DISABLE_NATIVE_LIBS, false);
    conf.setBoolean(OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_OPTIMIZED_ENABLED, true);
    conf.setEnum(HDDS_DB_PROFILE, DBProfile.TEST);
    conf.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);
    conf.setInt(OMStorage.TESTING_INIT_APPARENT_VERSION_KEY,
        OMLayoutFeature.BUCKET_LAYOUT_SUPPORT.serialize());
    conf.set(OZONE_OM_UPGRADE_FINALIZATION_CHECK_INTERVAL, "10ms");
    conf.setTimeDuration(OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setInt(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL, -1);
    conf.setTimeDuration(OZONE_OM_SNAPSHOT_CACHE_CLEANUP_SERVICE_RUN_INTERVAL, 100, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_OM_SNAPSHOT_COMPACTION_DAG_PRUNE_DAEMON_RUN_INTERVAL, 0, TimeUnit.SECONDS);

    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    store = client.getObjectStore();
    OzoneManagerProtocol omClient = store.getClientProxy().getOzoneManagerClient();
    omClient.finalizeUpgrade();
    OMUpgradeTestUtils.waitForFinalization(omClient);
  }

  @AfterAll
  void shutdown() {
    if (client != null) {
      org.apache.commons.io.IOUtils.closeQuietly(client);
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  void testFsoOptimizedDiffUnorderedThenOrdered() throws Exception {
    String volume = "vol-opt-fso-" + counter.incrementAndGet();
    String bucket = "buc-opt-fso-" + counter.incrementAndGet();
    org.apache.hadoop.ozone.DataTestUtil.createVolumeAndBucket(
        client, volume, bucket, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    String snap1 = "snap1";
    String snap2 = "snap2";
    String rootPath = String.format("%s://%s.%s/", OZONE_URI_SCHEME, bucket, volume);
    try (FileSystem fs = FileSystem.get(new URI(rootPath), cluster.getConf())) {
      createFileKey(fs, "/dir1/dir2/key1");
      store.createSnapshot(volume, bucket, snap1);
      fs.rename(new Path("/dir1/dir2"), new Path("/dir1/dir3"));
      fs.delete(new Path("/dir1/dir3/key1"), false);
      store.createSnapshot(volume, bucket, snap2);
    }

    List<DiffReportEntry> expectedUnordered = Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.DELETE, "dir1/dir3/key1"),
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.RENAME, "dir1/dir2", "dir1/dir3"));

    List<DiffReportEntry> expectedOrdered = Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.RENAME, "dir1/dir2", "dir1/dir3"),
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.DELETE, "dir1/dir3/key1"));

    SnapshotDiffResponse unordered = waitForDoneReport(volume, bucket, snap1, snap2, false);
    assertEquals("Report is dependency unordered.", unordered.getReason());
    assertThat(unordered.getSnapshotDiffReport().getDiffList())
        .containsExactlyInAnyOrderElementsOf(expectedUnordered);

    SnapshotDiffResponse ordered = waitForDoneReport(volume, bucket, snap1, snap2, true);
    assertEquals("Report is dependency ordered.", ordered.getReason());
    assertEquals(expectedOrdered, ordered.getSnapshotDiffReport().getDiffList());
  }

  @Test
  void testObsOptimizedDiff() throws Exception {
    String volume = "vol-opt-obs-" + counter.incrementAndGet();
    String bucket = "buc-opt-obs-" + counter.incrementAndGet();
    OzoneBucket ozoneBucket = org.apache.hadoop.ozone.DataTestUtil.createVolumeAndBucket(
        client, volume, bucket, BucketLayout.OBJECT_STORE);

    String snap1 = "snap1";
    String snap2 = "snap2";
    String key1 = "key1";
    String key2 = "key2";
    String renamedKey = "key3";
    createFileKey(ozoneBucket, key1);
    createFileKey(ozoneBucket, key2);
    store.createSnapshot(volume, bucket, snap1);
    ozoneBucket.renameKey(key2, renamedKey);
    GenericTestUtils.waitFor(() -> {
      try {
        ozoneBucket.getKey(renamedKey);
        return true;
      } catch (IOException e) {
        return false;
      }
    }, 300, 10000);
    ozoneBucket.deleteKey(key1);
    store.createSnapshot(volume, bucket, snap2);

    SnapshotDiffResponse response = waitForDoneReport(volume, bucket, snap1, snap2, false);
    assertEquals("Report is dependency ordered.", response.getReason());
    assertThat(response.getSnapshotDiffReport().getDiffList()).containsExactlyInAnyOrder(
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.DELETE, key1),
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.RENAME, key2, renamedKey));
  }

  private SnapshotDiffResponse waitForDoneReport(String volume, String bucket, String fromSnap,
      String toSnap, boolean orderedReport) throws IOException, InterruptedException {
    store.submitSnapshotDiff(volume, bucket, fromSnap, toSnap, false, false, orderedReport);
    SnapshotDiffResponse response;
    do {
      response = store.snapshotDiff(volume, bucket, fromSnap, toSnap, null, PAGE_SIZE);
      if (response.getJobStatus() == IN_PROGRESS || response.getJobStatus() == QUEUED) {
        Thread.sleep(Math.max(100L, response.getWaitTimeInMs()));
      }
    } while (response.getJobStatus() != DONE);
    return response;
  }

  private static void createFileKey(FileSystem fs, String path)
      throws IOException, InterruptedException, java.util.concurrent.TimeoutException {
    byte[] value = RandomStringUtils.secure().nextAscii(1024).getBytes(UTF_8);
    Path pathVal = new Path(path);
    try (FSDataOutputStream fileKey = fs.create(pathVal)) {
      fileKey.write(value);
    }
    GenericTestUtils.waitFor(() -> {
      try {
        fs.getFileStatus(pathVal);
        return true;
      } catch (IOException e) {
        return false;
      }
    }, 1000, 30000);
  }

  private static void createFileKey(OzoneBucket bucket, String key)
      throws IOException, InterruptedException, java.util.concurrent.TimeoutException {
    byte[] value = RandomStringUtils.secure().nextAscii(1024).getBytes(UTF_8);
    try (OzoneOutputStream fileKey = bucket.createKey(key, value.length)) {
      fileKey.write(value);
    }
    GenericTestUtils.waitFor(() -> {
      try {
        bucket.getKey(key);
        return true;
      } catch (IOException e) {
        return false;
      }
    }, 300, 10000);
  }
}
