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
import static org.apache.hadoop.hdds.utils.NativeConstants.ROCKS_TOOLS_NATIVE_PROPERTY;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.FILE_SYSTEM_OPTIMIZED;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.OBJECT_STORE;
import static org.apache.ozone.test.OzoneTestBase.uniqueObjectName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.ozone.DataTestUtil;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.service.SnapshotDeletingService;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Optimized snapshot diff integration tests (single cluster, native SST diff enabled).
 */
@EnabledIfSystemProperty(named = ROCKS_TOOLS_NATIVE_PROPERTY, matches = "true")
public class TestOptimizedSnapshotDiff extends OmSnapshotTests {

  public TestOptimizedSnapshotDiff() throws Exception {
    super(FILE_SYSTEM_OPTIMIZED, false, false, false, false, true);
  }

  @Override
  protected boolean isOptimizedSnapshotDiffOnlySuite() {
    return true;
  }

  @Test
  public void testOptimizedDiffUnorderedThenOrdered() throws Exception {
    String snap1 = "snap1";
    String snap2 = "snap2";
    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, getBucketName(), getVolumeName());
    try (FileSystem fs = FileSystem.get(new URI(rootPath), getCluster().getConf())) {
      createFileKey(fs, "/dir1/dir2/key1");
      createFileKey(fs, "/dir1/dir3/key1");
      createSnapshot(getVolumeName(), getBucketName(), snap1);
      fs.rename(new Path("/dir1/dir2/key1"), new Path("/dir1/dir3/renamed_key"));
      fs.delete(new Path("/dir1/dir2"), true);
      createSnapshot(getVolumeName(), getBucketName(), snap2);
    }

    List<DiffReportEntry> expectedUnordered = Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.DELETE, "dir1/dir2"),
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.RENAME, "dir1/dir2/key1", "dir1/dir3/renamed_key"));

    List<DiffReportEntry> expectedOrdered = Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.RENAME, "dir1/dir2/key1", "dir1/dir3/renamed_key"),
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.DELETE, "dir1/dir2"));

    SnapshotDiffResponse unordered = waitForDoneReportAfterSubmit(
        getVolumeName(), getBucketName(), snap1, snap2, false, false);
    assertEquals("Report is dependency unordered.", unordered.getReason());
    assertEquals(expectedUnordered, unordered.getSnapshotDiffReport().getDiffList());

    SnapshotDiffResponse ordered = waitForDoneReportAfterSubmit(
        getVolumeName(), getBucketName(), snap1, snap2, false, true);
    assertEquals("Report is dependency ordered.", ordered.getReason());
    assertEquals(expectedOrdered, ordered.getSnapshotDiffReport().getDiffList());
  }

  @Test
  public void testOptimizedObsDiff() throws Exception {
    String volume = uniqueObjectName("vol-opt-obs-");
    String bucket = uniqueObjectName("buc-opt-obs-");
    OzoneBucket obsBucket;
    try (OzoneClient obsClient = getCluster().newClient()) {
      obsBucket = DataTestUtil.createVolumeAndBucket(obsClient, volume, bucket, OBJECT_STORE);
      String snap1 = "snap1";
      String snap2 = "snap2";
      String key1 = "key1";
      String key2 = "key2";
      String renamedKey = "key3";
      createObsKey(obsBucket, key1);
      createObsKey(obsBucket, key2);
      createSnapshot(volume, bucket, snap1);
      obsBucket.renameKey(key2, renamedKey);
      GenericTestUtils.waitFor(() -> {
        try {
          obsBucket.getKey(renamedKey);
          return true;
        } catch (IOException e) {
          return false;
        }
      }, 300, 10000);
      obsBucket.deleteKey(key1);
      createSnapshot(volume, bucket, snap2);

      SnapshotDiffResponse response = waitForDoneReportAfterSubmit(
          volume, bucket, snap1, snap2, false, false);
      assertEquals("Report is dependency ordered.", response.getReason());
      assertThat(response.getSnapshotDiffReport().getDiffList()).containsExactlyInAnyOrder(
          SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.DELETE, key1),
          SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.RENAME, key2, renamedKey));
    }
  }

  @Test
  public void testForcedFullDiffAfterSnapshotPurge() throws Exception {
    String snap0 = "snap-purge-0";
    String snap1 = "snap-purge-1";
    String snap2 = "snap-purge-2";
    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, getBucketName(), getVolumeName());
    try (FileSystem fs = FileSystem.get(new URI(rootPath), getCluster().getConf())) {
      createFileKey(fs, "/purge/k1");
      createSnapshot(getVolumeName(), getBucketName(), snap0);
      createSnapshot(getVolumeName(), getBucketName(), snap1);
      assertThat(fs.rename(new Path("/purge/k1"), new Path("/purge/k1r"))).isTrue();
      createFileKey(fs, "/purge/k2");
      createSnapshot(getVolumeName(), getBucketName(), snap2);
    }

    // Purging snap0 moves lastTransactionInfo of the next snapshot (snap1) past the changes above.
    try (OzoneClient client = getCluster().newClient()) {
      client.getObjectStore().deleteSnapshot(getVolumeName(), getBucketName(), snap0);
    }
    SnapshotDeletingService snapshotDeletingService =
        new SnapshotDeletingService(1000, 60000, getCluster().getOzoneManager());
    try {
      GenericTestUtils.waitFor(() -> {
        try {
          snapshotDeletingService.runPeriodicalTaskNow();
          return getSnapshotInfo(snap0) == null;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }, 500, 60000);
    } finally {
      snapshotDeletingService.shutdown();
    }
    SnapshotInfo fromSnapshot = getSnapshotInfo(snap1);
    assertThat(TransactionInfo.fromByteString(fromSnapshot.getLastTransactionInfo()).getTransactionIndex())
        .isGreaterThan(TransactionInfo.fromByteString(fromSnapshot.getCreateTransactionInfo()).getTransactionIndex());

    for (boolean ordered : new boolean[] {false, true}) {
      SnapshotDiffResponse response = waitForDoneReportAfterSubmit(
          getVolumeName(), getBucketName(), snap1, snap2, true, ordered);
      assertThat(response.getSnapshotDiffReport().getDiffList()).containsExactlyInAnyOrder(
          SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.RENAME, "purge/k1", "purge/k1r"),
          SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.CREATE, "purge/k2"));
    }
  }

  @Test
  public void testForcedFullDiffFromNewerToOlderSnapshot() throws Exception {
    String snap1 = "snap-reverse-1";
    String snap2 = "snap-reverse-2";
    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, getBucketName(), getVolumeName());
    try (FileSystem fs = FileSystem.get(new URI(rootPath), getCluster().getConf())) {
      createFileKey(fs, "/reverse/k1");
      createSnapshot(getVolumeName(), getBucketName(), snap1);
      assertThat(fs.rename(new Path("/reverse/k1"), new Path("/reverse/k1r"))).isTrue();
      createFileKey(fs, "/reverse/k2");
      createSnapshot(getVolumeName(), getBucketName(), snap2);
    }

    // Every row of the older (to) snapshot was written before the newer (from) snapshot was created.
    for (boolean ordered : new boolean[] {false, true}) {
      SnapshotDiffResponse response = waitForDoneReportAfterSubmit(
          getVolumeName(), getBucketName(), snap2, snap1, true, ordered);
      assertThat(response.getSnapshotDiffReport().getDiffList()).containsExactlyInAnyOrder(
          SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.RENAME, "reverse/k1r", "reverse/k1"),
          SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.DELETE, "reverse/k2"));
    }
  }

  private SnapshotInfo getSnapshotInfo(String snapshotName) throws IOException {
    return getCluster().getOzoneManager().getMetadataManager().getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(getVolumeName(), getBucketName(), snapshotName));
  }

  private static void createObsKey(OzoneBucket bucket, String key) throws Exception {
    byte[] value = RandomStringUtils.secure().nextAscii(10240).getBytes(UTF_8);
    OzoneOutputStream fileKey = bucket.createKey(key, value.length);
    fileKey.write(value);
    fileKey.close();
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
