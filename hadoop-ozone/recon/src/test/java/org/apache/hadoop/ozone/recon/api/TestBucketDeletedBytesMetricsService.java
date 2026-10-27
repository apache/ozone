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

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_MINIMUM_API_DELAY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_THREAD_COUNT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketDeletedBytes;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils;
import org.apache.hadoop.ozone.recon.api.types.BucketDeletedBytesCompleteResponse;
import org.apache.hadoop.ozone.recon.api.types.BucketDeletedBytesMetrics;
import org.apache.hadoop.ozone.recon.api.types.BucketDeletedBytesProgressResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test class for {@link BucketDeletedBytesMetricsService}.
 */
public class TestBucketDeletedBytesMetricsService {

  @TempDir
  private Path temporaryFolder;

  @Test
  public void testCollectsMetricsAcrossBuckets() throws Exception {
    OzoneManagerServiceProvider ozoneManagerServiceProvider = mock(OzoneManagerServiceProvider.class);
    when(ozoneManagerServiceProvider.getBucketDeletedBytes("sampleVol/bucketOne"))
        .thenReturn(deletedBytes(100L, 50L, 2L, 1L, 0L, 0L, 50L, 0L));
    when(ozoneManagerServiceProvider.getBucketDeletedBytes("sampleVol/bucketTwo"))
        .thenReturn(deletedBytes(200L, 25L, 4L, 1L, 0L, 1L, 10L, 15L));

    BucketDeletedBytesMetricsService service = createService(
        createReconOmMetadataManager("sampleVol", "bucketOne", "bucketTwo"),
        ozoneManagerServiceProvider);

    BucketDeletedBytesCompleteResponse response = awaitCompletedCollection(service);

    assertEquals(2, response.getTotalBucketsQueried());
    assertEquals(0, response.getTotalBucketQueriesFailed());
    assertEquals(75L, response.getTotalPurgeableSize());
    assertEquals(300L, response.getTotalSnapshotTrappedSize());
    assertEquals(2L, response.getTotalPurgeableKeys());
    assertEquals(1L, response.getTotalPurgeableDirs());
    assertEquals(60L, response.getTotalPurgeableKeySize());
    assertEquals(15L, response.getTotalPurgeableDirSize());

    BucketDeletedBytesMetrics bucketOneMetrics = service.getPerBucketMetrics().get("sampleVol/bucketOne");
    assertEquals(100L, bucketOneMetrics.getSnapshotTrappedBytes());
    assertEquals(50L, bucketOneMetrics.getPurgeableBytes());

    BucketDeletedBytesMetrics bucketTwoMetrics = service.getPerBucketMetrics().get("sampleVol/bucketTwo");
    assertEquals(200L, bucketTwoMetrics.getSnapshotTrappedBytes());
    assertEquals(25L, bucketTwoMetrics.getPurgeableBytes());
  }

  @Test
  public void testFailedBucketQueryIsCountedAndSentineled() throws Exception {
    OzoneManagerServiceProvider ozoneManagerServiceProvider = mock(OzoneManagerServiceProvider.class);
    when(ozoneManagerServiceProvider.getBucketDeletedBytes("sampleVol/bucketOne"))
        .thenReturn(deletedBytes(100L, 50L, 2L, 1L, 0L, 0L, 50L, 0L));
    when(ozoneManagerServiceProvider.getBucketDeletedBytes("sampleVol/bucketTwo"))
        .thenThrow(new IOException("bucket unreachable"));

    BucketDeletedBytesMetricsService service = createService(
        createReconOmMetadataManager("sampleVol", "bucketOne", "bucketTwo"),
        ozoneManagerServiceProvider);

    BucketDeletedBytesCompleteResponse response = awaitCompletedCollection(service);

    assertEquals(2, response.getTotalBucketsQueried());
    assertEquals(1, response.getTotalBucketQueriesFailed());
    assertEquals(50L, response.getTotalPurgeableSize());
    assertEquals(100L, response.getTotalSnapshotTrappedSize());
    // The failed bucket's -1 sentinels must not be subtracted from the totals.
    assertEquals(1L, response.getTotalPurgeableKeys());
    assertEquals(0L, response.getTotalPurgeableDirs());
    assertEquals(50L, response.getTotalPurgeableKeySize());
    assertEquals(0L, response.getTotalPurgeableDirSize());

    BucketDeletedBytesMetrics failedBucketMetrics = service.getPerBucketMetrics().get("sampleVol/bucketTwo");
    assertEquals(-1L, failedBucketMetrics.getSnapshotTrappedBytes());
    assertEquals(-1L, failedBucketMetrics.getPurgeableBytes());
  }

  @Test
  public void testPurgeableSplitUnknownWhenOmDoesNotReportIt() throws Exception {
    OzoneManagerServiceProvider ozoneManagerServiceProvider = mock(OzoneManagerServiceProvider.class);
    when(ozoneManagerServiceProvider.getBucketDeletedBytes("sampleVol/bucketOne"))
        .thenReturn(deletedBytes(100L, 50L, 2L, 1L, 0L, 0L, 50L, 0L));
    // Older OM: no key/dir bytes split in the response.
    when(ozoneManagerServiceProvider.getBucketDeletedBytes("sampleVol/bucketTwo"))
        .thenReturn(deletedBytes(200L, 25L, 4L, 1L, 0L, 1L));

    BucketDeletedBytesMetricsService service = createService(
        createReconOmMetadataManager("sampleVol", "bucketOne", "bucketTwo"),
        ozoneManagerServiceProvider);

    BucketDeletedBytesCompleteResponse response = awaitCompletedCollection(service);

    assertEquals(75L, response.getTotalPurgeableSize());
    assertEquals(-1L, response.getTotalPurgeableKeySize());
    assertEquals(-1L, response.getTotalPurgeableDirSize());
  }

  @Test
  public void testNoBucketsFinishesImmediatelyWithZeroTotals() throws Exception {
    OzoneManagerServiceProvider ozoneManagerServiceProvider = mock(OzoneManagerServiceProvider.class);
    File omDbDir = Files.createDirectory(temporaryFolder.resolve("emptyOmDb")).toFile();
    OMMetadataManager omMetadataManager =
        OMMetadataManagerTestUtils.initializeEmptyOmMetadataManager(omDbDir);
    File reconOmDbDir = Files.createDirectory(temporaryFolder.resolve("emptyReconOmDb")).toFile();
    ReconOMMetadataManager reconOMMetadataManager =
        OMMetadataManagerTestUtils.getTestReconOmMetadataManager(omMetadataManager, reconOmDbDir);

    BucketDeletedBytesMetricsService service = createService(reconOMMetadataManager, ozoneManagerServiceProvider);

    BucketDeletedBytesCompleteResponse response = awaitCompletedCollection(service);

    assertEquals(0, response.getTotalBucketsQueried());
    assertEquals(0, response.getTotalBucketQueriesFailed());
    assertEquals(0L, response.getTotalPurgeableSize());
    assertEquals(0L, response.getTotalSnapshotTrappedSize());
    assertTrue(service.getPerBucketMetrics().isEmpty());
  }

  @Test
  public void testUninitializedOmDbFailsAndRetriesOnNextRequest() throws Exception {
    OzoneManagerServiceProvider ozoneManagerServiceProvider = mock(OzoneManagerServiceProvider.class);
    when(ozoneManagerServiceProvider.getBucketDeletedBytes("sampleVol/bucketOne"))
        .thenReturn(deletedBytes(100L, 50L, 2L, 1L, 0L, 0L));
    ReconOMMetadataManager loadedOmDb = createReconOmMetadataManager("sampleVol", "bucketOne");
    // Right after Recon startup the OM DB copy is not loaded yet and the bucket table is null.
    ReconOMMetadataManager reconOMMetadataManager = mock(ReconOMMetadataManager.class);
    when(reconOMMetadataManager.getBucketTable())
        .thenReturn(null)
        .thenReturn(loadedOmDb.getBucketTable());

    BucketDeletedBytesMetricsService service = createService(reconOMMetadataManager, ozoneManagerServiceProvider);

    Object firstResponse = service.getCollectedMetrics();
    assertTrue(firstResponse instanceof BucketDeletedBytesProgressResponse);
    assertEquals(BucketDeletedBytesMetricsService.MetricCollectionStatus.FAILED,
        ((BucketDeletedBytesProgressResponse) firstResponse).getStatus());

    // The failed start must not leave the collection marked as running.
    BucketDeletedBytesCompleteResponse response = awaitCompletedCollection(service);
    assertEquals(1, response.getTotalBucketsQueried());
    assertEquals(50L, response.getTotalPurgeableSize());
  }

  @Test
  public void testRateLimitingSkipsRedundantCollection() throws Exception {
    OzoneManagerServiceProvider ozoneManagerServiceProvider = mock(OzoneManagerServiceProvider.class);
    when(ozoneManagerServiceProvider.getBucketDeletedBytes("sampleVol/bucketOne"))
        .thenReturn(deletedBytes(100L, 50L, 2L, 1L, 0L, 0L));

    OzoneConfiguration config = new OzoneConfiguration();
    config.set(OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_MINIMUM_API_DELAY, "1h");
    config.set(OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_TIMEOUT, "10m");
    config.setInt(OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_THREAD_COUNT, 2);
    BucketDeletedBytesMetricsService service = new BucketDeletedBytesMetricsService(
        createReconOmMetadataManager("sampleVol", "bucketOne"), ozoneManagerServiceProvider, config);

    awaitCompletedCollection(service);
    // A second call within the rate-limit window must not re-query OM.
    service.getCollectedMetrics();
    service.getPerBucketMetrics();

    verify(ozoneManagerServiceProvider, times(1)).getBucketDeletedBytes(eq("sampleVol/bucketOne"));
  }

  private BucketDeletedBytesMetricsService createService(
      ReconOMMetadataManager reconOMMetadataManager,
      OzoneManagerServiceProvider ozoneManagerServiceProvider) {
    OzoneConfiguration config = new OzoneConfiguration();
    config.set(OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_MINIMUM_API_DELAY, "1m");
    config.set(OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_TIMEOUT, "10m");
    config.setInt(OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_THREAD_COUNT, 2);
    return new BucketDeletedBytesMetricsService(reconOMMetadataManager, ozoneManagerServiceProvider, config);
  }

  private ReconOMMetadataManager createReconOmMetadataManager(String volumeName, String... bucketNames)
      throws IOException {
    File omDbDir = Files.createDirectory(
        temporaryFolder.resolve("omDb-" + System.nanoTime())).toFile();
    OzoneConfiguration omConfiguration = new OzoneConfiguration();
    omConfiguration.set(OZONE_OM_DB_DIRS, omDbDir.getAbsolutePath());
    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(omConfiguration, null);

    String volumeKey = omMetadataManager.getVolumeKey(volumeName);
    omMetadataManager.getVolumeTable().put(volumeKey, OmVolumeArgs.newBuilder()
        .setVolume(volumeName)
        .setAdminName(OMMetadataManagerTestUtils.TEST_USER)
        .setOwnerName(OMMetadataManagerTestUtils.TEST_USER)
        .build());

    for (String bucketName : bucketNames) {
      OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .build();
      String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
      omMetadataManager.getBucketTable().put(bucketKey, bucketInfo);
    }

    File reconOmDbDir = Files.createDirectory(
        temporaryFolder.resolve("reconOmDb-" + System.nanoTime())).toFile();
    return OMMetadataManagerTestUtils.getTestReconOmMetadataManager(omMetadataManager, reconOmDbDir);
  }

  private BucketDeletedBytesCompleteResponse awaitCompletedCollection(BucketDeletedBytesMetricsService service)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + 10_000;
    Object response = service.getCollectedMetrics();
    while (!(response instanceof BucketDeletedBytesCompleteResponse)) {
      if (System.currentTimeMillis() > deadline) {
        fail("Bucket deleted bytes collection did not finish in time, last status: "
            + service.getCurrentStatus());
      }
      Thread.sleep(50);
      response = service.getCollectedMetrics();
    }
    return (BucketDeletedBytesCompleteResponse) response;
  }

  private static BucketDeletedBytes deletedBytes(long snapshotTrappedBytes, long purgeableBytes,
      long snapshotTrappedKeys, long purgeableKeys, long snapshotTrappedDirs, long purgeableDirs) {
    return BucketDeletedBytes.newBuilder()
        .setSnapshotTrappedBytes(snapshotTrappedBytes)
        .setPurgeableBytes(purgeableBytes)
        .setSnapshotTrappedKeys(snapshotTrappedKeys)
        .setPurgeableKeys(purgeableKeys)
        .setSnapshotTrappedDirs(snapshotTrappedDirs)
        .setPurgeableDirs(purgeableDirs)
        .build();
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private static BucketDeletedBytes deletedBytes(long snapshotTrappedBytes, long purgeableBytes,
      long snapshotTrappedKeys, long purgeableKeys, long snapshotTrappedDirs, long purgeableDirs,
      long purgeableKeyBytes, long purgeableDirBytes) {
    return BucketDeletedBytes.newBuilder()
        .setSnapshotTrappedBytes(snapshotTrappedBytes)
        .setPurgeableBytes(purgeableBytes)
        .setSnapshotTrappedKeys(snapshotTrappedKeys)
        .setPurgeableKeys(purgeableKeys)
        .setSnapshotTrappedDirs(snapshotTrappedDirs)
        .setPurgeableDirs(purgeableDirs)
        .setPurgeableKeyBytes(purgeableKeyBytes)
        .setPurgeableDirBytes(purgeableDirBytes)
        .build();
  }
}
