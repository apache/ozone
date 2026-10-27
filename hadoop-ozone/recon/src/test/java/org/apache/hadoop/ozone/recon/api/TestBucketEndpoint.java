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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.om.helpers.BucketDeletedBytes;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.recon.api.types.BucketDeletedBytesMetrics;
import org.apache.hadoop.ozone.recon.api.types.BucketObjectDBInfo;
import org.apache.hadoop.ozone.recon.api.types.BucketsResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link BucketEndpoint}.
 */
public class TestBucketEndpoint {

  @Test
  public void testGetBucketsMergesTrappedBytesFromMetricsService() throws Exception {
    ReconOMMetadataManager omMetadataManager = mock(ReconOMMetadataManager.class);
    OmBucketInfo bucketOne = OmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .build();
    OmBucketInfo bucketTwo = OmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketTwo")
        .build();
    when(omMetadataManager.listBucketsUnderVolume("sampleVol", "", 1000))
        .thenReturn(Arrays.asList(bucketOne, bucketTwo));

    BucketDeletedBytesMetricsService bucketDeletedBytesMetricsService =
        mock(BucketDeletedBytesMetricsService.class);
    Map<String, BucketDeletedBytesMetrics> perBucketMetrics = new HashMap<>();
    perBucketMetrics.put("sampleVol/bucketOne",
        new BucketDeletedBytesMetrics("sampleVol", "bucketOne", deletedBytes(100L, 50L, 2L, 1L, 0L, 0L)));
    when(bucketDeletedBytesMetricsService.getPerBucketMetrics()).thenReturn(perBucketMetrics);
    when(bucketDeletedBytesMetricsService.getCurrentStatus())
        .thenReturn(BucketDeletedBytesMetricsService.MetricCollectionStatus.IN_PROGRESS);

    BucketEndpoint bucketEndpoint = new BucketEndpoint(omMetadataManager, bucketDeletedBytesMetricsService);
    Response response = bucketEndpoint.getBuckets("sampleVol", 1000, "");

    BucketsResponse bucketsResponse = (BucketsResponse) response.getEntity();
    assertEquals(2, bucketsResponse.getTotalCount());
    assertEquals(BucketDeletedBytesMetricsService.MetricCollectionStatus.IN_PROGRESS,
        bucketsResponse.getTrappedBytesCollectionStatus());

    List<BucketObjectDBInfo> buckets = (List<BucketObjectDBInfo>) bucketsResponse.getBuckets();
    BucketObjectDBInfo bucketOneInfo = buckets.stream()
        .filter(b -> b.getName().equals("bucketOne")).findFirst().orElseThrow(AssertionError::new);
    assertEquals(100L, bucketOneInfo.getTrappedBytes());
    assertEquals(2L, bucketOneInfo.getTrappedKeys());

    BucketObjectDBInfo bucketTwoInfo = buckets.stream()
        .filter(b -> b.getName().equals("bucketTwo")).findFirst().orElseThrow(AssertionError::new);
    assertEquals(-1L, bucketTwoInfo.getTrappedBytes());
    assertEquals(-1L, bucketTwoInfo.getTrappedKeys());
  }

  @Test
  public void testGetBucketsWithNoCollectedMetricsFallsBackToSentinel() throws Exception {
    ReconOMMetadataManager omMetadataManager = mock(ReconOMMetadataManager.class);
    OmBucketInfo bucketOne = OmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .build();
    when(omMetadataManager.listBucketsUnderVolume("sampleVol", "", 1000))
        .thenReturn(Collections.singletonList(bucketOne));

    BucketDeletedBytesMetricsService bucketDeletedBytesMetricsService =
        mock(BucketDeletedBytesMetricsService.class);
    when(bucketDeletedBytesMetricsService.getPerBucketMetrics()).thenReturn(Collections.emptyMap());
    when(bucketDeletedBytesMetricsService.getCurrentStatus())
        .thenReturn(BucketDeletedBytesMetricsService.MetricCollectionStatus.NOT_STARTED);

    BucketEndpoint bucketEndpoint = new BucketEndpoint(omMetadataManager, bucketDeletedBytesMetricsService);
    Response response = bucketEndpoint.getBuckets("sampleVol", 1000, "");

    BucketsResponse bucketsResponse = (BucketsResponse) response.getEntity();
    List<BucketObjectDBInfo> buckets = (List<BucketObjectDBInfo>) bucketsResponse.getBuckets();
    assertEquals(1, buckets.size());
    assertEquals(-1L, buckets.get(0).getTrappedBytes());
    assertEquals(-1L, buckets.get(0).getTrappedKeys());
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
}
