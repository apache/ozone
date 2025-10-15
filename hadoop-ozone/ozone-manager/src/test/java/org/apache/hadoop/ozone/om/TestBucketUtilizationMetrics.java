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

import static org.apache.hadoop.ozone.OzoneConsts.QUOTA_RESET;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.om.BucketUtilizationMetrics.BucketMetricsInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.junit.jupiter.api.Test;

/**
 * Test class for BucketUtilizationMetrics.
 */
public class TestBucketUtilizationMetrics {

  private static final String VOLUME_NAME_1 = "volume1";
  private static final String VOLUME_NAME_2 = "volume2";
  private static final String BUCKET_NAME_1 = "bucket1";
  private static final String BUCKET_NAME_2 = "bucket2";
  private static final long USED_BYTES_1 = 100;
  private static final long USED_BYTES_2 = 200;
  private static final long SNAPSHOT_USED_BYTES_1 = 400;
  private static final long SNAPSHOT_USED_BYTES_2 = 800;
  private static final long QUOTA_IN_BYTES_1 = 600;
  private static final long QUOTA_IN_BYTES_2 = QUOTA_RESET;
  private static final long QUOTA_IN_NAMESPACE_1 = 1;
  private static final long QUOTA_IN_NAMESPACE_2 = 2;

  @Test
  void testBucketUtilizationMetrics() {
    OMMetadataManager omMetadataManager = mock(OMMetadataManager.class);

    Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>> entry1 = createMockEntry(VOLUME_NAME_1, BUCKET_NAME_1,
        USED_BYTES_1, SNAPSHOT_USED_BYTES_1, QUOTA_IN_BYTES_1, QUOTA_IN_NAMESPACE_1);
    Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>> entry2 = createMockEntry(VOLUME_NAME_2, BUCKET_NAME_2,
        USED_BYTES_2, SNAPSHOT_USED_BYTES_2, QUOTA_IN_BYTES_2, QUOTA_IN_NAMESPACE_2);

    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>>> bucketIterator = mock(Iterator.class);
    when(bucketIterator.hasNext())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);

    when(bucketIterator.next())
        .thenReturn(entry1)
        .thenReturn(entry2);

    when(omMetadataManager.getBucketIterator()).thenReturn(bucketIterator);

    MetricsRecordBuilder mb = mock(MetricsRecordBuilder.class);
    when(mb.setContext(anyString())).thenReturn(mb);
    when(mb.tag(any(MetricsInfo.class), anyString())).thenReturn(mb);
    when(mb.addGauge(any(MetricsInfo.class), anyInt())).thenReturn(mb);
    when(mb.addGauge(any(MetricsInfo.class), anyLong())).thenReturn(mb);

    MetricsCollector metricsCollector = mock(MetricsCollector.class);
    when(metricsCollector.addRecord(anyString())).thenReturn(mb);

    BucketUtilizationMetrics containerMetrics = new BucketUtilizationMetrics(omMetadataManager);

    containerMetrics.getMetrics(metricsCollector, true);

    verify(mb, times(1)).tag(BucketMetricsInfo.VolumeName, VOLUME_NAME_1);
    verify(mb, times(1)).tag(BucketMetricsInfo.BucketName, BUCKET_NAME_1);
    verify(mb, times(1)).addGauge(BucketMetricsInfo.BucketUsedBytes, USED_BYTES_1);
    verify(mb, times(1)).addGauge(BucketMetricsInfo.BucketSnapshotUsedBytes, SNAPSHOT_USED_BYTES_1);
    verify(mb, times(1)).addGauge(BucketMetricsInfo.BucketQuotaBytes, QUOTA_IN_BYTES_1);
    verify(mb, times(1)).addGauge(BucketMetricsInfo.BucketQuotaNamespace, QUOTA_IN_NAMESPACE_1);
    verify(mb, times(1)).addGauge(BucketMetricsInfo.BucketAvailableBytes,
        QUOTA_IN_BYTES_1 - USED_BYTES_1 - SNAPSHOT_USED_BYTES_1);

    verify(mb, times(1)).tag(BucketMetricsInfo.VolumeName, VOLUME_NAME_2);
    verify(mb, times(1)).tag(BucketMetricsInfo.BucketName, BUCKET_NAME_2);
    verify(mb, times(1)).addGauge(BucketMetricsInfo.BucketUsedBytes, USED_BYTES_2);
    verify(mb, times(1)).addGauge(BucketMetricsInfo.BucketSnapshotUsedBytes, SNAPSHOT_USED_BYTES_2);
    verify(mb, times(1)).addGauge(BucketMetricsInfo.BucketQuotaBytes, QUOTA_IN_BYTES_2);
    verify(mb, times(1)).addGauge(BucketMetricsInfo.BucketQuotaNamespace, QUOTA_IN_NAMESPACE_2);
    verify(mb, times(1)).addGauge(BucketMetricsInfo.BucketAvailableBytes, QUOTA_RESET);
  }

  private static Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>> createMockEntry(String volumeName,
      String bucketName, long usedBytes, long snapshotUsedBytes, long quotaInBytes, long quotaInNamespace) {
    Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>> entry = mock(Map.Entry.class);
    CacheValue<OmBucketInfo> cacheValue = mock(CacheValue.class);
    OmBucketInfo bucketInfo = mock(OmBucketInfo.class);

    when(bucketInfo.getVolumeName()).thenReturn(volumeName);
    when(bucketInfo.getBucketName()).thenReturn(bucketName);
    when(bucketInfo.getUsedBytes()).thenReturn(usedBytes);
    when(bucketInfo.getSnapshotUsedBytes()).thenReturn(snapshotUsedBytes);
    when(bucketInfo.getQuotaInBytes()).thenReturn(quotaInBytes);
    when(bucketInfo.getQuotaInNamespace()).thenReturn(quotaInNamespace);
    when(bucketInfo.getTotalBucketSpace()).thenReturn(usedBytes + snapshotUsedBytes);

    when(cacheValue.getCacheValue()).thenReturn(bucketInfo);

    when(entry.getValue()).thenReturn(cacheValue);

    return entry;
  }
}
