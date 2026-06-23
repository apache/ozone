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
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.VolumeUtilizationMetrics.VolumeMetricsInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.junit.jupiter.api.Test;

/**
 * Test class for VolumeUtilizationMetrics.
 */
public class TestVolumeUtilizationMetrics {

  private static final String VOLUME_NAME_1 = "volume1";
  private static final String VOLUME_NAME_2 = "volume2";
  private static final long QUOTA_IN_BYTES_1 = OzoneConsts.TB;
  private static final long QUOTA_IN_BYTES_2 = -1;
  private static final long QUOTA_IN_NAMESPACE_1 = 10;
  private static final long QUOTA_IN_NAMESPACE_2 = -1;
  private static final long USED_NAMESPACE_1 = 4;
  private static final long USED_NAMESPACE_2 = 0;

  @Test
  void testVolumeUtilizationMetrics() {
    OMMetadataManager omMetadataManager = mock(OMMetadataManager.class);

    Map.Entry<CacheKey<String>, CacheValue<OmVolumeArgs>> entry1 =
        createMockEntry(VOLUME_NAME_1, QUOTA_IN_BYTES_1, QUOTA_IN_NAMESPACE_1, USED_NAMESPACE_1);
    Map.Entry<CacheKey<String>, CacheValue<OmVolumeArgs>> entry2 =
        createMockEntry(VOLUME_NAME_2, QUOTA_IN_BYTES_2, QUOTA_IN_NAMESPACE_2, USED_NAMESPACE_2);

    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmVolumeArgs>>> volumeIterator = mock(Iterator.class);
    when(volumeIterator.hasNext())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);

    when(volumeIterator.next())
        .thenReturn(entry1)
        .thenReturn(entry2);

    when(omMetadataManager.getVolumeIterator()).thenReturn(volumeIterator);

    MetricsRecordBuilder mb = mock(MetricsRecordBuilder.class);
    when(mb.setContext(anyString())).thenReturn(mb);
    when(mb.tag(any(MetricsInfo.class), anyString())).thenReturn(mb);
    when(mb.addGauge(any(MetricsInfo.class), anyInt())).thenReturn(mb);
    when(mb.addGauge(any(MetricsInfo.class), anyLong())).thenReturn(mb);

    MetricsCollector metricsCollector = mock(MetricsCollector.class);
    when(metricsCollector.addRecord(anyString())).thenReturn(mb);

    VolumeUtilizationMetrics volumeMetrics = new VolumeUtilizationMetrics(omMetadataManager);
    volumeMetrics.getMetrics(metricsCollector, true);

    verify(mb, times(1)).tag(VolumeMetricsInfo.VolumeName, VOLUME_NAME_1);
    verify(mb, times(1)).addGauge(VolumeMetricsInfo.VolumeQuotaBytes, QUOTA_IN_BYTES_1);
    verify(mb, times(1)).addGauge(VolumeMetricsInfo.VolumeQuotaNamespace, QUOTA_IN_NAMESPACE_1);
    verify(mb, times(1)).addGauge(VolumeMetricsInfo.VolumeUsedNamespace, USED_NAMESPACE_1);

    verify(mb, times(1)).tag(VolumeMetricsInfo.VolumeName, VOLUME_NAME_2);
    verify(mb, times(1)).addGauge(VolumeMetricsInfo.VolumeQuotaBytes, QUOTA_IN_BYTES_2);
    verify(mb, times(1)).addGauge(VolumeMetricsInfo.VolumeQuotaNamespace, QUOTA_IN_NAMESPACE_2);
    verify(mb, times(1)).addGauge(VolumeMetricsInfo.VolumeUsedNamespace, USED_NAMESPACE_2);
  }

  private static Map.Entry<CacheKey<String>, CacheValue<OmVolumeArgs>> createMockEntry(
      String volumeName, long quotaInBytes, long quotaInNamespace, long usedNamespace) {
    Map.Entry<CacheKey<String>, CacheValue<OmVolumeArgs>> entry = mock(Map.Entry.class);
    CacheValue<OmVolumeArgs> cacheValue = mock(CacheValue.class);
    OmVolumeArgs volumeArgs = mock(OmVolumeArgs.class);

    when(volumeArgs.getVolume()).thenReturn(volumeName);
    when(volumeArgs.getQuotaInBytes()).thenReturn(quotaInBytes);
    when(volumeArgs.getQuotaInNamespace()).thenReturn(quotaInNamespace);
    when(volumeArgs.getUsedNamespace()).thenReturn(usedNamespace);

    when(cacheValue.getCacheValue()).thenReturn(volumeArgs);
    when(entry.getValue()).thenReturn(cacheValue);

    return entry;
  }
}
