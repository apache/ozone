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

import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;

/**
 * A class for collecting and reporting bucket utilization metrics.
 * <p>
 * Available metrics:
 * <ul>
 *   <li>Bytes used in bucket.
 *   <li>Bucket quote in bytes.
 *   <li>Bucket quota in namespace.
 *   <li>Bucket available space. Calculated from difference between used bytes in bucket and bucket quota.
 *   If bucket quote is not set then this metric show -1 as value.
 * </ul>
 */
@InterfaceAudience.Private
@Metrics(about = "Ozone Bucket Utilization Metrics", context = OzoneConsts.OZONE)
public class BucketUtilizationMetrics implements MetricsSource {

  private static final String SOURCE = BucketUtilizationMetrics.class.getSimpleName();

  private final OMMetadataManager metadataManager;

  public BucketUtilizationMetrics(OMMetadataManager metadataManager) {
    this.metadataManager = metadataManager;
  }

  public static BucketUtilizationMetrics create(OMMetadataManager metadataManager) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE, "Bucket Utilization Metrics", new BucketUtilizationMetrics(metadataManager));
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    Iterator<Entry<CacheKey<String>, CacheValue<OmBucketInfo>>> bucketIterator = metadataManager.getBucketIterator();

    while (bucketIterator.hasNext()) {
      Entry<CacheKey<String>, CacheValue<OmBucketInfo>> entry = bucketIterator.next();
      OmBucketInfo bucketInfo = entry.getValue().getCacheValue();
      if (bucketInfo == null) {
        continue;
      }

      long availableSpace;
      long quotaInBytes = bucketInfo.getQuotaInBytes();
      if (quotaInBytes == -1) {
        availableSpace = quotaInBytes;
      } else {
        availableSpace = Math.max(bucketInfo.getQuotaInBytes() - bucketInfo.getTotalBucketSpace(), 0);
      }

      collector.addRecord(SOURCE)
          .setContext("Bucket metrics")
          .tag(BucketMetricsInfo.VolumeName, bucketInfo.getVolumeName())
          .tag(BucketMetricsInfo.BucketName, bucketInfo.getBucketName())
          .addGauge(BucketMetricsInfo.BucketUsedBytes, bucketInfo.getUsedBytes())
          .addGauge(BucketMetricsInfo.BucketSnapshotUsedBytes, bucketInfo.getSnapshotUsedBytes())
          .addGauge(BucketMetricsInfo.BucketQuotaBytes, bucketInfo.getQuotaInBytes())
          .addGauge(BucketMetricsInfo.BucketQuotaNamespace, bucketInfo.getQuotaInNamespace())
          .addGauge(BucketMetricsInfo.BucketAvailableBytes, availableSpace);
    }
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE);
  }

  enum BucketMetricsInfo implements MetricsInfo {
    VolumeName("Volume Metrics."),
    BucketName("Bucket Metrics."),
    BucketUsedBytes("Bytes used by bucket in AOS."),
    BucketQuotaBytes("Bucket quota in bytes"),
    BucketSnapshotUsedBytes("Bucket quota bytes held in snapshots"),
    BucketQuotaNamespace("Bucket quota in namespace."),
    BucketAvailableBytes("Bucket available space.");

    private final String desc;

    BucketMetricsInfo(String desc) {
      this.desc = desc;
    }

    @Override
    public String description() {
      return desc;
    }
  }
}
