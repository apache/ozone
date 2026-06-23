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
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;

/**
 * A class for collecting and reporting volume utilization metrics.
 * <p>
 * Available metrics:
 * <ul>
 *   <li>Volume quota in bytes.
 *   <li>Volume quota in namespace (maximum number of buckets).
 *   <li>Volume used namespace (current number of buckets).
 * </ul>
 */
@InterfaceAudience.Private
@Metrics(about = "Ozone Volume Utilization Metrics", context = OzoneConsts.OZONE)
public class VolumeUtilizationMetrics implements MetricsSource {

  private static final String SOURCE = VolumeUtilizationMetrics.class.getSimpleName();

  private final OMMetadataManager metadataManager;

  public VolumeUtilizationMetrics(OMMetadataManager metadataManager) {
    this.metadataManager = metadataManager;
  }

  public static VolumeUtilizationMetrics create(OMMetadataManager metadataManager) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE, "Volume Utilization Metrics", new VolumeUtilizationMetrics(metadataManager));
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    Iterator<Entry<CacheKey<String>, CacheValue<OmVolumeArgs>>> volumeIterator = metadataManager.getVolumeIterator();

    while (volumeIterator.hasNext()) {
      Entry<CacheKey<String>, CacheValue<OmVolumeArgs>> entry = volumeIterator.next();
      OmVolumeArgs volumeArgs = entry.getValue().getCacheValue();
      if (volumeArgs == null) {
        continue;
      }

      collector.addRecord(SOURCE)
          .setContext("Volume metrics")
          .tag(VolumeMetricsInfo.VolumeName, volumeArgs.getVolume())
          .addGauge(VolumeMetricsInfo.VolumeQuotaBytes, volumeArgs.getQuotaInBytes())
          .addGauge(VolumeMetricsInfo.VolumeQuotaNamespace, volumeArgs.getQuotaInNamespace())
          .addGauge(VolumeMetricsInfo.VolumeUsedNamespace, volumeArgs.getUsedNamespace());
    }
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE);
  }

  enum VolumeMetricsInfo implements MetricsInfo {
    VolumeName("Volume name."),
    VolumeQuotaBytes("Volume quota in bytes."),
    VolumeQuotaNamespace("Volume quota in namespace."),
    VolumeUsedNamespace("Volume used namespace.");

    private final String desc;

    VolumeMetricsInfo(String desc) {
      this.desc = desc;
    }

    @Override
    public String description() {
      return desc;
    }
  }
}
