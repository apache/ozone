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

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.ozone.util.MetricUtil;

/**
 * This class is for maintaining Snapshot Manager statistics.
 */
@InterfaceAudience.Private
@Metrics(about = "Snapshot Manager Metrics", context = "dfs")
public final class OmSnapshotMetrics implements OmMetadataReaderMetrics, MetricsSource {
  private static final String SOURCE_NAME =
      OmSnapshotMetrics.class.getSimpleName();

  private final String sourceName = MetricUtil.getMetricsSourceName(SOURCE_NAME);
  private final MetricsRegistry registry = new MetricsRegistry(SOURCE_NAME);

  private @Metric MutableCounterLong numKeyLookup;
  private @Metric MutableCounterLong numKeyLookupFails;
  private @Metric MutableCounterLong numGetKeyInfo;
  private @Metric MutableCounterLong numGetKeyInfoFails;
  private @Metric MutableCounterLong numListStatus;
  private @Metric MutableCounterLong numListStatusFails;
  private @Metric MutableCounterLong numGetFileStatus;
  private @Metric MutableCounterLong numGetFileStatusFails;
  private @Metric MutableCounterLong numLookupFile;
  private @Metric MutableCounterLong numLookupFileFails;
  private @Metric MutableCounterLong numKeyLists;
  private @Metric MutableCounterLong numKeyListFails;
  private @Metric MutableCounterLong numGetAcl;
  private @Metric MutableCounterLong numKeyOps;
  private @Metric MutableCounterLong numFSOps;
  private @Metric MutableCounterLong numGetObjectTagging;
  private @Metric MutableCounterLong numGetObjectTaggingFails;
  private @Metric MutableCounterLong numGetBucketTagging;
  private @Metric MutableCounterLong numGetBucketTaggingFails;

  private OmSnapshotMetrics() {
  }

  public static OmSnapshotMetrics create() {
    OmSnapshotMetrics metrics = new OmSnapshotMetrics();
    metrics.register();
    return metrics;
  }

  public void register() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.register(sourceName, "Snapshot Manager Metrics", this);
  }

  public void unregister() {
    DefaultMetricsSystem.instance().unregisterSource(sourceName);
  }

  @Override
  public void incNumKeyLookups() {
    numKeyOps.incr();
    numKeyLookup.incr();
  }

  @Override
  public void incNumKeyLookupFails() {
    numKeyLookupFails.incr();
  }

  @Override
  public void incNumGetKeyInfo() {
    numKeyOps.incr();
    numGetKeyInfo.incr();
  }

  @Override
  public void incNumGetKeyInfoFails() {
    numGetKeyInfoFails.incr();
  }

  @Override
  public void incNumListStatus() {
    numKeyOps.incr();
    numFSOps.incr();
    numListStatus.incr();
  }

  @Override
  public void incNumListStatusFails() {
    numListStatusFails.incr();
  }

  @Override
  public void incNumGetFileStatus() {
    numKeyOps.incr();
    numFSOps.incr();
    numGetFileStatus.incr();
  }

  @Override
  public void incNumGetFileStatusFails() {
    numGetFileStatusFails.incr();
  }

  @Override
  public void incNumLookupFile() {
    numKeyOps.incr();
    numFSOps.incr();
    numLookupFile.incr();
  }

  @Override
  public void incNumLookupFileFails() {
    numLookupFileFails.incr();
  }

  @Override
  public void incNumKeyLists() {
    numKeyLists.incr();
  }

  @Override
  public void incNumKeyListFails() {
    numKeyListFails.incr();
  }

  @Override
  public void incNumGetAcl() {
    numGetAcl.incr();
  }

  @Override
  public void incNumGetObjectTagging() {
    numGetObjectTagging.incr();
    numKeyOps.incr();
  }

  @Override
  public void incNumGetObjectTaggingFails() {
    numGetObjectTaggingFails.incr();
  }

  @Override
  public void incNumGetBucketTagging() {
    numGetBucketTagging.incr();
  }

  @Override
  public void incNumGetBucketTaggingFails() {
    numGetBucketTaggingFails.incr();
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    registry.snapshot(collector.addRecord(registry.info()), all);
  }

}

