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

package org.apache.hadoop.ozone.recon.metrics;

import com.google.inject.Singleton;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.util.Time;

/**
 * Metrics for NSSummary tree traversals that detect invalid references.
 */
@InterfaceAudience.Private
@Singleton
@Metrics(about = "Recon NSSummary Metrics", context = OzoneConsts.OZONE)
public final class NSSummaryMetrics implements MetricsSource {

  private static final String SOURCE_NAME =
      NSSummaryMetrics.class.getSimpleName();

  private static final MetricsInfo INVALID_TREE_DETECTION_COUNT = Interns.info(
      "invalidTreeDetectionCount",
      "Number of NSSummary tree traversals that detected an invalid reference since Recon started");

  private static final MetricsInfo LAST_INVALID_TREE_DETECTION_MILLIS =
      Interns.info("lastInvalidTreeDetectionMillis",
          "Epoch time in milliseconds of the last NSSummary tree traversal that detected an invalid reference");

  private final AtomicLong invalidTreeDetectionCount = new AtomicLong();
  private final AtomicLong lastInvalidTreeDetectionMillis =
      new AtomicLong();

  public void register() {
    DefaultMetricsSystem.instance().register(
        SOURCE_NAME, "Recon NSSummary Metrics", this);
  }

  public void unregister() {
    DefaultMetricsSystem.instance().unregisterSource(SOURCE_NAME);
  }

  public void recordInvalidTreeDetection() {
    recordInvalidTreeDetection(Time.now());
  }

  void recordInvalidTreeDetection(long detectedMillis) {
    invalidTreeDetectionCount.incrementAndGet();
    lastInvalidTreeDetectionMillis.accumulateAndGet(
        detectedMillis, Math::max);
  }

  public long getInvalidTreeDetectionCount() {
    return invalidTreeDetectionCount.get();
  }

  public long getLastInvalidTreeDetectionMillis() {
    return lastInvalidTreeDetectionMillis.get();
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(SOURCE_NAME);
    builder.addCounter(INVALID_TREE_DETECTION_COUNT,
        getInvalidTreeDetectionCount());
    builder.addGauge(LAST_INVALID_TREE_DETECTION_MILLIS,
        getLastInvalidTreeDetectionMillis());
  }
}
