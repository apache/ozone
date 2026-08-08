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

package org.apache.hadoop.hdds.scm.container.export;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Metrics for async container ID export jobs on SCM.
 */
@Metrics(about = "SCM Container Export Metrics", context = OzoneConsts.OZONE)
public final class ExportMetrics {

  private static final String SOURCE_NAME = ExportMetrics.class.getSimpleName();

  @Metric(about = "Number of export jobs submitted")
  private MutableCounterLong numExportJobsSubmitted;

  @Metric(about = "Number of export jobs succeeded")
  private MutableCounterLong numExportJobsSucceeded;

  @Metric(about = "Number of export jobs failed")
  private MutableCounterLong numExportJobsFailed;

  @Metric(about = "Rows exported by the most recent successful export job.")
  private MutableGaugeLong lastExportRows;

  @Metric(about = "TAR bytes written by the most recent successful export job.")
  private MutableGaugeLong lastExportBytesWritten;

  private ExportMetrics() {
  }

  public static ExportMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME, "SCM Container Export IDs Metrics", new ExportMetrics());
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  public void incrExportJobsSubmitted() {
    numExportJobsSubmitted.incr();
  }

  public void incrExportJobsSucceeded() {
    numExportJobsSucceeded.incr();
  }

  public void incrExportJobsFailed() {
    numExportJobsFailed.incr();
  }

  public void recordLastSuccessfulExport(long rows, long bytesWritten) {
    lastExportRows.set(rows);
    lastExportBytesWritten.set(bytesWritten);
  }
}
