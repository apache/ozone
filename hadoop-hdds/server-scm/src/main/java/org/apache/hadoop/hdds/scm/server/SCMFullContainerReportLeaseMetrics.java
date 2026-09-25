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

package org.apache.hadoop.hdds.scm.server;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Metrics for full container report lease management.
 */
@Metrics(about = "SCM full container report lease metrics",
    context = OzoneConsts.OZONE)
public final class SCMFullContainerReportLeaseMetrics {
  private static final String SOURCE_NAME =
      SCMFullContainerReportLeaseMetrics.class.getSimpleName();

  private @Metric MutableCounterLong numFCRLeaseRequests;
  private @Metric MutableCounterLong numFCRLeasesGranted;
  private @Metric MutableCounterLong numFCRLeasesRejected;
  private @Metric MutableCounterLong numFCRLeaseExpired;
  private @Metric MutableCounterLong numFCRReportsRejectedInvalidLease;
  private @Metric MutableCounterLong numFCRReportsProcessedWithLease;
  private @Metric MutableGaugeLong numFCRLeasesOutstanding;

  private SCMFullContainerReportLeaseMetrics() {
  }

  public static SCMFullContainerReportLeaseMetrics create() {
    MetricsSystem metricsSystem = DefaultMetricsSystem.instance();
    return metricsSystem.register(SOURCE_NAME,
        "SCM Full Container Report Lease Metrics",
        new SCMFullContainerReportLeaseMetrics());
  }

  public void unregister() {
    DefaultMetricsSystem.instance().unregisterSource(SOURCE_NAME);
  }

  public void incNumFCRLeaseRequests() {
    numFCRLeaseRequests.incr();
  }

  public void incNumFCRLeasesGranted() {
    numFCRLeasesGranted.incr();
  }

  public void incNumFCRLeasesRejected() {
    numFCRLeasesRejected.incr();
  }

  public void incNumFCRLeaseExpired() {
    numFCRLeaseExpired.incr();
  }

  public void incNumFCRReportsRejectedInvalidLease() {
    numFCRReportsRejectedInvalidLease.incr();
  }

  public void incNumFCRReportsProcessedWithLease() {
    numFCRReportsProcessedWithLease.incr();
  }

  public void setNumFCRLeasesOutstanding(long count) {
    numFCRLeasesOutstanding.set(count);
  }

  public long getNumFCRLeaseRequests() {
    return numFCRLeaseRequests.value();
  }

  public long getNumFCRLeasesGranted() {
    return numFCRLeasesGranted.value();
  }

  public long getNumFCRLeasesRejected() {
    return numFCRLeasesRejected.value();
  }

  public long getNumFCRLeaseExpired() {
    return numFCRLeaseExpired.value();
  }

  public long getNumFCRReportsRejectedInvalidLease() {
    return numFCRReportsRejectedInvalidLease.value();
  }

  public long getNumFCRReportsProcessedWithLease() {
    return numFCRReportsProcessedWithLease.value();
  }

  public long getNumFCRLeasesOutstanding() {
    return numFCRLeasesOutstanding.value();
  }
}
