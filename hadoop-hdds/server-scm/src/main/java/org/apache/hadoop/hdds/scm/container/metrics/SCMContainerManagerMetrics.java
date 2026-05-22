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

package org.apache.hadoop.hdds.scm.container.metrics;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Class contains metrics related to ContainerManager.
 */
@Metrics(about = "SCM ContainerManager metrics", context = OzoneConsts.OZONE)
public final class SCMContainerManagerMetrics {

  private static final String SOURCE_NAME =
      SCMContainerManagerMetrics.class.getSimpleName();

  // These are the metrics which will be reset to zero after restart.
  // These metrics capture count of number of successful/failure operations
  // of create/delete containers in SCM.

  private @Metric MutableCounterLong numSuccessfulCreateContainers;
  private @Metric MutableCounterLong numFailureCreateContainers;
  private @Metric MutableCounterLong numSuccessfulDeleteContainers;
  private @Metric MutableCounterLong numFailureDeleteContainers;
  private @Metric MutableCounterLong numListContainerOps;

  private @Metric MutableCounterLong numContainerReportsProcessedSuccessful;
  private @Metric MutableCounterLong numContainerReportsProcessedFailed;
  private @Metric MutableCounterLong numICRReportsProcessedSuccessful;
  private @Metric MutableCounterLong numICRReportsProcessedFailed;
  private @Metric MutableCounterLong numFCRLeaseRequests;
  private @Metric MutableCounterLong numFCRLeasesGranted;
  private @Metric MutableCounterLong numFCRLeasesRejected;
  private @Metric MutableCounterLong numFCRLeaseExpired;
  private @Metric MutableCounterLong numFCRReportsRejectedInvalidLease;
  private @Metric MutableCounterLong numFCRReportsProcessedWithLease;
  private @Metric MutableGaugeLong numFCRLeasesOutstanding;

  private SCMContainerManagerMetrics() {
  }

  /**
   * Create and return metrics instance.
   * @return SCMContainerManagerMetrics
   */
  public static SCMContainerManagerMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME, "SCM ContainerManager Metrics",
        new SCMContainerManagerMetrics());
  }

  /**
   * Unregister metrics.
   */
  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  public void incNumSuccessfulCreateContainers() {
    this.numSuccessfulCreateContainers.incr();
  }

  public void incNumFailureCreateContainers() {
    this.numFailureCreateContainers.incr();
  }

  public void incNumSuccessfulDeleteContainers() {
    this.numSuccessfulDeleteContainers.incr();
  }

  public void incNumFailureDeleteContainers() {
    this.numFailureDeleteContainers.incr();
  }

  public void incNumListContainersOps() {
    this.numListContainerOps.incr();
  }

  public void incNumContainerReportsProcessedSuccessful() {
    this.numContainerReportsProcessedSuccessful.incr();
  }

  public void incNumContainerReportsProcessedFailed() {
    this.numContainerReportsProcessedFailed.incr();
  }

  public void incNumICRReportsProcessedSuccessful() {
    this.numICRReportsProcessedSuccessful.incr();
  }

  public void incNumICRReportsProcessedFailed() {
    this.numICRReportsProcessedFailed.incr();
  }

  public void incNumFCRLeaseRequests() {
    this.numFCRLeaseRequests.incr();
  }

  public void incNumFCRLeasesGranted() {
    this.numFCRLeasesGranted.incr();
  }

  public void incNumFCRLeasesRejected() {
    this.numFCRLeasesRejected.incr();
  }

  public void incNumFCRLeaseExpired() {
    this.numFCRLeaseExpired.incr();
  }

  public void incNumFCRReportsRejectedInvalidLease() {
    this.numFCRReportsRejectedInvalidLease.incr();
  }

  public void incNumFCRReportsProcessedWithLease() {
    this.numFCRReportsProcessedWithLease.incr();
  }

  public void setNumFCRLeasesOutstanding(long count) {
    this.numFCRLeasesOutstanding.set(count);
  }

  public long getNumContainerReportsProcessedSuccessful() {
    return numContainerReportsProcessedSuccessful.value();
  }

  public long getNumContainerReportsProcessedFailed() {
    return numContainerReportsProcessedFailed.value();
  }

  public long getNumICRReportsProcessedSuccessful() {
    return numICRReportsProcessedSuccessful.value();
  }

  public long getNumICRReportsProcessedFailed() {
    return numICRReportsProcessedFailed.value();
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

  public long getNumSuccessfulCreateContainers() {
    return numSuccessfulCreateContainers.value();
  }

  public long getNumFailureCreateContainers() {
    return numFailureCreateContainers.value();
  }

  public long getNumSuccessfulDeleteContainers() {
    return numSuccessfulDeleteContainers.value();
  }

  public long getNumFailureDeleteContainers() {
    return numFailureDeleteContainers.value();
  }

  public long getNumListContainersOps() {
    return numListContainerOps.value();
  }

}
