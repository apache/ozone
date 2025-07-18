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

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Class for tracking metrics related to container health task in Recon.
 */
@InterfaceAudience.Private
@Metrics(about = "Recon ContainerHealthTask Metrics", context = OzoneConsts.OZONE)
public final class ContainerHealthMetrics {

  private static final String SOURCE_NAME =
      ContainerHealthMetrics.class.getSimpleName();

  @Metric(about = "Number of missing containers detected in Recon.")
  private MutableGaugeLong missingContainerCount;

  @Metric(about = "Number of under replicated containers detected in Recon.")
  private MutableGaugeLong underReplicatedContainerCount;

  @Metric(about = "Number of replica mismatch containers detected in Recon.")
  private MutableGaugeLong replicaMisMatchContainerCount;

  private ContainerHealthMetrics() {
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  public static ContainerHealthMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
        "Recon Container Health Task Metrics",
        new ContainerHealthMetrics());
  }

  public void setMissingContainerCount(long missingContainerCount) {
    this.missingContainerCount.set(missingContainerCount);
  }

  public void setUnderReplicatedContainerCount(long underReplicatedContainerCount) {
    this.underReplicatedContainerCount.set(underReplicatedContainerCount);
  }

  public void setReplicaMisMatchContainerCount(long replicaMisMatchContainerCount) {
    this.replicaMisMatchContainerCount.set(replicaMisMatchContainerCount);
  }

  public long getMissingContainerCount() {
    return missingContainerCount.value();
  }

  public long getUnderReplicatedContainerCount() {
    return underReplicatedContainerCount.value();
  }

  public long getReplicaMisMatchContainerCount() {
    return replicaMisMatchContainerCount.value();
  }

}
