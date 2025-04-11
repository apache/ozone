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

package org.apache.hadoop.ozone.container.ozoneimpl;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * This class captures the container data scanner metrics on the data-node.
 **/
@InterfaceAudience.Private
@Metrics(about = "DataNode container data scanner metrics", context = "dfs")
public final class ContainerDataScannerMetrics
    extends AbstractContainerScannerMetrics {

  @Metric("disk bandwidth used by the container data scanner per volume")
  private MutableRate numBytesScanned;

  private String storageDirectory;

  public double getNumBytesScannedMean() {
    return numBytesScanned.lastStat().mean();
  }

  public long getNumBytesScannedSampleCount() {
    return numBytesScanned.lastStat().numSamples();
  }

  public double getNumBytesScannedStdDev() {
    return numBytesScanned.lastStat().stddev();
  }

  public void incNumBytesScanned(long bytes) {
    numBytesScanned.add(bytes);
  }

  private ContainerDataScannerMetrics(String name, MetricsSystem ms) {
    super(name, ms);
  }

  @SuppressWarnings("java:S2245") // no need for secure random
  public static ContainerDataScannerMetrics create(final String volumeName) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    String name = "ContainerDataScannerMetrics-" + (volumeName.isEmpty()
        ? "UndefinedDataNodeVolume" + ThreadLocalRandom.current().nextInt()
        : volumeName.replace(':', '-'));

    return ms.register(name, null, new ContainerDataScannerMetrics(name, ms));
  }

  @Metric("Returns the Directory name for the volume")
  public String getStorageDirectory() {
    return storageDirectory;
  }

  public void setStorageDirectory(final String volumeName) {
    this.storageDirectory = volumeName;
  }
}
