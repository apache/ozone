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

package org.apache.hadoop.ozone.container.common.volume;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import java.util.Collection;

/**
 * This class captures the volume scanner metrics on the data-node.
 **/
@InterfaceAudience.Private
@Metrics(about = "Datanode volume scanner metrics", context = "dfs")
public class StorageVolumeCheckerMetrics {
  private final String name;
  private final MetricsSystem ms;

  @Metric("number of volume checks")
  private MutableCounterLong numVolumeChecks;

  @Metric("number of all volume checks")
  private MutableCounterLong numAllVolumeChecks;

  @Metric("number of all volume sets checks")
  private MutableCounterLong numAllVolumeSetsChecks;

  @Metric("number of checks skipped because the minimum gap since the last check had not elapsed")
  private MutableCounterLong numSkippedChecks;

  public StorageVolumeCheckerMetrics(String name, MetricsSystem ms) {
    this.name = name;
    this.ms = ms;
  }

  public static StorageVolumeCheckerMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    String name = "Volume scanner metrics";
    return ms.register(name, null, new StorageVolumeCheckerMetrics(name, ms));
  }

  /**
   * Return the number of {@link StorageVolumeChecker#checkVolume} invocations.
   */
  public long getNumVolumeChecks() {
    return  numVolumeChecks.value();
  }

  public void incNumVolumeChecks() {
    numVolumeChecks.incr();
  }

  /**
   * Return the number of {@link StorageVolumeChecker#checkAllVolumes(Collection)} invocations.
   */
  public long getNumAllVolumeChecks() {
    return numAllVolumeChecks.value();
  }

  public void incNumAllVolumeChecks() {
    numAllVolumeChecks.incr();
  }

  /**
   * Return the number of {@link StorageVolumeChecker#checkAllVolumeSets()} invocations.
   */
  public long getNumAllVolumeSetsChecks() {
    return numAllVolumeSetsChecks.value();
  }

  public void incNumAllVolumeSetsChecks() {
    numAllVolumeSetsChecks.incr();
  }

  /**
   * Return the number of checks skipped because the minimum gap since the
   * last check had not elapsed.
   */
  public long getNumSkippedChecks() {
    return numSkippedChecks.value();
  }

  public void incNumSkippedChecks() {
    numSkippedChecks.incr();
  }

  public void unregister() {
    ms.unregisterSource(name);
  }
}
