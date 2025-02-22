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

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;

/**
 * Base class for container scanner metrics.
 */
@InterfaceAudience.Private
@Metrics(about = "Datanode container scanner metrics", context = "dfs")
public abstract class AbstractContainerScannerMetrics {

  private final String name;
  private final MetricsSystem ms;

  @Metric("number of containers scanned in the current iteration")
  private MutableGaugeInt numContainersScanned;
  @Metric("number of unhealthy containers found in the current iteration")
  private MutableGaugeInt numUnHealthyContainers;
  @Metric("number of iterations of scanner completed since the restart")
  private MutableCounterInt numScanIterations;

  public AbstractContainerScannerMetrics(String name, MetricsSystem ms) {
    this.name = name;
    this.ms = ms;
  }

  public int getNumContainersScanned() {
    return numContainersScanned.value();
  }

  public void incNumContainersScanned() {
    numContainersScanned.incr();
  }

  public void resetNumContainersScanned() {
    numContainersScanned.decr(getNumContainersScanned());
  }

  public int getNumUnHealthyContainers() {
    return numUnHealthyContainers.value();
  }

  public void incNumUnHealthyContainers() {
    numUnHealthyContainers.incr();
  }

  public void resetNumUnhealthyContainers() {
    numUnHealthyContainers.decr(getNumUnHealthyContainers());
  }

  public int getNumScanIterations() {
    return numScanIterations.value();
  }

  public void incNumScanIterations() {
    numScanIterations.incr();
  }

  public void unregister() {
    ms.unregisterSource(name);
  }

  public String getName() {
    return name;
  }
}
