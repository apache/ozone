/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.checksum;

import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * Class to collect metrics related to container merkle tree.
 */
public class ContainerMerkleTreeMetrics {
  private static final String METRICS_SOURCE_NAME = ContainerMerkleTreeMetrics.class.getSimpleName();

  public static ContainerMerkleTreeMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    // TODO: Remove when checksum manager is moved from KeyValueHandler.
    MetricsSource source = ms.getSource(METRICS_SOURCE_NAME);
    if (source != null) {
      ms.unregisterSource(METRICS_SOURCE_NAME);
    }
    return ms.register(METRICS_SOURCE_NAME, "Container Merkle Tree Metrics",
        new ContainerMerkleTreeMetrics());
  }

  public static void unregister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(METRICS_SOURCE_NAME);
  }

  @Metric(about = "Number of Merkle tree write failure")
  private MutableCounterLong numMerkleTreeWriteFailure;

  @Metric(about = "Number of Merkle tree read failure")
  private MutableCounterLong numMerkleTreeReadFailure;

  @Metric(about = "Merkle tree write latency")
  private MutableRate merkleTreeWriteLatencyNS;

  @Metric(about = "Merkle tree read latency")
  private MutableRate merkleTreeReadLatencyNS;

  @Metric(about = "Merkle tree creation latency")
  private MutableRate merkleTreeCreateLatencyNS;

  public void incrementMerkleTreeWriteFailures() {
    this.numMerkleTreeWriteFailure.incr();
  }

  public void incrementMerkleTreeReadFailures() {
    this.numMerkleTreeReadFailure.incr();
  }

  public MutableRate getWriteContainerMerkleTreeLatencyNS() {
    return this.merkleTreeWriteLatencyNS;
  }

  public MutableRate getReadContainerMerkleTreeLatencyNS() {
    return this.merkleTreeReadLatencyNS;
  }

  public MutableRate getCreateMerkleTreeLatencyNS() {
    return this.merkleTreeCreateLatencyNS;
  }
}
