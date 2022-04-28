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
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.hadoop.test.MetricsAsserts.getLongGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

/**
 * Tests for the ReplicationManagerMetrics class.
 */
public class TestReplicationManagerMetrics {

  private ReplicationManager replicationManager;
  private ReplicationManagerMetrics metrics;

  @Before
  public void setup() {
    ReplicationManagerReport report = new ReplicationManagerReport();

    // Each lifecycle state has a value from 1 to N. Set the value of the metric
    // to the value by incrementing the counter that number of times.
    for (HddsProtos.LifeCycleState s : HddsProtos.LifeCycleState.values()) {
      for (int i = 0; i < s.getNumber(); i++) {
        report.increment(s);
      }
    }
    // The ordinal starts from 0, so each state will have a value of its ordinal
    for (ReplicationManagerReport.HealthState s :
        ReplicationManagerReport.HealthState.values()) {
      for (int i = 0; i < s.ordinal(); i++) {
        report.increment(s);
      }
    }
    replicationManager = Mockito.mock(ReplicationManager.class);
    Mockito.when(replicationManager.getContainerReport()).thenReturn(report);
    metrics = ReplicationManagerMetrics.create(replicationManager);
  }

  @After
  public void after() {
    metrics.unRegister();
  }

  @Test
  public void testLifeCycleStateMetricsPresent() {
    Assert.assertEquals(HddsProtos.LifeCycleState.OPEN.getNumber(),
        getGauge("NumOpenContainers"));
    Assert.assertEquals(HddsProtos.LifeCycleState.CLOSING.getNumber(),
        getGauge("NumClosingContainers"));
    Assert.assertEquals(HddsProtos.LifeCycleState.QUASI_CLOSED.getNumber(),
        getGauge("NumQuasiClosedContainers"));
    Assert.assertEquals(HddsProtos.LifeCycleState.CLOSED.getNumber(),
        getGauge("NumClosedContainers"));
    Assert.assertEquals(HddsProtos.LifeCycleState.DELETING.getNumber(),
        getGauge("NumDeletingContainers"));
    Assert.assertEquals(HddsProtos.LifeCycleState.DELETED.getNumber(),
        getGauge("NumDeletedContainers"));
  }

  @Test
  public void testHealthStateMetricsPresent() {
    for (ReplicationManagerReport.HealthState s :
        ReplicationManagerReport.HealthState.values()) {
      Assert.assertEquals(s.ordinal(), getGauge(s.getMetricName()));
    }
  }

  private long getGauge(String metricName) {
    return getLongGauge(metricName,
        getMetrics(ReplicationManagerMetrics.METRICS_SOURCE_NAME));
  }

}
