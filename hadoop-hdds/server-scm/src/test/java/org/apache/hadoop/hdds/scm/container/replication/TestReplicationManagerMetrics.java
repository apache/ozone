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

package org.apache.hadoop.hdds.scm.container.replication;

import static org.apache.ozone.test.MetricsAsserts.getLongGauge;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the ReplicationManagerMetrics class.
 */
public class TestReplicationManagerMetrics {

  private ReplicationManagerMetrics metrics;

  @BeforeEach
  public void setup() {
    ConfigurationSource conf = new OzoneConfiguration();
    ReplicationManager.ReplicationManagerConfiguration rmConf = conf
        .getObject(ReplicationManager.ReplicationManagerConfiguration.class);
    ReplicationManager replicationManager = mock(ReplicationManager.class);
    ReplicationManagerReport report = new ReplicationManagerReport(rmConf.getContainerSampleLimit());

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
    when(replicationManager.getConfig()).thenReturn(rmConf);
    when(replicationManager.getContainerReport()).thenReturn(report);
    when(replicationManager.getContainerReplicaPendingOps())
        .thenReturn(mock(ContainerReplicaPendingOps.class));
    when(replicationManager.getQueue()).thenReturn(new ReplicationQueue());
    metrics = ReplicationManagerMetrics.create(replicationManager);
  }

  @AfterEach
  public void after() {
    metrics.unRegister();
  }

  @Test
  public void testLifeCycleStateMetricsPresent() {
    assertEquals(HddsProtos.LifeCycleState.OPEN.getNumber(), getGauge("OpenContainers"));
    assertEquals(HddsProtos.LifeCycleState.CLOSING.getNumber(), getGauge("ClosingContainers"));
    assertEquals(HddsProtos.LifeCycleState.QUASI_CLOSED.getNumber(), getGauge("QuasiClosedContainers"));
    assertEquals(HddsProtos.LifeCycleState.CLOSED.getNumber(), getGauge("ClosedContainers"));
    assertEquals(HddsProtos.LifeCycleState.DELETING.getNumber(), getGauge("DeletingContainers"));
    assertEquals(HddsProtos.LifeCycleState.DELETED.getNumber(), getGauge("DeletedContainers"));
  }

  @Test
  public void testHealthStateMetricsPresent() {
    for (ReplicationManagerReport.HealthState s :
        ReplicationManagerReport.HealthState.values()) {
      assertEquals(s.ordinal(), getGauge(s.getMetricName()));
    }
  }

  private long getGauge(String metricName) {
    return getLongGauge(metricName,
        getMetrics(ReplicationManagerMetrics.METRICS_SOURCE_NAME));
  }

}
