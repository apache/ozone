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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.DELETED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.DELETING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.OPEN;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.QUASI_CLOSED;
import static org.apache.hadoop.metrics2.lib.Interns.info;
import static org.apache.ozone.test.MetricsAsserts.eqName;
import static org.apache.ozone.test.MetricsAsserts.getIntGauge;
import static org.apache.ozone.test.MetricsAsserts.getLongGauge;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for Recon SCM container sync metrics.
 */
class TestReconScmContainerSyncMetrics {

  private ReconScmContainerSyncMetrics metrics;

  @BeforeEach
  void setUp() {
    metrics = ReconScmContainerSyncMetrics.create();
  }

  @AfterEach
  void tearDown() {
    metrics.unRegister();
  }

  @Test
  void testStateMetricsAreEmittedForReconciledStatesOnly() {
    metrics.setContainerSyncDurationMs(OPEN, 10L);
    metrics.setContainerSyncDurationMs(QUASI_CLOSED, 20L);
    metrics.setContainerSyncDurationMs(CLOSED, 30L);
    metrics.setContainerSyncDurationMs(DELETED, 40L);
    metrics.setContainerCountDrift(OPEN, 2L);
    metrics.setContainerCountDrift(QUASI_CLOSED, 0L);
    metrics.setContainerCountDrift(CLOSED, -3L);
    metrics.setContainerCountDrift(DELETED, 4L);
    metrics.setContainerCountDrift(CLOSING, 100L);
    metrics.setContainerSyncDurationMs(DELETING, 200L);
    metrics.setScmContainerSyncStatus(
        ReconScmContainerSyncMetrics.SCM_CONTAINER_SYNC_STATUS_SUCCESS);
    metrics.setScmContainerSyncDurationMs(500L);

    MetricsRecordBuilder builder = getMetrics(metrics);

    assertEquals(ReconScmContainerSyncMetrics.SCM_CONTAINER_SYNC_STATUS_SUCCESS,
        getIntGauge("scmContainerSyncStatus", builder));
    assertEquals(500L, getLongGauge("scmContainerSyncDurationMs", builder));
    assertEquals(10L, getLongGauge("openContainerSyncDurationMs", builder));
    assertEquals(20L,
        getLongGauge("quasiClosedContainerSyncDurationMs", builder));
    assertEquals(30L, getLongGauge("closedContainerSyncDurationMs", builder));
    assertEquals(40L, getLongGauge("deletedContainerSyncDurationMs", builder));
    assertEquals(2L, getLongGauge("openContainerCountDrift", builder));
    assertEquals(0L,
        getLongGauge("quasiClosedContainerCountDrift", builder));
    assertEquals(-3L, getLongGauge("closedContainerCountDrift", builder));
    assertEquals(4L, getLongGauge("deletedContainerCountDrift", builder));

    verify(builder, never()).addGauge(
        eqName(info("closingContainerSyncDurationMs", "")), eq(100L));
    verify(builder, never()).addGauge(
        eqName(info("deletingContainerSyncDurationMs", "")), eq(200L));
    verify(builder, never()).addGauge(
        eqName(info("closingContainerCountDrift", "")), eq(100L));
    verify(builder, never()).addGauge(
        eqName(info("deletingContainerCountDrift", "")), eq(200L));
  }
}
