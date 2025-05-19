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

package org.apache.hadoop.hdds.scm.pipeline;

import static org.apache.ozone.test.MetricsAsserts.assertCounter;
import static org.apache.ozone.test.MetricsAsserts.getLongCounter;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Test cases to verify the metrics exposed by SCMPipelineManager.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestSCMPipelineMetrics implements NonHATests.TestCase {

  private MiniOzoneCluster cluster;

  @BeforeEach
  public void setup() throws Exception {
    cluster = cluster();
  }

  /**
   * Verifies pipeline creation metric.
   */
  @Test
  public void testPipelineCreation() {
    MetricsRecordBuilder metrics = getMetrics(
        SCMPipelineMetrics.class.getSimpleName());
    long numPipelineCreated =
        getLongCounter("NumPipelineCreated", metrics);
    // Pipelines are created in background when the cluster starts.
    assertThat(numPipelineCreated).isGreaterThan(0);
  }

  /**
   * Verifies pipeline destroy metric.
   */
  @Test
  public void testPipelineDestroy() {
    final String sourceName = SCMPipelineMetrics.class.getSimpleName();
    final String metricName = "NumPipelineDestroyed";
    final long initialDestroyed = getLongCounter(metricName, getMetrics(sourceName));
    PipelineManager pipelineManager = cluster
        .getStorageContainerManager().getPipelineManager();
    Optional<Pipeline> pipeline = pipelineManager
        .getPipelines().stream().findFirst();
    assertTrue(pipeline.isPresent());
    assertDoesNotThrow(() -> {
      PipelineManager pm = cluster.getStorageContainerManager()
          .getPipelineManager();
      pm.closePipeline(pipeline.get().getId());
      pm.deletePipeline(pipeline.get().getId());
    });
    assertCounter(metricName, initialDestroyed + 1, getMetrics(sourceName));
  }

  @Test
  public void testNumBlocksAllocated() throws IOException, TimeoutException {
    AllocatedBlock block =
        cluster.getStorageContainerManager().getScmBlockManager()
            .allocateBlock(5,
                RatisReplicationConfig.getInstance(ReplicationFactor.ONE),
                "Test", new ExcludeList());
    MetricsRecordBuilder metrics =
        getMetrics(SCMPipelineMetrics.class.getSimpleName());
    Pipeline pipeline = block.getPipeline();
    final String metricName = SCMPipelineMetrics.getBlockAllocationMetricName(pipeline);
    long numBlocksAllocated = getLongCounter(metricName, metrics);
    assertThat(numBlocksAllocated).isPositive();

    // destroy the pipeline
    assertDoesNotThrow(() ->
        cluster.getStorageContainerManager().getClientProtocolServer()
            .closePipeline(pipeline.getId().getProtobuf()));

    MetricsRecordBuilder finalMetrics =
        getMetrics(SCMPipelineMetrics.class.getSimpleName());
    Throwable t = assertThrows(AssertionError.class, () ->
        getLongCounter(metricName, finalMetrics));
    assertThat(t).hasMessageContaining(metricName);
  }
}
