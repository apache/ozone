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

package org.apache.hadoop.ozone.container.metrics;

import static org.apache.commons.text.WordUtils.capitalize;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeQueueMetrics.COMMAND_DISPATCHER_QUEUE_PREFIX;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeQueueMetrics.STATE_CONTEXT_COMMAND_QUEUE_PREFIX;
import static org.apache.ozone.test.MetricsAsserts.getLongGauge;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeQueueMetrics;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Test for queue metrics of datanodes.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestDatanodeQueueMetrics implements NonHATests.TestCase {

  @Test
  public void testQueueMetrics() {
    for (SCMCommandProto.Type type: SCMCommandProto.Type.values()) {
      String typeSize = capitalize(String.valueOf(type)) + "Size";
      assertThat(getGauge(STATE_CONTEXT_COMMAND_QUEUE_PREFIX + typeSize))
          .isGreaterThanOrEqualTo(0);
      assertThat(getGauge(COMMAND_DISPATCHER_QUEUE_PREFIX + typeSize))
          .isGreaterThanOrEqualTo(0);
    }
  }

  private long getGauge(String metricName) {
    return getLongGauge(metricName,
        getMetrics(DatanodeQueueMetrics.METRICS_SOURCE_NAME));
  }
}
