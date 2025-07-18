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

package org.apache.hadoop.ozone.container.placement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.junit.jupiter.api.Test;

/**
 * Tests that test Metrics that support placement.
 */
public class TestDatanodeMetrics {
  @Test
  public void testSCMNodeMetric() {
    SCMNodeStat stat = new SCMNodeStat(100L, 10L, 90L, 0, 80, 0);
    assertEquals((long) stat.getCapacity().get(), 100L);
    assertEquals(10L, (long) stat.getScmUsed().get());
    assertEquals(90L, (long) stat.getRemaining().get());
    SCMNodeMetric metric = new SCMNodeMetric(stat);

    SCMNodeStat newStat = new SCMNodeStat(100L, 10L, 90L, 0, 80, 0);
    assertEquals(100L, (long) stat.getCapacity().get());
    assertEquals(10L, (long) stat.getScmUsed().get());
    assertEquals(90L, (long) stat.getRemaining().get());

    SCMNodeMetric newMetric = new SCMNodeMetric(newStat);
    assertTrue(metric.isEqual(newMetric.get()));

    newMetric.add(stat);
    assertTrue(newMetric.isGreater(metric.get()));

    SCMNodeMetric zeroMetric = new SCMNodeMetric(new SCMNodeStat());
    // Assert we can handle zero capacity.
    assertTrue(metric.isGreater(zeroMetric.get()));

    // Another case when nodes have similar weight
    SCMNodeStat stat1 = new SCMNodeStat(10000000L, 50L, 9999950L, 0, 100000, 0);
    SCMNodeStat stat2 = new SCMNodeStat(10000000L, 51L, 9999949L, 0, 100000, 0);
    assertTrue(new SCMNodeMetric(stat2).isGreater(stat1));
  }
}
