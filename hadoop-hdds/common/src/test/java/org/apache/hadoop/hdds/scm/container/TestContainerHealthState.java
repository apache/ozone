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

package org.apache.hadoop.hdds.scm.container;

import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.EMPTY;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.HEALTHY;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.MISSING;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.MISSING_UNDER_REPLICATED;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.MIS_REPLICATED;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.OPEN_UNHEALTHY;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.OPEN_WITHOUT_PIPELINE;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.OVER_REPLICATED;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.QUASI_CLOSED_STUCK;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.QUASI_CLOSED_STUCK_MISSING;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.QUASI_CLOSED_STUCK_OVER_REPLICATED;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.QUASI_CLOSED_STUCK_UNDER_REPLICATED;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.UNDER_REPLICATED;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.UNHEALTHY;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.UNHEALTHY_OVER_REPLICATED;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.UNHEALTHY_UNDER_REPLICATED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Tests for the ContainerHealthState enum.
 */
public class TestContainerHealthState {

  // ========== Basic State Tests ==========

  @Test
  public void testIndividualStateValues() {
    // Test individual states have correct values
    assertEquals(0, HEALTHY.getValue());
    assertEquals(1, UNDER_REPLICATED.getValue());
    assertEquals(2, MIS_REPLICATED.getValue());
    assertEquals(3, OVER_REPLICATED.getValue());
    assertEquals(4, MISSING.getValue());
    assertEquals(5, UNHEALTHY.getValue());
    assertEquals(6, EMPTY.getValue());
    assertEquals(7, OPEN_UNHEALTHY.getValue());
    assertEquals(8, QUASI_CLOSED_STUCK.getValue());
    assertEquals(9, OPEN_WITHOUT_PIPELINE.getValue());
  }

  @Test
  public void testCombinationStateValues() {
    // Test combination states have correct values
    assertEquals(100, UNHEALTHY_UNDER_REPLICATED.getValue());
    assertEquals(101, UNHEALTHY_OVER_REPLICATED.getValue());
    assertEquals(102, MISSING_UNDER_REPLICATED.getValue());
    assertEquals(103, QUASI_CLOSED_STUCK_UNDER_REPLICATED.getValue());
    assertEquals(104, QUASI_CLOSED_STUCK_OVER_REPLICATED.getValue());
    assertEquals(105, ContainerHealthState.QUASI_CLOSED_STUCK_MISSING.getValue());
  }

  @Test
  public void testDescriptions() {
    assertEquals("Container is healthy", HEALTHY.getDescription());
    assertEquals("Containers with insufficient replicas", UNDER_REPLICATED.getDescription());
    assertEquals("Containers with no online replicas", MISSING.getDescription());
  }

  @Test
  public void testMetricNames() {
    assertEquals("HealthyContainers", HEALTHY.getMetricName());
    assertEquals("UnderReplicatedContainers", UNDER_REPLICATED.getMetricName());
    assertEquals("MissingContainers", MISSING.getMetricName());
    assertEquals("UnhealthyUnderReplicatedContainers", UNHEALTHY_UNDER_REPLICATED.getMetricName());
  }

  // ========== FromValue Tests ==========

  @Test
  public void testFromValueIndividualStates() {
    assertEquals(HEALTHY, ContainerHealthState.fromValue((short) 0));
    assertEquals(UNDER_REPLICATED, ContainerHealthState.fromValue((short) 1));
    assertEquals(MIS_REPLICATED, ContainerHealthState.fromValue((short) 2));
    assertEquals(OVER_REPLICATED, ContainerHealthState.fromValue((short) 3));
    assertEquals(MISSING, ContainerHealthState.fromValue((short) 4));
    assertEquals(UNHEALTHY, ContainerHealthState.fromValue((short) 5));
    assertEquals(EMPTY, ContainerHealthState.fromValue((short) 6));
    assertEquals(OPEN_UNHEALTHY, ContainerHealthState.fromValue((short) 7));
    assertEquals(QUASI_CLOSED_STUCK, ContainerHealthState.fromValue((short) 8));
    assertEquals(OPEN_WITHOUT_PIPELINE, ContainerHealthState.fromValue((short) 9));
  }

  @Test
  public void testFromValueCombinationStates() {
    assertEquals(UNHEALTHY_UNDER_REPLICATED, ContainerHealthState.fromValue((short) 100));
    assertEquals(UNHEALTHY_OVER_REPLICATED, ContainerHealthState.fromValue((short) 101));
    assertEquals(MISSING_UNDER_REPLICATED, ContainerHealthState.fromValue((short) 102));
    assertEquals(QUASI_CLOSED_STUCK_UNDER_REPLICATED, ContainerHealthState.fromValue((short) 103));
    assertEquals(QUASI_CLOSED_STUCK_OVER_REPLICATED, ContainerHealthState.fromValue((short) 104));
    assertEquals(QUASI_CLOSED_STUCK_MISSING, ContainerHealthState.fromValue((short) 105));
  }

  @Test
  public void testFromValueUnknownReturnsHealthy() {
    // Unknown values should return HEALTHY
    assertEquals(HEALTHY, ContainerHealthState.fromValue((short) 999));
    assertEquals(HEALTHY, ContainerHealthState.fromValue((short) -1));
    assertEquals(HEALTHY, ContainerHealthState.fromValue((short) 200));
  }

  // ========== Enum Integrity Tests ==========

  @Test
  public void testAllEnumValuesAreUnique() {
    // Verify all enum constants have unique values
    java.util.Set<Short> values = new java.util.HashSet<>();
    for (ContainerHealthState state : ContainerHealthState.values()) {
      assertFalse(values.contains(state.getValue()),
          "Duplicate value found: " + state.getValue());
      values.add(state.getValue());
    }
  }

  @Test
  public void testIndividualStateCount() {
    // Should have 10 individual states (0-9)
    long individualCount = java.util.Arrays.stream(ContainerHealthState.values())
        .filter(s -> s.getValue() >= 0 && s.getValue() <= 99)
        .count();
    assertEquals(10, individualCount, "Expected 10 individual states");
  }

  @Test
  public void testCombinationStateCount() {
    // Should have 6 combination states (100-105)
    long combinationCount = java.util.Arrays.stream(ContainerHealthState.values())
        .filter(s -> s.getValue() >= 100)
        .count();
    assertEquals(6, combinationCount, "Expected 6 combination states");
  }

  @Test
  public void testNoGapsInIndividualValues() {
    // Individual states should be sequential: 0-9
    for (short i = 0; i <= 9; i++) {
      ContainerHealthState state = ContainerHealthState.fromValue(i);
      assertTrue(state.getValue() >= 0 && state.getValue() <= 9,
          "Value " + i + " should map to an individual state");
    }
  }

  @Test
  public void testNoGapsInCombinationValues() {
    // Combination states should be sequential: 100-105
    for (short i = 100; i <= 105; i++) {
      ContainerHealthState state = ContainerHealthState.fromValue(i);
      assertTrue(state.getValue() >= 100,
          "Value " + i + " should map to a combination state");
    }
  }
}
