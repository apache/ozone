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
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.EMPTY;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.MISSING;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.MISSING_UNDER_REPLICATED;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.MIS_REPLICATED;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.OPEN_UNHEALTHY;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.OPEN_WITHOUT_PIPELINE;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.OVER_REPLICATED;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.QUASI_CLOSED_STUCK;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.QUASI_CLOSED_STUCK_OVER_REPLICATED;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.QUASI_CLOSED_STUCK_UNDER_REPLICATED;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.UNDER_REPLICATED;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.UNHEALTHY;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.UNHEALTHY_OVER_REPLICATED;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.UNHEALTHY_UNDER_REPLICATED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Tests for the ContainerHealthState enum.
 */
public class TestContainerHealthState {

  // ========== Individual State Tests ==========

  @Test
  public void testIndividualStateProperties() {
    // Test HEALTHY state
    assertEquals(0, HEALTHY.getValue());
    assertTrue(HEALTHY.isIndividual());
    assertFalse(HEALTHY.isCombination());
    assertTrue(HEALTHY.isHealthy());
    assertEquals(1, HEALTHY.getHealthIssueCount());
    assertEquals("Container is healthy", HEALTHY.getDescription());

    // Test UNDER_REPLICATED state
    assertEquals(1, UNDER_REPLICATED.getValue());
    assertTrue(UNDER_REPLICATED.isIndividual());
    assertFalse(UNDER_REPLICATED.isCombination());
    assertFalse(UNDER_REPLICATED.isHealthy());
    assertEquals(1, UNDER_REPLICATED.getHealthIssueCount());
    assertEquals("Insufficient replicas", UNDER_REPLICATED.getDescription());

    // Test MISSING state
    assertEquals(4, MISSING.getValue());
    assertTrue(MISSING.isIndividual());
    assertFalse(MISSING.isCombination());
    assertFalse(MISSING.isHealthy());
    assertEquals(1, MISSING.getHealthIssueCount());
    assertEquals("No online replicas", MISSING.getDescription());
  }

  @Test
  public void testAllIndividualStatesHaveCorrectRange() {
    // Individual states should have values 0-99
    ContainerHealthState[] individualStates = {
        HEALTHY, UNDER_REPLICATED, MIS_REPLICATED, OVER_REPLICATED,
        MISSING, UNHEALTHY, EMPTY, OPEN_UNHEALTHY,
        QUASI_CLOSED_STUCK, OPEN_WITHOUT_PIPELINE
    };

    for (ContainerHealthState state : individualStates) {
      assertTrue(state.getValue() >= 0 && state.getValue() <= 99,
          "Individual state " + state + " should have value 0-99, got: " + state.getValue());
      assertTrue(state.isIndividual());
      assertFalse(state.isCombination());
      assertEquals(1, state.getHealthIssueCount(),
          "Individual state should have exactly 1 health issue");
    }
  }

  // ========== Combination State Tests ==========

  @Test
  public void testCombinationStateProperties() {
    // Test UNHEALTHY_UNDER_REPLICATED combination
    assertEquals(100, UNHEALTHY_UNDER_REPLICATED.getValue());
    assertFalse(UNHEALTHY_UNDER_REPLICATED.isIndividual());
    assertTrue(UNHEALTHY_UNDER_REPLICATED.isCombination());
    assertFalse(UNHEALTHY_UNDER_REPLICATED.isHealthy());
    assertEquals(2, UNHEALTHY_UNDER_REPLICATED.getHealthIssueCount());
    assertEquals("Inconsistent states with insufficient replicas", UNHEALTHY_UNDER_REPLICATED.getDescription());

    // Verify it contains the correct individual states
    assertTrue(UNHEALTHY_UNDER_REPLICATED.contains(UNHEALTHY));
    assertTrue(UNHEALTHY_UNDER_REPLICATED.contains(UNDER_REPLICATED));
    assertFalse(UNHEALTHY_UNDER_REPLICATED.contains(MISSING));
  }

  @Test
  public void testUnhealthyOverReplicatedCombination() {
    assertEquals(101, UNHEALTHY_OVER_REPLICATED.getValue());
    assertTrue(UNHEALTHY_OVER_REPLICATED.isCombination());
    assertEquals(2, UNHEALTHY_OVER_REPLICATED.getHealthIssueCount());

    // Verify components
    assertTrue(UNHEALTHY_OVER_REPLICATED.contains(UNHEALTHY));
    assertTrue(UNHEALTHY_OVER_REPLICATED.contains(OVER_REPLICATED));
    assertFalse(UNHEALTHY_OVER_REPLICATED.contains(UNDER_REPLICATED));
  }

  @Test
  public void testCombinationStateValuesAreUnique() {
    // Verify all combination states have unique values in the 100+ range
    assertEquals(100, UNHEALTHY_UNDER_REPLICATED.getValue());
    assertEquals(101, UNHEALTHY_OVER_REPLICATED.getValue());
    assertEquals(102, MISSING_UNDER_REPLICATED.getValue());
    assertEquals(103, QUASI_CLOSED_STUCK_UNDER_REPLICATED.getValue());
    assertEquals(104, QUASI_CLOSED_STUCK_OVER_REPLICATED.getValue());
    
    // Verify they are recognized as combinations
    assertTrue(UNHEALTHY_UNDER_REPLICATED.isCombination());
    assertTrue(UNHEALTHY_OVER_REPLICATED.isCombination());
    assertTrue(MISSING_UNDER_REPLICATED.isCombination());
    assertTrue(QUASI_CLOSED_STUCK_UNDER_REPLICATED.isCombination());
    assertTrue(QUASI_CLOSED_STUCK_OVER_REPLICATED.isCombination());
    
    // Verify they each have exactly 2 component states
    assertEquals(2, UNHEALTHY_UNDER_REPLICATED.getHealthIssueCount());
    assertEquals(2, UNHEALTHY_OVER_REPLICATED.getHealthIssueCount());
    assertEquals(2, MISSING_UNDER_REPLICATED.getHealthIssueCount());
    assertEquals(2, QUASI_CLOSED_STUCK_UNDER_REPLICATED.getHealthIssueCount());
    assertEquals(2, QUASI_CLOSED_STUCK_OVER_REPLICATED.getHealthIssueCount());
  }

  @Test
  public void testAllCombinationStatesHaveCorrectRange() {
    // Combination states should have values 100+
    ContainerHealthState[] combinationStates = {
        UNHEALTHY_UNDER_REPLICATED,
        UNHEALTHY_OVER_REPLICATED,
        MISSING_UNDER_REPLICATED,
        QUASI_CLOSED_STUCK_UNDER_REPLICATED,
        QUASI_CLOSED_STUCK_OVER_REPLICATED
    };

    for (ContainerHealthState state : combinationStates) {
      assertTrue(state.getValue() >= 100,
          "Combination state " + state + " should have value 100+, got: " + state.getValue());
      assertFalse(state.isIndividual());
      assertTrue(state.isCombination());
      assertTrue(state.getHealthIssueCount() >= 2,
          "Combination state should have at least 2 health issues");
    }
  }

  // ========== Contains Methods Tests ==========

  @Test
  public void testContainsIndividualState() {
    // Individual state contains itself
    assertTrue(UNDER_REPLICATED.contains(UNDER_REPLICATED));
    assertFalse(UNDER_REPLICATED.contains(MIS_REPLICATED));

    // Combination contains its components
    assertTrue(UNHEALTHY_UNDER_REPLICATED.contains(UNHEALTHY));
    assertTrue(UNHEALTHY_UNDER_REPLICATED.contains(UNDER_REPLICATED));
    assertFalse(UNHEALTHY_UNDER_REPLICATED.contains(MISSING));
  }

  @Test
  public void testContainsThrowsOnCombination() {
    // contains() should only accept individual states
    assertThrows(IllegalArgumentException.class, () -> {
      UNHEALTHY_UNDER_REPLICATED.contains(UNHEALTHY_OVER_REPLICATED);
    });
  }

  @Test
  public void testContainsAll() {
    Set<ContainerHealthState> states = EnumSet.of(UNHEALTHY, UNDER_REPLICATED);

    // Exact match
    assertTrue(UNHEALTHY_UNDER_REPLICATED.containsAll(states));

    // Subset - individual state doesn't contain all
    assertFalse(UNHEALTHY.containsAll(states));
    assertFalse(UNDER_REPLICATED.containsAll(states));

    // No overlap
    Set<ContainerHealthState> noOverlap = EnumSet.of(MISSING, OVER_REPLICATED);
    assertFalse(UNHEALTHY_UNDER_REPLICATED.containsAll(noOverlap));
  }

  @Test
  public void testContainsAny() {
    Set<ContainerHealthState> states = EnumSet.of(UNHEALTHY, MISSING);

    // Partial overlap - UNHEALTHY_UNDER_REPLICATED contains UNHEALTHY
    assertTrue(UNHEALTHY_UNDER_REPLICATED.containsAny(states));

    // Full overlap
    Set<ContainerHealthState> bothStates = EnumSet.of(UNHEALTHY, UNDER_REPLICATED);
    assertTrue(UNHEALTHY_UNDER_REPLICATED.containsAny(bothStates));

    // No overlap
    Set<ContainerHealthState> noOverlap = EnumSet.of(MISSING, EMPTY);
    assertFalse(UNHEALTHY_UNDER_REPLICATED.containsAny(noOverlap));
  }

  @Test
  public void testContainsAllWithCombinations() {
    // containsAll should flatten combinations
    Set<ContainerHealthState> states = EnumSet.of(UNHEALTHY_UNDER_REPLICATED);

    // UNHEALTHY_UNDER_REPLICATED contains itself (flattens to UNHEALTHY + UNDER_REPLICATED)
    assertTrue(UNHEALTHY_UNDER_REPLICATED.containsAll(states));

    // UNHEALTHY_OVER_REPLICATED doesn't contain UNDER_REPLICATED
    assertFalse(UNHEALTHY_OVER_REPLICATED.containsAll(states));
  }

  @Test
  public void testContainsAnyWithCombinations() {
    // containsAny should flatten combinations
    Set<ContainerHealthState> states = EnumSet.of(UNHEALTHY_UNDER_REPLICATED);

    // UNHEALTHY_OVER_REPLICATED has UNHEALTHY which overlaps with UNHEALTHY_UNDER_REPLICATED
    assertTrue(UNHEALTHY_OVER_REPLICATED.containsAny(states));

    // Individual MISSING has no overlap
    Set<ContainerHealthState> noOverlap = EnumSet.of(MISSING);
    assertFalse(UNHEALTHY_UNDER_REPLICATED.containsAny(noOverlap));
  }

  // ========== Get Individual States Tests ==========

  @Test
  public void testGetIndividualStates() {
    // Individual state returns itself
    Set<ContainerHealthState> healthyStates = HEALTHY.getIndividualStates();
    assertEquals(1, healthyStates.size());
    assertTrue(healthyStates.contains(HEALTHY));

    // Combination returns its components
    Set<ContainerHealthState> missingUnderStates = MISSING_UNDER_REPLICATED.getIndividualStates();
    assertEquals(2, missingUnderStates.size());
    assertTrue(missingUnderStates.contains(MISSING));
    assertTrue(missingUnderStates.contains(UNDER_REPLICATED));

    // Another combination
    Set<ContainerHealthState> quasiStuckOverStates =
        QUASI_CLOSED_STUCK_OVER_REPLICATED.getIndividualStates();
    assertEquals(2, quasiStuckOverStates.size());
    assertTrue(quasiStuckOverStates.contains(QUASI_CLOSED_STUCK));
    assertTrue(quasiStuckOverStates.contains(OVER_REPLICATED));
  }

  @Test
  public void testGetIndividualStatesReturnsImmutableCopy() {
    Set<ContainerHealthState> states = UNHEALTHY_UNDER_REPLICATED.getIndividualStates();

    // Should throw when trying to modify
    assertThrows(UnsupportedOperationException.class, () -> {
      states.add(MISSING);
    });
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
    assertEquals(UNHEALTHY_UNDER_REPLICATED,
        ContainerHealthState.fromValue((short) 100));
    assertEquals(UNHEALTHY_OVER_REPLICATED,
        ContainerHealthState.fromValue((short) 101));
    assertEquals(MISSING_UNDER_REPLICATED,
        ContainerHealthState.fromValue((short) 102));
    assertEquals(QUASI_CLOSED_STUCK_UNDER_REPLICATED,
        ContainerHealthState.fromValue((short) 103));
    assertEquals(QUASI_CLOSED_STUCK_OVER_REPLICATED,
        ContainerHealthState.fromValue((short) 104));
  }

  @Test
  public void testFromValueUnknownReturnsHealthy() {
    // Unknown values should return HEALTHY
    assertEquals(HEALTHY, ContainerHealthState.fromValue((short) 999));
    assertEquals(HEALTHY, ContainerHealthState.fromValue((short) -1));
    assertEquals(HEALTHY, ContainerHealthState.fromValue((short) 200));
  }

  // ========== FindBestMatch Tests ==========

  @Test
  public void testFindBestMatchNullOrEmpty() {
    assertEquals(HEALTHY, ContainerHealthState.findBestMatch(null));
    assertEquals(HEALTHY, ContainerHealthState.findBestMatch(new HashSet<>()));
  }

  @Test
  public void testFindBestMatchSingleState() {
    Set<ContainerHealthState> single = EnumSet.of(UNDER_REPLICATED);
    assertEquals(UNDER_REPLICATED, ContainerHealthState.findBestMatch(single));
  }

  @Test
  public void testFindBestMatchExactCombination() {
    Set<ContainerHealthState> missingUnder = EnumSet.of(MISSING, UNDER_REPLICATED);
    assertEquals(MISSING_UNDER_REPLICATED, ContainerHealthState.findBestMatch(missingUnder));

    Set<ContainerHealthState> unhealthyUnder = EnumSet.of(UNHEALTHY, UNDER_REPLICATED);
    assertEquals(UNHEALTHY_UNDER_REPLICATED,
        ContainerHealthState.findBestMatch(unhealthyUnder));
  }

  @Test
  public void testFindBestMatchPriorityFallback() {
    // No exact combination for MISSING + OVER_REPLICATED
    // Should return MISSING (highest priority)
    Set<ContainerHealthState> states = EnumSet.of(MISSING, OVER_REPLICATED);
    assertEquals(MISSING, ContainerHealthState.findBestMatch(states));

    // UNDER_REPLICATED > MIS_REPLICATED
    Set<ContainerHealthState> underMisOver =
        EnumSet.of(MIS_REPLICATED, OVER_REPLICATED);
    assertEquals(MIS_REPLICATED, ContainerHealthState.findBestMatch(underMisOver));
  }

  // ========== Combine Tests ==========

  @Test
  public void testCombine() {
    // combine() is an alias for findBestMatch()
    Set<ContainerHealthState> states = EnumSet.of(UNHEALTHY, UNDER_REPLICATED);
    assertEquals(UNHEALTHY_UNDER_REPLICATED, ContainerHealthState.combine(states));

    // No exact match - returns most critical
    Set<ContainerHealthState> noMatch = EnumSet.of(MISSING, OVER_REPLICATED);
    assertEquals(MISSING, ContainerHealthState.combine(noMatch));
  }

  // ========== ToString Tests ==========

  @Test
  public void testToStringHealthy() {
    assertEquals("HEALTHY", HEALTHY.toString());
  }

  @Test
  public void testToStringIndividual() {
    assertEquals("UNDER_REPLICATED", UNDER_REPLICATED.toString());
    assertEquals("MISSING", MISSING.toString());
  }

  @Test
  public void testToStringCombination() {
    String str = MISSING_UNDER_REPLICATED.toString();
    assertTrue(str.startsWith("MISSING_UNDER_REPLICATED ["));
    assertTrue(str.contains("MISSING"));
    assertTrue(str.contains("UNDER_REPLICATED"));
    assertTrue(str.endsWith("]"));

    String quasiStr = QUASI_CLOSED_STUCK_UNDER_REPLICATED.toString();
    assertTrue(quasiStr.contains("QUASI_CLOSED_STUCK"));
    assertTrue(quasiStr.contains("UNDER_REPLICATED"));
  }

  // ========== Edge Cases and Error Handling ==========

  @Test
  public void testHealthyStateIsSpecial() {
    assertTrue(HEALTHY.isHealthy());
    assertFalse(UNDER_REPLICATED.isHealthy());
    assertFalse(MISSING_UNDER_REPLICATED.isHealthy());

    // Only HEALTHY has health issue count of 1 and contains only itself
    assertEquals(1, HEALTHY.getHealthIssueCount());
    assertTrue(HEALTHY.contains(HEALTHY));
  }

  @Test
  public void testAllEnumValuesAreUnique() {
    Set<Short> values = new HashSet<>();
    for (ContainerHealthState state : ContainerHealthState.values()) {
      assertFalse(values.contains(state.getValue()),
          "Duplicate value found: " + state.getValue());
      values.add(state.getValue());
    }
  }

  @Test
  public void testIndividualStateCountMatchesDocumentation() {
    // Should have 10 individual states (0-9)
    long individualCount = java.util.Arrays.stream(ContainerHealthState.values())
        .filter(ContainerHealthState::isIndividual)
        .count();
    assertEquals(10, individualCount, "Expected 10 individual states");
  }

  @Test
  public void testCombinationStateCountMatchesDocumentation() {
    // Should have 5 combination states (100-104)
    long combinationCount = java.util.Arrays.stream(ContainerHealthState.values())
        .filter(ContainerHealthState::isCombination)
        .count();
    assertEquals(5, combinationCount, "Expected 5 combination states");
  }

  @Test
  public void testNoGapsInIndividualStateValues() {
    // Individual states should be sequential: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
    Set<Short> expectedValues = new HashSet<>();
    for (short i = 0; i <= 9; i++) {
      expectedValues.add(i);
    }

    Set<Short> actualValues = new HashSet<>();
    for (ContainerHealthState state : ContainerHealthState.values()) {
      if (state.isIndividual()) {
        actualValues.add(state.getValue());
      }
    }

    assertEquals(expectedValues, actualValues,
        "Individual states should have values 0-9 with no gaps");
  }

  @Test
  public void testNoGapsInCombinationStateValues() {
    // Combination states should be sequential: 100, 101, 102, 103, 104
    Set<Short> expectedValues = new HashSet<>();
    for (short i = 100; i <= 104; i++) {
      expectedValues.add(i);
    }

    Set<Short> actualValues = new HashSet<>();
    for (ContainerHealthState state : ContainerHealthState.values()) {
      if (state.isCombination()) {
        actualValues.add(state.getValue());
      }
    }

    assertEquals(expectedValues, actualValues,
        "Combination states should have values 100-104 with no gaps");
  }
}
