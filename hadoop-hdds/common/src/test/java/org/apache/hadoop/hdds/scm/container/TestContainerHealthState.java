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
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.MISSING_EMPTY;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.MIS_REPLICATED;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.OPEN_UNHEALTHY;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.OPEN_UNHEALTHY_WITHOUT_PIPELINE;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.OPEN_WITHOUT_PIPELINE;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.OVER_REPLICATED;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.OVER_REPLICATED_MIS_REPLICATED;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.QUASI_CLOSED_STUCK;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.REPLICA_MISMATCH;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.REPLICA_MISMATCH_UNDER_REPLICATED;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.UNDER_REPLICATED;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.UNDER_REPLICATED_EMPTY;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.UNDER_REPLICATED_MIS_REPLICATED;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.UNDER_REPLICATED_MIS_REPLICATED_EMPTY;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.UNHEALTHY;
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
        QUASI_CLOSED_STUCK, OPEN_WITHOUT_PIPELINE, REPLICA_MISMATCH
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
    // Test MISSING_EMPTY combination
    assertEquals(100, MISSING_EMPTY.getValue());
    assertFalse(MISSING_EMPTY.isIndividual());
    assertTrue(MISSING_EMPTY.isCombination());
    assertFalse(MISSING_EMPTY.isHealthy());
    assertEquals(2, MISSING_EMPTY.getHealthIssueCount());
    assertEquals("Missing and empty", MISSING_EMPTY.getDescription());

    // Verify it contains the correct individual states
    assertTrue(MISSING_EMPTY.contains(MISSING));
    assertTrue(MISSING_EMPTY.contains(EMPTY));
    assertFalse(MISSING_EMPTY.contains(UNDER_REPLICATED));
  }

  @Test
  public void testUnderReplicatedMisReplicatedCombination() {
    assertEquals(101, UNDER_REPLICATED_MIS_REPLICATED.getValue());
    assertTrue(UNDER_REPLICATED_MIS_REPLICATED.isCombination());
    assertEquals(2, UNDER_REPLICATED_MIS_REPLICATED.getHealthIssueCount());

    // Verify components
    assertTrue(UNDER_REPLICATED_MIS_REPLICATED.contains(UNDER_REPLICATED));
    assertTrue(UNDER_REPLICATED_MIS_REPLICATED.contains(MIS_REPLICATED));
    assertFalse(UNDER_REPLICATED_MIS_REPLICATED.contains(EMPTY));
  }

  @Test
  public void testTripleCombinationState() {
    // Test UNDER_REPLICATED_MIS_REPLICATED_EMPTY (3 individual states)
    assertEquals(104, UNDER_REPLICATED_MIS_REPLICATED_EMPTY.getValue());
    assertTrue(UNDER_REPLICATED_MIS_REPLICATED_EMPTY.isCombination());
    assertEquals(3, UNDER_REPLICATED_MIS_REPLICATED_EMPTY.getHealthIssueCount());

    // Verify all three components
    assertTrue(UNDER_REPLICATED_MIS_REPLICATED_EMPTY.contains(UNDER_REPLICATED));
    assertTrue(UNDER_REPLICATED_MIS_REPLICATED_EMPTY.contains(MIS_REPLICATED));
    assertTrue(UNDER_REPLICATED_MIS_REPLICATED_EMPTY.contains(EMPTY));
    assertFalse(UNDER_REPLICATED_MIS_REPLICATED_EMPTY.contains(MISSING));
  }

  @Test
  public void testAllCombinationStatesHaveCorrectRange() {
    // Combination states should have values 100+
    ContainerHealthState[] combinationStates = {
        MISSING_EMPTY,
        UNDER_REPLICATED_MIS_REPLICATED,
        UNDER_REPLICATED_EMPTY,
        OVER_REPLICATED_MIS_REPLICATED,
        UNDER_REPLICATED_MIS_REPLICATED_EMPTY,
        UNHEALTHY_UNDER_REPLICATED,
        OPEN_UNHEALTHY_WITHOUT_PIPELINE,
        REPLICA_MISMATCH_UNDER_REPLICATED
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
    assertTrue(UNDER_REPLICATED_EMPTY.contains(UNDER_REPLICATED));
    assertTrue(UNDER_REPLICATED_EMPTY.contains(EMPTY));
    assertFalse(UNDER_REPLICATED_EMPTY.contains(MISSING));
  }

  @Test
  public void testContainsThrowsOnCombination() {
    // contains() should only accept individual states
    assertThrows(IllegalArgumentException.class, () -> {
      UNDER_REPLICATED_EMPTY.contains(MISSING_EMPTY);
    });
  }

  @Test
  public void testContainsAll() {
    Set<ContainerHealthState> states = EnumSet.of(UNDER_REPLICATED, EMPTY);

    // Exact match
    assertTrue(UNDER_REPLICATED_EMPTY.containsAll(states));

    // Superset
    assertTrue(UNDER_REPLICATED_MIS_REPLICATED_EMPTY.containsAll(states));

    // Subset
    assertFalse(UNDER_REPLICATED.containsAll(states));

    // No overlap
    Set<ContainerHealthState> noOverlap = EnumSet.of(MISSING, OVER_REPLICATED);
    assertFalse(UNDER_REPLICATED_EMPTY.containsAll(noOverlap));
  }

  @Test
  public void testContainsAny() {
    Set<ContainerHealthState> states = EnumSet.of(UNDER_REPLICATED, MISSING);

    // Full overlap
    assertTrue(UNDER_REPLICATED_EMPTY.containsAny(states));

    // Partial overlap
    assertTrue(UNDER_REPLICATED_MIS_REPLICATED.containsAny(states));

    // No overlap
    Set<ContainerHealthState> noOverlap = EnumSet.of(MISSING, OVER_REPLICATED);
    assertFalse(UNDER_REPLICATED_EMPTY.containsAny(noOverlap));
  }

  @Test
  public void testContainsAllWithCombinations() {
    // containsAll should flatten combinations
    Set<ContainerHealthState> states = EnumSet.of(MISSING_EMPTY);

    // UNDER_REPLICATED_MIS_REPLICATED_EMPTY doesn't contain MISSING
    assertFalse(UNDER_REPLICATED_MIS_REPLICATED_EMPTY.containsAll(states));

    // MISSING_EMPTY contains itself
    assertTrue(MISSING_EMPTY.containsAll(states));
  }

  @Test
  public void testContainsAnyWithCombinations() {
    // containsAny should flatten combinations
    Set<ContainerHealthState> states = EnumSet.of(MISSING_EMPTY);

    // UNDER_REPLICATED_EMPTY has EMPTY which overlaps with MISSING_EMPTY
    assertTrue(UNDER_REPLICATED_EMPTY.containsAny(states));

    // OVER_REPLICATED_MIS_REPLICATED has no overlap
    assertFalse(OVER_REPLICATED_MIS_REPLICATED.containsAny(states));
  }

  // ========== Get Individual States Tests ==========

  @Test
  public void testGetIndividualStates() {
    // Individual state returns itself
    Set<ContainerHealthState> healthyStates = HEALTHY.getIndividualStates();
    assertEquals(1, healthyStates.size());
    assertTrue(healthyStates.contains(HEALTHY));

    // Combination returns its components
    Set<ContainerHealthState> missingEmptyStates = MISSING_EMPTY.getIndividualStates();
    assertEquals(2, missingEmptyStates.size());
    assertTrue(missingEmptyStates.contains(MISSING));
    assertTrue(missingEmptyStates.contains(EMPTY));

    // Triple combination
    Set<ContainerHealthState> tripleStates =
        UNDER_REPLICATED_MIS_REPLICATED_EMPTY.getIndividualStates();
    assertEquals(3, tripleStates.size());
    assertTrue(tripleStates.contains(UNDER_REPLICATED));
    assertTrue(tripleStates.contains(MIS_REPLICATED));
    assertTrue(tripleStates.contains(EMPTY));
  }

  @Test
  public void testGetIndividualStatesReturnsImmutableCopy() {
    Set<ContainerHealthState> states = UNDER_REPLICATED_EMPTY.getIndividualStates();

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
    assertEquals(REPLICA_MISMATCH, ContainerHealthState.fromValue((short) 10));
  }

  @Test
  public void testFromValueCombinationStates() {
    assertEquals(MISSING_EMPTY, ContainerHealthState.fromValue((short) 100));
    assertEquals(UNDER_REPLICATED_MIS_REPLICATED,
        ContainerHealthState.fromValue((short) 101));
    assertEquals(UNDER_REPLICATED_EMPTY, ContainerHealthState.fromValue((short) 102));
    assertEquals(OVER_REPLICATED_MIS_REPLICATED,
        ContainerHealthState.fromValue((short) 103));
    assertEquals(UNDER_REPLICATED_MIS_REPLICATED_EMPTY,
        ContainerHealthState.fromValue((short) 104));
    assertEquals(UNHEALTHY_UNDER_REPLICATED,
        ContainerHealthState.fromValue((short) 105));
    assertEquals(OPEN_UNHEALTHY_WITHOUT_PIPELINE,
        ContainerHealthState.fromValue((short) 106));
    assertEquals(REPLICA_MISMATCH_UNDER_REPLICATED,
        ContainerHealthState.fromValue((short) 107));
  }

  @Test
  public void testFromValueUnknownReturnsHealthy() {
    // Unknown values should return HEALTHY
    assertEquals(HEALTHY, ContainerHealthState.fromValue((short) 999));
    assertEquals(HEALTHY, ContainerHealthState.fromValue((short) -1));
    assertEquals(HEALTHY, ContainerHealthState.fromValue((short) 200));
  }

  // ========== FromReplicationManagerStates Tests ==========

  @Test
  public void testFromReplicationManagerStatesEmpty() {
    Set<ReplicationManagerReport.HealthState> emptySet = new HashSet<>();
    assertEquals(HEALTHY, ContainerHealthState.fromReplicationManagerStates(emptySet));
  }

  @Test
  public void testFromReplicationManagerStatesNull() {
    assertEquals(HEALTHY, ContainerHealthState.fromReplicationManagerStates(null));
  }

  @Test
  public void testFromReplicationManagerStatesSingleState() {
    Set<ReplicationManagerReport.HealthState> singleState =
        EnumSet.of(ReplicationManagerReport.HealthState.UNDER_REPLICATED);
    assertEquals(UNDER_REPLICATED,
        ContainerHealthState.fromReplicationManagerStates(singleState));
  }

  @Test
  public void testFromReplicationManagerStatesExactCombinationMatch() {
    // MISSING + EMPTY should match MISSING_EMPTY
    Set<ReplicationManagerReport.HealthState> missingEmpty = EnumSet.of(
        ReplicationManagerReport.HealthState.MISSING,
        ReplicationManagerReport.HealthState.EMPTY
    );
    assertEquals(MISSING_EMPTY,
        ContainerHealthState.fromReplicationManagerStates(missingEmpty));

    // UNDER_REPLICATED + MIS_REPLICATED should match
    Set<ReplicationManagerReport.HealthState> underMis = EnumSet.of(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED,
        ReplicationManagerReport.HealthState.MIS_REPLICATED
    );
    assertEquals(UNDER_REPLICATED_MIS_REPLICATED,
        ContainerHealthState.fromReplicationManagerStates(underMis));
  }

  @Test
  public void testFromReplicationManagerStatesNoExactMatch() {
    // UNDER_REPLICATED + OVER_REPLICATED has no exact combination
    // Should return most critical: UNDER_REPLICATED
    Set<ReplicationManagerReport.HealthState> underOver = EnumSet.of(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED,
        ReplicationManagerReport.HealthState.OVER_REPLICATED
    );
    assertEquals(UNDER_REPLICATED,
        ContainerHealthState.fromReplicationManagerStates(underOver));
  }

  @Test
  public void testFromReplicationManagerStatesTripleCombination() {
    Set<ReplicationManagerReport.HealthState> triple = EnumSet.of(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED,
        ReplicationManagerReport.HealthState.MIS_REPLICATED,
        ReplicationManagerReport.HealthState.EMPTY
    );
    assertEquals(UNDER_REPLICATED_MIS_REPLICATED_EMPTY,
        ContainerHealthState.fromReplicationManagerStates(triple));
  }

  @Test
  public void testFromReplicationManagerStatesPriority() {
    // When no exact match, should return most critical by priority
    // Priority: MISSING > OPEN_WITHOUT_PIPELINE > QUASI_CLOSED_STUCK >
    //           UNDER_REPLICATED > UNHEALTHY > OPEN_UNHEALTHY >
    //           MIS_REPLICATED > OVER_REPLICATED > EMPTY

    // Test MISSING takes priority over everything
    Set<ReplicationManagerReport.HealthState> withMissing = EnumSet.of(
        ReplicationManagerReport.HealthState.MISSING,
        ReplicationManagerReport.HealthState.OVER_REPLICATED
    );
    assertEquals(MISSING,
        ContainerHealthState.fromReplicationManagerStates(withMissing));

    // Test UNDER_REPLICATED > MIS_REPLICATED
    Set<ReplicationManagerReport.HealthState> underOver = EnumSet.of(
        ReplicationManagerReport.HealthState.MIS_REPLICATED,
        ReplicationManagerReport.HealthState.OVER_REPLICATED
    );
    assertEquals(MIS_REPLICATED,
        ContainerHealthState.fromReplicationManagerStates(underOver));
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
    Set<ContainerHealthState> missingEmpty = EnumSet.of(MISSING, EMPTY);
    assertEquals(MISSING_EMPTY, ContainerHealthState.findBestMatch(missingEmpty));

    Set<ContainerHealthState> underMis = EnumSet.of(UNDER_REPLICATED, MIS_REPLICATED);
    assertEquals(UNDER_REPLICATED_MIS_REPLICATED,
        ContainerHealthState.findBestMatch(underMis));
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
    Set<ContainerHealthState> states = EnumSet.of(MISSING, EMPTY);
    assertEquals(MISSING_EMPTY, ContainerHealthState.combine(states));

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
    String str = MISSING_EMPTY.toString();
    assertTrue(str.startsWith("MISSING_EMPTY ["));
    assertTrue(str.contains("MISSING"));
    assertTrue(str.contains("EMPTY"));
    assertTrue(str.endsWith("]"));

    String tripleStr = UNDER_REPLICATED_MIS_REPLICATED_EMPTY.toString();
    assertTrue(tripleStr.contains("UNDER_REPLICATED"));
    assertTrue(tripleStr.contains("MIS_REPLICATED"));
    assertTrue(tripleStr.contains("EMPTY"));
  }

  // ========== Edge Cases and Error Handling ==========

  @Test
  public void testHealthyStateIsSpecial() {
    assertTrue(HEALTHY.isHealthy());
    assertFalse(UNDER_REPLICATED.isHealthy());
    assertFalse(MISSING_EMPTY.isHealthy());

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
    // Should have 11 individual states (0-10)
    long individualCount = java.util.Arrays.stream(ContainerHealthState.values())
        .filter(ContainerHealthState::isIndividual)
        .count();
    assertEquals(11, individualCount, "Expected 11 individual states");
  }

  @Test
  public void testCombinationStateCountMatchesDocumentation() {
    // Should have 8 combination states (100-107)
    long combinationCount = java.util.Arrays.stream(ContainerHealthState.values())
        .filter(ContainerHealthState::isCombination)
        .count();
    assertEquals(8, combinationCount, "Expected 8 combination states");
  }

  @Test
  public void testNoGapsInIndividualStateValues() {
    // Individual states should be sequential: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    Set<Short> expectedValues = new HashSet<>();
    for (short i = 0; i <= 10; i++) {
      expectedValues.add(i);
    }

    Set<Short> actualValues = new HashSet<>();
    for (ContainerHealthState state : ContainerHealthState.values()) {
      if (state.isIndividual()) {
        actualValues.add(state.getValue());
      }
    }

    assertEquals(expectedValues, actualValues,
        "Individual states should have values 0-10 with no gaps");
  }

  @Test
  public void testNoGapsInCombinationStateValues() {
    // Combination states should be sequential: 100, 101, 102, 103, 104, 105, 106, 107
    Set<Short> expectedValues = new HashSet<>();
    for (short i = 100; i <= 107; i++) {
      expectedValues.add(i);
    }

    Set<Short> actualValues = new HashSet<>();
    for (ContainerHealthState state : ContainerHealthState.values()) {
      if (state.isCombination()) {
        actualValues.add(state.getValue());
      }
    }

    assertEquals(expectedValues, actualValues,
        "Combination states should have values 100-107 with no gaps");
  }
}
