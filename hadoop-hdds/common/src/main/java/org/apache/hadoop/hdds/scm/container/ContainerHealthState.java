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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Enum representing container health states.
 *
 * <p>Each health state has a unique short value (2 bytes). This enum provides
 * named constants for:
 * - Individual health states (e.g., UNDER_REPLICATED)
 * - Common combinations (e.g., MISSING_EMPTY)
 *
 * <p>Individual states use values 0-99, combinations use values 100+.
 * This allows for easy identification and provides room for thousands of
 * possible combinations.
 *
 * @see ReplicationManagerReport.HealthState
 */
public enum ContainerHealthState {

  // ========== Individual Health States (0-99) ==========

  /**
   * Container is healthy with no issues.
   */
  HEALTHY((short) 0, "Container is healthy"),

  /**
   * Container has insufficient replicas.
   */
  UNDER_REPLICATED((short) 1, "Insufficient replicas"),

  /**
   * Container violates placement policy.
   */
  MIS_REPLICATED((short) 2, "Violates placement policy"),

  /**
   * Container has excess replicas.
   */
  OVER_REPLICATED((short) 3, "Excess replicas"),

  /**
   * Critical: No online replicas available.
   */
  MISSING((short) 4, "No online replicas"),

  /**
   * Closed/Quasi-Closed with inconsistent replica states.
   */
  UNHEALTHY((short) 5, "Replicas in inconsistent states"),

  /**
   * Container has no blocks (metadata only).
   */
  EMPTY((short) 6, "No blocks"),

  /**
   * Open container with inconsistent replica states.
   */
  OPEN_UNHEALTHY((short) 7, "Open with inconsistent replica states"),

  /**
   * Container stuck in QUASI_CLOSED state.
   */
  QUASI_CLOSED_STUCK((short) 8, "Stuck in quasi-closed state"),

  /**
   * Open container without healthy pipeline.
   */
  OPEN_WITHOUT_PIPELINE((short) 9, "Open without healthy pipeline"),

  /**
   * Data checksum mismatch across replicas (Recon-specific).
   */
  REPLICA_MISMATCH((short) 10, "Data checksum mismatch"),

  // ========== Common Combinations (100+) ==========

  /**
   * Container is missing AND empty.
   * Common when a container was never written to and all replicas are lost.
   */
  MISSING_EMPTY((short) 100, "Missing and empty", MISSING, EMPTY),

  /**
   * Container is under-replicated AND mis-replicated.
   * Common when replicas are lost and remaining ones violate placement.
   */
  UNDER_REPLICATED_MIS_REPLICATED((short) 101,
      "Insufficient replicas with placement violation",
      UNDER_REPLICATED, MIS_REPLICATED),

  /**
   * Container is under-replicated AND empty.
   * Common for new containers that haven't been fully written.
   */
  UNDER_REPLICATED_EMPTY((short) 102,
      "Insufficient replicas and empty",
      UNDER_REPLICATED, EMPTY),

  /**
   * Container is over-replicated AND mis-replicated.
   * Common after rebalancing when replicas land on wrong racks.
   */
  OVER_REPLICATED_MIS_REPLICATED((short) 103,
      "Excess replicas with placement violation",
      OVER_REPLICATED, MIS_REPLICATED),

  /**
   * Container is under-replicated, mis-replicated, AND empty.
   * Critical combination for new containers with placement issues.
   */
  UNDER_REPLICATED_MIS_REPLICATED_EMPTY((short) 104,
      "Insufficient replicas, placement violation, and empty",
      UNDER_REPLICATED, MIS_REPLICATED, EMPTY),

  /**
   * Container is unhealthy AND under-replicated.
   * Common when replica state mismatch occurs during under-replication.
   */
  UNHEALTHY_UNDER_REPLICATED((short) 105,
      "Inconsistent states with insufficient replicas",
      UNHEALTHY, UNDER_REPLICATED),

  /**
   * Open container is unhealthy AND without pipeline.
   * Critical: Open container can't be written to.
   */
  OPEN_UNHEALTHY_WITHOUT_PIPELINE((short) 106,
      "Open with inconsistent states and no pipeline",
      OPEN_UNHEALTHY, OPEN_WITHOUT_PIPELINE),

  /**
   * Container has replica mismatch AND is under-replicated.
   * Data integrity issue with insufficient redundancy.
   */
  REPLICA_MISMATCH_UNDER_REPLICATED((short) 107,
      "Data mismatch with insufficient replicas",
      REPLICA_MISMATCH, UNDER_REPLICATED);

  // ========== Enum Fields ==========

  private final short value;
  private final String description;
  private final Set<ContainerHealthState> individualStates;

  // Static lookup map for efficient fromValue()
  private static final Map<Short, ContainerHealthState> VALUE_MAP = new HashMap<>();

  static {
    for (ContainerHealthState state : values()) {
      VALUE_MAP.put(state.value, state);
    }
  }

  // Individual states constructor
  ContainerHealthState(short value, String description) {
    this.value = value;
    this.description = description;
    this.individualStates = EnumSet.of(this);
  }

  // Combination states constructor
  ContainerHealthState(short value, String description,
                       ContainerHealthState... components) {
    this.value = value;
    this.description = description;
    this.individualStates = EnumSet.noneOf(ContainerHealthState.class);
    for (ContainerHealthState component : components) {
      if (component.isIndividual()) {
        this.individualStates.add(component);
      } else {
        // If component is itself a combination, add its individual states
        this.individualStates.addAll(component.individualStates);
      }
    }
  }

  public short getValue() {
    return value;
  }

  public String getDescription() {
    return description;
  }

  /**
   * Check if this is an individual state (value 0-99).
   *
   * @return true if individual state, false if combination
   */
  public boolean isIndividual() {
    return value >= 0 && value <= 99;
  }

  /**
   * Check if this is a combination state (value 100+).
   *
   * @return true if combination state
   */
  public boolean isCombination() {
    return value >= 100;
  }

  // ========== Query Methods ==========

  /**
   * Check if this health state contains a specific individual state.
   *
   * @param individualState An individual health state to check
   * @return true if this state contains the individual state
   *
   * <p>Example:
   * <pre>
   * UNDER_REPLICATED_EMPTY.contains(UNDER_REPLICATED) → true
   * UNDER_REPLICATED_EMPTY.contains(MISSING) → false
   * </pre>
   */
  public boolean contains(ContainerHealthState individualState) {
    if (!individualState.isIndividual()) {
      throw new IllegalArgumentException(
          "Argument must be an individual state, not: " + individualState);
    }
    return this.individualStates.contains(individualState);
  }

  /**
   * Check if this health state contains ALL of the specified states.
   *
   * @param states Set of health states to check
   * @return true if this state contains all specified states
   */
  public boolean containsAll(Set<ContainerHealthState> states) {
    Set<ContainerHealthState> flatStates = new HashSet<>();
    for (ContainerHealthState state : states) {
      if (state.isIndividual()) {
        flatStates.add(state);
      } else {
        flatStates.addAll(state.individualStates);
      }
    }
    return this.individualStates.containsAll(flatStates);
  }

  /**
   * Check if this health state contains ANY of the specified states.
   *
   * @param states Set of health states to check
   * @return true if this state contains at least one specified state
   */
  public boolean containsAny(Set<ContainerHealthState> states) {
    for (ContainerHealthState state : states) {
      if (state.isIndividual()) {
        if (this.individualStates.contains(state)) {
          return true;
        }
      } else {
        // Check if any of the combination's individual states overlap
        for (ContainerHealthState individual : state.individualStates) {
          if (this.individualStates.contains(individual)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * Get all individual health states present in this state.
   *
   * @return Set of individual health states
   *
   * <p>Example:
   * <pre>
   * UNDER_REPLICATED_EMPTY.getIndividualStates()
   *   → {UNDER_REPLICATED, EMPTY}
   * </pre>
   */
  public Set<ContainerHealthState> getIndividualStates() {
    return EnumSet.copyOf(individualStates);
  }

  /**
   * Count how many individual health states are present.
   *
   * @return Number of individual health issues
   */
  public int getHealthIssueCount() {
    return individualStates.size();
  }

  /**
   * Check if container is healthy (no issues).
   *
   * @return true if this is HEALTHY
   */
  public boolean isHealthy() {
    return this == HEALTHY;
  }

  // ========== Factory Methods ==========

  /**
   * Create health state from short value.
   * If value matches a named constant, returns that constant.
   * Otherwise, returns HEALTHY for unknown values.
   *
   * @param value The short value
   * @return ContainerHealthState enum constant
   */
  public static ContainerHealthState fromValue(short value) {
    ContainerHealthState state = VALUE_MAP.get(value);
    return state != null ? state : HEALTHY;
  }

  /**
   * Create health state from ReplicationManager health states.
   * Tries to find a matching named constant (individual or combination).
   * If no exact match exists, returns the most critical individual state.
   *
   * @param healthStates Set of RM health states
   * @return ContainerHealthState representing the state(s)
   */
  public static ContainerHealthState fromReplicationManagerStates(
      Set<ReplicationManagerReport.HealthState> healthStates) {

    if (healthStates == null || healthStates.isEmpty()) {
      return HEALTHY;
    }

    // Build set of ContainerHealthState equivalents
    Set<ContainerHealthState> containerStates = EnumSet.noneOf(ContainerHealthState.class);

    for (ReplicationManagerReport.HealthState rmState : healthStates) {
      switch (rmState) {
      case UNDER_REPLICATED:
        containerStates.add(UNDER_REPLICATED);
        break;
      case MIS_REPLICATED:
        containerStates.add(MIS_REPLICATED);
        break;
      case OVER_REPLICATED:
        containerStates.add(OVER_REPLICATED);
        break;
      case MISSING:
        containerStates.add(MISSING);
        break;
      case UNHEALTHY:
        containerStates.add(UNHEALTHY);
        break;
      case EMPTY:
        containerStates.add(EMPTY);
        break;
      case OPEN_UNHEALTHY:
        containerStates.add(OPEN_UNHEALTHY);
        break;
      case QUASI_CLOSED_STUCK:
        containerStates.add(QUASI_CLOSED_STUCK);
        break;
      case OPEN_WITHOUT_PIPELINE:
        containerStates.add(OPEN_WITHOUT_PIPELINE);
        break;
      default:
        // Unknown state, ignore
        break;
      }
    }

    // Try to find exact match in named constants
    return findBestMatch(containerStates);
  }

  /**
   * Find the best matching named constant for a set of individual states.
   * First tries to find an exact combination match.
   * If not found, returns the most critical individual state.
   *
   * @param states Set of individual health states
   * @return Best matching ContainerHealthState
   */
  public static ContainerHealthState findBestMatch(Set<ContainerHealthState> states) {
    if (states == null || states.isEmpty()) {
      return HEALTHY;
    }

    // Single state - return it directly
    if (states.size() == 1) {
      return states.iterator().next();
    }

    // Try to find exact combination match
    for (ContainerHealthState state : values()) {
      if (state.isCombination() && state.individualStates.equals(states)) {
        return state;
      }
    }

    // No exact match - return most critical individual state by priority
    // Priority: MISSING > OPEN_WITHOUT_PIPELINE > QUASI_CLOSED_STUCK >
    //          UNDER_REPLICATED > UNHEALTHY > OPEN_UNHEALTHY >
    //          MIS_REPLICATED > OVER_REPLICATED > EMPTY > REPLICA_MISMATCH
    if (states.contains(MISSING)) {
      return MISSING;
    }
    if (states.contains(OPEN_WITHOUT_PIPELINE)) {
      return OPEN_WITHOUT_PIPELINE;
    }
    if (states.contains(QUASI_CLOSED_STUCK)) {
      return QUASI_CLOSED_STUCK;
    }
    if (states.contains(UNDER_REPLICATED)) {
      return UNDER_REPLICATED;
    }
    if (states.contains(UNHEALTHY)) {
      return UNHEALTHY;
    }
    if (states.contains(OPEN_UNHEALTHY)) {
      return OPEN_UNHEALTHY;
    }
    if (states.contains(MIS_REPLICATED)) {
      return MIS_REPLICATED;
    }
    if (states.contains(OVER_REPLICATED)) {
      return OVER_REPLICATED;
    }
    if (states.contains(EMPTY)) {
      return EMPTY;
    }
    if (states.contains(REPLICA_MISMATCH)) {
      return REPLICA_MISMATCH;
    }

    return HEALTHY;
  }

  /**
   * Combine multiple individual health states into one.
   * Returns a named constant if one exists for this exact combination.
   * Otherwise returns the most critical individual state.
   *
   * @param states Set of individual health states
   * @return Combined health state or most critical individual
   */
  public static ContainerHealthState combine(Set<ContainerHealthState> states) {
    return findBestMatch(states);
  }

  // ========== String Representation ==========

  @Override
  public String toString() {
    if (isHealthy()) {
      return "HEALTHY";
    }

    if (isIndividual()) {
      return name();
    }

    // For combinations, show the individual states
    StringBuilder sb = new StringBuilder(name());
    sb.append(" [");
    boolean first = true;
    for (ContainerHealthState state : individualStates) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(state.name());
      first = false;
    }
    sb.append("]");

    return sb.toString();
  }
}
