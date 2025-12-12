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
 * <p>Each health state has a unique short value (2 bytes), description, and metric name.
 * This enum provides named constants for:
 * - Individual health states (e.g., UNDER_REPLICATED)
 * - Actual combinations that occur in replication handlers
 *
 * <p>Individual states use values 0-99, combinations use values 100+.
 *
 * <p>This enum replaces ReplicationManagerReport.HealthState and is used both for
 * tracking container health in ContainerInfo and for metrics/reporting.
 */
public enum ContainerHealthState {

  // ========== Individual Health States (0-99) ==========

  /**
   * Container is healthy with no issues.
   */
  HEALTHY((short) 0, 
      "Container is healthy",
      "HealthyContainers"),

  /**
   * Container has insufficient replicas.
   */
  UNDER_REPLICATED((short) 1, 
      "Containers with insufficient replicas",
      "UnderReplicatedContainers"),

  /**
   * Container violates placement policy.
   */
  MIS_REPLICATED((short) 2, 
      "Containers with insufficient racks",
      "MisReplicatedContainers"),

  /**
   * Container has excess replicas.
   */
  OVER_REPLICATED((short) 3, 
      "Containers with more replicas than required",
      "OverReplicatedContainers"),

  /**
   * Critical: No online replicas available.
   */
  MISSING((short) 4, 
      "Containers with no online replicas",
      "MissingContainers"),

  /**
   * Closed/Quasi-Closed with inconsistent replica states.
   */
  UNHEALTHY((short) 5, 
      "Containers Closed or Quasi_Closed having some replicas in a different state",
      "UnhealthyContainers"),

  /**
   * Container has no blocks (metadata only).
   */
  EMPTY((short) 6, 
      "Containers having no blocks",
      "EmptyContainers"),

  /**
   * Open container with inconsistent replica states.
   */
  OPEN_UNHEALTHY((short) 7, 
      "Containers open and having replicas with different states",
      "OpenUnhealthyContainers"),

  /**
   * Container stuck in QUASI_CLOSED state.
   */
  QUASI_CLOSED_STUCK((short) 8, 
      "Containers QuasiClosed with insufficient datanode origins",
      "StuckQuasiClosedContainers"),

  /**
   * Open container without healthy pipeline.
   */
  OPEN_WITHOUT_PIPELINE((short) 9, 
      "Containers in OPEN state without any healthy Pipeline",
      "OpenContainersWithoutPipeline"),

  // ========== Actual Combinations Found in Code (100+) ==========

  /**
   * Container is unhealthy AND under-replicated.
   * Occurs in RatisUnhealthyReplicationCheckHandler when container has only
   * unhealthy replicas and needs more replicas.
   */
  UNHEALTHY_UNDER_REPLICATED((short) 100,
      "Inconsistent states with insufficient replicas",
      "UnhealthyUnderReplicatedContainers",
      UNHEALTHY, UNDER_REPLICATED),

  /**
   * Container is unhealthy AND over-replicated.
   * Occurs in RatisUnhealthyReplicationCheckHandler when container has only
   * unhealthy replicas and has too many replicas.
   */
  UNHEALTHY_OVER_REPLICATED((short) 101,
      "Inconsistent states with excess replicas",
      "UnhealthyOverReplicatedContainers",
      UNHEALTHY, OVER_REPLICATED),

  /**
   * Container is missing AND under-replicated.
   * Occurs in ECReplicationCheckHandler when EC container is unrecoverable (missing)
   * and also has unreplicated offline indexes needing replication.
   */
  MISSING_UNDER_REPLICATED((short) 102,
      "No online replicas with offline indexes needing replication",
      "MissingUnderReplicatedContainers",
      MISSING, UNDER_REPLICATED),

  /**
   * Container is quasi-closed-stuck AND under-replicated.
   * Occurs in QuasiClosedStuckReplicationCheck when container is stuck in QUASI_CLOSED
   * and also needs more replicas.
   */
  QUASI_CLOSED_STUCK_UNDER_REPLICATED((short) 103,
      "Stuck in quasi-closed state with insufficient replicas",
      "QuasiClosedStuckUnderReplicatedContainers",
      QUASI_CLOSED_STUCK, UNDER_REPLICATED),

  /**
   * Container is quasi-closed-stuck AND over-replicated.
   * Occurs in QuasiClosedStuckReplicationCheck when container is stuck in QUASI_CLOSED
   * and also has excess replicas.
   */
  QUASI_CLOSED_STUCK_OVER_REPLICATED((short) 104,
      "Stuck in quasi-closed state with excess replicas",
      "QuasiClosedStuckOverReplicatedContainers",
      QUASI_CLOSED_STUCK, OVER_REPLICATED);

  // ========== Enum Fields ==========

  private final short value;
  private final String description;
  private final String metricName;
  private final Set<ContainerHealthState> individualStates;

  // Static lookup map for efficient fromValue()
  private static final Map<Short, ContainerHealthState> VALUE_MAP = new HashMap<>();

  static {
    for (ContainerHealthState state : values()) {
      VALUE_MAP.put(state.value, state);
    }
  }

  // Individual states constructor
  ContainerHealthState(short value, String description, String metricName) {
    this.value = value;
    this.description = description;
    this.metricName = metricName;
    // Initialize with HashSet to avoid enum initialization issues
    this.individualStates = new HashSet<>();
    this.individualStates.add(this);
  }

  // Combination states constructor
  ContainerHealthState(short value, String description, String metricName,
                       ContainerHealthState... components) {
    this.value = value;
    this.description = description;
    this.metricName = metricName;
    // Initialize with HashSet to avoid enum initialization issues
    this.individualStates = new HashSet<>();
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
   * Get the metric name for this health state.
   * Used for metrics reporting in ReplicationManager.
   *
   * @return Metric name string
   */
  public String getMetricName() {
    return metricName;
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
    // Check for UNHEALTHY + UNDER_REPLICATED
    if (states.contains(UNHEALTHY) && states.contains(UNDER_REPLICATED) && states.size() == 2) {
      return UNHEALTHY_UNDER_REPLICATED;
    }

    // Check for UNHEALTHY + OVER_REPLICATED
    if (states.contains(UNHEALTHY) && states.contains(OVER_REPLICATED) && states.size() == 2) {
      return UNHEALTHY_OVER_REPLICATED;
    }

    // Check for MISSING + UNDER_REPLICATED
    if (states.contains(MISSING) && states.contains(UNDER_REPLICATED) && states.size() == 2) {
      return MISSING_UNDER_REPLICATED;
    }

    // Check for QUASI_CLOSED_STUCK + UNDER_REPLICATED
    if (states.contains(QUASI_CLOSED_STUCK) && states.contains(UNDER_REPLICATED) && states.size() == 2) {
      return QUASI_CLOSED_STUCK_UNDER_REPLICATED;
    }

    // Check for QUASI_CLOSED_STUCK + OVER_REPLICATED
    if (states.contains(QUASI_CLOSED_STUCK) && states.contains(OVER_REPLICATED) && states.size() == 2) {
      return QUASI_CLOSED_STUCK_OVER_REPLICATED;
    }

    // No exact match - return most critical individual state by priority
    // Priority: MISSING > OPEN_WITHOUT_PIPELINE > QUASI_CLOSED_STUCK >
    //          UNDER_REPLICATED > UNHEALTHY > OPEN_UNHEALTHY >
    //          MIS_REPLICATED > OVER_REPLICATED > EMPTY
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
