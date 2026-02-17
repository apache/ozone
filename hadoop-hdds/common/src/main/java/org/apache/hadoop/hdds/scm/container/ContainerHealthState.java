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

import java.util.HashMap;
import java.util.Map;

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
      "UnhealthyUnderReplicatedContainers"),

  /**
   * Container is unhealthy AND over-replicated.
   * Occurs in RatisUnhealthyReplicationCheckHandler when container has only
   * unhealthy replicas and has too many replicas.
   */
  UNHEALTHY_OVER_REPLICATED((short) 101,
      "Inconsistent states with excess replicas",
      "UnhealthyOverReplicatedContainers"),

  /**
   * Container is missing AND under-replicated.
   * Occurs in ECReplicationCheckHandler when EC container is unrecoverable (missing)
   * and also has unreplicated offline indexes needing replication.
   */
  MISSING_UNDER_REPLICATED((short) 102,
      "No online replicas with offline indexes needing replication",
      "MissingUnderReplicatedContainers"),

  /**
   * Container is quasi-closed-stuck AND under-replicated.
   * Occurs in QuasiClosedStuckReplicationCheck when container is stuck in QUASI_CLOSED
   * and also needs more replicas.
   */
  QUASI_CLOSED_STUCK_UNDER_REPLICATED((short) 103,
      "Stuck in quasi-closed state with insufficient replicas",
      "QuasiClosedStuckUnderReplicatedContainers"),

  /**
   * Container is quasi-closed-stuck AND over-replicated.
   * Occurs in QuasiClosedStuckReplicationCheck when container is stuck in QUASI_CLOSED
   * and also has excess replicas.
   */
  QUASI_CLOSED_STUCK_OVER_REPLICATED((short) 104,
      "Stuck in quasi-closed state with excess replicas",
      "QuasiClosedStuckOverReplicatedContainers"),

  /**
   * Container is quasi-closed-stuck AND missing (no replicas).
   * Occurs when QuasiClosedContainerHandler marks as QUASI_CLOSED_STUCK,
   * then QuasiClosedStuckReplicationCheck finds no replicas.
   */
  QUASI_CLOSED_STUCK_MISSING((short) 105,
      "Stuck in quasi-closed state with no replicas",
      "QuasiClosedStuckMissingContainers");

  // ========== Enum Fields ==========

  private final short value;
  private final String description;
  private final String metricName;

  // Static lookup map for efficient fromValue()
  private static final Map<Short, ContainerHealthState> VALUE_MAP = new HashMap<>();

  static {
    for (ContainerHealthState state : values()) {
      VALUE_MAP.put(state.value, state);
    }
  }

  // Constructor for all states
  ContainerHealthState(short value, String description, String metricName) {
    this.value = value;
    this.description = description;
    this.metricName = metricName;
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

}
