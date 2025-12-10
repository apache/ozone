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
import java.util.Set;

/**
 * Filter criteria for querying containers by health state.
 *
 * <p>Supports exact match filtering: Container health state must exactly
 * match one of the specified filter states.
 *
 * <p>Example usage:
 * <pre>
 * // Find containers with exact state MISSING_EMPTY
 * HealthStateFilter filter = HealthStateFilter.of(
 *     ContainerHealthState.MISSING_EMPTY);
 *
 * // Find containers with state MISSING or EMPTY (either one)
 * HealthStateFilter filter = HealthStateFilter.of(
 *     EnumSet.of(ContainerHealthState.MISSING,
 *                ContainerHealthState.EMPTY));
 * </pre>
 */
public class HealthStateFilter {

  private final Set<ContainerHealthState> states;

  /**
   * Create filter for exact health state match.
   *
   * @param state The exact health state to match
   * @return HealthStateFilter configured for exact matching
   */
  public static HealthStateFilter of(ContainerHealthState state) {
    return new HealthStateFilter(EnumSet.of(state));
  }

  /**
   * Create filter for exact match against multiple states.
   * Container must match one of the specified states exactly.
   *
   * @param states Set of exact health states to match
   * @return HealthStateFilter configured for exact matching
   */
  public static HealthStateFilter of(Set<ContainerHealthState> states) {
    return new HealthStateFilter(states);
  }

  /**
   * Private constructor. Use static factory methods to create instances.
   *
   * @param states Set of health states for filtering
   */
  private HealthStateFilter(Set<ContainerHealthState> states) {
    if (states == null || states.isEmpty()) {
      throw new IllegalArgumentException("Health states cannot be null or empty");
    }
    this.states = EnumSet.copyOf(states);
  }

  /**
   * Get the health states in this filter.
   *
   * @return Immutable copy of the health states
   */
  public Set<ContainerHealthState> getStates() {
    return EnumSet.copyOf(states);
  }

  /**
   * Check if a container's health state matches this filter.
   * Returns true if the container's health state exactly matches
   * one of the filter states.
   *
   * @param containerHealthState The container's health state to check
   * @return true if the container matches the filter criteria, false otherwise
   */
  public boolean matches(ContainerHealthState containerHealthState) {
    if (containerHealthState == null) {
      return false;
    }
    // Exact match: container must exactly match one of the filter states
    return states.contains(containerHealthState);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("HealthStateFilter{states=[");

    boolean first = true;
    for (ContainerHealthState state : states) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(state.name());
      if (state.isCombination()) {
        sb.append("(").append(state.getValue()).append(")");
      }
      first = false;
    }

    sb.append("]}");
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HealthStateFilter that = (HealthStateFilter) o;
    return states.equals(that.states);
  }

  @Override
  public int hashCode() {
    return states.hashCode();
  }
}
