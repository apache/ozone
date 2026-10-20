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

package org.apache.hadoop.ozone.om.jobworker.node;

import com.google.common.collect.ImmutableSet;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * This class is used to capture the current status of a jobworker.
 * This includes its health (HEALTHY or STALE)
 * and its operation status (IN_SERVICE, DECOMMISSIONING, or DECOMMISSIONED)
 */
@Immutable
public final class JobworkerNodeStatus {

  private static final Set<HddsProtos.NodeOperationalState>
      DECOMMISSION_STATES = ImmutableSet.copyOf(EnumSet.of(
      HddsProtos.NodeOperationalState.DECOMMISSIONING,
      HddsProtos.NodeOperationalState.DECOMMISSIONED
  ));

  private static final Set<HddsProtos.NodeOperationalState>
      OUT_OF_SERVICE_STATES = ImmutableSet.copyOf(EnumSet.of(
      HddsProtos.NodeOperationalState.DECOMMISSIONING,
      HddsProtos.NodeOperationalState.DECOMMISSIONED
  ));

  public static Set<HddsProtos.NodeOperationalState> decommissionStates() {
    return DECOMMISSION_STATES;
  }

  public static Set<HddsProtos.NodeOperationalState> outOfServiceStates() {
    return OUT_OF_SERVICE_STATES;
  }

  private final HddsProtos.NodeOperationalState operationalState;
  private final HddsProtos.NodeState healthState;

  public JobworkerNodeStatus(HddsProtos.NodeOperationalState operationalState,
      HddsProtos.NodeState healthState) {
    this.operationalState = operationalState;
    this.healthState = healthState;
  }

  public static JobworkerNodeStatus inServiceHealthy() {
    return new JobworkerNodeStatus(HddsProtos.NodeOperationalState.IN_SERVICE,
        HddsProtos.NodeState.HEALTHY);
  }

  public HddsProtos.NodeOperationalState getOperationalState() {
    return operationalState;
  }

  public boolean isInService() {
    return operationalState == HddsProtos.NodeOperationalState.IN_SERVICE;
  }

  /**
   * Returns true if the nodeStatus indicates the node is in any decommission
   * state.
   *
   * @return True if the node is in any decommission state, false otherwise
   */
  public boolean isDecommission() {
    return DECOMMISSION_STATES.contains(operationalState);
  }

  /**
   * Returns true if the node is currently decommissioning.
   *
   * @return True if the node is decommissioning, false otherwise
   */
  public boolean isDecommissioning() {
    return operationalState == HddsProtos.NodeOperationalState.DECOMMISSIONING;
  }

  /**
   * Returns true if the node is decommissioned.
   *
   * @return True if the node is decommissioned, false otherwise
   */
  public boolean isDecommissioned() {
    return operationalState == HddsProtos.NodeOperationalState.DECOMMISSIONED;
  }

  /**
   * Returns true if the nodeStatus is HEALTHY and false otherwise.
   *
   * @return True if the node is HEALTHY, false otherwise.
   */
  public boolean isHealthy() {
    return healthState == HddsProtos.NodeState.HEALTHY;
  }

  /**
   * Returns true if the nodeStatus is STALE and false otherwise.
   *
   * @return True, the node is STALE, false otherwise.
   */
  public boolean isStale() {
    return healthState == HddsProtos.NodeState.STALE;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    JobworkerNodeStatus other = (JobworkerNodeStatus) obj;
    if (this.operationalState == other.operationalState &&
        this.healthState == other.healthState) {
      return true;
    }
    return false;
  }

  public HddsProtos.NodeState getHealthState() {
    return healthState;
  }

  @Override
  public int hashCode() {
    return Objects.hash(healthState, operationalState);
  }

  @Override
  public String toString() {
    return "OperationalState: " + operationalState + " Health: " + healthState;
  }

}
