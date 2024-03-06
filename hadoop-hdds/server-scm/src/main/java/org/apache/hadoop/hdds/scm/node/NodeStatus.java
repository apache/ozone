/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.node;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * This class is used to capture the current status of a datanode. This
 * includes its health (healthy, stale or dead) and its operation status (
 * in_service, decommissioned and maintenance mode) along with the expiry time
 * for the operational state (used with maintenance mode).
 */
public class NodeStatus implements Comparable<NodeStatus> {

  private static final Set<HddsProtos.NodeOperationalState>
      MAINTENANCE_STATES = ImmutableSet.copyOf(EnumSet.of(
          HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
          HddsProtos.NodeOperationalState.IN_MAINTENANCE
      ));

  private static final Set<HddsProtos.NodeOperationalState>
      DECOMMISSION_STATES = ImmutableSet.copyOf(EnumSet.of(
          HddsProtos.NodeOperationalState.DECOMMISSIONING,
          HddsProtos.NodeOperationalState.DECOMMISSIONED
      ));

  private static final Set<HddsProtos.NodeOperationalState>
      OUT_OF_SERVICE_STATES = ImmutableSet.copyOf(EnumSet.of(
          HddsProtos.NodeOperationalState.DECOMMISSIONING,
          HddsProtos.NodeOperationalState.DECOMMISSIONED,
          HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
          HddsProtos.NodeOperationalState.IN_MAINTENANCE
      ));

  public static Set<HddsProtos.NodeOperationalState> maintenanceStates() {
    return MAINTENANCE_STATES;
  }

  public static Set<HddsProtos.NodeOperationalState> decommissionStates() {
    return DECOMMISSION_STATES;
  }

  public static Set<HddsProtos.NodeOperationalState> outOfServiceStates() {
    return OUT_OF_SERVICE_STATES;
  }

  private HddsProtos.NodeOperationalState operationalState;
  private HddsProtos.NodeState health;
  private long opStateExpiryEpochSeconds;

  public NodeStatus(HddsProtos.NodeOperationalState operationalState,
                    HddsProtos.NodeState health) {
    this.operationalState = operationalState;
    this.health = health;
    this.opStateExpiryEpochSeconds = 0;
  }

  public NodeStatus(HddsProtos.NodeOperationalState operationalState,
                    HddsProtos.NodeState health,
                    long opStateExpireEpocSeconds) {
    this.operationalState = operationalState;
    this.health = health;
    this.opStateExpiryEpochSeconds = opStateExpireEpocSeconds;
  }

  public static NodeStatus inServiceHealthy() {
    return new NodeStatus(HddsProtos.NodeOperationalState.IN_SERVICE,
        HddsProtos.NodeState.HEALTHY);
  }

  public static NodeStatus inServiceHealthyReadOnly() {
    return new NodeStatus(HddsProtos.NodeOperationalState.IN_SERVICE,
        HddsProtos.NodeState.HEALTHY_READONLY);
  }

  public static NodeStatus inServiceStale() {
    return new NodeStatus(HddsProtos.NodeOperationalState.IN_SERVICE,
        HddsProtos.NodeState.STALE);
  }

  public static NodeStatus inServiceDead() {
    return new NodeStatus(HddsProtos.NodeOperationalState.IN_SERVICE,
        HddsProtos.NodeState.DEAD);
  }

  public boolean isNodeWritable() {
    return health == HddsProtos.NodeState.HEALTHY &&
        operationalState == HddsProtos.NodeOperationalState.IN_SERVICE;
  }

  public HddsProtos.NodeState getHealth() {
    return health;
  }

  public HddsProtos.NodeOperationalState getOperationalState() {
    return operationalState;
  }

  public long getOpStateExpiryEpochSeconds() {
    return opStateExpiryEpochSeconds;
  }

  public boolean operationalStateExpired() {
    if (0 == opStateExpiryEpochSeconds) {
      return false;
    }
    return System.currentTimeMillis() / 1000 >= opStateExpiryEpochSeconds;
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
   * Returns true if the node is in any maintenance state.
   *
   * @return True if the node is in any maintenance state, false otherwise
   */
  public boolean isMaintenance() {
    return MAINTENANCE_STATES.contains(operationalState);
  }

  /**
   * Returns true if the node is currently entering maintenance.
   *
   * @return True if the node is entering maintenance, false otherwise
   */
  public boolean isEnteringMaintenance() {
    return operationalState
        == HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
  }

  /**
   * Returns true if the node is currently in maintenance.
   *
   * @return True if the node is in maintenance, false otherwise.
   */
  public boolean isInMaintenance() {
    return operationalState == HddsProtos.NodeOperationalState.IN_MAINTENANCE;
  }

  /**
   * Returns true if the nodeStatus is healthy or healthy_readonly (ie not stale
   * or dead) and false otherwise.
   *
   * @return True if the node is healthy or healthy_readonly, false otherwise.
   */
  public boolean isHealthy() {
    return health == HddsProtos.NodeState.HEALTHY
        || health == HddsProtos.NodeState.HEALTHY_READONLY;
  }

  /**
   * Returns true if the nodeStatus is either healthy or stale and false
   * otherwise.
   *
   * @return True is the node is Healthy or Stale, false otherwise.
   */
  public boolean isAlive() {
    return health == HddsProtos.NodeState.HEALTHY
        || health == HddsProtos.NodeState.STALE;
  }

  /**
   * Returns true if the nodeStatus is dead and false otherwise.
   *
   * @return True is the node is Dead, false otherwise.
   */
  public boolean isDead() {
    return health == HddsProtos.NodeState.DEAD;
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
    NodeStatus other = (NodeStatus) obj;
    if (this.operationalState == other.operationalState &&
        this.health == other.health
        && this.opStateExpiryEpochSeconds == other.opStateExpiryEpochSeconds) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(health, operationalState, opStateExpiryEpochSeconds);
  }

  @Override
  public String toString() {
    return "OperationalState: " + operationalState + " Health: " + health +
        " OperationStateExpiry: " + opStateExpiryEpochSeconds;
  }

  @Override
  public int compareTo(NodeStatus o) {
    int order = Boolean.compare(o.isHealthy(), isHealthy());
    if (order == 0) {
      order = Boolean.compare(isDead(), o.isDead());
    }
    if (order == 0) {
      order = operationalState.compareTo(o.operationalState);
    }
    return order;
  }
}
