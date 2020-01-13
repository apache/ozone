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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

import java.util.Objects;

/**
 * This class is used to capture the current status of a datanode. This
 * includes its health (healthy, stale or dead) and its operation status (
 * in_service, decommissioned and maintenance mode) along with the expiry time
 * for the operational state (used with maintenance mode).
 */
public class NodeStatus {

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

  public static NodeStatus inServiceStale() {
    return new NodeStatus(HddsProtos.NodeOperationalState.IN_SERVICE,
        HddsProtos.NodeState.STALE);
  }

  public static NodeStatus inServiceDead() {
    return new NodeStatus(HddsProtos.NodeOperationalState.IN_SERVICE,
        HddsProtos.NodeState.DEAD);
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

  /**
   * Returns true if the nodeStatus indicates the node is in any decommission
   * state.
   *
   * @return True if the node is in any decommission state, false otherwise
   */
  public boolean isDecommission() {
    return operationalState == HddsProtos.NodeOperationalState.DECOMMISSIONING
        || operationalState == HddsProtos.NodeOperationalState.DECOMMISSIONED;
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
    return operationalState
        == HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE
        || operationalState == HddsProtos.NodeOperationalState.IN_MAINTENANCE;
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
   * Returns true if the nodeStatus is healthy (ie not stale or dead) and false
   * otherwise.
   *
   * @return True if the node is Healthy, false otherwise
   */
  public boolean isHealthy() {
    return health == HddsProtos.NodeState.HEALTHY;
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
    return "OperationalState: "+operationalState+" Health: "+health+
        " OperastionStateExpiry: "+opStateExpiryEpochSeconds;
  }

}
