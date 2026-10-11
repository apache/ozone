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

package org.apache.hadoop.hdds.scm.node;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DEAD;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.STALE;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;

/**
 * The status of a datanode including {@link NodeState}, {@link NodeOperationalState}
 * and the expiry time for the operational state,
 * where the expiry time is used in maintenance mode.
 * <p>
 * This class is value-based.
 */
public final class NodeStatus {
  /** For the {@link NodeStatus} objects with {@link #opStateExpiryEpochSeconds} == 0. */
  private static final Map<NodeOperationalState, Map<NodeState, NodeStatus>> CONSTANTS;

  static {
    final Map<NodeOperationalState, Map<NodeState, NodeStatus>> map = new EnumMap<>(NodeOperationalState.class);
    for (NodeOperationalState op : NodeOperationalState.values()) {
      final EnumMap<NodeState, NodeStatus> healthMap = new EnumMap<>(NodeState.class);
      for (NodeState health : NodeState.values()) {
        healthMap.put(health, new NodeStatus(health, op, 0));
      }
      map.put(op, Collections.unmodifiableMap(healthMap));
    }
    CONSTANTS = Collections.unmodifiableMap(map);
  }

  private static final Set<NodeOperationalState> OUT_OF_SERVICE_STATES = Collections.unmodifiableSet(
      EnumSet.of(DECOMMISSIONING, DECOMMISSIONED, ENTERING_MAINTENANCE, IN_MAINTENANCE));

  private static final NodeStatus IN_SERVICE_HEALTHY = valueOf(IN_SERVICE, HEALTHY);

  private static final Set<NodeOperationalState> MAINTENANCE_STATES = Collections.unmodifiableSet(
      EnumSet.of(ENTERING_MAINTENANCE, IN_MAINTENANCE));

  private static final Set<NodeOperationalState> DECOMMISSION_STATES = Collections.unmodifiableSet(
      EnumSet.of(DECOMMISSIONING, DECOMMISSIONED));

  private static final NodeStatus IN_SERVICE_STALE = NodeStatus.valueOf(IN_SERVICE, STALE);

  private static final NodeStatus IN_SERVICE_DEAD = NodeStatus.valueOf(IN_SERVICE, DEAD);

  private final NodeState health;
  private final NodeOperationalState operationalState;
  private final long opStateExpiryEpochSeconds;

  /** @return a {@link NodeStatus} object with {@link #opStateExpiryEpochSeconds} == 0. */
  public static NodeStatus valueOf(NodeOperationalState op, NodeState health) {
    return valueOf(op, health, 0);
  }

  /** @return a {@link NodeStatus} object. */
  public static NodeStatus valueOf(NodeOperationalState op, NodeState health, long opExpiryEpochSeconds) {
    Objects.requireNonNull(op, "op == null");
    Objects.requireNonNull(health, "health == null");
    return opExpiryEpochSeconds == 0 ? CONSTANTS.get(op).get(health)
        : new NodeStatus(health, op, opExpiryEpochSeconds);
  }

  /**
   * @return the set consists of {@link NodeOperationalState#ENTERING_MAINTENANCE}
   *                         and {@link NodeOperationalState#IN_MAINTENANCE}.
   */
  public static Set<NodeOperationalState> maintenanceStates() {
    return MAINTENANCE_STATES;
  }

  /**
   * @return the set consists of {@link NodeOperationalState#DECOMMISSIONING}
   *                         and {@link NodeOperationalState#DECOMMISSIONED}.
   */
  public static Set<NodeOperationalState> decommissionStates() {
    return DECOMMISSION_STATES;
  }

  /**
   * @return the set consists of {@link NodeOperationalState#DECOMMISSIONING},
   *                             {@link NodeOperationalState#DECOMMISSIONED},
   *                             {@link NodeOperationalState#ENTERING_MAINTENANCE}
   *                         and {@link NodeOperationalState#IN_MAINTENANCE}.
   */
  public static Set<NodeOperationalState> outOfServiceStates() {
    return OUT_OF_SERVICE_STATES;
  }

  /** @return the status of {@link NodeOperationalState#IN_SERVICE} and {@link NodeState#HEALTHY}. */
  public static NodeStatus inServiceHealthy() {
    return IN_SERVICE_HEALTHY;
  }

  /** @return the status of {@link NodeOperationalState#IN_SERVICE} and {@link NodeState#STALE}. */
  public static NodeStatus inServiceStale() {
    return IN_SERVICE_STALE;
  }

  /** @return the status of {@link NodeOperationalState#IN_SERVICE} and {@link NodeState#DEAD}. */
  public static NodeStatus inServiceDead() {
    return IN_SERVICE_DEAD;
  }

  private NodeStatus(NodeState health, NodeOperationalState op, long opExpiryEpochSeconds) {
    this.health = health;
    this.operationalState = op;
    this.opStateExpiryEpochSeconds = opExpiryEpochSeconds;
  }

  /** @return the status with the new health. */
  public NodeStatus newNodeState(NodeState newHealth) {
    return NodeStatus.valueOf(operationalState, newHealth, opStateExpiryEpochSeconds);
  }

  /** @return the status with the new op state and expiry epoch seconds. */
  public NodeStatus newOperationalState(NodeOperationalState op, long opExpiryEpochSeconds) {
    return NodeStatus.valueOf(op, health, opExpiryEpochSeconds);
  }

  /** Is this node writeable ({@link NodeState#HEALTHY} and {@link NodeOperationalState#IN_SERVICE}) ? */
  public boolean isNodeWritable() {
    return health == HEALTHY && operationalState == IN_SERVICE;
  }

  public NodeState getHealth() {
    return health;
  }

  public NodeOperationalState getOperationalState() {
    return operationalState;
  }

  public long getOpStateExpiryEpochSeconds() {
    return opStateExpiryEpochSeconds;
  }

  /** Is the op expired? */
  public boolean operationalStateExpired() {
    return opStateExpiryEpochSeconds != 0
        && System.currentTimeMillis() >= 1000 * opStateExpiryEpochSeconds;
  }

  /** @return true iff the node is {@link NodeOperationalState#IN_SERVICE}. */
  public boolean isInService() {
    return operationalState == IN_SERVICE;
  }

  /**
   * @return true iff the node is {@link NodeOperationalState#DECOMMISSIONING}
   *                           or {@link NodeOperationalState#DECOMMISSIONED}.
   */
  public boolean isDecommission() {
    return DECOMMISSION_STATES.contains(operationalState);
  }

  /** @return true iff the node is {@link NodeOperationalState#DECOMMISSIONING}. */
  public boolean isDecommissioning() {
    return operationalState == DECOMMISSIONING;
  }

  /** @return true iff the node is {@link NodeOperationalState#DECOMMISSIONED}. */
  public boolean isDecommissioned() {
    return operationalState == DECOMMISSIONED;
  }

  /**
   * @return true iff the node is {@link NodeOperationalState#ENTERING_MAINTENANCE}
   *                           or {@link NodeOperationalState#IN_MAINTENANCE}.
   */
  public boolean isMaintenance() {
    return maintenanceStates().contains(operationalState);
  }

  /** @return true iff the node is {@link NodeOperationalState#ENTERING_MAINTENANCE}. */
  public boolean isEnteringMaintenance() {
    return operationalState == ENTERING_MAINTENANCE;
  }

  /** @return true iff the node is {@link NodeOperationalState#IN_MAINTENANCE}. */
  public boolean isInMaintenance() {
    return operationalState == IN_MAINTENANCE;
  }

  /** @return true iff this node is {@link NodeState#HEALTHY} */
  public boolean isHealthy() {
    return health == HEALTHY;
  }

  /** @return true iff this node is {@link NodeState#HEALTHY} or {@link NodeState#STALE}. */
  public boolean isAlive() {
    return health == HEALTHY
        || health == STALE;
  }

  /** @return true iff the node is {@link NodeState#DEAD}. */
  public boolean isDead() {
    return health == DEAD;
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
    final NodeStatus that = (NodeStatus) obj;
    return this.operationalState == that.operationalState
        && this.health == that.health
        && this.opStateExpiryEpochSeconds == that.opStateExpiryEpochSeconds;
  }

  @Override
  public int hashCode() {
    return Objects.hash(health, operationalState, opStateExpiryEpochSeconds);
  }

  @Override
  public String toString() {
    final String expiry = opStateExpiryEpochSeconds == 0 ? "no expiry"
        : "expiry: " + opStateExpiryEpochSeconds + "s";
    return operationalState + "(" + expiry + ")-" + health;
  }
}
