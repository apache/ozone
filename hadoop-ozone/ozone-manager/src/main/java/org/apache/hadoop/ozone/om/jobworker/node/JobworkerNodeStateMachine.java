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

import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.common.statemachine.StateMachine;

/**
 * State machine for Jobworker node health states.
 *
 * There are only two main states for a Jobworker:
 * - HEALTHY: The node is online and can accept tasks
 * - STALE: The node has not sent heartbeats for a while and is considered unreachable
 *
 * State transition flows:
 *
 * HEALTHY ---(TIMEOUT)---> STALE
 * STALE ---(RESTORE)---> HEALTHY
 */
public class JobworkerNodeStateMachine {

  /**
   * Node's state transition events.
   */
  public enum NodeLifeCycleEvent {
    TIMEOUT, // Node has not sent heartbeats for a while
    RESTORE  // Node has resumed sending heartbeats
  }

  private final StateMachine<NodeState, NodeLifeCycleEvent> nodeHealthSM;

  /**
   * Initialize the Jobworker node state machine.
   */
  public JobworkerNodeStateMachine() {
    Set<NodeState> finalStates = new HashSet<>();
    this.nodeHealthSM = new StateMachine<>(NodeState.HEALTHY, finalStates);
    initializeStateMachine();
  }

  /**
   * Initialize the state machine with allowed transitions.
   */
  private void initializeStateMachine() {
    // Healthy node times out and becomes stale
    nodeHealthSM.addTransition(NodeState.HEALTHY, NodeState.STALE,
        NodeLifeCycleEvent.TIMEOUT);

    // Stale node restores communication and becomes healthy
    nodeHealthSM.addTransition(NodeState.STALE, NodeState.HEALTHY,
        NodeLifeCycleEvent.RESTORE);
  }

  /**
   * Get the next state based on current state and event.
   *
   * @param currentState Current state of the node
   * @param event Event that triggered the state transition
   * @return The new state after the transition
   * @throws InvalidStateTransitionException If the transition is not allowed
   */
  public NodeState getNextState(NodeState currentState,
                                NodeLifeCycleEvent event) throws InvalidStateTransitionException {
    return nodeHealthSM.getNextState(currentState, event);
  }
}
