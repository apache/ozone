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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for JobWorker node state transitions.
 */
public class TestJobworkerNodeStateTransitions {

  private JobworkerNodeStateMachine stateMachine;

  @BeforeEach
  public void setUp() {
    stateMachine = new JobworkerNodeStateMachine();
  }

  @Test
  public void testValidTransitions() throws InvalidStateTransitionException {
    // Test transition from HEALTHY to STALE
    assertEquals(NodeState.STALE,
        stateMachine.getNextState(NodeState.HEALTHY,
            JobworkerNodeStateMachine.NodeLifeCycleEvent.TIMEOUT));

    // Test transition from STALE to HEALTHY
    assertEquals(NodeState.HEALTHY,
        stateMachine.getNextState(NodeState.STALE,
            JobworkerNodeStateMachine.NodeLifeCycleEvent.RESTORE));
  }

  @Test
  public void testInvalidTransitions() {
    // Unlike datanode, jobworker doesn't have a DEAD state in its state machine
    assertThrows(InvalidStateTransitionException.class, () ->
        stateMachine.getNextState(NodeState.STALE,
            JobworkerNodeStateMachine.NodeLifeCycleEvent.TIMEOUT));
  }

  @Test
  public void testNodeLifeCycleEvents() {
    JobworkerNodeStateMachine.NodeLifeCycleEvent[] events =
        JobworkerNodeStateMachine.NodeLifeCycleEvent.values();
    assertEquals(2, events.length,
        "Expected exactly two node lifecycle events: TIMEOUT and RESTORE");

    assertArrayEquals(
        new JobworkerNodeStateMachine.NodeLifeCycleEvent[] {
            JobworkerNodeStateMachine.NodeLifeCycleEvent.TIMEOUT,
            JobworkerNodeStateMachine.NodeLifeCycleEvent.RESTORE
        },
        events);
  }
}
