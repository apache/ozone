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

package org.apache.hadoop.ozone.common;

import static org.apache.hadoop.ozone.common.TestStateMachine.STATES.CLEANUP;
import static org.apache.hadoop.ozone.common.TestStateMachine.STATES.CLOSED;
import static org.apache.hadoop.ozone.common.TestStateMachine.STATES.CREATING;
import static org.apache.hadoop.ozone.common.TestStateMachine.STATES.FINAL;
import static org.apache.hadoop.ozone.common.TestStateMachine.STATES.INIT;
import static org.apache.hadoop.ozone.common.TestStateMachine.STATES.OPERATIONAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import org.apache.commons.collections4.SetUtils;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.common.statemachine.StateMachine;
import org.junit.jupiter.api.Test;

/**
 * This class is to test ozone common state machine.
 */
public class TestStateMachine {

  /**
   * STATES used by the test state machine.
   */
  public enum STATES { INIT, CREATING, OPERATIONAL, CLOSED, CLEANUP, FINAL }

  /**
   * EVENTS used by the test state machine.
   */
  public enum EVENTS { ALLOCATE, CREATE, UPDATE, CLOSE, DELETE, TIMEOUT }

  @Test
  public void testStateMachineStates() throws InvalidStateTransitionException {
    Set<STATES> finals = new HashSet<>();
    finals.add(FINAL);

    StateMachine<STATES, EVENTS> stateMachine =
        new StateMachine<>(INIT, finals);

    stateMachine.addTransition(INIT, CREATING, EVENTS.ALLOCATE);
    stateMachine.addTransition(CREATING, OPERATIONAL, EVENTS.CREATE);
    stateMachine.addTransition(OPERATIONAL, OPERATIONAL, EVENTS.UPDATE);
    stateMachine.addTransition(OPERATIONAL, CLEANUP, EVENTS.DELETE);
    stateMachine.addTransition(OPERATIONAL, CLOSED, EVENTS.CLOSE);
    stateMachine.addTransition(CREATING, CLEANUP, EVENTS.TIMEOUT);

    // Initial and Final states
    assertEquals(INIT, stateMachine.getInitialState(),
        "Initial State");
    assertTrue(SetUtils.isEqualSet(finals, stateMachine.getFinalStates()), "Final States");

    // Valid state transitions
    assertEquals(OPERATIONAL, stateMachine.getNextState(CREATING,
        EVENTS.CREATE), "STATE should be OPERATIONAL after being created");
    assertEquals(OPERATIONAL, stateMachine.getNextState(OPERATIONAL,
        EVENTS.UPDATE), "STATE should be OPERATIONAL after being updated");
    assertEquals(CLEANUP, stateMachine.getNextState(OPERATIONAL,
        EVENTS.DELETE), "STATE should be CLEANUP after being deleted");
    assertEquals(CLEANUP, stateMachine.getNextState(CREATING,
        EVENTS.TIMEOUT), "STATE should be CLEANUP after being timeout");
    assertEquals(CLOSED, stateMachine.getNextState(OPERATIONAL,
        EVENTS.CLOSE), "STATE should be CLOSED after being closed");

    // Negative cases: invalid transition
    assertThrowsExactly(InvalidStateTransitionException.class, () ->
        stateMachine.getNextState(OPERATIONAL, EVENTS.CREATE), "Invalid event");

    assertThrowsExactly(InvalidStateTransitionException.class, () ->
        stateMachine.getNextState(CREATING, EVENTS.CLOSE), "Invalid event");
  }

}
