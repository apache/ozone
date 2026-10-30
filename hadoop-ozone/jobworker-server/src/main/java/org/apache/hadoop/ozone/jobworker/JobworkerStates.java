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

package org.apache.hadoop.ozone.jobworker;

/**
 * States that a jobworker can be in. GetNextState will move this enum from
 * getInitState to getLastState.
 */
public enum JobworkerStates {
  INIT(1),
  RUNNING(2),
  SHUTDOWN(3);
  private final int value;

  /**
   * Constructs states.
   *
   * @param value Enum Value
   */
  JobworkerStates(int value) {
    this.value = value;
  }

  /**
   * Returns the first State.
   *
   * @return First State.
   */
  public static JobworkerStates getInitState() {
    return INIT;
  }

  /**
   * The last state of endpoint states.
   *
   * @return last state.
   */
  public static JobworkerStates getLastState() {
    return SHUTDOWN;
  }

  /**
   * returns the numeric value associated with the endPoint.
   *
   * @return int.
   */
  public int getValue() {
    return value;
  }

  /**
   * Returns the next logical state that endPoint should move to. This
   * function assumes the States are sequentially numbered.
   *
   * @return NextState.
   */
  public JobworkerStates getNextState() {
    if (this.value < getLastState().getValue()) {
      int stateValue = this.getValue() + 1;
      for (JobworkerStates iter : values()) {
        if (stateValue == iter.getValue()) {
          return iter;
        }
      }
    }
    return getLastState();
  }

  public boolean isTransitionAllowedTo(JobworkerStates newState) {
    return newState.getValue() > getValue();
  }
}
