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

package org.apache.hadoop.hdds.server;

import java.util.List;

/**
 * JMX view of {@link StartupProgress}.
 * <p>
 * Times are in milliseconds. A remaining time of -1 means it cannot be estimated (yet).
 */
public interface StartupProgressMXBean {

  /**
   * Status of a startup phase or step.
   */
  enum Status {
    PENDING,
    RUNNING,
    COMPLETE,
    FAILED
  }

  /**
   * @return true if all phases are complete.
   */
  boolean isComplete();

  /**
   * @return true if any phase has failed.
   */
  boolean isFailed();

  /**
   * @return time from the beginning of the first phase until the end of the last phase, or until now if
   *     startup is still in progress.
   */
  long getElapsedTime();

  /**
   * @return all phases in the order they are executed.
   */
  List<PhaseInfo> getPhases();

  /**
   * JMX view of a startup phase.
   */
  interface PhaseInfo {
    String getName();

    Status getStatus();

    long getElapsedTime();

    long getRemainingTime();

    /** @return sum of the number of completed work items of all steps. */
    long getDone();

    /** @return sum of the total number of work items of all steps. */
    long getTotal();

    List<StepInfo> getSteps();
  }

  /**
   * JMX view of a step within a startup phase.
   */
  interface StepInfo {
    String getName();

    Status getStatus();

    long getElapsedTime();

    long getRemainingTime();

    long getDone();

    long getTotal();
  }
}
