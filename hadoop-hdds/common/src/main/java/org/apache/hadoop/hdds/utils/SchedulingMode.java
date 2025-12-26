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

package org.apache.hadoop.hdds.utils;

/**
 * Defines the scheduling mode for {@link BackgroundService}.
 */
public enum SchedulingMode {
  /**
   * Fixed rate mode.
   * Attempts to execute tasks at a fixed rate. When task execution time
   * exceeds the interval, the next task starts immediately after the
   * previous one completes.
   * <p>
   * Example: if a task takes 10 seconds and interval is 3 seconds,
   * the next task starts at 10 seconds (immediately after completion).
   * Actual period = max(task execution time, interval).
   */
  FIXED_RATE,
  
  /**
   * Fixed delay mode.
   * Ensures a fixed delay from task completion to next task start.
   * The next batch of tasks will start exactly 'interval' time after
   * the previous batch completes. This is true scheduleWithFixedDelay behavior.
   * <p>
   * Example: if a task takes 10 seconds and interval is 3 seconds,
   * the next task will start at 13 seconds (10s task + 3s delay).
   * Actual period = task execution time + interval.
   */
  FIXED_DELAY;
  
  /**
   * Returns whether this mode requires sleeping after task completion.
   *
   * @return true if should sleep after task completion
   */
  public boolean shouldSleepAfterCompletion() {
    return this == FIXED_DELAY;
  }
}

