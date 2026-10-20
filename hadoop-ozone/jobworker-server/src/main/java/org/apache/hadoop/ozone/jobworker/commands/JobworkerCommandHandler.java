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

package org.apache.hadoop.ozone.jobworker.commands;

import java.util.function.Consumer;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos;
import org.apache.hadoop.ozone.jobworker.JobworkerConnectionManager;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;

/**
 * Generic interface for jobworker command handlers.
 * Defines the contract for all command handlers in the system.
 */
public interface JobworkerCommandHandler {

  /**
   * Handles a given jobworker command.
   *
   * @param command           - Jobworker Command
   * @param context           - Current Context.
   * @param connectionManager - The OMs that we are talking to.
   */
  void handle(JobworkerCommand<?> command, JobworkerStateContext context,
              JobworkerConnectionManager connectionManager);

  /**
   * Returns the command type that this command handler handles.
   * Used for registering handlers with the dispatcher.
   *
   * @return JobworkerCommandType
   */
  JobworkerServiceProtocolProtos.OMJobworkerCommandProto.Type getCommandType();

  /**
   * Returns number of times this handler has been invoked.
   *
   * @return int
   */
  int getInvocationCount();

  /**
   * Returns the average time this function takes to run.
   *
   * @return long
   */
  long getAverageRunTime();

  /**
   * Returns the total time this function takes to run.
   *
   * @return long
   */
  long getTotalRunTime();

  /**
   * Default implementation for updating command status.
   *
   * @param context          Context containing the command status
   * @param command          Command to update status for
   * @param cmdStatusUpdater Function to update the status
   */
  default void updateCommandStatus(JobworkerStateContext context,
                                   JobworkerCommand<?> command,
                                   Consumer<JobworkerCommandStatus> cmdStatusUpdater) {
    JobworkerCommandManager commandManager = context.getCommandManager();
    commandManager.updateCommand(command, cmdStatusUpdater);
  }

  /**
   * Override for any command with an internal threadpool, and stop the
   * executor when this method is invoked.
   */
  default void stop() {
    // Default implementation does nothing
  }

  /**
   * Returns the queued command count for this handler.
   *
   * @return The number of queued commands inside this handler.
   */
  int getQueuedCount();

  /**
   * Returns the maximum number of threads allowed in the thread pool for this
   * handler. If the subclass does not override this method, the default
   * implementation will return -1, indicating that the maximum pool size is not
   * applicable or not defined.
   *
   * @return The maximum number of threads allowed in the thread pool,
   * or -1 if not applicable or not defined.
   */
  default int getThreadPoolMaxPoolSize() {
    return -1;
  }

  /**
   * Returns the number of threads currently executing tasks in the thread pool
   * for this handler. If the subclass does not override this method,
   * the default implementation will return -1, indicating that the number of
   * active threads is not applicable or not defined.
   *
   * @return The number of threads currently executing tasks in the thread pool,
   * or -1 if not applicable or not defined.
   */
  default int getThreadPoolActivePoolSize() {
    return -1;
  }

  /**
   * Clear or reset any resources related to this handler if needed.
   * This is called when a handler is being shutdown or needs to be reset.
   */
  default void clear() {
    // Default implementation does nothing
  }
}
