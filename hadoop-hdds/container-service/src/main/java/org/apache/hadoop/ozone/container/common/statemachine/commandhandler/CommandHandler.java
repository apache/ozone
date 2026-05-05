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

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import java.util.function.Consumer;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;

/**
 * Generic interface for handlers.
 */
public interface CommandHandler {

  /**
   * Handles a given SCM command.
   * @param command - SCM Command
   * @param container - Ozone Container.
   * @param context - Current Context.
   * @param connectionManager - The SCMs that we are talking to.
   */
  void handle(SCMCommand<?> command, OzoneContainer container,
      StateContext context, SCMConnectionManager connectionManager);

  /**
   * Returns the command type that this command handler handles.
   * @return Type
   */
  SCMCommandProto.Type getCommandType();

  /**
   * Returns number of times this handler has been invoked.
   * @return int
   */
  int getInvocationCount();

  /**
   * Returns the average time this function takes to run.
   * @return  long
   */
  long getAverageRunTime();

  /**
   * Returns the total time this function takes to run.
   * @return  long
   */
  long getTotalRunTime();

  /**
   * Default implementation for updating command status.
   */
  default void updateCommandStatus(StateContext context, SCMCommand<?> command,
      Consumer<CommandStatus> cmdStatusUpdater, Logger log) {
    if (!context.updateCommandStatus(command.getId(), cmdStatusUpdater)) {
      log.warn("{} with Id:{} not found.", command.getType(),
          command.getId());
    }
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
   * for this handler.If the subclass does not override this method,
   * the default implementation will return -1, indicating that the number of
   * active threads is not applicable or not defined.
   *
   * @return The number of threads currently executing tasks in the thread pool,
   * or -1 if not applicable or not defined.
   */
  default int getThreadPoolActivePoolSize() {
    return -1;
  }

}
