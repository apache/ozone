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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.ozone.container.common.helpers.CommandHandlerMetrics;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dispatches command to the correct handler.
 */
public final class CommandDispatcher {
  static final Logger LOG =
      LoggerFactory.getLogger(CommandDispatcher.class);
  private final StateContext context;
  private final Map<Type, CommandHandler> handlerMap;
  private final OzoneContainer container;
  private final SCMConnectionManager connectionManager;
  private final CommandHandlerMetrics commandHandlerMetrics;

  /**
   * Constructs a command dispatcher.
   *
   * @param container - Ozone Container
   * @param context - Context
   * @param handlers - Set of handlers.
   */
  private CommandDispatcher(OzoneContainer container, SCMConnectionManager
      connectionManager, StateContext context,
      CommandHandler... handlers) {
    this.context = context;
    this.container = container;
    this.connectionManager = connectionManager;
    handlerMap = new HashMap<>();
    for (CommandHandler h : handlers) {
      if (handlerMap.containsKey(h.getCommandType())) {
        LOG.error("Duplicate handler for the same command. Exiting. Handle " +
            "key : {}", h.getCommandType().getDescriptorForType().getName());
        throw new IllegalArgumentException("Duplicate handler for the same " +
            "command.");
      }
      handlerMap.put(h.getCommandType(), h);
    }
    commandHandlerMetrics = CommandHandlerMetrics.create(handlerMap);
  }

  @VisibleForTesting
  public CommandHandler getCloseContainerHandler() {
    return handlerMap.get(Type.closeContainerCommand);
  }

  @VisibleForTesting
  public CommandHandler getDeleteBlocksCommandHandler() {
    return handlerMap.get(Type.deleteBlocksCommand);
  }

  public ClosePipelineCommandHandler getClosePipelineCommandHandler() {
    return (ClosePipelineCommandHandler) handlerMap.get(Type.closePipelineCommand);
  }

  /**
   * Dispatch the command to the correct handler.
   *
   * @param command - SCM Command.
   */
  public void handle(SCMCommand<?> command) {
    Objects.requireNonNull(command, "command == null");
    CommandHandler handler = handlerMap.get(command.getType());
    if (handler != null) {
      commandHandlerMetrics.increaseCommandCount(command.getType());
      try {
        handler.handle(command, container, context, connectionManager);
      } catch (Exception ex) {
        LOG.error("Exception while handle command, ", ex);
      }
    } else {
      LOG.error("Unknown SCM Command queued. There is no handler for this " +
          "command. Command: {}", command.getType().getDescriptorForType()
          .getName());
    }
  }

  public void stop() {
    for (CommandHandler c : handlerMap.values()) {
      c.stop();
    }
    commandHandlerMetrics.unRegister();
  }

  /**
   * For each registered handler, call its getQueuedCount method to retrieve the
   * number of queued commands. The returned EnumCounters will contain an entry for every
   * registered command in the dispatcher, with a value of zero if there are no
   * queued commands.
   * @return EnumCounters of CommandType with the queued command count.
   */
  public EnumCounters<Type> getQueuedCommandCount() {
    EnumCounters<Type> counts = new EnumCounters<>(Type.class);
    for (Map.Entry<Type, CommandHandler> entry : handlerMap.entrySet()) {
      counts.set(entry.getKey(), entry.getValue().getQueuedCount());
    }
    return counts;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Helper class to construct command dispatcher.
   */
  public static class Builder {
    private final List<CommandHandler> handlerList;
    private OzoneContainer container;
    private StateContext context;
    private SCMConnectionManager connectionManager;

    public Builder() {
      handlerList = new LinkedList<>();
    }

    /**
     * Adds a handler.
     *
     * @param handler - handler
     * @return Builder
     */
    public Builder addHandler(CommandHandler handler) {
      Objects.requireNonNull(handler, "handler == null");
      handlerList.add(handler);
      return this;
    }

    /**
     * Add the OzoneContainer.
     *
     * @param ozoneContainer - ozone container.
     * @return Builder
     */
    public Builder setContainer(OzoneContainer ozoneContainer) {
      Objects.requireNonNull(ozoneContainer,  "ozoneContainer == null");
      this.container = ozoneContainer;
      return this;
    }

    /**
     * Set the Connection Manager.
     *
     * @param scmConnectionManager
     * @return this
     */
    public Builder setConnectionManager(SCMConnectionManager
        scmConnectionManager) {
      Objects.requireNonNull(scmConnectionManager, "scmConnectionManager == null");
      this.connectionManager = scmConnectionManager;
      return this;
    }

    /**
     * Sets the Context.
     *
     * @param stateContext - StateContext
     * @return this
     */
    public Builder setContext(StateContext stateContext) {
      Objects.requireNonNull(stateContext, "stateContext == null");
      this.context = stateContext;
      return this;
    }

    /**
     * Builds a command Dispatcher.
     * @return Command Dispatcher.
     */
    public CommandDispatcher build() {
      Objects.requireNonNull(connectionManager, "connectionManager == null");
      Objects.requireNonNull(container, "container == null");
      Objects.requireNonNull(context, "context == null");
      Preconditions.checkArgument(!this.handlerList.isEmpty(),
          "The number of command handlers must be greater than 0.");
      return new CommandDispatcher(this.container, this.connectionManager,
          this.context, handlerList.toArray(
              new CommandHandler[handlerList.size()]));
    }
  }
}
