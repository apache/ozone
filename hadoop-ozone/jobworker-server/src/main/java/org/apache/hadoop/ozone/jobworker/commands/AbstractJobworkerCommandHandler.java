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

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandExecutionResultsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandResultCode;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.ozone.jobworker.JobworkerConnectionManager;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for JobworkerCommandHandler implementations.
 * Handles command status tracking and transitions with support for
 * concurrent command execution.
 */
public abstract class AbstractJobworkerCommandHandler implements JobworkerCommandHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractJobworkerCommandHandler.class);

  private final AtomicInteger invocationCount = new AtomicInteger(0);
  private final AtomicLong totalTime = new AtomicLong(0);
  private final AtomicInteger queuedCount = new AtomicInteger(0);
  private final ExecutorService executorService;
  private final OMJobworkerCommandProto.Type type;
  private volatile FaultInjector injector;

  /**
   * Create a new command handler for the specified type using the provided executor service.
   *
   * @param type The command type this handler processes
   * @param executorService The executor service to use for command processing
   */
  protected AbstractJobworkerCommandHandler(OMJobworkerCommandProto.Type type,
                                            ExecutorService executorService) {
    this.type = type;
    this.executorService = executorService;
  }

  /**
   * Validates whether a state transition is allowed.
   */
  private boolean isValidStateTransition(CommandStatus.Status currentStatus,
                                         CommandStatus.Status newStatus) {
    if (currentStatus == newStatus) {
      return true;
    }

    switch (currentStatus) {
    case PENDING:
      return newStatus == CommandStatus.Status.EXECUTING ||
          newStatus == CommandStatus.Status.FAILED;

    case EXECUTING:
      return newStatus == CommandStatus.Status.SUCCEEDED ||
          newStatus == CommandStatus.Status.FAILED;

    case SUCCEEDED:
    case FAILED:
      // Terminal states cannot transition
      return false;

    default:
      LOG.warn("Unknown command status: {}", currentStatus);
      return false;
    }
  }

  public void updateCommandStatus(JobworkerStateContext context, JobworkerCommand<?> command,
      CommandStatus.Status newStatus) {
    updateCommandStatus(context, command, newStatus, null, null);
  }

  public void updateCommandStatus(JobworkerStateContext context, JobworkerCommand<?> command,
      CommandStatus.Status newStatus, String message, CommandResultCode resultCode) {
    updateCommandStatus(context, command, status -> {
      CommandStatus.Status currentStatus = status.getStatus();
      if (isValidStateTransition(currentStatus, newStatus)) {
        status.updateStatusAndMessage(newStatus, message, resultCode);
      } else {
        LOG.warn("Invalid state transition attempted for command {} from {} to {}",
            command.getId(), currentStatus, newStatus);
      }
    });
  }

  public void setExecutionResults(JobworkerStateContext context, JobworkerCommand<?> command,
      CommandExecutionResultsProto resultsProto) {
    updateCommandStatus(context, command, status -> {
      if (resultsProto != null) {
        LOG.debug("Set execution results for {} command {}", command.getId(), command.getType());
        status.setExecutionResults(resultsProto);
      } else {
        LOG.warn("Cannot set execution results for {} command {}", command.getId(), command.getType());
      }
    });
  }

  @Override
  public final void handle(JobworkerCommand<?> command,
                           JobworkerStateContext context,
                           JobworkerConnectionManager connectionManager) {
    if (!validateCommand(command, context)) {
      return;
    }

    invocationCount.incrementAndGet();
    queuedCount.incrementAndGet();

    try {
      executorService.submit(createCommandTask(command, context, connectionManager));
    } catch (RejectedExecutionException e) {
      handleExecutionRejection(command, context, e);
    } catch (Throwable t) {
      handleUnexpectedError(command, context, t);
    }
  }

  private boolean validateCommand(JobworkerCommand<?> command, JobworkerStateContext context) {
    if (command.getType() != getCommandType()) {
      LOG.warn("Command type mismatch: got {}, expected {}",
               command.getType(), getCommandType());
      updateCommandStatus(context, command, CommandStatus.Status.FAILED,
          "Command type mismatch", CommandResultCode.TYPE_MISMATCH);
      return false;
    }

    if (command.hasExpired(System.currentTimeMillis())) {
      LOG.warn("Command {} has expired and will not be processed", command.getId());
      updateCommandStatus(context, command, CommandStatus.Status.FAILED,
          "Command expired", CommandResultCode.COMMAND_EXPIRED);
      return false;
    }

    return true;
  }

  private Runnable createCommandTask(JobworkerCommand<?> command,
                                     JobworkerStateContext context,
                                     JobworkerConnectionManager connectionManager) {
    return () -> {
      long startTime = System.currentTimeMillis();
      try {
        executeCommand(command, context, connectionManager);
      } finally {
        long executionTime = System.currentTimeMillis() - startTime;
        totalTime.addAndGet(executionTime);
        queuedCount.decrementAndGet();
      }
    };
  }

  private void executeCommand(JobworkerCommand<?> command,
                              JobworkerStateContext context,
                              JobworkerConnectionManager connectionManager) {
    try {
      updateCommandStatus(context, command, CommandStatus.Status.EXECUTING);
      if (injector != null) {
        injector.pause();
      }
      boolean success = processCommand(command, context, connectionManager);

      context.getCommandManager().updateCommand(command, commandStatus -> {
        if (!commandStatus.isTerminalState()) {
          if (success) {
            updateCommandStatus(context, command, CommandStatus.Status.SUCCEEDED);
          } else {
            updateCommandStatus(context, command, CommandStatus.Status.FAILED,
                "Fail to process Command ", CommandResultCode.OTHER_ERROR);
          }
        }
      });
    } catch (Exception e) {
      LOG.error("Error processing command {} of type {}",
                command.getId(), command.getType(), e);
      updateCommandStatus(context, command, CommandStatus.Status.FAILED,
                         e.getMessage(), CommandResultCode.OTHER_ERROR);
    }
  }

  private void handleExecutionRejection(JobworkerCommand<?> command,
                                        JobworkerStateContext context,
                                        RejectedExecutionException e) {
    LOG.error("Command execution rejected: {}", e.getMessage());
    updateCommandStatus(context, command, CommandStatus.Status.FAILED,
        "Command execution rejected", CommandResultCode.COMMAND_REJECTED);
    queuedCount.decrementAndGet();
  }

  private void handleUnexpectedError(JobworkerCommand<?> command,
                                     JobworkerStateContext context,
                                     Throwable t) {
    LOG.error("Unexpected error submitting command {}: {}",
              command.getId(), t.getMessage(), t);
    updateCommandStatus(context, command, CommandStatus.Status.FAILED,
        "Unexpected submission error", CommandResultCode.UNEXPECTED_ERROR);
    queuedCount.decrementAndGet();
  }

  /**
   * Process a command. This is the method that subclasses should implement
   * to handle their specific command types.
   *
   * @param command           The command to process
   * @param context           The state context
   * @param connectionManager The connection manager
   * @return true if the command was processed successfully, false otherwise
   * @throws Exception if an error occurs processing the command
   */
  protected abstract boolean processCommand(JobworkerCommand<?> command,
                                         JobworkerStateContext context,
                                         JobworkerConnectionManager connectionManager)
      throws Exception;

  @Override
  public OMJobworkerCommandProto.Type getCommandType() {
    return type;
  }

  @Override
  public int getInvocationCount() {
    return invocationCount.get();
  }

  @Override
  public long getAverageRunTime() {
    int count = invocationCount.get();
    return count > 0 ? totalTime.get() / count : 0;
  }

  @Override
  public long getTotalRunTime() {
    return totalTime.get();
  }

  @Override
  public int getQueuedCount() {
    return queuedCount.get();
  }

  @VisibleForTesting
  public void setQueuedCount(int queuedCount) {
    this.queuedCount.set(queuedCount);
  }

  @VisibleForTesting
  public void setInjector(FaultInjector injector) {
    this.injector = injector;
  }
}
