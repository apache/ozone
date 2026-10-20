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

package org.apache.hadoop.ozone.om.jobworker.command;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandResultCode;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.jobworker.commands.OMJobworkerCommand;
import org.apache.hadoop.ozone.om.jobworker.node.JobworkerNodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used to process and track commands sent from OzoneManager to JobWorker nodes.
 * This class contains the core logic for command processing and tracking.
 */

public class OMJobworkerCommandManager implements java.io.Closeable {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMJobworkerCommandManager.class);
  private final JobworkerNodeManager jobworkerNodeManager;
  private final Map<OMJobworkerCommandProto.Type, JobworkerCommandListener> listeners;
  private final Map<OMJobworkerCommandProto.Type, Map<Long, JobworkerCommandInfo>> commandInfoMaps;
  private final CommandTimeoutChecker timeoutChecker;
  private final String omServiceId;
  // Term of this OM if it is the ready leader, otherwise empty.
  private final Supplier<OptionalLong> leaderTerm;

  private static final long DEFAULT_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(10);

  /**
   * Create a new JobworkerCommandProcessor.
   *
   * @param jobworkerNodeManager the jobworkerNodeManager instance
   * @param omServiceId the service ID of this OM
   * @param leaderTerm supplies the Raft term of this OM if it is the ready leader, otherwise empty
   */
  public OMJobworkerCommandManager(JobworkerNodeManager jobworkerNodeManager, String omServiceId,
      Supplier<OptionalLong> leaderTerm) {
    Objects.requireNonNull(jobworkerNodeManager, "jobworkerNodeManager cannot be null");
    Objects.requireNonNull(omServiceId, "omServiceId cannot be null");
    Objects.requireNonNull(leaderTerm, "leaderTerm cannot be null");
    this.jobworkerNodeManager = jobworkerNodeManager;
    this.omServiceId = omServiceId;
    this.leaderTerm = leaderTerm;
    this.listeners = new ConcurrentHashMap<>();
    this.commandInfoMaps = new ConcurrentHashMap<>();
    this.timeoutChecker = new CommandTimeoutChecker(listeners, commandInfoMaps);
    this.timeoutChecker.startChecking();
  }

  /**
   * Register a command listener for a specific command type.
   *
   * @param commandType the command type
   * @param listener the listener for the command type
   */
  public void registerHandler(OMJobworkerCommandProto.Type commandType,
                              JobworkerCommandListener listener) {
    listeners.put(commandType, listener);
    commandInfoMaps.put(commandType, new ConcurrentHashMap<>());
  }

  public int getInFlightCommandCount(OMJobworkerCommandProto.Type commandType) {
    if (commandInfoMaps.get(commandType) == null) {
      return 0;
    }
    return commandInfoMaps.get(commandType).size();
  }

  public boolean isCommandInFlight(OMJobworkerCommandProto.Type commandType, long commandId) {
    if (commandInfoMaps.get(commandType) == null) {
      return false;
    }
    return commandInfoMaps.get(commandType).containsKey(commandId);
  }

  /**
   * Get the listener for a specific command type (for testing purposes).
   *
   * @param commandType the command type
   * @return the listener for the command type, or null if not found
   */
  @VisibleForTesting
  public JobworkerCommandListener getListener(OMJobworkerCommandProto.Type commandType) {
    return listeners.get(commandType);
  }

  /**
   * Set the listener for a specific command type (for testing purposes).
   *
   * @param commandType the command type
   * @param listener the listener to set
   */
  @VisibleForTesting
  public void setListener(OMJobworkerCommandProto.Type commandType, JobworkerCommandListener listener) {
    listeners.put(commandType, listener);
  }

  /**
   * Validates whether a state transition is allowed according to the rules:
   * - PENDING -> EXECUTING: Successful resource allocation
   * - PENDING -> FAILED: Resource allocation failed, not in IN_SERVICE status, low term command
   * - PENDING -> SUCCEEDED: Successfully executed.
   * - EXECUTING -> FAILED: Failure during execution, not in IN_SERVICE status
   * - EXECUTING -> SUCCEEDED: Successfully executed.
   */
  private boolean isValidStateTransition(CommandStatus.Status currentStatus,
                                         CommandStatus.Status newStatus) {
    if (currentStatus == newStatus) {
      return true;
    }

    switch (currentStatus) {
    case PENDING:
      return newStatus == CommandStatus.Status.EXECUTING ||
          newStatus == CommandStatus.Status.FAILED ||
          newStatus == CommandStatus.Status.SUCCEEDED;

    case EXECUTING:
      return newStatus == CommandStatus.Status.SUCCEEDED ||
          newStatus == CommandStatus.Status.FAILED;

    case SUCCEEDED:
    case FAILED:
      LOG.warn("Command in terminal state {} cannot transition to {}",
          currentStatus, newStatus);
      return false;

    default:
      LOG.warn("Unknown command status: {}", currentStatus);
      return false;
    }
  }

  /**
   * Process a status update for a command.
   *
   * @param jobworkerDetails details of the JobWorker reporting the status
   * @param cmdStatus the command status update
   */
  protected void processStatusUpdate(JobworkerDetails jobworkerDetails,
                                     CommandStatus cmdStatus) {
    long cmdId = cmdStatus.getCmdId();
    OMJobworkerCommandProto.Type type = cmdStatus.getType();
    JobworkerCommandListener listener = listeners.get(type);
    if (listener == null) {
      LOG.warn("Received status update for unknown type {} from JobWorker {}",
          type, jobworkerDetails.getUuidString());
      return;
    }
    Map<Long, JobworkerCommandInfo> commandInfo = commandInfoMaps.get(type);
    if (commandInfo == null) {
      LOG.warn("Received status update for unknown type {} from JobWorker {}",
          type, jobworkerDetails.getUuidString());
      return;
    }
    JobworkerCommandInfo statusInfo = commandInfo.get(cmdId);
    if (statusInfo == null) {
      LOG.warn("Received status update for unknown command ID {} from JobWorker {}",
          cmdId, jobworkerDetails.getUuidString());
      return;
    }

    if (!statusInfo.getJobworkerUuid().equals(jobworkerDetails.getUuid())
        || !omServiceId.equals(cmdStatus.getOmServiceId())) {
      LOG.warn("Ignoring status for command {} from a different worker or OM service", cmdId);
      return;
    }
    if (cmdStatus.hasTerm() && cmdStatus.getTerm() != statusInfo.getTerm()) {
      LOG.warn("Ignoring status for command {} from term {}, the command was sent in term {}",
          cmdId, cmdStatus.getTerm(), statusInfo.getTerm());
      return;
    }
    synchronized (statusInfo) {
      if (commandInfo.get(cmdId) != statusInfo || statusInfo.isTerminalState()) {
        return;
      }
      try {
        CommandStatus.Status currentStatus = statusInfo.getStatus();
        CommandStatus.Status newStatus = cmdStatus.getStatus();
        CommandResultCode resultCode = cmdStatus.getResultCode();

        if (!isValidStateTransition(currentStatus, newStatus)) {
          LOG.warn("Invalid state transition attempted for command ID {} from {} to {} by JobWorker {}",
              cmdId, currentStatus, newStatus, jobworkerDetails.getUuidString());
          return;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Command {} status updated from {} to {} by JobWorker {}",
              cmdId, currentStatus, newStatus,
              jobworkerDetails.getUuidString());
        }
        statusInfo.setStatus(newStatus);
        statusInfo.updateLastStatusUpdateTime();
        statusInfo.setResultCode(resultCode);

        if (cmdStatus.hasMsg()) {
          statusInfo.setMessage(cmdStatus.getMsg());
        }

        switch (newStatus) {
        case SUCCEEDED:
          listener.onCommandSucceeded(statusInfo, cmdStatus.getExecutionResults(), jobworkerDetails);
          break;
        case FAILED:
          listener.onCommandFailed(statusInfo, cmdStatus.getExecutionResults(), jobworkerDetails);
          break;
        case EXECUTING:
          listener.onCommandExecuting(statusInfo, jobworkerDetails);
          break;
        default:
          break;
        }
      } finally {
        if (statusInfo.isTerminalState()) {
          commandInfo.remove(statusInfo.getCommandId(), statusInfo);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Remove Terminal State Command {} State {} Type {}",
                statusInfo.getCommandId(), statusInfo.getStatus(), statusInfo.getCommandType());
          }
        }
      }
    }
  }

  /**
   * Mark specific commands as failed due to the JobWorker becoming stale.
   *
   * @param commands List of commands to mark as failed
   * @param jobworkerDetails Details of the stale JobWorker
   * @return The number of commands that were marked as failed
   */
  public void markCommandsFailedForJobworker(List<OMJobworkerCommand> commands,
                                            JobworkerDetails jobworkerDetails) {
    if (commands == null || commands.isEmpty()) {
      return;
    }

    for (OMJobworkerCommand command : commands) {
      long cmdId = command.getId();
      OMJobworkerCommandProto.Type cmdType = command.getType();
      CommandStatus commandStatus =
          CommandStatus.newBuilder()
              .setOmServiceId(omServiceId)
              .setCmdId(cmdId)
              .setType(cmdType)
              .setStatus(CommandStatus.Status.FAILED)
              .build();
      processStatusUpdate(jobworkerDetails, commandStatus);
    }
  }

  /**
   * Queue a command to a JobWorker and track its status.
   * TODO(JW): Since this is queueing, we can rename this to addCommand / queueCommand
   *
   * @param jobworkerUuid UUID of the target JobWorker
   * @param command command to send
   * @return the command ID
   */
  public long sendCommand(UUID jobworkerUuid, OMJobworkerCommand command) throws IOException {
    OptionalLong term = leaderTerm.get();
    if (!term.isPresent()) {
      throw new IOException("Cannot send Jobworker command " + command.getId() + ", OM is not the ready leader");
    }
    command.setTerm(term.getAsLong());
    OMJobworkerCommandProto.Type type = command.getType();
    JobworkerCommandListener listener = listeners.get(type);
    if (listener == null) {
      throw new IOException("unknown type " + type);
    }
    Map<Long, JobworkerCommandInfo> commandInfoMap = commandInfoMaps.get(type);
    if (commandInfoMap == null) {
      throw new IOException("unknown type " + type);
    }

    JobworkerCommandInfo commandInfo =
        new JobworkerCommandInfo(command, jobworkerUuid, CommandStatus.Status.PENDING);
    if (commandInfoMap.putIfAbsent(command.getId(), commandInfo) != null) {
      throw new IOException("Command is already in flight: " + command.getId());
    }
    try {
      jobworkerNodeManager.addOMJobworkerCommand(jobworkerUuid, command);
    } catch (RuntimeException e) {
      commandInfoMap.remove(command.getId(), commandInfo);
      throw e;
    }
    if (!term.equals(leaderTerm.get())) {
      // Leadership changed while queueing; onLeadershipChange may already have run, so do not leave it behind.
      commandInfoMap.remove(command.getId(), commandInfo);
      throw new IOException("Cannot send Jobworker command " + command.getId() + ", OM leadership changed");
    }
    LOG.debug("Queued command {} of type {} in term {} to JobWorker {}",
        command.getId(), command.getType(), command.getTerm(), jobworkerUuid);
    return command.getId();
  }

  public void markCommandSentForJobworker(OMJobworkerCommandProto command, UUID jobworkerUuid) {
    JobworkerCommandListener listener = listeners.get(command.getCommandType());
    if (listener == null) {
      LOG.warn("Encountered command with unknown type {}", command.getCommandType());
      return;
    }
    listener.onSendCommand(command, jobworkerUuid);
  }

  /**
   * Forget all in-flight commands after this OM gained or lost leadership, or the term changed.
   * The outcome of each forgotten command is unknown, so its listener is notified through
   * {@link JobworkerCommandListener#onStatusUpdateTimeout}. Owning workflows re-issue the work from their own state.
   */
  public void onLeadershipChange() {
    for (Map.Entry<OMJobworkerCommandProto.Type, Map<Long, JobworkerCommandInfo>> entry
        : commandInfoMaps.entrySet()) {
      JobworkerCommandListener listener = listeners.get(entry.getKey());
      Map<Long, JobworkerCommandInfo> commandInfoMap = entry.getValue();
      for (JobworkerCommandInfo statusInfo : new ArrayList<>(commandInfoMap.values())) {
        synchronized (statusInfo) {
          if (!commandInfoMap.remove(statusInfo.getCommandId(), statusInfo) || statusInfo.isTerminalState()) {
            continue;
          }
          LOG.info("Dropping in-flight command {} of type {} sent in term {} to JobWorker {} on leadership change",
              statusInfo.getCommandId(), statusInfo.getCommandType(), statusInfo.getTerm(),
              statusInfo.getJobworkerUuid());
          if (listener != null) {
            try {
              listener.onStatusUpdateTimeout(statusInfo, statusInfo.getJobworkerUuid());
            } catch (RuntimeException e) {
              LOG.error("Listener failed for dropped command {}", statusInfo.getCommandId(), e);
            }
          }
        }
      }
    }
  }

  /**
   * For testing only - get access to the command info maps.
   */
  @VisibleForTesting
  Map<OMJobworkerCommandProto.Type, Map<Long, JobworkerCommandInfo>> getCommandInfoMaps() {
    return commandInfoMaps;
  }

  /**
   * For testing only - reset the command info maps.
   */
  @VisibleForTesting
  public void resetCommandInfoMaps() {
    commandInfoMaps.forEach((type, commandInfoMap) -> {
      commandInfoMap.clear();
    });
  }

  public void cleanupCommandsById(OMJobworkerCommandProto.Type commandType,
      List<Long> commandIds) {
    if (commandIds == null || commandIds.isEmpty()) {
      return;
    }
    Map<Long, JobworkerCommandInfo> commandInfoMap = commandInfoMaps.get(commandType);
    if (commandInfoMap == null) {
      return;
    }
    List<Long> removed = new ArrayList<>();
    for (Long commandId : commandIds) {
      JobworkerCommandInfo commandInfo = commandInfoMap.remove(commandId);
      if (commandInfo != null) {
        removed.add(commandInfo.getCommandId());
      }
    }
    if (!removed.isEmpty()) {
      LOG.debug("Cleaned up {} in-flight {} commands", removed.size(), commandType);
    }
  }

  /**
   * For testing only - get the command timeout checker instance.
   */
  @VisibleForTesting
  CommandTimeoutChecker getCommandTimeoutChecker() {
    return timeoutChecker;
  }

  /**
   * Inner class responsible for checking command timeouts.
   * This design encapsulates all timeout checking logic and thread management.
   */
  static final class CommandTimeoutChecker {
    private static CommandTimeoutChecker instance;
    private final ScheduledExecutorService executor;
    private final Map<OMJobworkerCommandProto.Type, JobworkerCommandListener> listeners;
    private final Map<OMJobworkerCommandProto.Type, Map<Long, JobworkerCommandInfo>> commandInfoMaps;
    private Supplier<Long> clock = System::currentTimeMillis;
    private static long checkIntervalSeconds = 60;
    private static int checkTimes = 0;

    private CommandTimeoutChecker(Map<OMJobworkerCommandProto.Type, JobworkerCommandListener> listeners,
                                  Map<OMJobworkerCommandProto.Type, Map<Long, JobworkerCommandInfo>> commandInfoMaps) {
      this.listeners = listeners;
      this.commandInfoMaps = commandInfoMaps;
      this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "JobworkerCommandTimeoutChecker");
        t.setDaemon(true);
        return t;
      });
    }

    private void startChecking() {
      executor.scheduleAtFixedRate(() -> {
        for (OMJobworkerCommandProto.Type type : listeners.keySet()) {
          try {
            checkTimeoutsForProcessor(type);
            checkTimes++;
          } catch (Exception e) {
            LOG.error("Error checking command timeouts for processor {}", type, e);
          }
        }
      }, checkIntervalSeconds, checkIntervalSeconds, TimeUnit.SECONDS);

      LOG.info("CommandTimeoutChecker scheduler started with checkIntervalSeconds {}", checkIntervalSeconds);
    }

    /**
     * Check for timed-out commands in a specific processor.
     * Leverages the monotonically increasing command IDs to optimize the check.
     */
    @VisibleForTesting
    void checkTimeoutsForProcessor(OMJobworkerCommandProto.Type type) {
      Map<Long, JobworkerCommandInfo> commandStatusMap = commandInfoMaps.get(type);
      JobworkerCommandListener commandListener = listeners.get(type);
      if (commandListener == null || commandStatusMap == null) {
        LOG.warn("unknown command type {} ", type);
        return;
      }
      long cutoffTime = getCurrentTimeMillis() - DEFAULT_TIMEOUT_MS;
      for (JobworkerCommandInfo statusInfo : commandStatusMap.values()) {
        synchronized (statusInfo) {
          if (commandStatusMap.get(statusInfo.getCommandId()) != statusInfo
              || statusInfo.isTerminalState() || statusInfo.getLastStatusUpdateTime() > cutoffTime) {
            continue;
          }
          LOG.warn("Command {} of type {} sent to JobWorker {} has timed out in status update {}",
              statusInfo.getCommandId(), statusInfo.getCommandType(),
              statusInfo.getJobworkerUuid(), statusInfo.getStatus());
          try {
            commandListener.onStatusUpdateTimeout(statusInfo, statusInfo.getJobworkerUuid());
          } finally {
            commandStatusMap.remove(statusInfo.getCommandId(), statusInfo);
          }
        }
      }
    }

    /**
     * Set the check interval.
     *
     * @param intervalSeconds interval in seconds
     */
    @VisibleForTesting
    static void setCheckIntervalForTesting(long intervalSeconds) {
      checkIntervalSeconds = intervalSeconds;
    }

    /**
     * Get the current time in milliseconds.
     *
     * @return Current time in milliseconds
     */
    private long getCurrentTimeMillis() {
      return clock.get();
    }

    /**
     * Set the clock for testing.
     *
     * @param clock Supplier that provides the current time
     */
    @VisibleForTesting
    void setClock(Supplier<Long> clock) {
      this.clock = clock;
    }

    /**
     * Get the current timeout in milliseconds.
     *
     * @return Timeout in milliseconds
     */
    @VisibleForTesting
    long getTimeoutMs() {
      return DEFAULT_TIMEOUT_MS;
    }

    public ScheduledExecutorService getExecutor() {
      return executor;
    }

    @VisibleForTesting
    public static int getCheckTimes() {
      return checkTimes;
    }

    public void close() {
      ServerUtils.executorServiceShutdownGraceful(executor);
    }
  }

  public void close() {
    timeoutChecker.close();
  }
}
