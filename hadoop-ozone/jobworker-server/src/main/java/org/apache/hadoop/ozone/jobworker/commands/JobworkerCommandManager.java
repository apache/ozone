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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandResultCode;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerCommandRecord;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.ozone.jobworker.JobworkerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobworkerCommandManager is responsible for managing command queue, command status tracking,
 * and leadership term management for the JobWorker component.
 */

public class JobworkerCommandManager {

  private static final Logger LOG = LoggerFactory.getLogger(JobworkerCommandManager.class);

  private final Queue<JobworkerCommand> commandQueue;
  private final Map<String, Map<Long, JobworkerCommandStatus>> cmdStatusMap;
  private final Lock lock;
  private final int maxCommandQueueLimit;

  // OM leadership terms, mapped by service ID
  private final Map<String, OptionalLong> termOfLeaderOM;
  private JobworkerCommandJournal journal;
  private final Set<String> acceptedAttempts = new HashSet<>();

  public void setJournal(JobworkerCommandJournal journal) {
    this.journal = journal;
    for (JobworkerCommandRecord record
        : journal.records()) {
      if (!record.getAcknowledged()) {
        restoreStatus(record.getStatus());
      }
    }
  }

  private void restoreStatus(CommandStatus proto) {
    JobworkerCommandStatus status = JobworkerCommandStatus.fromProto(proto);
    status.setPersistence(updated -> {
      try {
        journal.update(updated);
      } catch (IOException e) {
        throw new UncheckedIOException("Cannot persist Jobworker status", e);
      }
    });
    cmdStatusMap.computeIfAbsent(proto.getOmServiceId(), ignore -> new ConcurrentHashMap<>())
        .put(proto.getCmdId(), status);
  }

  public void acceptDurableCommand(OMJobworkerCommandProto command, String service, long term)
      throws IOException {
    if (journal == null) {
      throw new IOException("No persistent Jobworker journal is configured");
    }
    lock.lock();
    try {
      JobworkerCommandRecord receipt =
          journal.receipt(service, command);
      if (receipt.getAcknowledged()) {
        return;
      }
      if (getCmdStatus(service, command.getCmdId()) == null) {
        restoreStatus(receipt.getStatus());
      }
      if (receipt.getStatus().getStatus() == CommandStatus.Status.PENDING
          && !command.getReconcileOnly()
          && !acceptedAttempts.contains(command.getAttemptId()) && commandQueue.size() < maxCommandQueueLimit) {
        commandQueue.add(JobworkerCommand.fromEnvelope(receipt.getCommand(), service, term));
        acceptedAttempts.add(command.getAttemptId());
      }
    } finally {
      lock.unlock();
    }
  }

  public void acknowledge(CommandStatus result) throws IOException {
    if (journal == null) {
      return;
    }
    lock.lock();
    try {
      if (journal.acknowledge(result)) {
        Map<Long, JobworkerCommandStatus> statuses = cmdStatusMap.get(result.getOmServiceId());
        if (statuses != null) {
          statuses.remove(result.getCmdId());
        }
        acceptedAttempts.remove(result.getAttemptId());
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Constructs a new JobworkerCommandManager.
   *
   * @param conf Configuration source
   */
  public JobworkerCommandManager(ConfigurationSource conf) {
    this.maxCommandQueueLimit = conf.getObject(JobworkerConfiguration.class)
        .getCommandQueueLimit();
    this.commandQueue = new LinkedList<>();
    this.cmdStatusMap = new ConcurrentHashMap<>();
    this.lock = new ReentrantLock();
    this.termOfLeaderOM = new ConcurrentHashMap<>();
  }

  /**
   * Get the lock used for command queue synchronization.
   *
   * @return the lock
   */
  public Lock getLock() {
    return lock;
  }

  /**
   * Returns the command status for a given command ID.
   *
   * @param omServiceId The OM service ID
   * @param key         The command ID
   * @return CommandStatus or null
   */
  public JobworkerCommandStatus getCmdStatus(String omServiceId, Long key) {
    if (cmdStatusMap.get(omServiceId) != null) {
      return cmdStatusMap.get(omServiceId).get(key);
    }
    return null;
  }

  /**
   * Adds a command status for a given command.
   *
   * @param cmd JobworkerCommand
   */
  public void addCmdStatus(JobworkerCommand cmd) {
    String omServiceId = cmd.getOmServiceId();
    JobworkerCommandStatus status = JobworkerCommandStatus.newBuilder()
        .setCmdId(cmd.getId())
        .setType(cmd.getType())
        .setStatus(CommandStatus.Status.PENDING)
        .setOmServiceId(omServiceId)
        .build();
    cmdStatusMap.computeIfAbsent(omServiceId, ignore -> new ConcurrentHashMap<>())
        .put(cmd.getId(), status);
  }

  /**
   * Adds a failed command status for a given command.
   *
   * @param cmd JobworkerCommand
   * @param msg The failed Command message
   * @param resultCode The failed result code
   *
   */
  public void addFailedCmd(JobworkerCommand cmd, String msg, CommandResultCode resultCode) {
    String omServiceId = cmd.getOmServiceId();
    JobworkerCommandStatus.Builder builder = JobworkerCommandStatus.newBuilder()
        .setCmdId(cmd.getId())
        .setType(cmd.getType())
        .setStatus(CommandStatus.Status.FAILED)
        .setOmServiceId(omServiceId);
    if (msg != null) {
      builder.setMsg(msg);
    }
    if (resultCode != null) {
      builder.setResultCode(resultCode);
    }
    cmdStatusMap.computeIfAbsent(omServiceId, ignore -> new ConcurrentHashMap<>())
        .put(cmd.getId(), builder.build());
  }

  /**
   * Get map holding all command status objects.
   *
   * @return map of command statuses
   */
  public Map<String, Map<Long, JobworkerCommandStatus>> getCommandStatusMap() {
    return cmdStatusMap;
  }

  /**
   * After startup, jobworker needs to detect latest leader OM for each service
   * before handling any JobworkerCommand, so that it won't be disturbed by
   * stale leader OMs.
   * <p>
   * The rule is: after we have received commands with terms from a service,
   * initialize the termOfLeaderOM for that service with the max term found.
   * <p>
   * This init process also works for non-HA mode. In that case, term of all
   * commands will be 0.
   *
   * @param omServiceId The OM service ID
   */
  private void initTermOfLeaderOM(String omServiceId) {
    // only init once for each service ID
    if (termOfLeaderOM.containsKey(omServiceId)) {
      return;
    }

    // Initialize with OptionalLong.empty() until we get commands
    termOfLeaderOM.putIfAbsent(omServiceId, OptionalLong.empty());

    lock.lock();
    try {
      // Check if we have any commands yet to initialize with a term
      OptionalLong maxTerm = commandQueue.stream()
          .filter(cmd -> omServiceId.equals(cmd.getOmServiceId()))
          .mapToLong(JobworkerCommand::getTerm)
          .max();

      if (maxTerm.isPresent()) {
        updateTermOfLeaderOM(omServiceId, maxTerm.getAsLong());
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Update the term of leader OM for a specific service ID.
   *
   * @param omServiceId The OM service ID
   * @param newTerm     New term value
   */
  public void updateTermOfLeaderOM(String omServiceId, long newTerm) {
    termOfLeaderOM.compute(omServiceId, (service, currentTerm) ->
        currentTerm == null || !currentTerm.isPresent() || currentTerm.getAsLong() < newTerm
            ? OptionalLong.of(newTerm) : currentTerm);
  }

  public void updateCommand(JobworkerCommand command,
                            CommandStatus.Status status, String message, CommandResultCode resultCode) {
    long commandId = command.getId();
    String omServiceId = command.getOmServiceId();
    Map<Long, JobworkerCommandStatus> commands = cmdStatusMap.get(omServiceId);
    if (commands != null && commands.get(commandId) != null) {
      commands.get(commandId).updateStatusAndMessage(status, message, resultCode);
    } else {
      LOG.warn("CommandStatus Type {} with ID: {} not found.", command.getType(),
          command.getId());
    }
  }

  public void updateCommand(JobworkerCommand command,
                            Consumer<JobworkerCommandStatus> cmdStatusUpdater) {
    long commandId = command.getId();
    String omServiceId = command.getOmServiceId();
    Map<Long, JobworkerCommandStatus> commands = cmdStatusMap.get(omServiceId);
    if (commands != null && commands.get(commandId) != null) {
      cmdStatusUpdater.accept(commands.get(commandId));
    } else {
      LOG.warn("CommandStatus Type {} with ID: {} not found.", command.getType(),
          command.getId());
    }
  }

  /**
   * Returns the next command or null if queue is empty.
   *
   * @return JobworkerCommand or Null
   */
  public JobworkerCommand getNextCommand() {
    lock.lock();
    try {
      while (true) {
        JobworkerCommand command = commandQueue.poll();
        if (command == null) {
          return null;
        }

        if (command.getAttemptId() != null) {
          // A committed assignment remains valid across leader changes; a new term is not a new attempt.
          return command;
        }

        String omServiceId = command.getOmServiceId();
        if (!termOfLeaderOM.containsKey(omServiceId)) {
          initTermOfLeaderOM(omServiceId);
        }

        updateTermOfLeaderOM(omServiceId, command.getTerm());
        OptionalLong currentTerm = termOfLeaderOM.get(omServiceId);
        if (!currentTerm.isPresent()) {
          // updateTermOfLeaderOM will update current Term,
          // so normal business logic cannot reach here
          LOG.error("No Term found for OM service id {}", omServiceId);
          updateCommand(command, CommandStatus.Status.FAILED, "\"No Term found for OM service",
              CommandResultCode.UNEXPECTED_ERROR);
          return null;
        }

        if (command.getTerm() == currentTerm.getAsLong()) {
          return command;
        }

        // If we get here, this command is from a stale leader
        LOG.warn("Detect and drop a JobworkerCommand {} from stale leader OM for service {}," +
                " stale term {}, latest term {}.",
            command, omServiceId, command.getTerm(), currentTerm.getAsLong());
        updateCommand(command, CommandStatus.Status.FAILED, "Stale command", CommandResultCode.STALE_TERM);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Adds a command to the command queue.
   *
   * @param command - JobworkerCommand
   */
  public void addCommand(JobworkerCommand command) {
    lock.lock();
    try {
      if (getCmdStatus(command.getOmServiceId(), command.getId()) != null) {
        LOG.debug("Ignoring duplicate live command {} from {}", command.getId(), command.getOmServiceId());
        return;
      }
      if (commandQueue.size() >= maxCommandQueueLimit) {
        // TODO jobworker add metrics
        LOG.warn("Ignore command {} as command queue crosses max limit {}.",
            command.getType(), maxCommandQueueLimit);
        addFailedCmd(command, "Command queue is full", CommandResultCode.COMMAND_QUEUE_FULL);
        return;
      }

      String omServiceId = command.getOmServiceId();
      if (!termOfLeaderOM.containsKey(omServiceId)) {
        initTermOfLeaderOM(omServiceId);
      }
      updateTermOfLeaderOM(omServiceId, command.getTerm());

      commandQueue.add(command);
      addCmdStatus(command);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Gets a summary of commands in the queue by type.
   *
   * @return Map of command types to counts
   */
  public Map<OMJobworkerCommandProto.Type, Integer> getCommandQueueSummary() {
    Map<OMJobworkerCommandProto.Type, Integer> summary = new HashMap<>();
    lock.lock();
    try {
      for (JobworkerCommand cmd : commandQueue) {
        summary.put(cmd.getType(), summary.getOrDefault(cmd.getType(), 0) + 1);
      }
    } finally {
      lock.unlock();
    }
    return summary;
  }

  /**
   * Get the leader Term of the OM Group based on omServiceId.
   *
   * @param omServiceId
   * @return
   */
  public OptionalLong getTermOfLeaderOMByServiceId(String omServiceId) {
    return termOfLeaderOM.getOrDefault(omServiceId, OptionalLong.empty());
  }
}
