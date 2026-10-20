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

package org.apache.hadoop.ozone.om.jobworker;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.ozone.jobworker.commands.OMJobworkerCommand;

/**
 * Command Queue is a queue of commands for the jobworker.
 * <p>
 * Ozone managers can add commands for jobworker into this queue.
 * These commands will be sent in the order in which they were queued.
 * <p>
 * This class is thread safe and uses a read-write lock to protect access to
 * the internal state.
 */
public class OMJobworkerCommandQueue {
  private final Map<UUID, Commands> commandMap;
  private long commandsInQueue;
  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

  /**
   * Returns the number of commands in the queue.
   *
   * @return Command Count.
   */
  public long getCommandsInQueue() {
    rwLock.readLock().lock();
    try {
      return commandsInQueue;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * Constructs a Command Queue.
   */
  public OMJobworkerCommandQueue() {
    commandMap = new HashMap<>();
    commandsInQueue = 0;
  }

  /**
   * Remove the queued commands of all jobworkers.
   */
  public void clear() {
    rwLock.writeLock().lock();
    try {
      commandMap.clear();
      commandsInQueue = 0;
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * Clear all commands for a specific JobWorker.
   * @param jobworkerId UUID of the JobWorker
   * @return Number of commands that were cleared
   */
  public int clear(UUID jobworkerId) {
    rwLock.writeLock().lock();
    try {
      Commands commands = commandMap.remove(jobworkerId);
      if (commands != null) {
        int commandCount = commands.getCommandCount();
        commandsInQueue -= commandCount;
        Preconditions.checkState(commandsInQueue >= 0);
        return commandCount;
      }
      return 0;
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * Returns a list of Commands for the jobworker to execute, if we have no
   * commands returns an empty list, otherwise the current set of
   * commands are returned, and a command map set to an empty list again.
   *
   * @param jobworkerId jobworker UUID
   * @return List of OMJobworkerCommand Commands.
   */
  @SuppressWarnings("unchecked")
  public List<OMJobworkerCommand> pollCommand(final UUID jobworkerId) {
    rwLock.writeLock().lock();
    try {
      Commands commands = commandMap.remove(jobworkerId);
      List<OMJobworkerCommand> cmdList = null;
      if (commands != null) {
        cmdList = commands.pollAllCommands();
        commandsInQueue -= cmdList.size();
        // A post-condition really.
        Preconditions.checkState(commandsInQueue >= 0);
      }
      return commands == null ? Collections.emptyList() : cmdList;
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * Returns the count of commands of the give type currently queued for the
   * given jobworker.
   *
   * @param jobworkerUuid Jobworker UUID.
   * @param commandType   The type of command for which to get the count.
   * @return The currently queued command count, or zero if none are queued.
   */
  public int getJobworkerCommandCount(
      final UUID jobworkerUuid, OMJobworkerCommandProto.Type commandType) {
    rwLock.readLock().lock();
    try {
      Commands commands = commandMap.get(jobworkerUuid);
      if (commands == null) {
        return 0;
      }
      return commands.getCommandSummary(commandType);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * Adds a Command to the OM Queue to send the command to jobworker.
   *
   * @param jobworkerUuid JobworkerDetails.Uuid
   * @param command       - Command
   */
  public void addCommand(final UUID jobworkerUuid, final OMJobworkerCommand command) {
    rwLock.writeLock().lock();
    try {
      commandMap.computeIfAbsent(jobworkerUuid, s -> new Commands()).add(command);
      commandsInQueue++;
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * Class that stores commands for a jobworker.
   */
  private static class Commands {
    private List<OMJobworkerCommand> commands = new ArrayList<>();
    private final Map<OMJobworkerCommandProto.Type, List<OMJobworkerCommand>> commandsByType =
        new EnumMap<>(OMJobworkerCommandProto.Type.class);

    public int getCommandSummary(OMJobworkerCommandProto.Type commandType) {
      List<OMJobworkerCommand> cmds = commandsByType.get(commandType);
      return cmds != null ? cmds.size() : 0;
    }

    public Map<OMJobworkerCommandProto.Type, Integer> getAllCommandsSummary() {
      Map<OMJobworkerCommandProto.Type, Integer> summary = new HashMap<>();
      for (Map.Entry<OMJobworkerCommandProto.Type, List<OMJobworkerCommand>> entry :
          commandsByType.entrySet()) {
        summary.put(entry.getKey(), entry.getValue().size());
      }
      return summary;
    }

    /**
     * Returns the total count of commands for this jobworker.
     *
     * @return command count
     */
    private int getCommandCount() {
      return commands.size();
    }

    /**
     * Adds a command to the list.
     *
     * @param command OMJobworkerCommand
     */
    public void add(OMJobworkerCommand command) {
      OMJobworkerCommandProto.Type type = command.getType();
      commands.add(command);
      commandsByType.computeIfAbsent(type, k -> new ArrayList<>()).add(command);
    }

    /**
     * Returns the commands for this jobworker.
     *
     * @return command list.
     */
    public List<OMJobworkerCommand> pollAllCommands() {
      List<OMJobworkerCommand> temp = this.commands;
      commands = new ArrayList<>();
      commandsByType.clear();
      return temp;
    }
  }
}
