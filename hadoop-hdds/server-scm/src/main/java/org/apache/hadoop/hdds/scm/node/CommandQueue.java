/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.node;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Command Queue is queue of commands for the datanode.
 * <p>
 * Node manager, container Manager and Ozone managers can queue commands for
 * datanodes into this queue. These commands will be send in the order in which
 * there where queued.
 */
public class CommandQueue {
  private final Map<UUID, Commands> commandMap;
  private final Lock lock;
  private long commandsInQueue;

  /**
   * Returns number of commands in queue.
   * @return Command Count.
   */
  public long getCommandsInQueue() {
    return commandsInQueue;
  }

  /**
   * Constructs a Command Queue.
   * TODO : Add a flusher thread that throws away commands older than a certain
   * time period.
   */
  public CommandQueue() {
    commandMap = new HashMap<>();
    lock = new ReentrantLock();
    commandsInQueue = 0;
  }

  /**
   * This function is used only for test purposes.
   */
  @VisibleForTesting
  public void clear() {
    lock.lock();
    try {
      commandMap.clear();
      commandsInQueue = 0;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns  a list of Commands for the datanode to execute, if we have no
   * commands returns a empty list otherwise the current set of
   * commands are returned and command map set to empty list again.
   *
   * @param datanodeUuid Datanode UUID
   * @return List of SCM Commands.
   */
  @SuppressWarnings("unchecked")
  List<SCMCommand> getCommand(final UUID datanodeUuid) {
    lock.lock();
    try {
      Commands cmds = commandMap.remove(datanodeUuid);
      List<SCMCommand> cmdList = null;
      if (cmds != null) {
        cmdList = cmds.getCommands();
        commandsInQueue -= cmdList.size() > 0 ? cmdList.size() : 0;
        // A post condition really.
        Preconditions.checkState(commandsInQueue >= 0);
      }
      return cmds == null ? Collections.emptyList() : cmdList;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns the count of commands of the give type currently queued for the
   * given datanode.
   * @param datanodeUuid Datanode UUID.
   * @param commandType The type of command for which to get the count.
   * @return The currently queued command count, or zero if none are queued.
   */
  public int getDatanodeCommandCount(
      final UUID datanodeUuid, SCMCommandProto.Type commandType) {
    lock.lock();
    try {
      Commands commands = commandMap.get(datanodeUuid);
      if (commands == null) {
        return 0;
      }
      return commands.getCommandSummary(commandType);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Adds a Command to the SCM Queue to send the command to container.
   *
   * @param datanodeUuid DatanodeDetails.Uuid
   * @param command    - Command
   */
  public void addCommand(final UUID datanodeUuid, final SCMCommand
      command) {
    lock.lock();
    try {
      commandMap.computeIfAbsent(datanodeUuid, s -> new Commands())
          .add(command);
      commandsInQueue++;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Class that stores commands for a datanode.
   */
  private static class Commands {
    private long updateTime = 0;
    private long readTime = 0;
    private List<SCMCommand> commands = new ArrayList<>();
    private final Map<SCMCommandProto.Type, Integer> summary = new HashMap<>();

    /**
     * Gets the last time the commands for this node was updated.
     * @return Time stamp
     */
    public long getUpdateTime() {
      return updateTime;
    }

    /**
     * Gets the last read time.
     * @return last time when these commands were read from this queue.
     */
    public long getReadTime() {
      return readTime;
    }

    /**
     * Adds a command to the list.
     *
     * @param command SCMCommand
     */
    public void add(SCMCommand command) {
      this.commands.add(command);
      summary.put(command.getType(),
          summary.getOrDefault(command.getType(), 0) + 1);
      updateTime = Time.monotonicNow();
    }

    public int getCommandSummary(SCMCommandProto.Type commandType) {
      return summary.getOrDefault(commandType, 0);
    }

    /**
     * Returns the commands for this datanode.
     * @return command list.
     */
    public List<SCMCommand> getCommands() {
      List<SCMCommand> temp = this.commands;
      this.commands = new ArrayList<>();
      summary.clear();
      readTime = Time.monotonicNow();
      return temp;
    }
  }
}
