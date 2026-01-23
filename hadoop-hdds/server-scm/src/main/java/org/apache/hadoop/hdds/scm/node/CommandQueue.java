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

package org.apache.hadoop.hdds.scm.node;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

/**
 * Command Queue is queue of commands for the datanode.
 * <p>
 * Node manager, container Manager and Ozone managers can queue commands for
 * datanodes into this queue. These commands will be sent in the order in which
 * they were queued.
 *
 * Note this class is not thread safe, and accesses must be protected by a lock.
 */
public class CommandQueue {
  private final Map<DatanodeID, Commands> commandMap;
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
   */
  public CommandQueue() {
    commandMap = new HashMap<>();
    commandsInQueue = 0;
  }

  /**
   * This function is used only for test purposes.
   */
  @VisibleForTesting
  public void clear() {
    commandMap.clear();
    commandsInQueue = 0;
  }

  /**
   * Returns  a list of Commands for the datanode to execute, if we have no
   * commands returns a empty list otherwise the current set of
   * commands are returned and command map set to empty list again.
   *
   * @return List of SCM Commands.
   */
  @SuppressWarnings("unchecked")
  List<SCMCommand<?>> getCommand(final DatanodeID datanodeID) {
    Commands cmds = commandMap.remove(datanodeID);
    List<SCMCommand<?>> cmdList = null;
    if (cmds != null) {
      cmdList = cmds.getCommands();
      commandsInQueue -= !cmdList.isEmpty() ? cmdList.size() : 0;
      // A post condition really.
      Preconditions.checkState(commandsInQueue >= 0);
    }
    return cmds == null ? Collections.emptyList() : cmdList;
  }

  /**
   * Returns the count of commands of the give type currently queued for the
   * given datanode. Note that any commands which return false for their
   * Command.contributesToQueueSize() method will not be included in the count.
   * At the current time, only low priority ReplicateContainerCommands meet this
   * condition.
   * @param datanodeID Datanode ID.
   * @param commandType The type of command for which to get the count.
   * @return The currently queued command count, or zero if none are queued.
   */
  public int getDatanodeCommandCount(
      final DatanodeID datanodeID, SCMCommandProto.Type commandType) {
    Commands commands = commandMap.get(datanodeID);
    if (commands == null) {
      return 0;
    }
    return commands.getCommandSummary(commandType);
  }

  /**
   * Return a summary of all commands currently queued for the given Datanode.
   * Note that any commands which return false for their
   * Command.contributesToQueueSize() method will not be included in the count.
   * At the current time, only low priority ReplicateContainerCommands meet this
   * condition.
   * @return A map containing the command summary. Note the returned map is a
   *         copy of the internal map and can be modified safely by the caller.
   */
  public Map<SCMCommandProto.Type, Integer> getDatanodeCommandSummary(
      final DatanodeID datanodeID) {
    Commands commands = commandMap.get(datanodeID);
    if (commands == null) {
      return Collections.emptyMap();
    }
    return commands.getAllCommandsSummary();
  }

  /** Adds a Command to the SCM Queue to send the command to container. */
  public void addCommand(final DatanodeID datanodeID, final SCMCommand<?> command) {
    commandMap.computeIfAbsent(datanodeID, s -> new Commands()).add(command);
    commandsInQueue++;
  }

  /**
   * Class that stores commands for a datanode.
   */
  private static class Commands {
    private List<SCMCommand<?>> commands = new ArrayList<>();
    private final Map<SCMCommandProto.Type, Integer> summary = new HashMap<>();

    /**
     * Adds a command to the list.
     *
     * @param command SCMCommand
     */
    public void add(SCMCommand<?> command) {
      this.commands.add(command);
      if (command.contributesToQueueSize()) {
        summary.put(command.getType(),
            summary.getOrDefault(command.getType(), 0) + 1);
      }
    }

    public int getCommandSummary(SCMCommandProto.Type commandType) {
      return summary.getOrDefault(commandType, 0);
    }

    public Map<SCMCommandProto.Type, Integer> getAllCommandsSummary() {
      return new HashMap<>(summary);
    }

    /**
     * Returns the commands for this datanode.
     * @return command list.
     */
    public List<SCMCommand<?>> getCommands() {
      List<SCMCommand<?>> temp = this.commands;
      this.commands = new ArrayList<>();
      summary.clear();
      return temp;
    }
  }
}
