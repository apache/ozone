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

package org.apache.hadoop.ozone.protocol.commands;

import com.google.protobuf.Message;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.server.events.IdentifiableEventPayload;

/**
 * Command for the datanode with the destination address.
 */
public final class CommandForDatanode<T extends Message> implements IdentifiableEventPayload {

  private final DatanodeID datanodeId;
  private final SCMCommand<T> command;

  public CommandForDatanode(DatanodeDetails datanode, SCMCommand<T> command) {
    this(datanode.getID(), command);
  }

  public CommandForDatanode(DatanodeID datanodeId, SCMCommand<T> command) {
    this.datanodeId = datanodeId;
    this.command = command;
  }

  public DatanodeID getDatanodeId() {
    return datanodeId;
  }

  public SCMCommand<T> getCommand() {
    return command;
  }

  @Override
  public long getId() {
    return command.getId();
  }

  @Override
  public String toString() {
    return "CommandForDatanode{" + datanodeId + ", " + command + '}';
  }
}
