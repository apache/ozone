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

import java.util.Objects;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SetNodeOperationalStateCommandProto;

/**
 * A command used to persist the current node operational state on the datanode.
 */
public class SetNodeOperationalStateCommand
    extends SCMCommand<SetNodeOperationalStateCommandProto> {

  private final HddsProtos.NodeOperationalState opState;
  private long stateExpiryEpochSeconds;

  /**
   * Ctor that creates a SetNodeOperationalStateCommand.
   *
   * @param id    - Command ID. Something a time stamp would suffice.
   * @param state - OperationalState that want the node to be set into.
   * @param stateExpiryEpochSeconds The epoch time when the state should
   *                                expire, or zero for the state to remain
   *                                indefinitely.
   */
  public SetNodeOperationalStateCommand(long id,
      HddsProtos.NodeOperationalState state, long stateExpiryEpochSeconds) {
    super(id);
    this.opState = state;
    this.stateExpiryEpochSeconds = stateExpiryEpochSeconds;
  }

  /**
   * Returns the type of this command.
   *
   * @return Type  - This is setNodeOperationalStateCommand.
   */
  @Override
  public SCMCommandProto.Type getType() {
    return SCMCommandProto.Type.setNodeOperationalStateCommand;
  }

  /**
   * Gets the protobuf message of this object.
   *
   * @return A protobuf message.
   */
  @Override
  public SetNodeOperationalStateCommandProto getProto() {
    return SetNodeOperationalStateCommandProto.newBuilder()
        .setCmdId(getId())
        .setNodeOperationalState(opState)
        .setStateExpiryEpochSeconds(stateExpiryEpochSeconds).build();
  }

  public HddsProtos.NodeOperationalState getOpState() {
    return opState;
  }

  public long getStateExpiryEpochSeconds() {
    return stateExpiryEpochSeconds;
  }

  public static SetNodeOperationalStateCommand getFromProtobuf(
      SetNodeOperationalStateCommandProto cmdProto) {
    Objects.requireNonNull(cmdProto, "cmdProto == null");
    return new SetNodeOperationalStateCommand(cmdProto.getCmdId(),
        cmdProto.getNodeOperationalState(),
        cmdProto.getStateExpiryEpochSeconds());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getType())
        .append(": cmdID: ").append(getId())
        .append(", encodedToken: \"").append(getEncodedToken()).append('"')
        .append(", term: ").append(getTerm())
        .append(", deadlineMsSinceEpoch: ").append(getDeadline())
        .append(", opState: ").append(opState)
        .append(", stateExpiryEpochSeconds: ").append(stateExpiryEpochSeconds);
    return sb.toString();
  }
}
