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
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeleteContainerCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;

/**
 * SCM command which tells the datanode to delete a container.
 */
public class DeleteContainerCommand extends
    SCMCommand<DeleteContainerCommandProto> {

  private final long containerId;
  private final boolean force;
  private int replicaIndex = 0;

  /**
   * DeleteContainerCommand, to send a command for datanode to delete a
   * container.
   * @param containerId
   */
  public DeleteContainerCommand(long containerId) {
    this(containerId, false);
  }

  /**
   * DeleteContainerCommand, to send a command for datanode to delete a
   * container.
   * @param containerId
   * @param forceFlag if this is set to true, we delete container without
   * checking state of the container.
   */

  public DeleteContainerCommand(long containerId, boolean forceFlag) {
    this.containerId = containerId;
    this.force = forceFlag;
  }

  public DeleteContainerCommand(ContainerID containerID, boolean forceFlag) {
    this.containerId = containerID.getId();
    this.force = forceFlag;
  }

  public void setReplicaIndex(int index) {
    replicaIndex = index;
  }

  @Override
  public SCMCommandProto.Type getType() {
    return SCMCommandProto.Type.deleteContainerCommand;
  }

  @Override
  public DeleteContainerCommandProto getProto() {
    DeleteContainerCommandProto.Builder builder =
        DeleteContainerCommandProto.newBuilder();
    builder.setCmdId(getId())
        .setContainerID(getContainerID()).setForce(force);
    builder.setReplicaIndex(replicaIndex);
    return builder.build();
  }

  public long getContainerID() {
    return containerId;
  }

  public boolean isForce() {
    return force;
  }

  public static DeleteContainerCommand getFromProtobuf(
      DeleteContainerCommandProto protoMessage) {
    Objects.requireNonNull(protoMessage, "protoMessage == null");

    DeleteContainerCommand cmd =
        new DeleteContainerCommand(protoMessage.getContainerID(),
            protoMessage.getForce());
    if (protoMessage.hasReplicaIndex()) {
      cmd.setReplicaIndex(protoMessage.getReplicaIndex());
    }
    return cmd;
  }

  public int getReplicaIndex() {
    return replicaIndex;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getType())
        .append(": cmdID: ").append(getId())
        .append(", encodedToken: \"").append(getEncodedToken()).append('"')
        .append(", term: ").append(getTerm())
        .append(", deadlineMsSinceEpoch: ").append(getDeadline())
        .append(", containerID: ").append(getContainerID())
        .append(", replicaIndex: ").append(getReplicaIndex())
        .append(", force: ").append(force);
    return sb.toString();
  }
}
