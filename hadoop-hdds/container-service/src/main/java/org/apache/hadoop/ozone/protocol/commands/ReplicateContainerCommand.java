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
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicateContainerCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicateContainerCommandProto.Builder;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicationCommandPriority;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;

/**
 * SCM command to request push-replication of a container to a target datanode.
 */
public final class ReplicateContainerCommand
    extends SCMCommand<ReplicateContainerCommandProto> {

  private final long containerID;
  private final DatanodeDetails targetDatanode;
  private int replicaIndex = 0;
  private ReplicationCommandPriority priority =
      ReplicationCommandPriority.NORMAL;

  public static ReplicateContainerCommand toTarget(long containerID,
      DatanodeDetails target) {
    return new ReplicateContainerCommand(containerID, target);
  }

  private ReplicateContainerCommand(long containerID, DatanodeDetails target) {
    this.containerID = containerID;
    this.targetDatanode = Objects.requireNonNull(target, "target == null");
  }

  // Should be called only for protobuf conversion
  private ReplicateContainerCommand(long containerID, long id,
      DatanodeDetails targetDatanode) {
    super(id);
    this.containerID = containerID;
    this.targetDatanode = Objects.requireNonNull(targetDatanode, "target == null");
  }

  public void setReplicaIndex(int index) {
    replicaIndex = index;
  }

  public void setPriority(ReplicationCommandPriority priority) {
    this.priority = priority;
  }

  @Override
  public Type getType() {
    return SCMCommandProto.Type.replicateContainerCommand;
  }

  @Override
  public boolean contributesToQueueSize() {
    return priority == ReplicationCommandPriority.NORMAL;
  }

  @Override
  public ReplicateContainerCommandProto getProto() {
    Builder builder = ReplicateContainerCommandProto.newBuilder()
        .setCmdId(getId())
        .setContainerID(containerID)
        .setReplicaIndex(replicaIndex)
        .setTarget(targetDatanode.getProtoBufMessage())
        .setPriority(priority);
    return builder.build();
  }

  public static ReplicateContainerCommand getFromProtobuf(
      ReplicateContainerCommandProto protoMessage) {
    Objects.requireNonNull(protoMessage, "protoMessage == null");

    DatanodeDetails targetNode =
        DatanodeDetails.getFromProtoBuf(protoMessage.getTarget());

    ReplicateContainerCommand cmd =
        new ReplicateContainerCommand(protoMessage.getContainerID(),
            protoMessage.getCmdId(), targetNode);
    if (protoMessage.hasReplicaIndex()) {
      cmd.setReplicaIndex(protoMessage.getReplicaIndex());
    }
    if (protoMessage.hasPriority()) {
      cmd.setPriority(protoMessage.getPriority());
    }
    return cmd;
  }

  public long getContainerID() {
    return containerID;
  }

  public DatanodeDetails getTargetDatanode() {
    return targetDatanode;
  }

  public int getReplicaIndex() {
    return replicaIndex;
  }

  public ReplicationCommandPriority getPriority() {
    return priority;
  }

  @Override
  public String toString() {
    return getType()
        + ": cmdID: " + getId()
        + ", encodedToken: \"" + getEncodedToken() + '"'
        + ", term: " + getTerm()
        + ", deadlineMsSinceEpoch: " + getDeadline()
        + ", containerId=" + getContainerID()
        + ", replicaIndex=" + getReplicaIndex()
        + ", targetNode=" + targetDatanode
        + ", priority=" + priority;
  }
}
