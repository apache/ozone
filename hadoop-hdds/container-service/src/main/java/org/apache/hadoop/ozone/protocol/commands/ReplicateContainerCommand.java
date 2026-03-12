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

import static java.util.Collections.emptyList;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicateContainerCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicateContainerCommandProto.Builder;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicationCommandPriority;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;

/**
 * SCM command to request replication of a container.
 */
public final class ReplicateContainerCommand
    extends SCMCommand<ReplicateContainerCommandProto> {

  private final long containerID;
  private final List<DatanodeDetails> sourceDatanodes;
  private final DatanodeDetails targetDatanode;
  private int replicaIndex = 0;
  private ReplicationCommandPriority priority =
      ReplicationCommandPriority.NORMAL;

  public static ReplicateContainerCommand fromSources(long containerID,
      List<DatanodeDetails> sourceDatanodes) {
    return new ReplicateContainerCommand(containerID, sourceDatanodes, null);
  }

  public static ReplicateContainerCommand toTarget(long containerID,
      DatanodeDetails target) {
    return new ReplicateContainerCommand(containerID, emptyList(), target);
  }

  public static ReplicateContainerCommand forTest(long containerID) {
    return new ReplicateContainerCommand(containerID, emptyList(), null);
  }

  private ReplicateContainerCommand(long containerID,
      List<DatanodeDetails> sourceDatanodes, DatanodeDetails target) {
    this.containerID = containerID;
    this.sourceDatanodes = sourceDatanodes;
    this.targetDatanode = target;
  }

  // Should be called only for protobuf conversion
  private ReplicateContainerCommand(long containerID,
      List<DatanodeDetails> sourceDatanodes, long id,
      DatanodeDetails targetDatanode) {
    super(id);
    this.containerID = containerID;
    this.sourceDatanodes = sourceDatanodes;
    this.targetDatanode = targetDatanode;
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
        .setContainerID(containerID);
    for (DatanodeDetails dd : sourceDatanodes) {
      builder.addSources(dd.getProtoBufMessage());
    }
    builder.setReplicaIndex(replicaIndex);
    if (targetDatanode != null) {
      builder.setTarget(targetDatanode.getProtoBufMessage());
    }
    builder.setPriority(priority);
    return builder.build();
  }

  public static ReplicateContainerCommand getFromProtobuf(
      ReplicateContainerCommandProto protoMessage) {
    Objects.requireNonNull(protoMessage, "protoMessage == null");

    List<DatanodeDetailsProto> sources = protoMessage.getSourcesList();
    List<DatanodeDetails> sourceNodes = !sources.isEmpty()
        ? sources.stream()
            .map(DatanodeDetails::getFromProtoBuf)
            .collect(Collectors.toList())
        : emptyList();
    DatanodeDetails targetNode = protoMessage.hasTarget()
        ? DatanodeDetails.getFromProtoBuf(protoMessage.getTarget())
        : null;

    ReplicateContainerCommand cmd =
        new ReplicateContainerCommand(protoMessage.getContainerID(),
            sourceNodes, protoMessage.getCmdId(), targetNode);
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

  public List<DatanodeDetails> getSourceDatanodes() {
    return sourceDatanodes;
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
    StringBuilder sb = new StringBuilder();
    sb.append(getType());
    sb.append(": cmdID: ").append(getId())
        .append(", encodedToken: \"").append(getEncodedToken()).append('"')
        .append(", term: ").append(getTerm())
        .append(", deadlineMsSinceEpoch: ").append(getDeadline());
    sb.append(", containerId=").append(getContainerID());
    sb.append(", replicaIndex=").append(getReplicaIndex());
    if (targetDatanode != null) {
      sb.append(", targetNode=").append(targetDatanode);
    } else {
      sb.append(", sourceNodes=").append(sourceDatanodes);
    }
    sb.append(", priority=").append(priority);
    return sb.toString();
  }
}
