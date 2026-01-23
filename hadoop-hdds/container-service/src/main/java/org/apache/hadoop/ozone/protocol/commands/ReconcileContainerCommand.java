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

import static java.util.Collections.emptySet;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReconcileContainerCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;

/**
 * Asks datanodes to reconcile the specified container with other container replicas.
 */
public class ReconcileContainerCommand extends SCMCommand<ReconcileContainerCommandProto> {

  private final Set<DatanodeDetails> peerDatanodes;

  public ReconcileContainerCommand(long containerID, Set<DatanodeDetails> peerDatanodes) {
    // Container ID serves as command ID, since only one reconciliation should be in progress at a time.
    super(containerID);
    this.peerDatanodes = peerDatanodes;
  }

  @Override
  public SCMCommandProto.Type getType() {
    return SCMCommandProto.Type.reconcileContainerCommand;
  }

  @Override
  public ReconcileContainerCommandProto getProto() {
    ReconcileContainerCommandProto.Builder builder = ReconcileContainerCommandProto.newBuilder()
        .setContainerID(getId());
    for (DatanodeDetails dd : peerDatanodes) {
      builder.addPeers(dd.getProtoBufMessage());
    }
    return builder.build();
  }

  public Set<DatanodeDetails> getPeerDatanodes() {
    return peerDatanodes;
  }

  public long getContainerID() {
    return getId();
  }

  public static ReconcileContainerCommand getFromProtobuf(ReconcileContainerCommandProto protoMessage) {
    Objects.requireNonNull(protoMessage, "protoMessage == null");

    List<HddsProtos.DatanodeDetailsProto> peers = protoMessage.getPeersList();
    Set<DatanodeDetails> peerNodes = !peers.isEmpty()
        ? peers.stream()
        .map(DatanodeDetails::getFromProtoBuf)
        .collect(Collectors.toSet())
        : emptySet();

    return new ReconcileContainerCommand(protoMessage.getContainerID(), peerNodes);
  }

  @Override
  public String toString() {
    return getType() +
        ": containerId=" + getContainerID() +
        ", peerNodes=" + peerDatanodes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReconcileContainerCommand that = (ReconcileContainerCommand) o;
    return getContainerID() == that.getContainerID();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getContainerID());
  }
}
