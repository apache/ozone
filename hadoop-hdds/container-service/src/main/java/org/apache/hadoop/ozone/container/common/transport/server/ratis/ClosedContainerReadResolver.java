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

package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import java.io.IOException;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.ratis.ContainerCommandRequestMessage;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.server.api.DataStreamApi;

/**
 * Serves ReadBlock on a Ratis read-only data stream, without a Raft group, for a container that is not open or closing.
 * SCM gives the read pipeline of such a container a random ID, and no Raft group has this ID.
 * The container no longer changes through Raft, so this datanode can serve the read from its replica.
 */
final class ClosedContainerReadResolver implements DataStreamApi.Resolver {
  private final ContainerDispatcher dispatcher;
  private final ContainerController containerController;
  private final String datanodeUuid;

  ClosedContainerReadResolver(ContainerDispatcher dispatcher, ContainerController containerController,
      DatanodeDetails datanode) {
    this.dispatcher = dispatcher;
    this.containerController = containerController;
    this.datanodeUuid = datanode.getUuidString();
  }

  /**
   * @return the API to serve a ReadBlock of a container on this datanode that is not open or closing. Otherwise, null:
   *     Ratis then serves the request with its Raft group.
   */
  @Override
  public DataStreamApi resolve(RaftClientRequest request) throws IOException {
    final ContainerCommandRequestProto requestProto = ContainerCommandRequestMessage.toProto(
        request.getMessage().getContent(), request.getRaftGroupId());
    if (requestProto.getCmdType() != Type.ReadBlock
        || requestProto.hasDatanodeUuid() && !datanodeUuid.equals(requestProto.getDatanodeUuid())) {
      return null;
    }

    final Container<?> container = containerController.getContainer(requestProto.getContainerID());
    // Like SCM, which keeps the pipeline with the Raft group for a container that is open or closing
    if (container == null || container.getContainerData().isOpen() || container.getContainerData().isClosing()) {
      return null;
    }
    return (message, stream) -> ContainerStateMachine.streamReadBlock(dispatcher, requestProto, stream);
  }
}
