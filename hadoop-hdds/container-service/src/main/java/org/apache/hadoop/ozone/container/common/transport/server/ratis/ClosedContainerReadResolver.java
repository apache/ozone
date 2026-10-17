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

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.ratis.ContainerCommandRequestMessage;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.server.api.DataStreamApi;

/** Resolves group-independent reads against immutable local containers. */
final class ClosedContainerReadResolver implements DataStreamApi.Resolver {
  private final ContainerDispatcher dispatcher;
  private final ContainerController containerController;
  private final String datanodeUuid;

  ClosedContainerReadResolver(ContainerDispatcher dispatcher,
      ContainerController containerController, DatanodeDetails datanode) {
    this.dispatcher = dispatcher;
    this.containerController = containerController;
    this.datanodeUuid = datanode.getUuidString();
  }

  @Override
  public DataStreamApi resolve(RaftClientRequest request)
      throws IOException {
    final ContainerCommandRequestProto requestProto = ContainerCommandRequestMessage.toProto(
        request.getMessage().getContent(), request.getRaftGroupId());
    if (requestProto.getCmdType() != Type.ReadBlock
        || requestProto.hasDatanodeUuid()
            && !datanodeUuid.equals(requestProto.getDatanodeUuid())) {
      return null;
    }

    final Container<?> container =
        containerController.getContainer(requestProto.getContainerID());
    if (container == null || container.getContainerState() != CLOSED) {
      return null;
    }

    return new DataStreamApi() {
      @Override
      public long transferTo(Message ignored, WritableByteChannel stream)
          throws IOException {
        return ContainerStateMachine.streamReadBlock(dispatcher, requestProto, stream);
      }
    };
  }
}
