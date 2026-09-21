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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.ratis.ContainerCommandRequestMessage;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TestClosedContainerReadResolver {
  private static final long CONTAINER_ID = 1L;

  private ContainerController containerController;
  private Container<?> container;
  private ClosedContainerReadResolver resolver;
  private String datanodeUuid;

  @BeforeEach
  void setUp() {
    datanodeUuid = UUID.randomUUID().toString();
    final DatanodeDetails datanode = mock(DatanodeDetails.class);
    when(datanode.getUuidString()).thenReturn(datanodeUuid);
    containerController = mock(ContainerController.class);
    container = mock(Container.class);
    when(containerController.getContainer(CONTAINER_ID)).thenReturn(container);
    resolver = new ClosedContainerReadResolver(
        mock(ContainerDispatcher.class), containerController, datanode);
  }

  @Test
  void resolvesReadBlockForClosedContainer() throws IOException {
    when(container.getContainerState()).thenReturn(State.CLOSED);

    assertNotNull(resolver.resolve(newRequest(Type.ReadBlock, datanodeUuid)));
  }

  @ParameterizedTest
  @EnumSource(names = {"OPEN", "CLOSING", "QUASI_CLOSED", "UNHEALTHY",
      "INVALID", "DELETED", "RECOVERING"})
  void declinesReadBlockForContainerInOtherState(State state)
      throws IOException {
    when(container.getContainerState()).thenReturn(state);

    assertNull(resolver.resolve(newRequest(Type.ReadBlock, datanodeUuid)));
  }

  @Test
  void declinesReadBlockForMissingContainer() throws IOException {
    when(containerController.getContainer(CONTAINER_ID)).thenReturn(null);

    assertNull(resolver.resolve(newRequest(Type.ReadBlock, datanodeUuid)));
  }

  @Test
  void declinesOtherCommandForClosedContainer() throws IOException {
    when(container.getContainerState()).thenReturn(State.CLOSED);

    assertNull(resolver.resolve(newRequest(Type.GetBlock, datanodeUuid)));
  }

  @Test
  void declinesRequestForAnotherDatanode() throws IOException {
    when(container.getContainerState()).thenReturn(State.CLOSED);

    assertNull(resolver.resolve(
        newRequest(Type.ReadBlock, UUID.randomUUID().toString())));
  }

  private static RaftClientRequest newRequest(Type type, String targetUuid) {
    final ContainerCommandRequestProto proto =
        ContainerCommandRequestProto.newBuilder()
            .setCmdType(type)
            .setContainerID(CONTAINER_ID)
            .setDatanodeUuid(targetUuid)
            .build();
    final RaftClientRequest request = mock(RaftClientRequest.class);
    when(request.getMessage()).thenReturn(
        ContainerCommandRequestMessage.toMessage(proto, null));
    when(request.getRaftGroupId()).thenReturn(RaftGroupId.randomId());
    return request;
  }
}
