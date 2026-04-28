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

package org.apache.hadoop.ozone.container.ozoneimpl;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.OPEN;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.UNHEALTHY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ContainerController}.
 */
public class TestContainerController {

  private ContainerSet containerSet;
  private Handler handler;
  private ContainerController controller;

  @BeforeEach
  public void setup() {
    containerSet = mock(ContainerSet.class);
    handler = mock(Handler.class);

    Map<ContainerProtos.ContainerType, Handler> handlers = new HashMap<>();
    handlers.put(ContainerProtos.ContainerType.KeyValueContainer, handler);

    controller = new ContainerController(containerSet, handlers);
  }

  @Test
  public void testMarkContainerHealthyReturnsFalseWhenNotFound()
      throws IOException {
    final long containerID = 1L;
    when(containerSet.getContainer(containerID)).thenReturn(null);

    assertFalse(controller.markContainerHealthy(containerID));
    verifyNoInteractions(handler);
  }

  @Test
  public void testMarkContainerHealthyReturnsFalseWhenOpen()
      throws Exception {
    final long containerID = 1L;
    Container<?> container = mockContainer(containerID, OPEN);

    assertFalse(controller.markContainerHealthy(containerID));
    verify(handler, never()).markContainerHealthy(container);
  }

  @Test
  public void testMarkContainerHealthyReturnsFalseWhenClosed()
      throws Exception {
    final long containerID = 1L;
    Container<?> container = mockContainer(containerID, CLOSED);

    assertFalse(controller.markContainerHealthy(containerID));
    verify(handler, never()).markContainerHealthy(container);
  }

  @Test
  public void testMarkContainerHealthyPropagatesHandlerSuccess()
      throws Exception {
    final long containerID = 1L;
    Container<?> container = mockContainer(containerID, UNHEALTHY);
    when(handler.markContainerHealthy(container)).thenReturn(true);

    assertTrue(controller.markContainerHealthy(containerID));
    verify(handler).markContainerHealthy(container);
  }

  @Test
  public void testMarkContainerHealthyPropagatesHandlerFailure()
      throws Exception {
    final long containerID = 1L;
    Container<?> container = mockContainer(containerID, UNHEALTHY);
    when(handler.markContainerHealthy(container)).thenReturn(false);

    assertFalse(controller.markContainerHealthy(containerID));
    verify(handler).markContainerHealthy(container);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private Container<?> mockContainer(long containerID,
      ContainerProtos.ContainerDataProto.State state) {
    Container container = mock(Container.class);
    when(containerSet.getContainer(containerID)).thenReturn(container);
    when(container.getContainerType())
        .thenReturn(ContainerProtos.ContainerType.KeyValueContainer);
    when(container.getContainerState()).thenReturn(state);
    return container;
  }
}
