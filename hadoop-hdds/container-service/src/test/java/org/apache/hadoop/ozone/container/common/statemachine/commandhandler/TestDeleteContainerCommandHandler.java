/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.ozone.test.TestClock;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.mockito.Mockito.times;

/**
 * Test for the DeleteContainerCommandHandler.
 */
public class TestDeleteContainerCommandHandler {

  @Test
  public void testExpiredCommandsAreNotProcessed() throws IOException {
    TestClock clock = new TestClock(Instant.now(), ZoneId.systemDefault());
    DeleteContainerCommandHandler handler =
        new DeleteContainerCommandHandler(clock, newDirectExecutorService());
    OzoneContainer ozoneContainer = Mockito.mock(OzoneContainer.class);
    ContainerController controller = Mockito.mock(ContainerController.class);
    Mockito.when(ozoneContainer.getController()).thenReturn(controller);

    DeleteContainerCommand command1 = new DeleteContainerCommand(1L);
    command1.setDeadline(clock.millis() + 10000);
    DeleteContainerCommand command2 = new DeleteContainerCommand(2L);
    command2.setDeadline(clock.millis() + 20000);
    DeleteContainerCommand command3 = new DeleteContainerCommand(3L);
    // No deadline on the 3rd command

    clock.fastForward(15000);
    handler.handle(command1, ozoneContainer, null, null);
    Assertions.assertEquals(1, handler.getTimeoutCount());
    handler.handle(command2, ozoneContainer, null, null);
    handler.handle(command3, ozoneContainer, null, null);
    Assertions.assertEquals(1, handler.getTimeoutCount());
    Assertions.assertEquals(3, handler.getInvocationCount());
    Mockito.verify(controller, times(0))
        .deleteContainer(1L, false);
    Mockito.verify(controller, times(1))
        .deleteContainer(2L, false);
    Mockito.verify(controller, times(1))
        .deleteContainer(3L, false);
  }

}
