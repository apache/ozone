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

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import static java.util.Collections.singletonMap;
import static org.apache.hadoop.ozone.OzoneConsts.GB;
import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.container.keyvalue.ContainerLayoutTestInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;

/**
 * Test cases to verify CloseContainerCommandHandler in datanode.
 */
public class TestCloseContainerCommandHandler {

  private static final long CONTAINER_ID = 123L;

  private OzoneContainer ozoneContainer;
  private StateContext context;
  private XceiverServerSpi writeChannel;
  private Container container;
  private Handler containerHandler;
  private PipelineID pipelineID;
  private PipelineID nonExistentPipelineID = PipelineID.randomId();
  private ContainerController controller;
  private ContainerSet containerSet;
  private CloseContainerCommandHandler subject =
      new CloseContainerCommandHandler(1, 1000, "");

  private ContainerLayoutVersion layoutVersion;

  public void initLayoutVersion(ContainerLayoutVersion layout)
      throws Exception {
    this.layoutVersion = layout;
    init();
  }

  private void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    context = ContainerTestUtils.getMockContext(randomDatanodeDetails(), conf);
    pipelineID = PipelineID.randomId();

    KeyValueContainerData data = new KeyValueContainerData(CONTAINER_ID,
        layoutVersion, GB,
        pipelineID.getId().toString(), null);

    container = new KeyValueContainer(data, conf);
    containerSet = newContainerSet();
    containerSet.addContainer(container);

    containerHandler = mock(Handler.class);
    controller = new ContainerController(containerSet,
        singletonMap(ContainerProtos.ContainerType.KeyValueContainer,
            containerHandler));

    writeChannel = mock(XceiverServerSpi.class);
    ozoneContainer = mock(OzoneContainer.class);
    when(ozoneContainer.getController()).thenReturn(controller);
    when(ozoneContainer.getContainerSet()).thenReturn(containerSet);
    when(ozoneContainer.getWriteChannel()).thenReturn(writeChannel);
    when(writeChannel.isExist(pipelineID.getProtobuf())).thenReturn(true);
    when(writeChannel.isExist(nonExistentPipelineID.getProtobuf()))
        .thenReturn(false);
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void closeContainerWithPipeline(ContainerLayoutVersion layout)
      throws Exception {
    initLayoutVersion(layout);
    // close a container that's associated with an existing pipeline
    subject.handle(closeWithKnownPipeline(), ozoneContainer, context, null);
    waitTillFinishExecution(subject);
    verify(containerHandler)
        .markContainerForClose(container);
    verify(writeChannel)
        .submitRequest(any(), eq(pipelineID.getProtobuf()));
    verify(containerHandler, never())
        .quasiCloseContainer(eq(container), any());
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void closeContainerWithoutPipeline(ContainerLayoutVersion layout)
      throws Exception {
    initLayoutVersion(layout);
    // close a container that's NOT associated with an open pipeline
    subject.handle(closeWithUnknownPipeline(), ozoneContainer, context, null);
    waitTillFinishExecution(subject);

    verify(containerHandler)
        .markContainerForClose(container);
    verify(writeChannel, never())
        .submitRequest(any(), any());
    // Container in CLOSING state is moved to UNHEALTHY if pipeline does not
    // exist. Container should not exist in CLOSING state without a pipeline.
    verify(containerHandler)
        .quasiCloseContainer(eq(container), any());
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void closeContainerWithForceFlagSet(ContainerLayoutVersion layout)
      throws Exception {
    initLayoutVersion(layout);
    // close a container that's associated with an existing pipeline
    subject.handle(forceCloseWithoutPipeline(), ozoneContainer, context, null);
    waitTillFinishExecution(subject);

    verify(containerHandler)
        .markContainerForClose(container);
    verify(writeChannel, never()).submitRequest(any(), any());
    verify(containerHandler).closeContainer(container);
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void forceCloseQuasiClosedContainer(ContainerLayoutVersion layout)
      throws Exception {
    initLayoutVersion(layout);
    // force-close a container that's already quasi closed
    container.getContainerData()
        .setState(ContainerProtos.ContainerDataProto.State.QUASI_CLOSED);

    subject.handle(forceCloseWithoutPipeline(), ozoneContainer, context, null);
    waitTillFinishExecution(subject);

    verify(writeChannel, never())
        .submitRequest(any(), any());
    verify(containerHandler)
        .closeContainer(container);
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void forceCloseOpenContainer(ContainerLayoutVersion layout)
      throws Exception {
    initLayoutVersion(layout);
    // force-close a container that's NOT associated with an open pipeline
    subject.handle(forceCloseWithoutPipeline(), ozoneContainer, context, null);
    waitTillFinishExecution(subject);

    verify(writeChannel, never())
        .submitRequest(any(), any());
    // Container in CLOSING state is moved to CLOSED if pipeline does not
    // exist and force is set to TRUE.
    verify(containerHandler)
        .closeContainer(container);
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void forceCloseOpenContainerWithPipeline(ContainerLayoutVersion layout)
      throws Exception {
    initLayoutVersion(layout);
    // force-close a container that's associated with an existing pipeline
    subject.handle(forceCloseWithPipeline(), ozoneContainer, context, null);
    waitTillFinishExecution(subject);

    verify(containerHandler)
        .markContainerForClose(container);
    verify(writeChannel)
        .submitRequest(any(), any());
    verify(containerHandler, never())
        .quasiCloseContainer(eq(container), any());
    verify(containerHandler, never())
        .closeContainer(container);
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void closeAlreadyClosedContainer(ContainerLayoutVersion layout)
      throws Exception {
    initLayoutVersion(layout);
    container.getContainerData()
        .setState(ContainerProtos.ContainerDataProto.State.CLOSED);

    // Since the container is already closed, these commands should do nothing,
    // neither should they fail
    subject.handle(closeWithUnknownPipeline(), ozoneContainer, context, null);
    waitTillFinishExecution(subject);
    subject.handle(closeWithKnownPipeline(), ozoneContainer, context, null);
    waitTillFinishExecution(subject);

    verify(containerHandler, never())
        .markContainerForClose(container);
    verify(containerHandler, never())
        .quasiCloseContainer(eq(container), any());
    verify(containerHandler, never())
        .closeContainer(container);
    verify(writeChannel, never())
        .submitRequest(any(), any());
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void closeNonExistenceContainer(ContainerLayoutVersion layout) throws Exception {
    initLayoutVersion(layout);
    long containerID = 1L;

    Exception e = assertThrows(ContainerNotFoundException.class, () -> controller.markContainerForClose(containerID));
    assertThat(e).hasMessageContaining(" " + ContainerID.valueOf(containerID) + " ");
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void closeMissingContainer(ContainerLayoutVersion layout)
      throws Exception {
    initLayoutVersion(layout);
    long containerID = 2L;
    containerSet.getMissingContainerSet().add(containerID);

    Exception e = assertThrows(ContainerNotFoundException.class, () -> controller.markContainerForClose(containerID));
    assertThat(e).hasMessageContaining(" " + ContainerID.valueOf(containerID) + " ");
  }

  private CloseContainerCommand closeWithKnownPipeline() {
    return new CloseContainerCommand(CONTAINER_ID, pipelineID);
  }

  private CloseContainerCommand closeWithUnknownPipeline() {
    return new CloseContainerCommand(CONTAINER_ID, nonExistentPipelineID);
  }

  private CloseContainerCommand forceCloseWithPipeline() {
    return new CloseContainerCommand(CONTAINER_ID, pipelineID, true);
  }

  private CloseContainerCommand forceCloseWithoutPipeline() {
    return new CloseContainerCommand(CONTAINER_ID, nonExistentPipelineID, true);
  }

  /**
   * Creates a random DatanodeDetails.
   * @return DatanodeDetails
   */
  private static DatanodeDetails randomDatanodeDetails() {
    String ipAddress = "127.0.0.1";
    DatanodeDetails.Port containerPort = DatanodeDetails.newStandalonePort(0);
    DatanodeDetails.Port ratisPort = DatanodeDetails.newRatisPort(0);
    DatanodeDetails.Port restPort = DatanodeDetails.newRestPort(0);
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(UUID.randomUUID())
        .setHostName("localhost")
        .setIpAddress(ipAddress)
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort);
    return builder.build();
  }

  private void waitTillFinishExecution(
      CloseContainerCommandHandler closeHandler)
      throws InterruptedException, TimeoutException {
    GenericTestUtils.waitFor(()
        -> closeHandler.getQueuedCount() <= 0, 10, 3000);
  }

  @Test
  public void testThreadPoolPoolSize() {
    assertEquals(1, subject.getThreadPoolMaxPoolSize());
  }

}
