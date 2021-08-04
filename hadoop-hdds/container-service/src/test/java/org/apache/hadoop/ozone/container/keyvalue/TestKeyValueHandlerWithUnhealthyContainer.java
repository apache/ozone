/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.keyvalue;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.TestHddsDispatcher;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;

import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_UNHEALTHY;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.SUCCESS;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNKNOWN_BCSID;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.DATANODE_UUID;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getDummyCommandRequestProto;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test that KeyValueHandler fails certain operations when the
 * container is unhealthy.
 */
public class TestKeyValueHandlerWithUnhealthyContainer {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestKeyValueHandlerWithUnhealthyContainer.class);

  @Test
  public void testRead() throws IOException {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleReadContainer(
            getDummyCommandRequestProto(ContainerProtos.Type.ReadContainer),
            container);
    assertThat(response.getResult(), is(SUCCESS));
  }

  @Test
  public void testGetBlock() throws IOException {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleGetBlock(
            getDummyCommandRequestProto(ContainerProtos.Type.GetBlock),
            container);
    assertThat(response.getResult(), is(UNKNOWN_BCSID));
  }

  @Test
  public void testGetCommittedBlockLength() throws IOException {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleGetCommittedBlockLength(
            getDummyCommandRequestProto(
                ContainerProtos.Type.GetCommittedBlockLength),
            container);
    assertThat(response.getResult(), is(UNKNOWN_BCSID));
  }

  @Test
  public void testReadChunk() throws IOException {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleReadChunk(
            getDummyCommandRequestProto(
                ContainerProtos.Type.ReadChunk),
            container, null);
    assertThat(response.getResult(), is(UNKNOWN_BCSID));
  }

  @Test
  public void testDeleteChunk() throws IOException {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleDeleteChunk(
            getDummyCommandRequestProto(
                ContainerProtos.Type.DeleteChunk),
            container);
    assertThat(response.getResult(), is(CONTAINER_UNHEALTHY));
  }

  @Test
  public void testGetSmallFile() throws IOException {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleGetSmallFile(
            getDummyCommandRequestProto(
                ContainerProtos.Type.GetSmallFile),
            container);
    assertThat(response.getResult(), is(UNKNOWN_BCSID));
  }

  // -- Helper methods below.

  private KeyValueHandler getDummyHandler() throws IOException {
    DatanodeDetails dnDetails = DatanodeDetails.newBuilder()
        .setUuid(UUID.fromString(DATANODE_UUID))
        .setHostName("dummyHost")
        .setIpAddress("1.2.3.4")
        .build();
    DatanodeStateMachine stateMachine = mock(DatanodeStateMachine.class);
    when(stateMachine.getDatanodeDetails()).thenReturn(dnDetails);

    return new KeyValueHandler(
        new OzoneConfiguration(),
        stateMachine.getDatanodeDetails().getUuidString(),
        mock(ContainerSet.class),
        mock(MutableVolumeSet.class),
        mock(ContainerMetrics.class), TestHddsDispatcher.NO_OP_ICR_SENDER);
  }

  private KeyValueContainer getMockUnhealthyContainer() {
    KeyValueContainerData containerData = mock(KeyValueContainerData.class);
    when(containerData.getState()).thenReturn(
        ContainerProtos.ContainerDataProto.State.UNHEALTHY);
    when(containerData.getBlockCommitSequenceId()).thenReturn(100L);
    when(containerData.getProtoBufMessage()).thenReturn(ContainerProtos
        .ContainerDataProto.newBuilder().setContainerID(1).build());
    return new KeyValueContainer(containerData, new OzoneConfiguration());
  }

}
