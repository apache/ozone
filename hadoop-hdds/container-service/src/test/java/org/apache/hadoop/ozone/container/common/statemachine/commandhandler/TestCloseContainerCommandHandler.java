/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.RatisHelper;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.SupportedRpcType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

/**
 * Test cases to verify CloseContainerCommandHandler in datanode.
 */
public class TestCloseContainerCommandHandler {

  private static final StateContext CONTEXT = Mockito.mock(StateContext.class);
  private static File testDir;


  private OzoneContainer getOzoneContainer(final OzoneConfiguration conf,
      final DatanodeDetails datanodeDetails) throws IOException {
    testDir = GenericTestUtils.getTestDir(
        TestCloseContainerCommandHandler.class.getName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getPath());
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, testDir.getPath());

    return new OzoneContainer(datanodeDetails, conf, CONTEXT);
  }


  @Test
  public void testCloseContainerViaRatis()
      throws IOException, InterruptedException {
    final OzoneConfiguration conf = new OzoneConfiguration();
    final DatanodeDetails datanodeDetails = randomDatanodeDetails();
    final OzoneContainer container = getOzoneContainer(conf, datanodeDetails);
    container.getDispatcher().setScmId(UUID.randomUUID().toString());
    container.start();
    // Give some time for ratis for leader election.
    final PipelineID pipelineID = PipelineID.randomId();
    final RaftGroupId raftGroupId = RaftGroupId.valueOf(pipelineID.getId());
    final RetryPolicy retryPolicy = RatisHelper.createRetryPolicy(conf);
    final RaftPeer peer = RatisHelper.toRaftPeer(datanodeDetails);
    final RaftGroup group = RatisHelper.newRaftGroup(raftGroupId,
        Collections.singleton(datanodeDetails));
    final RaftClient client = RatisHelper.newRaftClient(
        SupportedRpcType.GRPC, peer, retryPolicy);
    System.out.println(client.groupAdd(group, peer.getId()).isSuccess());
    Thread.sleep(2000);
    final ContainerID containerId = ContainerID.valueof(1);
    ContainerProtos.ContainerCommandRequestProto.Builder request =
        ContainerProtos.ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.CreateContainer);
    request.setContainerID(containerId.getId());
    request.setCreateContainer(
        ContainerProtos.CreateContainerRequestProto.getDefaultInstance());
    request.setTraceID(UUID.randomUUID().toString());
    request.setDatanodeUuid(datanodeDetails.getUuidString());
    container.getWriteChannel().submitRequest(
        request.build(), pipelineID.getProtobuf());

    Assert.assertEquals(ContainerProtos.ContainerDataProto.State.OPEN,
        container.getContainerSet().getContainer(
            containerId.getId()).getContainerState());

    // We have created a container via ratis. Now close the container on ratis.
    final CloseContainerCommandHandler closeHandler =
        new CloseContainerCommandHandler();
    final CloseContainerCommand command = new CloseContainerCommand(
        containerId.getId(), pipelineID);
    final DatanodeStateMachine datanodeStateMachine = Mockito.mock(
        DatanodeStateMachine.class);

    Mockito.when(datanodeStateMachine.getDatanodeDetails())
        .thenReturn(datanodeDetails);
    Mockito.when(CONTEXT.getParent()).thenReturn(datanodeStateMachine);

    closeHandler.handle(command, container, CONTEXT, null);

    Assert.assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED,
        container.getContainerSet().getContainer(
            containerId.getId()).getContainerState());

    Mockito.verify(datanodeStateMachine, Mockito.times(2)).triggerHeartbeat();
    container.stop();
  }

  @Test
  public void testCloseContainerViaStandalone()
      throws IOException, InterruptedException {
    final OzoneConfiguration conf = new OzoneConfiguration();
    final DatanodeDetails datanodeDetails = randomDatanodeDetails();
    final OzoneContainer container = getOzoneContainer(conf, datanodeDetails);
    container.getDispatcher().setScmId(UUID.randomUUID().toString());
    container.start();
    // Give some time for ratis for leader election.
    final PipelineID pipelineID = PipelineID.randomId();
    final RaftGroupId raftGroupId = RaftGroupId.valueOf(pipelineID.getId());
    final RetryPolicy retryPolicy = RatisHelper.createRetryPolicy(conf);
    final RaftPeer peer = RatisHelper.toRaftPeer(datanodeDetails);
    final RaftGroup group = RatisHelper.newRaftGroup(raftGroupId,
        Collections.singleton(datanodeDetails));
    final RaftClient client = RatisHelper.newRaftClient(
        SupportedRpcType.GRPC, peer, retryPolicy);
    System.out.println(client.groupAdd(group, peer.getId()).isSuccess());
    Thread.sleep(2000);
    final ContainerID containerId = ContainerID.valueof(2);
    ContainerProtos.ContainerCommandRequestProto.Builder request =
        ContainerProtos.ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.CreateContainer);
    request.setContainerID(containerId.getId());
    request.setCreateContainer(
        ContainerProtos.CreateContainerRequestProto.getDefaultInstance());
    request.setTraceID(UUID.randomUUID().toString());
    request.setDatanodeUuid(datanodeDetails.getUuidString());
    container.getWriteChannel().submitRequest(
        request.build(), pipelineID.getProtobuf());

    Assert.assertEquals(ContainerProtos.ContainerDataProto.State.OPEN,
        container.getContainerSet().getContainer(
            containerId.getId()).getContainerState());

    // We have created a container via ratis. Now quasi close it
    final CloseContainerCommandHandler closeHandler =
        new CloseContainerCommandHandler();
    // Specify a pipeline which doesn't exist in the datanode.
    final CloseContainerCommand command = new CloseContainerCommand(
        containerId.getId(), PipelineID.randomId());
    final DatanodeStateMachine datanodeStateMachine = Mockito.mock(
        DatanodeStateMachine.class);

    Mockito.when(datanodeStateMachine.getDatanodeDetails())
        .thenReturn(datanodeDetails);
    Mockito.when(CONTEXT.getParent()).thenReturn(datanodeStateMachine);

    closeHandler.handle(command, container, CONTEXT, null);

    Assert.assertEquals(ContainerProtos.ContainerDataProto.State.QUASI_CLOSED,
        container.getContainerSet().getContainer(
            containerId.getId()).getContainerState());

    Mockito.verify(datanodeStateMachine, Mockito.times(2)).triggerHeartbeat();
    container.stop();
  }

  /**
   * Creates a random DatanodeDetails.
   * @return DatanodeDetails
   */
  private static DatanodeDetails randomDatanodeDetails() {
    String ipAddress = "127.0.0.1";
    DatanodeDetails.Port containerPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.STANDALONE, 0);
    DatanodeDetails.Port ratisPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.RATIS, 0);
    DatanodeDetails.Port restPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.REST, 0);
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(UUID.randomUUID().toString())
        .setHostName("localhost")
        .setIpAddress(ipAddress)
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort);
    return builder.build();
  }

  @AfterClass
  public static void teardown() throws IOException {
    FileUtils.deleteDirectory(testDir);
  }
}