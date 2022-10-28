/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.CreatePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Test for the CommandQueue class.
 */

public class TestCommandQueue {

  @Test
  public void testSummaryUpdated() {
    CommandQueue commandQueue = new CommandQueue();
    long containerID = 1;
    SCMCommand<?> closeContainerCommand =
        new CloseContainerCommand(containerID, PipelineID.randomId());
    SCMCommand<?> createPipelineCommand =
        new CreatePipelineCommand(PipelineID.randomId(),
            HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, Collections.emptyList());

    UUID datanode1UUID = UUID.randomUUID();
    UUID datanode2UUID = UUID.randomUUID();

    commandQueue.addCommand(datanode1UUID, closeContainerCommand);
    commandQueue.addCommand(datanode1UUID, closeContainerCommand);
    commandQueue.addCommand(datanode1UUID, createPipelineCommand);

    commandQueue.addCommand(datanode2UUID, closeContainerCommand);
    commandQueue.addCommand(datanode2UUID, createPipelineCommand);
    commandQueue.addCommand(datanode2UUID, createPipelineCommand);

    // Check zero returned for unknown DN
    Assert.assertEquals(0, commandQueue.getDatanodeCommandCount(
        UUID.randomUUID(), SCMCommandProto.Type.closeContainerCommand));

    Assert.assertEquals(2, commandQueue.getDatanodeCommandCount(
        datanode1UUID, SCMCommandProto.Type.closeContainerCommand));
    Assert.assertEquals(1, commandQueue.getDatanodeCommandCount(
        datanode1UUID, SCMCommandProto.Type.createPipelineCommand));
    Assert.assertEquals(0, commandQueue.getDatanodeCommandCount(
        datanode1UUID, SCMCommandProto.Type.closePipelineCommand));

    Assert.assertEquals(1, commandQueue.getDatanodeCommandCount(
        datanode2UUID, SCMCommandProto.Type.closeContainerCommand));
    Assert.assertEquals(2, commandQueue.getDatanodeCommandCount(
        datanode2UUID, SCMCommandProto.Type.createPipelineCommand));

    // Ensure the counts are cleared when the commands are retrieved
    List<SCMCommand> cmds = commandQueue.getCommand(datanode1UUID);
    Assert.assertEquals(3, cmds.size());

    Assert.assertEquals(0, commandQueue.getDatanodeCommandCount(
        datanode1UUID, SCMCommandProto.Type.closeContainerCommand));
    Assert.assertEquals(0, commandQueue.getDatanodeCommandCount(
        datanode1UUID, SCMCommandProto.Type.createPipelineCommand));
    Assert.assertEquals(0, commandQueue.getDatanodeCommandCount(
        datanode1UUID, SCMCommandProto.Type.closePipelineCommand));

    Assert.assertEquals(1, commandQueue.getDatanodeCommandCount(
        datanode2UUID, SCMCommandProto.Type.closeContainerCommand));
    Assert.assertEquals(2, commandQueue.getDatanodeCommandCount(
        datanode2UUID, SCMCommandProto.Type.createPipelineCommand));

    // Ensure the commands are zeroed when the queue is cleared
    commandQueue.clear();
    Assert.assertEquals(0, commandQueue.getDatanodeCommandCount(
        datanode2UUID, SCMCommandProto.Type.closeContainerCommand));
    Assert.assertEquals(0, commandQueue.getDatanodeCommandCount(
        datanode2UUID, SCMCommandProto.Type.createPipelineCommand));
  }

}
