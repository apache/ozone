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

package org.apache.hadoop.ozone.container;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .DatanodeBlockID;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.ozoneimpl.TestOzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.ozone.container.ozoneimpl.TestOzoneContainer
    .writeChunkForContainer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Tests ozone containers replication.
 */
public class TestContainerReplication {
  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  @Test
  public void testContainerReplication() throws Exception {
    //GIVEN
    OzoneConfiguration conf = newOzoneConfiguration();

    long containerId = 1L;

    MiniOzoneCluster cluster =
        MiniOzoneCluster.newBuilder(conf).setNumDatanodes(2)
            .setRandomContainerPort(true).build();
    cluster.waitForClusterToBeReady();

    HddsDatanodeService firstDatanode = cluster.getHddsDatanodes().get(0);

    //copy from the first datanode
    List<DatanodeDetails> sourceDatanodes = new ArrayList<>();
    sourceDatanodes.add(firstDatanode.getDatanodeDetails());

    Pipeline sourcePipelines =
        ContainerTestHelper.createPipeline(sourceDatanodes);

    //create a new client
    XceiverClientSpi client = new XceiverClientGrpc(sourcePipelines, conf);
    client.connect();

    //New container for testing
    TestOzoneContainer.createContainerForTesting(client, containerId);

    ContainerCommandRequestProto requestProto =
        writeChunkForContainer(client, containerId, 1024);

    DatanodeBlockID blockID = requestProto.getWriteChunk().getBlockID();

    // Put Block to the test container
    ContainerCommandRequestProto putBlockRequest = ContainerTestHelper
        .getPutBlockRequest(sourcePipelines, requestProto.getWriteChunk());

    ContainerCommandResponseProto response =
        client.sendCommand(putBlockRequest);

    Assert.assertNotNull(response);
    Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());

    HddsDatanodeService destinationDatanode =
        chooseDatanodeWithoutContainer(sourcePipelines,
            cluster.getHddsDatanodes());

    // Close the container
    ContainerCommandRequestProto closeContainerRequest = ContainerTestHelper
        .getCloseContainer(sourcePipelines, containerId);
    response = client.sendCommand(closeContainerRequest);
    Assert.assertNotNull(response);
    Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());

    //WHEN: send the order to replicate the container
    cluster.getStorageContainerManager().getScmNodeManager()
        .addDatanodeCommand(destinationDatanode.getDatanodeDetails().getUuid(),
            new ReplicateContainerCommand(containerId,
                sourcePipelines.getNodes()));

    DatanodeStateMachine destinationDatanodeDatanodeStateMachine =
        destinationDatanode.getDatanodeStateMachine();

    //wait for the replication
    GenericTestUtils.waitFor(()
        -> destinationDatanodeDatanodeStateMachine.getSupervisor()
        .getReplicationCounter() > 0, 1000, 20_000);

    OzoneContainer ozoneContainer =
        destinationDatanodeDatanodeStateMachine.getContainer();

    Container container =
        ozoneContainer
            .getContainerSet().getContainer(containerId);

    Assert.assertNotNull(
        "Container is not replicated to the destination datanode",
        container);

    Assert.assertNotNull(
        "ContainerData of the replicated container is null",
        container.getContainerData());

    KeyValueHandler handler = (KeyValueHandler) ozoneContainer.getDispatcher()
        .getHandler(ContainerType.KeyValueContainer);

    BlockData key = handler.getBlockManager()
        .getBlock(container, BlockID.getFromProtobuf(blockID));

    Assert.assertNotNull(key);
    Assert.assertEquals(1, key.getChunks().size());
    Assert.assertEquals(requestProto.getWriteChunk().getChunkData(),
        key.getChunks().get(0));
  }

  private HddsDatanodeService chooseDatanodeWithoutContainer(Pipeline pipeline,
      List<HddsDatanodeService> dataNodes) {
    for (HddsDatanodeService datanode : dataNodes) {
      if (!pipeline.getNodes().contains(datanode.getDatanodeDetails())) {
        return datanode;
      }
    }
    throw new AssertionError(
        "No datanode outside of the pipeline");
  }

  private static OzoneConfiguration newOzoneConfiguration() {
    return new OzoneConfiguration();
  }

}
