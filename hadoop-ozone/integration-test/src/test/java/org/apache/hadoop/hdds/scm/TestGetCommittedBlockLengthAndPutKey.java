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

package org.apache.hadoop.hdds.scm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Container calls.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestGetCommittedBlockLengthAndPutKey implements NonHATests.TestCase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestGetCommittedBlockLengthAndPutKey.class);
  private OzoneConfiguration ozoneConfig;
  private StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private XceiverClientManager xceiverClientManager;

  @BeforeAll
  void init() throws Exception {
    ozoneConfig = cluster().getConf();
    storageContainerLocationClient =
        cluster().getStorageContainerLocationClient();
    xceiverClientManager = new XceiverClientManager(ozoneConfig);
  }

  @AfterAll
  void cleanup() {
    IOUtils.cleanupWithLogger(null, storageContainerLocationClient);
  }

  @Test
  public void tesGetCommittedBlockLength() throws Exception {
    final AtomicReference<ContainerProtos.GetCommittedBlockLengthResponseProto>
        response = new AtomicReference<>();
    ContainerWithPipeline container = storageContainerLocationClient
        .allocateContainer(SCMTestUtils.getReplicationType(ozoneConfig),
            HddsProtos.ReplicationFactor.ONE, OzoneConsts.OZONE);
    long containerID = container.getContainerInfo().getContainerID();
    Pipeline pipeline = container.getPipeline();
    XceiverClientSpi client = xceiverClientManager.acquireClient(pipeline);
    //create the container
    ContainerProtocolCalls.createContainer(client, containerID, null);

    BlockID blockID = ContainerTestHelper.getTestBlockID(containerID);
    byte[] data =
        RandomStringUtils.secure().next(RandomUtils.secure().randomInt(1, 1024)).getBytes(UTF_8);
    ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
        ContainerTestHelper
            .getWriteChunkRequest(container.getPipeline(), blockID,
                data.length);
    client.sendCommand(writeChunkRequest);
    // Now, explicitly make a putKey request for the block.
    ContainerProtos.ContainerCommandRequestProto putKeyRequest =
        ContainerTestHelper
            .getPutBlockRequest(pipeline, writeChunkRequest.getWriteChunk());
    client.sendCommand(putKeyRequest);
    GenericTestUtils.waitFor(() -> {
      try {
        response.set(ContainerProtocolCalls
            .getCommittedBlockLength(client, blockID, null));
      } catch (IOException e) {
        LOG.debug("Ignore the exception till wait: {}", e.getMessage());
        return false;
      }
      return true;
    }, 500, 5000);
    // make sure the block ids in the request and response are same.
    assertEquals(blockID, BlockID.getFromProtobuf(response.get().getBlockID()));
    assertEquals(data.length, response.get().getBlockLength());
    xceiverClientManager.releaseClient(client, false);
  }

  @Test
  public void testGetCommittedBlockLengthForInvalidBlock() throws Exception {
    ContainerWithPipeline container = storageContainerLocationClient
        .allocateContainer(SCMTestUtils.getReplicationType(ozoneConfig),
            HddsProtos.ReplicationFactor.ONE, OzoneConsts.OZONE);
    long containerID = container.getContainerInfo().getContainerID();
    XceiverClientSpi client = xceiverClientManager
        .acquireClient(container.getPipeline());
    ContainerProtocolCalls.createContainer(client, containerID, null);

    BlockID blockID = ContainerTestHelper.getTestBlockID(containerID);
    // move the container to closed state
    ContainerProtocolCalls.closeContainer(client, containerID, null);

    // There is no block written inside the container. The request should fail.
    Throwable t = assertThrows(StorageContainerException.class,
        () -> ContainerProtocolCalls.getCommittedBlockLength(client, blockID,
            null));
    assertThat(t.getMessage()).contains("Unable to find the block");

    xceiverClientManager.releaseClient(client, false);
  }

  @Test
  public void tesPutKeyResposne() throws Exception {
    ContainerProtos.PutBlockResponseProto response;
    ContainerWithPipeline container = storageContainerLocationClient
        .allocateContainer(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.ONE, OzoneConsts.OZONE);
    long containerID = container.getContainerInfo().getContainerID();
    Pipeline pipeline = container.getPipeline();
    XceiverClientSpi client = xceiverClientManager.acquireClient(pipeline);
    //create the container
    ContainerProtocolCalls.createContainer(client, containerID, null);

    BlockID blockID = ContainerTestHelper.getTestBlockID(containerID);
    byte[] data =
        RandomStringUtils.secure().next(RandomUtils.secure().randomInt(1, 1024)).getBytes(UTF_8);
    ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
        ContainerTestHelper
            .getWriteChunkRequest(container.getPipeline(), blockID,
                data.length);
    client.sendCommand(writeChunkRequest);
    // Now, explicitly make a putKey request for the block.
    ContainerProtos.ContainerCommandRequestProto putKeyRequest =
        ContainerTestHelper
            .getPutBlockRequest(pipeline, writeChunkRequest.getWriteChunk());
    response = client.sendCommand(putKeyRequest).getPutBlock();
    assertEquals(response.getCommittedBlockLength().getBlockLength(), data.length);
    assertThat(response.getCommittedBlockLength().getBlockID().getBlockCommitSequenceId())
        .isGreaterThan(0);
    BlockID responseBlockID = BlockID
        .getFromProtobuf(response.getCommittedBlockLength().getBlockID());
    blockID
        .setBlockCommitSequenceId(responseBlockID.getBlockCommitSequenceId());
    // make sure the block ids in the request and response are same.
    // This will also ensure that closing the container committed the block
    // on the Datanodes.
    assertEquals(responseBlockID, blockID);
    xceiverClientManager.releaseClient(client, false);
  }
}
