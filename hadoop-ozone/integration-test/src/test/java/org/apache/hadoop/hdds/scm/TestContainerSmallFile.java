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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.NonHATests;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Test Container calls.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestContainerSmallFile implements NonHATests.TestCase {

  private OzoneConfiguration ozoneConfig;
  private StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private XceiverClientManager xceiverClientManager;

  @BeforeAll
  void init() throws Exception {
    ozoneConfig = cluster().getConf();
    storageContainerLocationClient = cluster()
        .getStorageContainerLocationClient();
    xceiverClientManager = new XceiverClientManager(ozoneConfig);
  }

  @AfterAll
  void cleanup() {
    IOUtils.cleanupWithLogger(null, storageContainerLocationClient);
  }

  @Test
  public void testAllocateWrite() throws Exception {
    ContainerWithPipeline container =
        storageContainerLocationClient.allocateContainer(
            SCMTestUtils.getReplicationType(ozoneConfig),
            HddsProtos.ReplicationFactor.ONE, OzoneConsts.OZONE);
    XceiverClientSpi client = xceiverClientManager
        .acquireClient(container.getPipeline());
    ContainerProtocolCalls.createContainer(client,
        container.getContainerInfo().getContainerID(), null);

    BlockID blockID = ContainerTestHelper.getTestBlockID(
        container.getContainerInfo().getContainerID());
    ContainerProtocolCalls.writeSmallFile(client, blockID,
        "data123".getBytes(UTF_8), null);
    ContainerProtos.GetSmallFileResponseProto response =
        ContainerProtocolCalls.readSmallFile(client, blockID, null);
    String readData = response.getData().getDataBuffers().getBuffersList()
        .get(0).toStringUtf8();
    assertEquals("data123", readData);
    xceiverClientManager.releaseClient(client, false);
  }

  @Test
  public void testInvalidBlockRead() throws Exception {
    ContainerWithPipeline container =
        storageContainerLocationClient.allocateContainer(
            SCMTestUtils.getReplicationType(ozoneConfig),
            HddsProtos.ReplicationFactor.ONE, OzoneConsts.OZONE);
    XceiverClientSpi client = xceiverClientManager
        .acquireClient(container.getPipeline());
    ContainerProtocolCalls.createContainer(client,
        container.getContainerInfo().getContainerID(), null);

    BlockID blockID = ContainerTestHelper.getTestBlockID(
        container.getContainerInfo().getContainerID());
    // Try to read a Key Container Name
    assertThrowsExactly(StorageContainerException.class,
        () -> ContainerProtocolCalls.readSmallFile(client, blockID, null),
        "Unable to find the block");
    xceiverClientManager.releaseClient(client, false);
  }

  @Test
  public void testInvalidContainerRead() throws Exception {
    long nonExistContainerID = 8888L;
    ContainerWithPipeline container =
        storageContainerLocationClient.allocateContainer(
            SCMTestUtils.getReplicationType(ozoneConfig),
            HddsProtos.ReplicationFactor.ONE, OzoneConsts.OZONE);
    XceiverClientSpi client = xceiverClientManager
        .acquireClient(container.getPipeline());
    ContainerProtocolCalls.createContainer(client,
        container.getContainerInfo().getContainerID(), null);
    BlockID blockID = ContainerTestHelper.getTestBlockID(
        container.getContainerInfo().getContainerID());
    ContainerProtocolCalls.writeSmallFile(client, blockID,
        "data123".getBytes(UTF_8), null);

    assertThrowsExactly(StorageContainerException.class,
        () -> ContainerProtocolCalls.readSmallFile(client,
            ContainerTestHelper.getTestBlockID(nonExistContainerID),
            null),
        "ContainerID 8888 does not exist");
    xceiverClientManager.releaseClient(client, false);
  }

  @Test
  public void testReadWriteWithBCSId() throws Exception {
    ContainerWithPipeline container =
        storageContainerLocationClient.allocateContainer(
            HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.ONE, OzoneConsts.OZONE);
    XceiverClientSpi client = xceiverClientManager
        .acquireClient(container.getPipeline());
    ContainerProtocolCalls.createContainer(client,
        container.getContainerInfo().getContainerID(), null);

    BlockID blockID1 = ContainerTestHelper.getTestBlockID(
        container.getContainerInfo().getContainerID());
    ContainerProtos.PutSmallFileResponseProto responseProto =
        ContainerProtocolCalls
            .writeSmallFile(client, blockID1, "data123".getBytes(UTF_8), null);
    long bcsId = responseProto.getCommittedBlockLength().getBlockID()
        .getBlockCommitSequenceId();

    blockID1.setBlockCommitSequenceId(bcsId + 1);
    //read a file with higher bcsId than the container bcsId
    StorageContainerException sce =
        assertThrows(StorageContainerException.class, () ->
            ContainerProtocolCalls.readSmallFile(client, blockID1, null));
    assertSame(ContainerProtos.Result.UNKNOWN_BCSID, sce.getResult());

    // write a new block again to bump up the container bcsId
    BlockID blockID2 = ContainerTestHelper
        .getTestBlockID(container.getContainerInfo().getContainerID());
    ContainerProtocolCalls
        .writeSmallFile(client, blockID2, "data123".getBytes(UTF_8), null);

    blockID1.setBlockCommitSequenceId(bcsId + 1);
    //read a file with higher bcsId than the committed bcsId for the block
    sce = assertThrows(StorageContainerException.class, () ->
        ContainerProtocolCalls.readSmallFile(client, blockID1, null));
    assertSame(ContainerProtos.Result.BCSID_MISMATCH, sce.getResult());

    blockID1.setBlockCommitSequenceId(bcsId);
    ContainerProtos.GetSmallFileResponseProto response =
        ContainerProtocolCalls.readSmallFile(client, blockID1, null);
    String readData = response.getData().getDataBuffers().getBuffersList()
        .get(0).toStringUtf8();
    assertEquals("data123", readData);
    xceiverClientManager.releaseClient(client, false);
  }

  @Test
  public void testEcho() throws Exception {
    ContainerWithPipeline container =
        storageContainerLocationClient.allocateContainer(
            SCMTestUtils.getReplicationType(ozoneConfig),
            HddsProtos.ReplicationFactor.ONE, OzoneConsts.OZONE);
    XceiverClientSpi client = xceiverClientManager
        .acquireClient(container.getPipeline());
    ContainerProtocolCalls.createContainer(client,
        container.getContainerInfo().getContainerID(), null);
    ByteString byteString = UnsafeByteOperations.unsafeWrap(new byte[0]);
    ContainerProtos.EchoResponseProto response =
        ContainerProtocolCalls.echo(client, "", container.getContainerInfo().getContainerID(), byteString, 1, 0, true);
    assertEquals(1, response.getPayload().size());
    xceiverClientManager.releaseClient(client, false);
  }
}


