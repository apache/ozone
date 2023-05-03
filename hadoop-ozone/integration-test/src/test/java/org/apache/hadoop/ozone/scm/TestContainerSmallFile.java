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
package org.apache.hadoop.ozone.scm;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.protocolPB
    .StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Test Container calls.
 */
@Timeout(300)
public class TestContainerSmallFile {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration ozoneConfig;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static XceiverClientManager xceiverClientManager;

  @BeforeAll
  public static void init() throws Exception {
    ozoneConfig = new OzoneConfiguration();
    ozoneConfig.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, PlacementPolicy.class);
    cluster = MiniOzoneCluster.newBuilder(ozoneConfig).setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
    xceiverClientManager = new XceiverClientManager(ozoneConfig);
  }

  @AfterAll
  public static void shutdown() throws InterruptedException {
    if (cluster != null) {
      cluster.shutdown();
    }
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
    Assertions.assertEquals("data123", readData);
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
    Assertions.assertThrowsExactly(StorageContainerException.class,
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

    Assertions.assertThrowsExactly(StorageContainerException.class,
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
        Assertions.assertThrows(StorageContainerException.class, () ->
            ContainerProtocolCalls.readSmallFile(client, blockID1, null));
    Assertions.assertSame(ContainerProtos.Result.UNKNOWN_BCSID,
        sce.getResult());

    // write a new block again to bump up the container bcsId
    BlockID blockID2 = ContainerTestHelper
        .getTestBlockID(container.getContainerInfo().getContainerID());
    ContainerProtocolCalls
        .writeSmallFile(client, blockID2, "data123".getBytes(UTF_8), null);

    blockID1.setBlockCommitSequenceId(bcsId + 1);
    //read a file with higher bcsId than the committed bcsId for the block
    sce = Assertions.assertThrows(StorageContainerException.class, () ->
        ContainerProtocolCalls.readSmallFile(client, blockID1, null));
    Assertions.assertSame(ContainerProtos.Result.BCSID_MISMATCH,
        sce.getResult());

    blockID1.setBlockCommitSequenceId(bcsId);
    ContainerProtos.GetSmallFileResponseProto response =
        ContainerProtocolCalls.readSmallFile(client, blockID1, null);
    String readData = response.getData().getDataBuffers().getBuffersList()
        .get(0).toStringUtf8();
    Assertions.assertEquals("data123", readData);
    xceiverClientManager.releaseClient(client, false);
  }
}


