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
import org.apache.hadoop.hdds.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementPolicy;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.protocolPB
    .StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;

/**
 * Test Container calls.
 */
public class TestContainerSmallFile {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration ozoneConfig;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static XceiverClientManager xceiverClientManager;
  private static String containerOwner = "OZONE";

  @BeforeClass
  public static void init() throws Exception {
    ozoneConfig = new OzoneConfiguration();
    ozoneConfig.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, ContainerPlacementPolicy.class);
    cluster = MiniOzoneCluster.newBuilder(ozoneConfig).setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
    xceiverClientManager = new XceiverClientManager(ozoneConfig);
  }

  @AfterClass
  public static void shutdown() throws InterruptedException {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.cleanupWithLogger(null, storageContainerLocationClient);
  }

  @Test
  public void testAllocateWrite() throws Exception {
    String traceID = UUID.randomUUID().toString();
    ContainerInfo container =
        storageContainerLocationClient.allocateContainer(
            xceiverClientManager.getType(),
            HddsProtos.ReplicationFactor.ONE, containerOwner);
    XceiverClientSpi client = xceiverClientManager.acquireClient(
        container.getPipeline(), container.getContainerID());
    ContainerProtocolCalls.createContainer(client,
        container.getContainerID(), traceID);

    BlockID blockID = ContainerTestHelper.getTestBlockID(
        container.getContainerID());
    ContainerProtocolCalls.writeSmallFile(client, blockID,
        "data123".getBytes(), traceID);
    ContainerProtos.GetSmallFileResponseProto response =
        ContainerProtocolCalls.readSmallFile(client, blockID, traceID);
    String readData = response.getData().getData().toStringUtf8();
    Assert.assertEquals("data123", readData);
    xceiverClientManager.releaseClient(client);
  }

  @Test
  public void testInvalidKeyRead() throws Exception {
    String traceID = UUID.randomUUID().toString();
    ContainerInfo container =
        storageContainerLocationClient.allocateContainer(
            xceiverClientManager.getType(),
            HddsProtos.ReplicationFactor.ONE, containerOwner);
    XceiverClientSpi client = xceiverClientManager.acquireClient(
        container.getPipeline(), container.getContainerID());
    ContainerProtocolCalls.createContainer(client,
        container.getContainerID(), traceID);

    thrown.expect(StorageContainerException.class);
    thrown.expectMessage("Unable to find the key");

    BlockID blockID = ContainerTestHelper.getTestBlockID(
        container.getContainerID());
    // Try to read a Key Container Name
    ContainerProtos.GetSmallFileResponseProto response =
        ContainerProtocolCalls.readSmallFile(client, blockID, traceID);
    xceiverClientManager.releaseClient(client);
  }

  @Test
  public void testInvalidContainerRead() throws Exception {
    String traceID = UUID.randomUUID().toString();
    long nonExistContainerID = 8888L;
    ContainerInfo container =
        storageContainerLocationClient.allocateContainer(
            xceiverClientManager.getType(),
            HddsProtos.ReplicationFactor.ONE, containerOwner);
    XceiverClientSpi client = xceiverClientManager.
        acquireClient(container.getPipeline(), container.getContainerID());
    ContainerProtocolCalls.createContainer(client,
        container.getContainerID(), traceID);
    BlockID blockID = ContainerTestHelper.getTestBlockID(
        container.getContainerID());
    ContainerProtocolCalls.writeSmallFile(client, blockID,
        "data123".getBytes(), traceID);


    thrown.expect(StorageContainerException.class);
    thrown.expectMessage("Unable to find the container");

    // Try to read a invalid key
    ContainerProtos.GetSmallFileResponseProto response =
        ContainerProtocolCalls.readSmallFile(client,
            ContainerTestHelper.getTestBlockID(
                nonExistContainerID), traceID);
    xceiverClientManager.releaseClient(client);
  }


}


