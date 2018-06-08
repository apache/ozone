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
package org.apache.hadoop.ozone.scm;

import com.google.common.cache.Cache;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.protocolPB
    .StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.hadoop.hdds.scm
    .ScmConfigKeys.SCM_CONTAINER_CLIENT_MAX_SIZE_KEY;

/**
 * Test for XceiverClientManager caching and eviction.
 */
@RunWith(Parameterized.class)
public class TestXceiverClientManager {
  private static OzoneConfiguration config;
  private static MiniOzoneCluster cluster;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static String containerOwner = "OZONE";
  private static boolean shouldUseGrpc;

  @Parameterized.Parameters
  public static Collection<Object[]> withGrpc() {
    return Arrays.asList(new Object[][] {{false}, {true}});
  }

  public TestXceiverClientManager(boolean useGrpc) {
    shouldUseGrpc = useGrpc;
  }

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void init() throws Exception {
    config = new OzoneConfiguration();
    config.setBoolean(ScmConfigKeys.DFS_CONTAINER_GRPC_ENABLED_KEY,
        shouldUseGrpc);
    cluster = MiniOzoneCluster.newBuilder(config)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
  }

  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.cleanupWithLogger(null, storageContainerLocationClient);
  }

  @Test
  public void testCaching() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(ScmConfigKeys.DFS_CONTAINER_GRPC_ENABLED_KEY,
        shouldUseGrpc);
    XceiverClientManager clientManager = new XceiverClientManager(conf);

    ContainerInfo container1 = storageContainerLocationClient
        .allocateContainer(clientManager.getType(), clientManager.getFactor(),
            containerOwner);
    XceiverClientSpi client1 = clientManager.acquireClient(container1.getPipeline(),
        container1.getContainerID());
    Assert.assertEquals(1, client1.getRefcount());

    ContainerInfo container2 = storageContainerLocationClient
        .allocateContainer(clientManager.getType(), clientManager.getFactor(),
            containerOwner);
    XceiverClientSpi client2 = clientManager.acquireClient(container2.getPipeline(),
        container2.getContainerID());
    Assert.assertEquals(1, client2.getRefcount());

    XceiverClientSpi client3 = clientManager.acquireClient(container1.getPipeline(),
        container1.getContainerID());
    Assert.assertEquals(2, client3.getRefcount());
    Assert.assertEquals(2, client1.getRefcount());
    Assert.assertEquals(client1, client3);
    clientManager.releaseClient(client1);
    clientManager.releaseClient(client2);
    clientManager.releaseClient(client3);
  }

  @Test
  public void testFreeByReference() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(SCM_CONTAINER_CLIENT_MAX_SIZE_KEY, 1);
    conf.setBoolean(ScmConfigKeys.DFS_CONTAINER_GRPC_ENABLED_KEY,
        shouldUseGrpc);
    XceiverClientManager clientManager = new XceiverClientManager(conf);
    Cache<Long, XceiverClientSpi> cache =
        clientManager.getClientCache();

    ContainerInfo container1 =
        storageContainerLocationClient.allocateContainer(
            clientManager.getType(), HddsProtos.ReplicationFactor.ONE,
            containerOwner);
    XceiverClientSpi client1 = clientManager.acquireClient(container1.getPipeline(),
        container1.getContainerID());
    Assert.assertEquals(1, client1.getRefcount());
    Assert.assertEquals(container1.getPipeline(),
        client1.getPipeline());

    ContainerInfo container2 =
        storageContainerLocationClient.allocateContainer(
            clientManager.getType(),
            HddsProtos.ReplicationFactor.ONE, containerOwner);
    XceiverClientSpi client2 = clientManager.acquireClient(container2.getPipeline(),
        container2.getContainerID());
    Assert.assertEquals(1, client2.getRefcount());
    Assert.assertNotEquals(client1, client2);

    // least recent container (i.e containerName1) is evicted
    XceiverClientSpi nonExistent1 = cache.getIfPresent(container1.getContainerID());
    Assert.assertEquals(null, nonExistent1);
    // However container call should succeed because of refcount on the client.
    String traceID1 = "trace" + RandomStringUtils.randomNumeric(4);
    ContainerProtocolCalls.createContainer(client1,
        container1.getContainerID(),  traceID1);

    // After releasing the client, this connection should be closed
    // and any container operations should fail
    clientManager.releaseClient(client1);

    String expectedMessage = shouldUseGrpc ? "Channel shutdown invoked" :
        "This channel is not connected.";
    try {
      ContainerProtocolCalls.createContainer(client1,
          container1.getContainerID(), traceID1);
      Assert.fail("Create container should throw exception on closed"
          + "client");
    } catch (Exception e) {
      Assert.assertEquals(e.getClass(), IOException.class);
      Assert.assertTrue(e.getMessage().contains(expectedMessage));
    }
    clientManager.releaseClient(client2);
  }

  @Test
  public void testFreeByEviction() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(SCM_CONTAINER_CLIENT_MAX_SIZE_KEY, 1);
    conf.setBoolean(ScmConfigKeys.DFS_CONTAINER_GRPC_ENABLED_KEY,
        shouldUseGrpc);
    XceiverClientManager clientManager = new XceiverClientManager(conf);
    Cache<Long, XceiverClientSpi> cache =
        clientManager.getClientCache();

    ContainerInfo container1 =
        storageContainerLocationClient.allocateContainer(
            clientManager.getType(),
            clientManager.getFactor(), containerOwner);
    XceiverClientSpi client1 = clientManager.acquireClient(container1.getPipeline(),
        container1.getContainerID());
    Assert.assertEquals(1, client1.getRefcount());

    clientManager.releaseClient(client1);
    Assert.assertEquals(0, client1.getRefcount());

    ContainerInfo container2 = storageContainerLocationClient
        .allocateContainer(clientManager.getType(), clientManager.getFactor(),
            containerOwner);
    XceiverClientSpi client2 = clientManager.acquireClient(container2.getPipeline(),
        container2.getContainerID());
    Assert.assertEquals(1, client2.getRefcount());
    Assert.assertNotEquals(client1, client2);


    // now client 1 should be evicted
    XceiverClientSpi nonExistent = cache.getIfPresent(container1.getContainerID());
    Assert.assertEquals(null, nonExistent);

    // Any container operation should now fail
    String traceID2 = "trace" + RandomStringUtils.randomNumeric(4);
    String expectedMessage = shouldUseGrpc ? "Channel shutdown invoked" :
        "This channel is not connected.";
    try {
      ContainerProtocolCalls.createContainer(client1,
          container1.getContainerID(), traceID2);
      Assert.fail("Create container should throw exception on closed"
          + "client");
    } catch (Exception e) {
      Assert.assertEquals(e.getClass(), IOException.class);
      Assert.assertTrue(e.getMessage().contains(expectedMessage));
    }
    clientManager.releaseClient(client2);
  }
}
