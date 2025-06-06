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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_METADATA_DIR_NAME;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import com.google.common.cache.Cache;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager.ScmClientConfig;
import org.apache.hadoop.hdds.scm.client.ClientTrustManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test for XceiverClientManager caching and eviction.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestXceiverClientManager implements NonHATests.TestCase {

  private StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  @BeforeAll
  void init() throws Exception {
    storageContainerLocationClient = cluster()
        .getStorageContainerLocationClient();
  }

  @AfterAll
  void shutdown() {
    IOUtils.cleanupWithLogger(null, storageContainerLocationClient);
  }

  @ParameterizedTest(name = "Ozone security enabled: {0}")
  @ValueSource(booleans = {false, true})
  public void testCaching(boolean securityEnabled, @TempDir Path metaDir) throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, securityEnabled);
    conf.set(HDDS_METADATA_DIR_NAME, metaDir.toString());

    ClientTrustManager trustManager = mock(ClientTrustManager.class);
    try (XceiverClientManager clientManager = new XceiverClientManager(conf,
        conf.getObject(ScmClientConfig.class), trustManager)) {

      ContainerWithPipeline container1 = storageContainerLocationClient
          .allocateContainer(
              SCMTestUtils.getReplicationType(conf),
              HddsProtos.ReplicationFactor.ONE,
              OzoneConsts.OZONE);
      XceiverClientSpi client1 = clientManager
          .acquireClient(container1.getPipeline());
      assertEquals(1, client1.getRefcount());

      ContainerWithPipeline container2 = storageContainerLocationClient
          .allocateContainer(
              SCMTestUtils.getReplicationType(conf),
              HddsProtos.ReplicationFactor.THREE,
              OzoneConsts.OZONE);
      XceiverClientSpi client2 = clientManager
          .acquireClient(container2.getPipeline());
      assertEquals(1, client2.getRefcount());

      XceiverClientSpi client3 = clientManager
          .acquireClient(container1.getPipeline());
      assertEquals(2, client3.getRefcount());
      assertEquals(2, client1.getRefcount());
      assertEquals(client1, client3);
      clientManager.releaseClient(client1, true);
      clientManager.releaseClient(client2, true);
      clientManager.releaseClient(client3, true);
      assertEquals(0, clientManager.getClientCache().size());
    }
  }

  @Test
  public void testFreeByReference(@TempDir Path metaDir) throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    ScmClientConfig clientConfig = conf.getObject(ScmClientConfig.class);
    clientConfig.setMaxSize(1);
    conf.set(HDDS_METADATA_DIR_NAME, metaDir.toString());
    try (XceiverClientManager clientManager =
        new XceiverClientManager(conf, clientConfig, null)) {
      Cache<String, XceiverClientSpi> cache =
          clientManager.getClientCache();

      ContainerWithPipeline container1 =
          storageContainerLocationClient.allocateContainer(
              SCMTestUtils.getReplicationType(conf),
              HddsProtos.ReplicationFactor.ONE,
              OzoneConsts.OZONE);
      XceiverClientSpi client1 = clientManager
          .acquireClient(container1.getPipeline());
      assertEquals(1, client1.getRefcount());
      assertEquals(container1.getPipeline(),
          client1.getPipeline());

      ContainerWithPipeline container2 =
          storageContainerLocationClient.allocateContainer(
              SCMTestUtils.getReplicationType(conf),
              HddsProtos.ReplicationFactor.THREE,
              OzoneConsts.OZONE);
      XceiverClientSpi client2 = clientManager
          .acquireClient(container2.getPipeline());
      assertEquals(1, client2.getRefcount());
      assertNotEquals(client1, client2);

      // least recent container (i.e containerName1) is evicted
      XceiverClientSpi nonExistent1 = cache.getIfPresent(
          container1.getContainerInfo().getPipelineID().getId().toString()
              + container1.getContainerInfo().getReplicationType());
      assertNull(nonExistent1);
      // However container call should succeed because of refcount on the client
      ContainerProtocolCalls.createContainer(client1,
          container1.getContainerInfo().getContainerID(), null);

      // After releasing the client, this connection should be closed
      // and any container operations should fail
      clientManager.releaseClient(client1, false);

      // Create container should throw exception on closed client
      Throwable t = assertThrows(IOException.class,
          () -> ContainerProtocolCalls.createContainer(client1,
              container1.getContainerInfo().getContainerID(), null));
      assertThat(t.getMessage()).contains("This channel is not connected");

      clientManager.releaseClient(client2, false);
    }
  }

  @Test
  public void testFreeByEviction(@TempDir Path metaDir) throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    ScmClientConfig clientConfig = conf.getObject(ScmClientConfig.class);
    clientConfig.setMaxSize(1);
    conf.set(HDDS_METADATA_DIR_NAME, metaDir.toString());
    try (XceiverClientManager clientManager =
        new XceiverClientManager(conf, clientConfig, null)) {
      Cache<String, XceiverClientSpi> cache =
          clientManager.getClientCache();

      ContainerWithPipeline container1 =
          storageContainerLocationClient.allocateContainer(
              SCMTestUtils.getReplicationType(conf),
              HddsProtos.ReplicationFactor.ONE,
              OzoneConsts.OZONE);
      XceiverClientSpi client1 = clientManager
          .acquireClient(container1.getPipeline());
      assertEquals(1, client1.getRefcount());

      clientManager.releaseClient(client1, false);
      assertEquals(0, client1.getRefcount());

      ContainerWithPipeline container2 =
          storageContainerLocationClient.allocateContainer(
              SCMTestUtils.getReplicationType(conf),
              HddsProtos.ReplicationFactor.THREE,
              OzoneConsts.OZONE);
      XceiverClientSpi client2 = clientManager
          .acquireClient(container2.getPipeline());
      assertEquals(1, client2.getRefcount());
      assertNotEquals(client1, client2);

      // now client 1 should be evicted
      XceiverClientSpi nonExistent = cache.getIfPresent(
          container1.getContainerInfo().getPipelineID().getId().toString()
              + container1.getContainerInfo().getReplicationType());
      assertNull(nonExistent);

      // Any container operation should now fail
      Throwable t = assertThrows(IOException.class,
          () -> ContainerProtocolCalls.createContainer(client1,
              container1.getContainerInfo().getContainerID(), null));
      assertThat(t.getMessage()).contains("This channel is not connected");

      clientManager.releaseClient(client2, false);
    }
  }

  @Test
  public void testFreeByRetryFailure() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    ScmClientConfig clientConfig = conf.getObject(ScmClientConfig.class);
    clientConfig.setMaxSize(1);
    try (XceiverClientManager clientManager =
        new XceiverClientManager(conf, clientConfig, null)) {
      Cache<String, XceiverClientSpi> cache =
          clientManager.getClientCache();

      // client is added in cache
      ContainerWithPipeline container1 =
          storageContainerLocationClient.allocateContainer(
              SCMTestUtils.getReplicationType(conf),
              SCMTestUtils.getReplicationFactor(conf),
              OzoneConsts.OZONE);
      XceiverClientSpi client1 =
          clientManager.acquireClient(container1.getPipeline());
      XceiverClientSpi client1SecondRef =
          clientManager.acquireClient(container1.getPipeline());
      assertEquals(2, client1.getRefcount());

      // client should be invalidated in the cache
      clientManager.releaseClient(client1, true);
      assertEquals(1, client1.getRefcount());
      assertNull(cache.getIfPresent(
          container1.getContainerInfo().getPipelineID().getId().toString()
              + container1.getContainerInfo().getReplicationType()));

      // new client should be added in cache
      XceiverClientSpi client2 =
          clientManager.acquireClient(container1.getPipeline());
      assertNotEquals(client1, client2);
      assertEquals(1, client2.getRefcount());

      // on releasing the old client the cache entry should not be invalidated
      clientManager.releaseClient(client1, true);
      assertEquals(0, client1.getRefcount());
      assertNotNull(cache.getIfPresent(
          container1.getContainerInfo().getPipelineID().getId().toString()
              + container1.getContainerInfo().getReplicationType()));

      // cleanup
      clientManager.releaseClient(client1SecondRef, false);
      clientManager.releaseClient(client2, false);
    }
  }
}
