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
package org.apache.hadoop.hdds.scm;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager.ScmClientConfig;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for short-circuit enabled XceiverClientManager.
 */
@Timeout(300)
public class TestXceiverClientManagerSC {

  private static OzoneConfiguration config;
  private static MiniOzoneCluster cluster;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  @TempDir
  private static File dir;

  @BeforeAll
  public static void init() throws Exception {
    config = new OzoneConfiguration();
    OzoneClientConfig clientConfig = config.getObject(OzoneClientConfig.class);
    clientConfig.setShortCircuit(true);
    config.setFromObject(clientConfig);
    config.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH, new File(dir, "ozone-socket").getAbsolutePath());
    cluster = MiniOzoneCluster.newBuilder(config)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
  }

  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.cleanupWithLogger(null, storageContainerLocationClient);
  }

  @Test
  @Unhealthy("Run it locally since it requires libhadoop.so.")
  public void testAllocateShortCircuitClient() throws IOException {
    try (XceiverClientManager clientManager = new XceiverClientManager(config,
        config.getObject(ScmClientConfig.class), null)) {

      ContainerWithPipeline container1 = storageContainerLocationClient
          .allocateContainer(
              SCMTestUtils.getReplicationType(config),
              HddsProtos.ReplicationFactor.THREE,
              OzoneConsts.OZONE);
      XceiverClientSpi client1 = clientManager.acquireClientForReadData(container1.getPipeline(), true);
      assertEquals(1, client1.getRefcount());
      assertTrue(client1 instanceof XceiverClientShortCircuit);
      XceiverClientSpi client2 = clientManager.acquireClientForReadData(container1.getPipeline(), true);
      assertTrue(client2 instanceof XceiverClientShortCircuit);
      assertEquals(2, client2.getRefcount());
      assertEquals(2, client1.getRefcount());
      assertEquals(client1, client2);
      clientManager.releaseClient(client1, true);
      clientManager.releaseClient(client2, true);
      assertEquals(0, clientManager.getClientCache().size());

      XceiverClientSpi client3 = clientManager.acquireClientForReadData(container1.getPipeline(), false);
      assertTrue(client3 instanceof XceiverClientGrpc);
    }
  }
}
