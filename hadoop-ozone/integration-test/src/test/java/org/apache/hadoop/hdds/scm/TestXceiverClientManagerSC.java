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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager.ScmClientConfig;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test for short-circuit enabled XceiverClientManager.
 * Add Environment variables
 *   LD_LIBRARY_PATH=$PROJECT_DIR$/target/native-lib
 *   DYLD_LIBRARY_PATH=$PROJECT_DIR$/target/native-lib
 *   to intellij run configuration to run it locally.
 *   Dynamically set the java.library.path in java code doesn't affect the library loading
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
    config.setBoolean(OzoneClientConfig.OZONE_READ_SHORT_CIRCUIT, true);
    config.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH, new File(dir, "ozone-socket").getAbsolutePath());
    DomainSocket.disableBindPathValidation();
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
      assertEquals(0, clientManager.getClientCache().estimatedSize());

      XceiverClientSpi client3 = clientManager.acquireClientForReadData(container1.getPipeline(), false);
      assertTrue(client3 instanceof XceiverClientGrpc);
    }
  }

  /**
   * The client a read is expected to get. EC reads, both the single-replica pipeline that
   * {@code ECBlockInputStream} builds and the full EC pipeline, stay on gRPC. A standalone pipeline with no replica
   * index short-circuits.
   */
  private enum ReadClient {
    EC_REPLICA(XceiverClientGrpc.class),
    EC_PIPELINE(ECXceiverClientGrpc.class),
    STANDALONE(XceiverClientShortCircuit.class);

    private final Class<? extends XceiverClientSpi> clientType;

    ReadClient(Class<? extends XceiverClientSpi> clientType) {
      this.clientType = clientType;
    }
  }

  @ParameterizedTest
  @EnumSource(ReadClient.class)
  public void testReadClientType(ReadClient readClient) throws IOException {
    try (XceiverClientManager clientManager = new XceiverClientManager(config,
        config.getObject(ScmClientConfig.class), null)) {
      XceiverClientSpi client = clientManager.acquireClientForReadData(pipeline(readClient), true);
      assertInstanceOf(readClient.clientType, client);
      clientManager.releaseClient(client, true);
    }
  }

  private static Pipeline pipeline(ReadClient readClient) {
    DatanodeDetails dn = cluster.getHddsDatanodes().get(0).getDatanodeDetails();
    switch (readClient) {
    case EC_REPLICA:
      // same pipeline as ECBlockInputStream creates to read one replica of an EC block group
      return Pipeline.newBuilder()
          .setReplicationConfig(StandaloneReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE))
          .setNodes(Collections.singletonList(dn))
          .setId(dn.getID())
          .setReplicaIndexes(Collections.singletonMap(dn, 1))
          .setState(Pipeline.PipelineState.CLOSED)
          .build();
    case EC_PIPELINE:
      List<DatanodeDetails> nodes = new ArrayList<>();
      Map<DatanodeDetails, Integer> replicaIndexes = new HashMap<>();
      for (HddsDatanodeService datanode : cluster.getHddsDatanodes()) {
        DatanodeDetails node = datanode.getDatanodeDetails();
        nodes.add(node);
        replicaIndexes.put(node, nodes.size());
      }
      return Pipeline.newBuilder()
          .setReplicationConfig(new ECReplicationConfig(2, 1))
          .setNodes(nodes)
          .setId(PipelineID.randomId())
          .setReplicaIndexes(replicaIndexes)
          .setState(Pipeline.PipelineState.CLOSED)
          .build();
    case STANDALONE:
      return Pipeline.newBuilder()
          .setReplicationConfig(StandaloneReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE))
          .setNodes(Collections.singletonList(dn))
          .setId(PipelineID.randomId())
          .setState(Pipeline.PipelineState.CLOSED)
          .build();
    default:
      throw new IllegalStateException("Unexpected read client " + readClient);
    }
  }
}
