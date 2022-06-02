/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.storage;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig.EcCodec;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ListBlockResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.WritableECContainerProvider.WritableECContainerProviderConfig;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This class tests container commands on EC containers.
 */
public class TestContainerCommandsEC {

  private static MiniOzoneCluster cluster;
  private static StorageContainerManager scm;
  private static OzoneClient client;
  private static ObjectStore store;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static final String SCM_ID = UUID.randomUUID().toString();
  private static final String CLUSTER_ID = UUID.randomUUID().toString();
  private static final int EC_DATA = 3;
  private static final int EC_PARITY = 2;
  private static final EcCodec EC_CODEC = EcCodec.RS;
  private static final int EC_CHUNK_SIZE = 1024;
  private static final int STRIPE_DATA_SIZE = EC_DATA * EC_CHUNK_SIZE;
  private static final int NUM_DN = EC_DATA + EC_PARITY;

  // Each key size will be in range [min, max), min inclusive, max exclusive
  private static final int[][] KEY_SIZE_RANGES = new int[][]{
      {1, EC_CHUNK_SIZE},
      {EC_CHUNK_SIZE, EC_CHUNK_SIZE + 1},
      {EC_CHUNK_SIZE + 1, STRIPE_DATA_SIZE},
      {STRIPE_DATA_SIZE, STRIPE_DATA_SIZE + 1},
      {STRIPE_DATA_SIZE + 1, STRIPE_DATA_SIZE + EC_CHUNK_SIZE},
      {STRIPE_DATA_SIZE + EC_CHUNK_SIZE, STRIPE_DATA_SIZE * 2},
  };
  private static byte[][] values;
  private static long containerID;
  private static Pipeline pipeline;
  private static List<DatanodeDetails> datanodeDetails;
  private List<XceiverClientSpi> clients = null;
  private static OzoneConfiguration config;

  @BeforeAll
  public static void init() throws Exception {
    config = new OzoneConfiguration();
    config.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    config.setBoolean(OzoneConfigKeys.OZONE_ACL_ENABLED, true);
    config.set(OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS,
        OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    startCluster(config);
    prepareData(KEY_SIZE_RANGES);
  }

  @AfterAll
  public static void stop() throws IOException {
    stopCluster();
  }

  private Pipeline createSingleNodePipeline(Pipeline ecPipeline,
      DatanodeDetails node, int replicaIndex) {

    Map<DatanodeDetails, Integer> indicesForSinglePipeline = new HashMap<>();
    indicesForSinglePipeline.put(node, replicaIndex);

    return Pipeline.newBuilder()
        .setId(ecPipeline.getId())
        .setReplicationConfig(ecPipeline.getReplicationConfig())
        .setState(ecPipeline.getPipelineState())
        .setNodes(ImmutableList.of(node))
        .setReplicaIndexes(indicesForSinglePipeline)
        .build();
  }

  @BeforeEach
  public void connectToDatanodes() {
    clients = new ArrayList<>(datanodeDetails.size());
    for (int i = 0; i < datanodeDetails.size(); i++) {
      clients.add(new XceiverClientGrpc(
          createSingleNodePipeline(pipeline, datanodeDetails.get(i), i + 1),
          cluster.getConf()));
    }
  }

  @AfterEach
  public void closeClients() {
    if (clients == null) {
      return;
    }
    for (XceiverClientSpi c : clients) {
      c.close();
    }
    clients = null;
  }

  private Function<Integer, Integer> chunksInReplicaFunc(int i) {
    if (i < EC_DATA) {
      return (keySize) -> {
        int dataBlocks = (keySize + EC_CHUNK_SIZE - 1) / EC_CHUNK_SIZE;
        return (dataBlocks + EC_DATA - 1 - i) / EC_DATA;
      };
    } else {
      return (keySize) -> (keySize + STRIPE_DATA_SIZE - 1) / STRIPE_DATA_SIZE;
    }
  }

  @Test
  public void testListBlock() throws Exception {
    for (int i = 0; i < datanodeDetails.size(); i++) {
      final int minKeySize = i < EC_DATA ? i * EC_CHUNK_SIZE : 0;
      final int numExpectedBlocks = (int) Arrays.stream(values)
          .mapToInt(v -> v.length).filter(s -> s > minKeySize).count();
      Function<Integer, Integer> expectedChunksFunc = chunksInReplicaFunc(i);
      final int numExpectedChunks = Arrays.stream(values)
          .mapToInt(v -> v.length).map(expectedChunksFunc::apply).sum();
      if (numExpectedBlocks == 0) {
        final int j = i;
        Throwable t = Assertions.assertThrows(StorageContainerException.class,
            () -> ContainerProtocolCalls.listBlock(clients.get(j),
                containerID, null, numExpectedBlocks + 1, null));
        Assertions.assertEquals(
            "ContainerID " + containerID + " does not exist", t.getMessage());
        continue;
      }
      ListBlockResponseProto response = ContainerProtocolCalls.listBlock(
          clients.get(i), containerID, null, numExpectedBlocks + 1, null);
      Assertions.assertEquals(numExpectedBlocks, response.getBlockDataCount(),
          "blocks count doesn't match on DN " + i);
      Assertions.assertEquals(numExpectedChunks,
          response.getBlockDataList().stream()
              .mapToInt(BlockData::getChunksCount).sum(),
          "chunks count doesn't match on DN " + i);
    }
  }

  @Test
  public void testCreateRecoveryContainer() throws Exception {
    XceiverClientManager xceiverClientManager =
        new XceiverClientManager(config);
    ECReplicationConfig replicationConfig = new ECReplicationConfig(3, 2);
    Pipeline newPipeline =
        scm.getPipelineManager().createPipeline(replicationConfig);
    scm.getPipelineManager().activatePipeline(newPipeline.getId());
    ContainerInfo container =
        scm.getContainerManager().allocateContainer(replicationConfig, "test");
    scm.getContainerManager().getContainerStateManager()
        .addContainer(container.getProtobuf());

    XceiverClientSpi dnClient = xceiverClientManager.acquireClient(
        createSingleNodePipeline(newPipeline, newPipeline.getNodes().get(0),
            2));
    try {
      // To create the actual situation, container would have been in closed
      // state at SCM.
      scm.getContainerManager().getContainerStateManager()
          .updateContainerState(container.containerID().getProtobuf(),
              HddsProtos.LifeCycleEvent.FINALIZE);
      scm.getContainerManager().getContainerStateManager()
          .updateContainerState(container.containerID().getProtobuf(),
              HddsProtos.LifeCycleEvent.CLOSE);

      //Create the recovering container in DN.
      ContainerProtocolCalls.createRecoveringContainer(dnClient,
          container.containerID().getProtobuf().getId(), null);

      BlockID blockID = ContainerTestHelper
          .getTestBlockID(container.containerID().getProtobuf().getId());
      byte[] data = "TestData".getBytes(UTF_8);
      ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
          ContainerTestHelper.newWriteChunkRequestBuilder(newPipeline, blockID,
              ChunkBuffer.wrap(ByteBuffer.wrap(data)), 0).build();
      dnClient.sendCommand(writeChunkRequest);

      // Now, explicitly make a putKey request for the block.
      ContainerProtos.ContainerCommandRequestProto putKeyRequest =
          ContainerTestHelper.getPutBlockRequest(newPipeline,
              writeChunkRequest.getWriteChunk());
      dnClient.sendCommand(putKeyRequest);

      ContainerProtos.ReadContainerResponseProto readContainerResponseProto =
          ContainerProtocolCalls.readContainer(dnClient,
              container.containerID().getProtobuf().getId(), null);
      Assert.assertEquals(ContainerProtos.ContainerDataProto.State.RECOVERING,
          readContainerResponseProto.getContainerData().getState());
      // Container at SCM should be still in closed state.
      Assert.assertEquals(HddsProtos.LifeCycleState.CLOSED,
          scm.getContainerManager().getContainerStateManager()
              .getContainer(container.containerID()).getState());
      // close container call
      ContainerProtocolCalls.closeContainer(dnClient,
          container.containerID().getProtobuf().getId(), null);
      // Make sure we have the container and readable.
      readContainerResponseProto = ContainerProtocolCalls
          .readContainer(dnClient,
              container.containerID().getProtobuf().getId(), null);
      Assert.assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED,
          readContainerResponseProto.getContainerData().getState());
      ContainerProtos.ReadChunkResponseProto readChunkResponseProto =
          ContainerProtocolCalls.readChunk(dnClient,
              writeChunkRequest.getWriteChunk().getChunkData(), blockID, null,
              null);
      ByteBuffer[] readOnlyByteBuffersArray = BufferUtils
          .getReadOnlyByteBuffersArray(
              readChunkResponseProto.getDataBuffers().getBuffersList());
      Assert.assertEquals(readOnlyByteBuffersArray[0].limit(), data.length);
      byte[] readBuff = new byte[readOnlyByteBuffersArray[0].limit()];
      readOnlyByteBuffersArray[0].get(readBuff, 0, readBuff.length);
      Assert.assertArrayEquals(data, readBuff);
    } finally {
      xceiverClientManager.releaseClient(dnClient, false);
    }
  }

  public static void startCluster(OzoneConfiguration conf) throws Exception {

    // Set minimum pipeline to 1 to ensure all data is written to
    // the same container group
    WritableECContainerProviderConfig writableECContainerProviderConfig =
        conf.getObject(WritableECContainerProviderConfig.class);
    writableECContainerProviderConfig.setMinimumPipelines(1);
    conf.setFromObject(writableECContainerProviderConfig);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(NUM_DN)
        .setScmId(SCM_ID)
        .setClusterId(CLUSTER_ID)
        .build();
    cluster.waitForClusterToBeReady();
    scm = cluster.getStorageContainerManager();
    client = OzoneClientFactory.getRpcClient(conf);
    store = client.getObjectStore();
    storageContainerLocationClient =
        cluster.getStorageContainerLocationClient();
  }

  public static void prepareData(int[][] ranges) throws Exception {
    final String volumeName = UUID.randomUUID().toString();
    final String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    final ReplicationConfig repConfig =
        new ECReplicationConfig(EC_DATA, EC_PARITY, EC_CODEC, EC_CHUNK_SIZE);
    values = new byte[ranges.length][];
    for (int i = 0; i < ranges.length; i++) {
      int keySize = RandomUtils.nextInt(ranges[i][0], ranges[i][1]);
      values[i] = RandomUtils.nextBytes(keySize);
      final String keyName = UUID.randomUUID().toString();
      try (OutputStream out = bucket.createKey(
          keyName, values[i].length, repConfig, new HashMap<>())) {
        out.write(values[i]);
      }
    }
    List<ContainerID> containerIDs =
        new ArrayList<>(scm.getContainerManager().getContainerIDs());
    Assertions.assertEquals(1, containerIDs.size());
    containerID = containerIDs.get(0).getId();
    List<Pipeline> pipelines = scm.getPipelineManager().getPipelines(repConfig);
    Assertions.assertEquals(1, pipelines.size());
    pipeline = pipelines.get(0);
    datanodeDetails = pipeline.getNodes();
  }

  public static void stopCluster() throws IOException {
    if (client != null) {
      client.close();
    }

    if (storageContainerLocationClient != null) {
      storageContainerLocationClient.close();
    }

    if (cluster != null) {
      cluster.shutdown();
    }
  }

}
