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

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.RatisTestHelper;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ratis.proto.RaftProtos;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * This class tests the 2 way and 3 way commit in Ratis.
 */
public class TestCommitInRatis {
  private static final String VOLUME_NAME = "watchforcommithandlingtest";
  private static final int CHUNK_SIZE = 100;
  private static final int FLUSH_SIZE = 2 * CHUNK_SIZE;
  private static final int MAX_FLUSH_SIZE = 2 * FLUSH_SIZE;
  private static final int BLOCK_SIZE = 2 * MAX_FLUSH_SIZE;

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  private void startCluster(OzoneConfiguration conf) throws Exception {
    // Make sure the pipeline does not get destroyed quickly
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 60000,
        TimeUnit.SECONDS);
    DatanodeRatisServerConfig ratisServerConfig =
        conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setRequestTimeOut(Duration.ofSeconds(3));
    ratisServerConfig.setWatchTimeOut(Duration.ofSeconds(10));
    conf.setFromObject(ratisServerConfig);

    RatisClientConfig.RaftConfig raftClientConfig =
        conf.getObject(RatisClientConfig.RaftConfig.class);
    raftClientConfig.setRpcRequestTimeout(Duration.ofSeconds(3));
    raftClientConfig.setRpcWatchRequestTimeout(Duration.ofSeconds(10));
    conf.setFromObject(raftClientConfig);

    ClientConfigForTesting.newBuilder(StorageUnit.BYTES)
        .setBlockSize(BLOCK_SIZE)
        .setChunkSize(CHUNK_SIZE)
        .setStreamBufferFlushSize(FLUSH_SIZE)
        .setStreamBufferMaxSize(MAX_FLUSH_SIZE)
        .applyTo(conf);

    conf.setQuietMode(false);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    // the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getRpcClient(conf);
    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(VOLUME_NAME);
    objectStore.getVolume(VOLUME_NAME).createBucket(VOLUME_NAME);
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
  }

  private void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @ParameterizedTest
  @EnumSource(value = RaftProtos.ReplicationLevel.class, names = {"MAJORITY_COMMITTED", "ALL_COMMITTED"})
  public void test2WayCommitForRetryfailure(RaftProtos.ReplicationLevel watchType) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    RatisClientConfig ratisClientConfig = conf.getObject(RatisClientConfig.class);
    ratisClientConfig.setWatchType(watchType.toString());
    conf.setFromObject(ratisClientConfig);
    startCluster(conf);
    LogCapturer logCapturer = LogCapturer.captureLogs(XceiverClientRatis.class);
    XceiverClientManager clientManager = new XceiverClientManager(conf);

    ContainerWithPipeline container1 = storageContainerLocationClient
        .allocateContainer(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, OzoneConsts.OZONE);
    XceiverClientSpi xceiverClient = clientManager
        .acquireClient(container1.getPipeline());
    assertEquals(1, xceiverClient.getRefcount());
    assertEquals(container1.getPipeline(),
        xceiverClient.getPipeline());
    Pipeline pipeline = xceiverClient.getPipeline();
    XceiverClientRatis ratisClient = (XceiverClientRatis) xceiverClient;
    XceiverClientReply reply = xceiverClient.sendCommandAsync(
        ContainerTestHelper.getCreateContainerRequest(
            container1.getContainerInfo().getContainerID(),
            xceiverClient.getPipeline()));
    reply.getResponse().get();
    assertEquals(3, ratisClient.getCommitInfoMap().size());
    // wait for the container to be created on all the nodes
    xceiverClient.watchForCommit(reply.getLogIndex()).get();
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      // shutdown the ratis follower
      if (RatisTestHelper.isRatisFollower(dn, pipeline)) {
        cluster.shutdownHddsDatanode(dn.getDatanodeDetails());
        break;
      }
    }
    reply = xceiverClient.sendCommandAsync(ContainerTestHelper
        .getCloseContainer(pipeline,
            container1.getContainerInfo().getContainerID()));
    reply.getResponse().get();
    xceiverClient.watchForCommit(reply.getLogIndex()).get();

    if (watchType == RaftProtos.ReplicationLevel.ALL_COMMITTED) {
      // commitInfo Map will be reduced to 2 here
      assertEquals(2, ratisClient.getCommitInfoMap().size());
      assertThat(logCapturer.getOutput()).contains("ALL_COMMITTED way commit failed");
      assertThat(logCapturer.getOutput()).contains("Committed by majority");
    } else {
      // there will still be 3 here for MAJORITY_COMMITTED
      assertEquals(3, ratisClient.getCommitInfoMap().size());
    }
    clientManager.releaseClient(xceiverClient, false);
    logCapturer.stopCapturing();
    shutdown();
  }
}
