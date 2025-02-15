/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm;

import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocolPB.
    StorageContainerLocationProtocolClientSideTranslatorPB;
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
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.proto.RaftProtos;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.
    OZONE_SCM_STALENODE_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This class tests the 2 way and 3 way commit in Ratis.
 */
@Timeout(300)
public class TestCommitInRatis {
  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private ObjectStore objectStore;
  private String volumeName;
  private String bucketName;
  private int chunkSize;
  private int flushSize;
  private int maxFlushSize;
  private int blockSize;
  private StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  private void startCluster(OzoneConfiguration conf) throws Exception {
    chunkSize = 100;
    flushSize = 2 * chunkSize;
    maxFlushSize = 2 * flushSize;
    blockSize = 2 * maxFlushSize;

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
        .setBlockSize(blockSize)
        .setChunkSize(chunkSize)
        .setStreamBufferFlushSize(flushSize)
        .setStreamBufferMaxSize(maxFlushSize)
        .applyTo(conf);

    conf.setQuietMode(false);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    // the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    volumeName = "watchforcommithandlingtest";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
  }


  /**
   * Shutdown MiniDFSCluster.
   */
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
    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(XceiverClientRatis.LOG);
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
