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
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.storage.BlockOutputStream;
import org.apache.hadoop.hdds.scm.storage.RatisBlockOutputStream;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.RatisTestHelper;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ozone.test.tag.Flaky;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * This class verifies the watchForCommit Handling by xceiverClient.
 */
@Flaky("HDDS-5818")
public class TestWatchForCommit {
  private static final int CHUNK_SIZE = 100;
  private static final int FLUSH_SIZE = 2 * CHUNK_SIZE;
  private static final int MAX_FLUSH_SIZE = 2 * FLUSH_SIZE;
  private static final int BLOCK_SIZE = 2 * MAX_FLUSH_SIZE;

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private OzoneClient client;
  private ObjectStore objectStore;
  private String volumeName;
  private String bucketName;
  private String keyString;
  private StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setStreamBufferFlushDelay(false);
    conf.setFromObject(clientConfig);

    conf.set(OzoneConfigKeys.OZONE_SCM_CLOSE_CONTAINER_WAIT_DURATION, "2s");
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_SCRUB_INTERVAL, "2s");
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT, "5s");
    conf.setQuietMode(false);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);

    RatisClientConfig ratisClientConfig =
        conf.getObject(RatisClientConfig.class);
    ratisClientConfig.setWriteRequestTimeout(Duration.ofSeconds(10));
    ratisClientConfig.setWatchRequestTimeout(Duration.ofSeconds(10));
    conf.setFromObject(ratisClientConfig);

    DatanodeRatisServerConfig ratisServerConfig =
        conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setRequestTimeOut(Duration.ofSeconds(3));
    ratisServerConfig.setWatchTimeOut(Duration.ofSeconds(3));
    conf.setFromObject(ratisServerConfig);

    RatisClientConfig.RaftConfig raftClientConfig =
        conf.getObject(RatisClientConfig.RaftConfig.class);
    raftClientConfig.setRpcRequestTimeout(Duration.ofSeconds(3));
    raftClientConfig.setRpcWatchRequestTimeout(Duration.ofSeconds(10));
    conf.setFromObject(raftClientConfig);

    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 30, TimeUnit.SECONDS);

    ClientConfigForTesting.newBuilder(StorageUnit.BYTES)
        .setBlockSize(BLOCK_SIZE)
        .setChunkSize(CHUNK_SIZE)
        .setStreamBufferFlushSize(FLUSH_SIZE)
        .setStreamBufferMaxSize(MAX_FLUSH_SIZE)
        .applyTo(conf);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(9)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.THREE, 60000);
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    keyString = UUID.randomUUID().toString();
    volumeName = "watchforcommithandlingtest";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
  }

  @AfterEach
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private String getKeyName() {
    return UUID.randomUUID().toString();
  }

  @Test
  public void testWatchForCommitWithKeyWrite() throws Exception {
    String keyName = getKeyName();
    OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS, 0);
    int dataLength = MAX_FLUSH_SIZE + 50;
    // write data more than 1 chunk
    byte[] data1 =
        ContainerTestHelper.getFixedLengthString(keyString, dataLength)
            .getBytes(UTF_8);
    key.write(data1);
    KeyOutputStream keyOutputStream =
        assertInstanceOf(KeyOutputStream.class, key.getOutputStream());

    OutputStream stream = keyOutputStream.getStreamEntries().get(0)
        .getOutputStream();
    assertInstanceOf(BlockOutputStream.class, stream);
    RatisBlockOutputStream blockOutputStream = (RatisBlockOutputStream) stream;
    // we have just written data more than flush Size(2 chunks), at this time
    // buffer pool will have 3 buffers allocated worth of chunk size
    assertEquals(4, blockOutputStream.getBufferPool().getSize());
    // writtenDataLength as well flushedDataLength will be updated here
    assertEquals(dataLength, blockOutputStream.getWrittenDataLength());
    assertEquals(MAX_FLUSH_SIZE,
        blockOutputStream.getTotalDataFlushedLength());
    // since data equals to maxBufferSize is written, this will be a blocking
    // call and hence will wait for atleast flushSize worth of data to get
    // acked by all servers right here
    assertThat(blockOutputStream.getTotalAckDataLength())
        .isGreaterThanOrEqualTo(FLUSH_SIZE);
    // watchForCommit will clean up atleast one entry from the map where each
    // entry corresponds to flushSize worth of data
    assertThat(blockOutputStream.getCommitIndex2flushedDataMap().size())
        .isLessThanOrEqualTo(1);
    // Now do a flush. This will flush the data and update the flush length and
    // the map.
    key.flush();
    // Since the data in the buffer is already flushed, flush here will have
    // no impact on the counters and data structures
    assertEquals(4, blockOutputStream.getBufferPool().getSize());
    assertEquals(dataLength, blockOutputStream.getWrittenDataLength());
    assertEquals(dataLength, blockOutputStream.getTotalDataFlushedLength());
    // flush will make sure one more entry gets updated in the map
    assertThat(blockOutputStream.getCommitIndex2flushedDataMap().size())
        .isLessThanOrEqualTo(2);
    XceiverClientRatis raftClient =
        (XceiverClientRatis) blockOutputStream.getXceiverClient();
    assertEquals(3, raftClient.getCommitInfoMap().size());
    Pipeline pipeline = raftClient.getPipeline();
    cluster.shutdownHddsDatanode(pipeline.getNodes().get(0));
    cluster.shutdownHddsDatanode(pipeline.getNodes().get(1));
    // again write data with more than max buffer limit. This will call
    // watchForCommit again. Since the commit will happen 2 way, the
    // commitInfoMap will get updated for servers which are alive
    // 4 writeChunks = maxFlushSize + 2 putBlocks  will be discarded here
    // once exception is hit
    key.write(data1);
    // As a part of handling the exception, 4 failed writeChunks  will be
    // rewritten plus one partial chunk plus two putBlocks for flushSize
    // and one flush for partial chunk
    key.flush();
    // Make sure the retryCount is reset after the exception is handled
    assertEquals(0, keyOutputStream.getRetryCount());
    // now close the stream, It will update the ack length after watchForCommit
    key.close();
    assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    // make sure the bufferPool is empty
    assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
    assertThat(blockOutputStream.getCommitIndex2flushedDataMap()).isEmpty();
    validateData(keyName, data1);
  }

  @ParameterizedTest
  @EnumSource(value = RaftProtos.ReplicationLevel.class, names = {"MAJORITY_COMMITTED", "ALL_COMMITTED"})
  public void testWatchForCommitForRetryfailure(RaftProtos.ReplicationLevel watchType) throws Exception {
    LogCapturer logCapturer = LogCapturer.captureLogs(XceiverClientRatis.class);
    RatisClientConfig ratisClientConfig = conf.getObject(RatisClientConfig.class);
    ratisClientConfig.setWatchType(watchType.toString());
    conf.setFromObject(ratisClientConfig);
    try (XceiverClientManager clientManager = new XceiverClientManager(conf)) {
      ContainerWithPipeline container1 = storageContainerLocationClient
          .allocateContainer(HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.THREE, OzoneConsts.OZONE);
      XceiverClientSpi xceiverClient = clientManager
          .acquireClient(container1.getPipeline());
      try {
        assertEquals(1, xceiverClient.getRefcount());
        assertEquals(container1.getPipeline(), xceiverClient.getPipeline());
        Pipeline pipeline = xceiverClient.getPipeline();
        TestHelper.createPipelineOnDatanode(pipeline, cluster);
        XceiverClientReply reply = xceiverClient.sendCommandAsync(
            ContainerTestHelper.getCreateContainerRequest(
                container1.getContainerInfo().getContainerID(),
                xceiverClient.getPipeline()));
        reply.getResponse().get();
        long index = reply.getLogIndex();
        cluster.shutdownHddsDatanode(pipeline.getNodes().get(0));
        cluster.shutdownHddsDatanode(pipeline.getNodes().get(1));
        // emulate closing pipeline when SCM detects DEAD datanodes
        cluster.getStorageContainerManager()
            .getPipelineManager().closePipeline(pipeline.getId());
        // again write data with more than max buffer limit. This wi
        // just watch for a log index which in not updated in the commitInfo Map
        // as well as there is no logIndex generate in Ratis.
        // The basic idea here is just to test if its throws an exception.
        ExecutionException e = assertThrows(ExecutionException.class,
            () -> xceiverClient.watchForCommit(index + RandomUtils.secure().randomInt(0, 100) + 10)
                .get());
        // since the timeout value is quite long, the watch request will either
        // fail with NotReplicated exceptio, RetryFailureException or
        // RuntimeException
        assertFalse(HddsClientUtils
            .checkForException(e) instanceof TimeoutException);
        // client should not attempt to watch with
        // MAJORITY_COMMITTED replication level, except the grpc IO issue
        if (!logCapturer.getOutput().contains("Connection refused")) {
          assertThat(e.getMessage()).doesNotContain("Watch-MAJORITY_COMMITTED");
        }
      } finally {
        clientManager.releaseClient(xceiverClient, false);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = RaftProtos.ReplicationLevel.class, names = {"MAJORITY_COMMITTED", "ALL_COMMITTED"})
  public void test2WayCommitForTimeoutException(RaftProtos.ReplicationLevel watchType) throws Exception {
    LogCapturer logCapturer = LogCapturer.captureLogs(XceiverClientRatis.class);
    RatisClientConfig ratisClientConfig = conf.getObject(RatisClientConfig.class);
    ratisClientConfig.setWatchType(watchType.toString());
    conf.setFromObject(ratisClientConfig);
    try (XceiverClientManager clientManager = new XceiverClientManager(conf)) {

      ContainerWithPipeline container1 = storageContainerLocationClient
          .allocateContainer(HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.THREE, OzoneConsts.OZONE);
      XceiverClientSpi xceiverClient = clientManager
          .acquireClient(container1.getPipeline());
      try {
        assertEquals(1, xceiverClient.getRefcount());
        assertEquals(container1.getPipeline(), xceiverClient.getPipeline());
        Pipeline pipeline = xceiverClient.getPipeline();
        TestHelper.createPipelineOnDatanode(pipeline, cluster);
        XceiverClientRatis ratisClient = (XceiverClientRatis) xceiverClient;
        XceiverClientReply reply = xceiverClient.sendCommandAsync(
            ContainerTestHelper.getCreateContainerRequest(
                container1.getContainerInfo().getContainerID(),
                xceiverClient.getPipeline()));
        reply.getResponse().get();
        assertEquals(3, ratisClient.getCommitInfoMap().size());
        List<DatanodeDetails> nodesInPipeline = pipeline.getNodes();
        for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
          // shutdown the ratis follower
          if (nodesInPipeline.contains(dn.getDatanodeDetails())
              && RatisTestHelper.isRatisFollower(dn, pipeline)) {
            cluster.shutdownHddsDatanode(dn.getDatanodeDetails());
            break;
          }
        }
        reply = xceiverClient.sendCommandAsync(ContainerTestHelper
            .getCloseContainer(pipeline,
                container1.getContainerInfo().getContainerID()));
        reply.getResponse().get();
        xceiverClient.watchForCommit(reply.getLogIndex()).get();

        // commitInfo Map will be reduced to 2 here
        if (watchType == RaftProtos.ReplicationLevel.ALL_COMMITTED) {
          assertEquals(2, ratisClient.getCommitInfoMap().size());
          String output = logCapturer.getOutput();
          assertThat(output).contains("ALL_COMMITTED way commit failed");
          assertThat(output).contains("Committed by majority");
        } else {
          assertEquals(3, ratisClient.getCommitInfoMap().size());
        }
      } finally {
        clientManager.releaseClient(xceiverClient, false);
      }
    }
    logCapturer.stopCapturing();
  }

  @Test
  public void testWatchForCommitForGroupMismatchException() throws Exception {
    try (XceiverClientManager clientManager = new XceiverClientManager(conf)) {
      ContainerWithPipeline container1 = storageContainerLocationClient
          .allocateContainer(HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.THREE, OzoneConsts.OZONE);
      XceiverClientSpi xceiverClient = clientManager
          .acquireClient(container1.getPipeline());
      try {
        assertEquals(1, xceiverClient.getRefcount());
        assertEquals(container1.getPipeline(), xceiverClient.getPipeline());
        Pipeline pipeline = xceiverClient.getPipeline();
        XceiverClientRatis ratisClient = (XceiverClientRatis) xceiverClient;
        long containerId = container1.getContainerInfo().getContainerID();
        XceiverClientReply reply = xceiverClient.sendCommandAsync(
            ContainerTestHelper.getCreateContainerRequest(containerId,
                xceiverClient.getPipeline()));
        reply.getResponse().get();
        assertEquals(3, ratisClient.getCommitInfoMap().size());
        List<Pipeline> pipelineList = new ArrayList<>();
        pipelineList.add(pipeline);
        TestHelper.waitForPipelineClose(pipelineList, cluster);
        // just watch for a log index which in not updated in the commitInfo Map
        // as well as there is no logIndex generate in Ratis.
        // The basic idea here is just to test if its throws an exception.
        final Exception e = assertThrows(Exception.class,
            () -> xceiverClient.watchForCommit(reply.getLogIndex() + RandomUtils.secure().randomInt(0, 100) + 10)
                .get());
        assertInstanceOf(GroupMismatchException.class, HddsClientUtils.checkForException(e));
      } finally {
        clientManager.releaseClient(xceiverClient, false);
      }
    }
  }

  private OzoneOutputStream createKey(String keyName, ReplicationType type,
      long size) throws Exception {
    return TestHelper
        .createKey(keyName, type, size, objectStore, volumeName, bucketName);
  }

  private void validateData(String keyName, byte[] data) throws Exception {
    TestHelper
        .validateData(keyName, data, objectStore, volumeName, bucketName);
  }
}
