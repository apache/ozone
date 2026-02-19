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

package org.apache.hadoop.ozone.client.rpc;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.RatisTestHelper;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.ContainerStateMachine;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests delete key operation with inadequate datanodes.
 */
public class TestDeleteWithInAdequateDN {

  private static MiniOzoneCluster cluster;
  private static OzoneClient client;
  private static ObjectStore objectStore;
  private static String volumeName;
  private static String bucketName;
  private static XceiverClientManager xceiverClientManager;
  private static final int FACTOR_THREE_PIPELINE_COUNT = 1;

  @BeforeAll
  public static void init() throws Exception {
    final int numOfDatanodes = 3;

    OzoneConfiguration conf = new OzoneConfiguration();

    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    conf.setInt(ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT, numOfDatanodes + FACTOR_THREE_PIPELINE_COUNT);
    // Make the stale, dead and server failure timeout higher so that a dead
    // node is not detecte at SCM as well as the pipeline close action
    // never gets initiated early at Datanode in the test.
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 1000,
        TimeUnit.SECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL, 2000,
        TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_PIPELINE_DESTROY_TIMEOUT, 1000,
        TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_PIPELINE_CREATION_INTERVAL, 1000,
        TimeUnit.SECONDS);

    DatanodeRatisServerConfig ratisServerConfig =
        conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setNoLeaderTimeout(Duration.ofSeconds(1000));
    ratisServerConfig.setRequestTimeOut(Duration.ofSeconds(3));
    ratisServerConfig.setWatchTimeOut(Duration.ofSeconds(3));
    conf.setFromObject(ratisServerConfig);

    RatisClientConfig.RaftConfig raftClientConfig =
        conf.getObject(RatisClientConfig.RaftConfig.class);
    raftClientConfig.setRpcRequestTimeout(Duration.ofSeconds(3));
    raftClientConfig.setRpcWatchRequestTimeout(Duration.ofSeconds(10));
    conf.setFromObject(raftClientConfig);

    conf.setTimeDuration(OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL,
        1, TimeUnit.SECONDS);

    ScmConfig scmConfig = conf.getObject(ScmConfig.class);
    scmConfig.setBlockDeletionInterval(Duration.ofSeconds(1));
    conf.setFromObject(scmConfig);

    DatanodeConfiguration datanodeConfiguration = conf.getObject(
        DatanodeConfiguration.class);
    datanodeConfiguration.setBlockDeletionInterval(Duration.ofMillis(100));
    conf.setFromObject(datanodeConfiguration);

    RatisClientConfig ratisClientConfig =
        conf.getObject(RatisClientConfig.class);
    ratisClientConfig.setWriteRequestTimeout(Duration.ofSeconds(30));
    ratisClientConfig.setWatchRequestTimeout(Duration.ofSeconds(30));
    conf.setFromObject(ratisClientConfig);

    conf.setQuietMode(false);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numOfDatanodes)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(THREE, 60000);
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    xceiverClientManager = new XceiverClientManager(conf);
    volumeName = "testcontainerstatemachinefailures";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (xceiverClientManager != null) {
      xceiverClientManager.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * The test simulates an inadequate DN scenario by first writing key thereby
   * creating a container on 3 dns of the cluster. Then, a dn is shutdown and a
   * close container cmd gets issued so that in the leader and the alive
   * follower, container gets closed. And then, key is deleted and the node is
   * started up again so that it rejoins the ring and starts applying the
   * transaction from where it left by fetching the entries from the leader.
   * Until and unless this follower catches up and its replica gets closed, the
   * data is not deleted from any of the nodes which have the closed replica.
   */
  @Test
  void testDeleteKeyWithInAdequateDN() throws Exception {
    String keyName = "ratis";
    OzoneOutputStream key =
        objectStore.getVolume(volumeName).getBucket(bucketName)
            .createKey(keyName, 0,
                ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
                    ReplicationFactor.THREE), new HashMap<>());
    byte[] testData = "ratis".getBytes(UTF_8);
    // First write and flush creates a container in the datanode
    key.write(testData);
    key.flush();

    KeyOutputStream groupOutputStream = (KeyOutputStream) key.getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
        groupOutputStream.getLocationInfoList();
    Assumptions.assumeTrue(1 == locationInfoList.size(),
        "Expected exactly a single location, but got: " +
            locationInfoList.size());
    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);
    long containerID = omKeyLocationInfo.getContainerID();
    // A container is created on the datanode. Now figure out a follower node to
    // kill.
    HddsDatanodeService follower = null;
    HddsDatanodeService leader = null;

    List<Pipeline> pipelineList =
        cluster.getStorageContainerManager().getPipelineManager()
            .getPipelines(RatisReplicationConfig.getInstance(THREE));
    Assumptions.assumeTrue(!pipelineList.isEmpty());
    Pipeline pipeline = pipelineList.get(0);
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      if (RatisTestHelper.isRatisFollower(dn, pipeline)) {
        follower = dn;
      } else if (RatisTestHelper.isRatisLeader(dn, pipeline)) {
        leader = dn;
      }
    }
    assertNotNull(follower);
    assertNotNull(leader);
    //ensure that the chosen follower is still a follower
    Assumptions.assumeTrue(RatisTestHelper.isRatisFollower(follower, pipeline));
    // shutdown the  follower node
    cluster.shutdownHddsDatanode(follower.getDatanodeDetails());
    key.write(testData);
    key.close();

    // now move the container to the closed on the datanode.
    ContainerProtos.ContainerCommandRequestProto.Builder request =
        ContainerProtos.ContainerCommandRequestProto.newBuilder();
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    request.setCmdType(ContainerProtos.Type.CloseContainer);
    request.setContainerID(containerID);
    request.setCloseContainer(
        ContainerProtos.CloseContainerRequestProto.getDefaultInstance());
    XceiverClientSpi xceiverClient =
        xceiverClientManager.acquireClient(pipeline);
    try {
      xceiverClient.sendCommand(request.build());
    } finally {
      xceiverClientManager.releaseClient(xceiverClient, false);
    }

    ContainerStateMachine stateMachine =
        (ContainerStateMachine) RatisTestHelper
            .getStateMachine(leader, pipeline);
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName).setBucketName(bucketName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setKeyName(keyName).build();
    OmKeyInfo info = cluster.getOzoneManager().lookupKey(keyArgs);
    BlockID blockID = info.getKeyLocationVersions().get(0)
        .getLocationList().get(0).getBlockID();
    OzoneContainer ozoneContainer;
    final DatanodeStateMachine dnStateMachine =
        leader.getDatanodeStateMachine();
    ozoneContainer = dnStateMachine.getContainer();
    KeyValueHandler keyValueHandler =
        (KeyValueHandler) ozoneContainer.getDispatcher()
            .getHandler(ContainerProtos.ContainerType.KeyValueContainer);
    Container container =
        ozoneContainer.getContainerSet().getContainer(blockID.getContainerID());
    BlockData blockData =
        keyValueHandler.getBlockManager().getBlock(container, blockID);
    //cluster.getOzoneManager().deleteKey(keyArgs);
    client.getObjectStore().getVolume(volumeName).getBucket(bucketName).
        deleteKey("ratis");
    OzoneTestUtils.flushAndWaitForDeletedBlockLog(cluster.getStorageContainerManager());
    // make sure the chunk was never deleted on the leader even though
    // deleteBlock handler is invoked

    for (ContainerProtos.ChunkInfo chunkInfo : blockData.getChunks()) {
      keyValueHandler.getChunkManager()
          .readChunk(container, blockID, ChunkInfo.getFromProtoBuf(chunkInfo),
              null);
    }
    long numReadStateMachineOps =
        stateMachine.getMetrics().getNumReadStateMachineOps();
    assertEquals(0, stateMachine.getMetrics().getNumReadStateMachineFails());
    stateMachine.evictStateMachineCache();
    cluster.restartHddsDatanode(follower.getDatanodeDetails(), false);
    // wait for the raft server to come up and join the ratis ring
    Thread.sleep(10000);

    // Make sure the readStateMachine call got triggered after the follower
    // caught up
    assertThat(stateMachine.getMetrics().getNumReadStateMachineOps())
        .isGreaterThan(numReadStateMachineOps);
    assertEquals(0, stateMachine.getMetrics().getNumReadStateMachineFails());
    // wait for the chunk to get deleted now
    Thread.sleep(10000);
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      keyValueHandler =
          (KeyValueHandler) dn.getDatanodeStateMachine().getContainer()
              .getDispatcher()
              .getHandler(ContainerProtos.ContainerType.KeyValueContainer);
      // make sure the chunk is now deleted on the all dns
      KeyValueHandler finalKeyValueHandler = keyValueHandler;
      StorageContainerException e = assertThrows(StorageContainerException.class, () -> {
        for (ContainerProtos.ChunkInfo chunkInfo : blockData.getChunks()) {
          finalKeyValueHandler.getChunkManager().readChunk(container, blockID,
                  ChunkInfo.getFromProtoBuf(chunkInfo), null);
        }
      });
      assertSame(ContainerProtos.Result.UNABLE_TO_FIND_CHUNK, e.getResult());
    }
  }
}
