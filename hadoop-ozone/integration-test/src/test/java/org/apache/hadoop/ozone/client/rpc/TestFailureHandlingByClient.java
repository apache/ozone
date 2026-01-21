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
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.BlockOutputStreamEntry;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.apache.ratis.proto.RaftProtos;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests Exception handling by Ozone Client.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestFailureHandlingByClient {

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private OzoneClient client;
  private ObjectStore objectStore;
  private int chunkSize;
  private int blockSize;
  private String volumeName;
  private String bucketName;
  private String keyString;
  private final List<DatanodeDetails> restartDataNodes = new ArrayList<>();

  @BeforeAll
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    chunkSize = (int) OzoneConsts.MB;
    blockSize = 4 * chunkSize;
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 100, TimeUnit.SECONDS);

    RatisClientConfig ratisClientConfig =
        conf.getObject(RatisClientConfig.class);
    ratisClientConfig.setWriteRequestTimeout(Duration.ofSeconds(30));
    ratisClientConfig.setWatchRequestTimeout(Duration.ofSeconds(30));
    conf.setFromObject(ratisClientConfig);

    conf.setTimeDuration(
        OzoneConfigKeys.HDDS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY,
        1, TimeUnit.SECONDS);
    conf.setBoolean(
        OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY, true);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 2);
    conf.setInt(ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT, 15);
    DatanodeRatisServerConfig ratisServerConfig =
        conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setRequestTimeOut(Duration.ofSeconds(3));
    ratisServerConfig.setWatchTimeOut(Duration.ofSeconds(3));
    conf.setFromObject(ratisServerConfig);

    RatisClientConfig.RaftConfig raftClientConfig =
        conf.getObject(RatisClientConfig.RaftConfig.class);
    raftClientConfig.setRpcRequestTimeout(Duration.ofSeconds(3));
    raftClientConfig.setRpcWatchRequestTimeout(Duration.ofSeconds(3));
    conf.setFromObject(raftClientConfig);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setStreamBufferFlushDelay(false);
    conf.setFromObject(clientConfig);

    conf.setQuietMode(false);
    conf.setClass(NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        StaticMapping.class, DNSToSwitchMapping.class);
    StaticMapping.addNodeToRack(NetUtils.normalizeHostNames(
        Collections.singleton(HddsUtils.getHostName(conf))).get(0),
        "/rack1");
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(10).build();
    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    keyString = UUID.randomUUID().toString();
    volumeName = "datanodefailurehandlingtest";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  @BeforeEach
  public void restartDownDataNodes() throws Exception {
    if (restartDataNodes.isEmpty()) {
      return;
    }
    for (DatanodeDetails dataNode : restartDataNodes) {
      cluster.restartHddsDatanode(dataNode, false);
    }
    restartDataNodes.clear();
    cluster.waitForClusterToBeReady();
  }

  @AfterAll
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testBlockWritesWithDnFailures() throws Exception {
    String keyName = UUID.randomUUID().toString();
    byte[] data = ContainerTestHelper.getFixedLengthString(
        keyString, 2 * chunkSize + chunkSize / 2).getBytes(UTF_8);
    try (OzoneOutputStream key = createKey(keyName, RATIS, 0)) {
      key.write(data);

      // get the name of a valid container
      KeyOutputStream groupOutputStream =
          assertInstanceOf(KeyOutputStream.class, key.getOutputStream());
      // assert that the exclude list's expire time equals to
      // default value 600000 ms in OzoneClientConfig.java
      assertEquals(groupOutputStream.getExcludeList().getExpiryTime(), 600000);
      List<OmKeyLocationInfo> locationInfoList =
          groupOutputStream.getLocationInfoList();
      assertEquals(1, locationInfoList.size());
      long containerId = locationInfoList.get(0).getContainerID();
      ContainerInfo container = cluster.getStorageContainerManager()
          .getContainerManager()
          .getContainer(ContainerID.valueOf(containerId));
      Pipeline pipeline =
          cluster.getStorageContainerManager().getPipelineManager()
              .getPipeline(container.getPipelineID());
      List<DatanodeDetails> datanodes = pipeline.getNodes();
      cluster.shutdownHddsDatanode(datanodes.get(0));
      cluster.shutdownHddsDatanode(datanodes.get(1));
      restartDataNodes.add(datanodes.get(0));
      restartDataNodes.add(datanodes.get(1));
      // The write will fail but exception will be handled and length will be
      // updated correctly in OzoneManager once the steam is closed
    }
    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setKeyName(keyName)
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);

    assertEquals(data.length, keyInfo.getDataSize());
    validateData(keyName, data);

    // Verify that the block information is updated correctly in the DB on
    // failures
    testBlockCountOnFailures(keyInfo);
  }

  /**
   * Test whether blockData and Container metadata (block count and used
   * bytes) is updated correctly when there is a write failure.
   * We can combine this test with {@link #testBlockWritesWithDnFailures()}
   * as that test also simulates a write failure and client writes failed
   * chunk writes to a new block.
   */
  private void testBlockCountOnFailures(OmKeyInfo omKeyInfo) throws Exception {
    // testBlockWritesWithDnFailures writes chunkSize*2.5 size of data into
    // KeyOutputStream. But before closing the outputStream, 2 of the DNs in
    // the pipeline being written to are closed. So the key will be written
    // to 2 blocks as atleast the last 0.5 chunk would not be committed to the
    // first block before the stream is closed.
    /**
     * There are 3 possible scenarios:
     * 1. Block1 has 2 chunks and OMKeyInfo also has 2 chunks against this block
     *    => Block2 should have 1 chunk
     *    (2 chunks were written to Block1, committed and acknowledged by
     *    CommitWatcher)
     * 2. Block1 has 1 chunk and OMKeyInfo has 1 chunk against this block
     *    => Block2 should have 2 chunks
     *    (Possibly 2 chunks were written but only 1 was committed to the
     *    block)
     * 3. Block1 has 2 chunks but OMKeyInfo has only 1 chunk against this block
     *    => Block2 should have 2 chunks
     *    (This happens when the 2nd chunk has been committed to Block1 but
     *    not acknowledged by CommitWatcher before pipeline shutdown)
     */

    // Get information about the first and second block (in different pipelines)
    List<OmKeyLocationInfo> locationList = omKeyInfo.getLatestVersionLocations()
        .getLocationList();
    long containerId1 = locationList.get(0).getContainerID();
    List<DatanodeDetails> block1DNs = locationList.get(0).getPipeline()
        .getNodes();
    long containerId2 = locationList.get(1).getContainerID();
    List<DatanodeDetails> block2DNs = locationList.get(1).getPipeline()
        .getNodes();


    int block2ExpectedChunkCount;
    if (locationList.get(0).getLength() == 2L * chunkSize) {
      // Scenario 1
      block2ExpectedChunkCount = 1;
    } else {
      // Scenario 2
      block2ExpectedChunkCount = 2;
    }

    // For the first block, first 2 DNs in the pipeline are shutdown (to
    // simulate a failure). It should have 1 or 2 chunks (depending on
    // whether the DN CommitWatcher successfully acknowledged the 2nd chunk
    // write or not). The 3rd chunk would not exist on the first pipeline as
    // the pipeline would be closed before the last 0.5 chunk was committed
    // to the block.
    KeyValueContainerData containerData1 =
        ((KeyValueContainer) cluster.getHddsDatanode(block1DNs.get(2))
            .getDatanodeStateMachine().getContainer().getContainerSet()
            .getContainer(containerId1)).getContainerData();
    try (DBHandle containerDb1 = BlockUtils.getDB(containerData1, conf)) {
      BlockData blockData1 = containerDb1.getStore().getBlockDataTable().get(
          containerData1.getBlockKey(locationList.get(0).getBlockID()
              .getLocalID()));
      // The first Block could have 1 or 2 chunkSize of data
      int block1NumChunks = blockData1.getChunks().size();
      assertThat(block1NumChunks).isGreaterThanOrEqualTo(1);

      assertEquals((long) chunkSize * block1NumChunks, blockData1.getSize());
    }

    // Verify that the second block has the remaining 0.5*chunkSize of data
    KeyValueContainerData containerData2 =
        ((KeyValueContainer) cluster.getHddsDatanode(block2DNs.get(0))
            .getDatanodeStateMachine().getContainer().getContainerSet()
            .getContainer(containerId2)).getContainerData();
    try (DBHandle containerDb2 = BlockUtils.getDB(containerData2, conf)) {
      BlockData blockData2 = containerDb2.getStore().getBlockDataTable().get(
          containerData2.getBlockKey(locationList.get(1).getBlockID()
              .getLocalID()));
      // The second Block should have 0.5 chunkSize of data
      assertEquals(block2ExpectedChunkCount,
          blockData2.getChunks().size());
      int expectedBlockSize;
      if (block2ExpectedChunkCount == 1) {
        expectedBlockSize = chunkSize / 2;
      } else {
        expectedBlockSize = chunkSize + chunkSize / 2;
      }
      assertEquals(expectedBlockSize, blockData2.getSize());
    }
  }

  @Test
  public void testWriteSmallFile() throws Exception {
    String keyName = UUID.randomUUID().toString();
    String data = ContainerTestHelper
        .getFixedLengthString(keyString,  chunkSize / 2);

    BlockID blockId;
    try (OzoneOutputStream key = createKey(keyName, RATIS, 0)) {
      key.write(data.getBytes(UTF_8));
      // get the name of a valid container
      KeyOutputStream keyOutputStream =
          assertInstanceOf(KeyOutputStream.class, key.getOutputStream());
      List<OmKeyLocationInfo> locationInfoList =
          keyOutputStream.getLocationInfoList();
      long containerId = locationInfoList.get(0).getContainerID();
      blockId = locationInfoList.get(0).getBlockID();
      ContainerInfo container =
          cluster.getStorageContainerManager().getContainerManager()
              .getContainer(ContainerID.valueOf(containerId));
      Pipeline pipeline =
          cluster.getStorageContainerManager().getPipelineManager()
              .getPipeline(container.getPipelineID());
      List<DatanodeDetails> datanodes = pipeline.getNodes();

      cluster.shutdownHddsDatanode(datanodes.get(0));
      cluster.shutdownHddsDatanode(datanodes.get(1));
      restartDataNodes.add(datanodes.get(0));
      restartDataNodes.add(datanodes.get(1));
    }

    // this will throw AlreadyClosedException and and current stream
    // will be discarded and write a new block
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setKeyName(keyName)
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);

    // Make sure a new block is written
    assertNotEquals(
        keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly().get(0)
            .getBlockID(), blockId);
    assertEquals(data.getBytes(UTF_8).length, keyInfo.getDataSize());
    validateData(keyName, data.getBytes(UTF_8));
  }

  @Test
  public void testContainerExclusionWithClosedContainerException()
      throws Exception {
    String keyName = UUID.randomUUID().toString();
    String data = ContainerTestHelper
        .getFixedLengthString(keyString,  chunkSize);

    BlockID blockId;
    try (OzoneOutputStream key = createKey(keyName, RATIS, blockSize)) {
      // get the name of a valid container
      KeyOutputStream keyOutputStream =
          assertInstanceOf(KeyOutputStream.class, key.getOutputStream());
      List<BlockOutputStreamEntry> streamEntryList =
          keyOutputStream.getStreamEntries();

      // Assert that 1 block will be preallocated
      assertEquals(1, streamEntryList.size());
      key.write(data.getBytes(UTF_8));
      key.flush();
      long containerId = streamEntryList.get(0).getBlockID().getContainerID();
      blockId = streamEntryList.get(0).getBlockID();
      List<Long> containerIdList = new ArrayList<>();
      containerIdList.add(containerId);

      // below check will assert if the container does not get closed
      TestHelper
          .waitForContainerClose(cluster, containerIdList.toArray(new Long[0]));

      // This write will hit ClosedContainerException and this container should
      // will be added in the excludelist
      key.write(data.getBytes(UTF_8));
      key.flush();

      assertThat(keyOutputStream.getExcludeList().getContainerIds())
          .contains(ContainerID.valueOf(containerId));
      assertThat(keyOutputStream.getExcludeList().getDatanodes()).isEmpty();
      assertThat(keyOutputStream.getExcludeList().getPipelineIds()).isEmpty();

      // The close will just write to the buffer
    }

    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setKeyName(keyName)
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);

    // Make sure a new block is written
    assertNotEquals(
        keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly().get(0)
            .getBlockID(), blockId);
    assertEquals(2L * data.getBytes(UTF_8).length, keyInfo.getDataSize());
    validateData(keyName, data.concat(data).getBytes(UTF_8));
  }

  @ParameterizedTest
  @EnumSource(value = RaftProtos.ReplicationLevel.class, names = {"MAJORITY_COMMITTED", "ALL_COMMITTED"})
  @Flaky("HDDS-13972")
  public void testDatanodeExclusionWithMajorityCommit(RaftProtos.ReplicationLevel type) throws Exception {
    OzoneConfiguration localConfig = new OzoneConfiguration(conf);
    RatisClientConfig ratisClientConfig = localConfig.getObject(RatisClientConfig.class);
    ratisClientConfig.setWatchType(type.toString());
    localConfig.setFromObject(ratisClientConfig);
    OzoneClient localClient = OzoneClientFactory.getRpcClient(localConfig);
    ObjectStore localObjectStore = localClient.getObjectStore();

    String keyName = UUID.randomUUID().toString();
    String data = ContainerTestHelper
        .getFixedLengthString(keyString, chunkSize);

    BlockID blockId;
    try (OzoneOutputStream key = TestHelper.createKey(keyName, RATIS, blockSize, localObjectStore, volumeName,
        bucketName)) {
      // get the name of a valid container
      KeyOutputStream keyOutputStream =
          assertInstanceOf(KeyOutputStream.class, key.getOutputStream());
      List<BlockOutputStreamEntry> streamEntryList =
          keyOutputStream.getStreamEntries();

      // Assert that 1 block will be preallocated
      assertEquals(1, streamEntryList.size());
      key.write(data.getBytes(UTF_8));
      key.flush();
      long containerId = streamEntryList.get(0).getBlockID().getContainerID();
      blockId = streamEntryList.get(0).getBlockID();
      ContainerInfo container =
          cluster.getStorageContainerManager().getContainerManager()
              .getContainer(ContainerID.valueOf(containerId));
      Pipeline pipeline =
          cluster.getStorageContainerManager().getPipelineManager()
              .getPipeline(container.getPipelineID());
      List<DatanodeDetails> datanodes = pipeline.getNodes();

      // shutdown 1 datanode. This will make sure the 2 way commit happens for
      // next write ops.
      cluster.shutdownHddsDatanode(datanodes.get(0));
      restartDataNodes.add(datanodes.get(0));

      HddsDatanodeService hddsDatanode = cluster.getHddsDatanode(datanodes.get(0));
      GenericTestUtils.waitFor(hddsDatanode::isStopped, 1000, 30000);

      key.write(data.getBytes(UTF_8));
      key.write(data.getBytes(UTF_8));
      key.flush();

      if (type == RaftProtos.ReplicationLevel.ALL_COMMITTED) {
        assertThat(keyOutputStream.getExcludeList().getDatanodes())
            .contains(datanodes.get(0));
      }
      assertThat(keyOutputStream.getExcludeList().getContainerIds()).isEmpty();
      assertThat(keyOutputStream.getExcludeList().getPipelineIds()).isEmpty();
      // The close will just write to the buffer
    }

    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setKeyName(keyName)
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);

    // Make sure a new block is written
    assertNotEquals(
        keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly().get(0)
            .getBlockID(), blockId);
    assertEquals(3L * data.getBytes(UTF_8).length, keyInfo.getDataSize());
    TestHelper.validateData(keyName, data.concat(data).concat(data).getBytes(UTF_8),
            localObjectStore, volumeName, bucketName);
    IOUtils.closeQuietly(localClient);
  }

  @Test
  public void testPipelineExclusionWithPipelineFailure() throws Exception {
    String keyName = UUID.randomUUID().toString();
    String data = ContainerTestHelper
        .getFixedLengthString(keyString,  chunkSize);
    BlockID blockId;
    try (OzoneOutputStream key = createKey(keyName, RATIS, blockSize)) {
      // get the name of a valid container
      KeyOutputStream keyOutputStream =
          assertInstanceOf(KeyOutputStream.class, key.getOutputStream());
      List<BlockOutputStreamEntry> streamEntryList =
          keyOutputStream.getStreamEntries();

      // Assert that 1 block will be preallocated
      assertEquals(1, streamEntryList.size());
      key.write(data.getBytes(UTF_8));
      key.flush();
      long containerId = streamEntryList.get(0).getBlockID().getContainerID();
      blockId = streamEntryList.get(0).getBlockID();
      ContainerInfo container =
          cluster.getStorageContainerManager().getContainerManager()
              .getContainer(ContainerID.valueOf(containerId));
      Pipeline pipeline =
          cluster.getStorageContainerManager().getPipelineManager()
              .getPipeline(container.getPipelineID());
      List<DatanodeDetails> datanodes = pipeline.getNodes();

      // Two nodes, next write will hit AlreadyClosedException , the pipeline
      // will be added in the exclude list
      cluster.shutdownHddsDatanode(datanodes.get(0));
      cluster.shutdownHddsDatanode(datanodes.get(1));
      restartDataNodes.add(datanodes.get(0));
      restartDataNodes.add(datanodes.get(1));

      key.write(data.getBytes(UTF_8));
      key.write(data.getBytes(UTF_8));
      key.flush();
      assertThat(keyOutputStream.getExcludeList().getPipelineIds())
          .contains(pipeline.getId());
      assertThat(keyOutputStream.getExcludeList().getContainerIds()).isEmpty();
      assertThat(keyOutputStream.getExcludeList().getDatanodes()).isEmpty();
      // The close will just write to the buffer
    }

    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setKeyName(keyName)
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);

    // Make sure a new block is written
    assertNotEquals(
        keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly().get(0)
            .getBlockID(), blockId);
    assertEquals(3L * data.getBytes(UTF_8).length, keyInfo.getDataSize());
    validateData(keyName, data.concat(data).concat(data).getBytes(UTF_8));
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
