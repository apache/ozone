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
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.ContainerStateMachine;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.StatemachineImplTestUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests the containerStateMachine failure handling.
 */

public class TestValidateBCSIDOnRestart {
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static OzoneClient client;
  private static ObjectStore objectStore;
  private static String volumeName;
  private static String bucketName;

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setStreamBufferFlushDelay(false);
    conf.setFromObject(clientConfig);

    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 200, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 10, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_PIPELINE_DESTROY_TIMEOUT, 1,
        TimeUnit.SECONDS);

    RatisClientConfig ratisClientConfig =
        conf.getObject(RatisClientConfig.class);
    ratisClientConfig.setWriteRequestTimeout(Duration.ofSeconds(10));
    ratisClientConfig.setWatchRequestTimeout(Duration.ofSeconds(10));
    conf.setFromObject(ratisClientConfig);

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

    cluster =
        MiniOzoneCluster.newBuilder(conf).setNumDatanodes(2)
            .build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.ONE, 60000);
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    volumeName = "testcontainerstatemachinefailures";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testValidateBCSIDOnDnRestart() throws Exception {
    OzoneOutputStream key =
        objectStore.getVolume(volumeName).getBucket(bucketName)
            .createKey("ratis", 1024,
                ReplicationConfig.fromTypeAndFactor(
                    ReplicationType.RATIS,
                    ReplicationFactor.ONE), new HashMap<>());
    // First write and flush creates a container in the datanode
    key.write("ratis".getBytes(UTF_8));
    key.flush();
    key.write("ratis".getBytes(UTF_8));
    KeyOutputStream groupOutputStream = (KeyOutputStream) key.getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
        groupOutputStream.getLocationInfoList();
    assertEquals(1, locationInfoList.size());
    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);
    HddsDatanodeService dn = TestHelper.getDatanodeService(omKeyLocationInfo,
        cluster);
    ContainerData containerData =
        TestHelper.getDatanodeService(omKeyLocationInfo, cluster)
            .getDatanodeStateMachine()
            .getContainer().getContainerSet()
            .getContainer(omKeyLocationInfo.getContainerID())
            .getContainerData();
    KeyValueContainerData keyValueContainerData =
        assertInstanceOf(KeyValueContainerData.class, containerData);
    key.close();

    long containerID = omKeyLocationInfo.getContainerID();
    int index = cluster.getHddsDatanodeIndex(dn.getDatanodeDetails());
    // delete the container db file
    FileUtil.fullyDelete(new File(keyValueContainerData.getContainerPath()));

    HddsDatanodeService dnService = cluster.getHddsDatanodes().get(index);
    OzoneContainer ozoneContainer =
        dnService.getDatanodeStateMachine()
            .getContainer();
    ozoneContainer.getContainerSet().removeContainer(containerID);
    ContainerStateMachine stateMachine =
        (ContainerStateMachine) TestHelper.getStateMachine(cluster.
                getHddsDatanodes().get(index),
            omKeyLocationInfo.getPipeline());
    SimpleStateMachineStorage storage =
        (SimpleStateMachineStorage) stateMachine.getStateMachineStorage();
    stateMachine.takeSnapshot();
    final Path parentPath = StatemachineImplTestUtil.findLatestSnapshot(storage)
        .getFile().getPath();
    stateMachine.buildMissingContainerSet(parentPath.toFile());
    // Since the snapshot threshold is set to 1, since there are
    // applyTransactions, we should see snapshots
    assertThat(parentPath.getParent().toFile().listFiles().length).isGreaterThan(0);

    // make sure the missing containerSet is not empty
    HddsDispatcher dispatcher = (HddsDispatcher) ozoneContainer.getDispatcher();
    assertThat(dispatcher.getMissingContainerSet()).isNotEmpty();
    assertThat(dispatcher.getMissingContainerSet()).contains(containerID);
    // write a new key
    key = objectStore.getVolume(volumeName).getBucket(bucketName)
        .createKey("ratis", 1024,
            ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
                ReplicationFactor.ONE), new HashMap<>());
    // First write and flush creates a container in the datanode
    key.write("ratis1".getBytes(UTF_8));
    key.flush();
    groupOutputStream = (KeyOutputStream) key.getOutputStream();
    locationInfoList = groupOutputStream.getLocationInfoList();
    assertEquals(1, locationInfoList.size());
    omKeyLocationInfo = locationInfoList.get(0);
    key.close();
    containerID = omKeyLocationInfo.getContainerID();
    dn = TestHelper.getDatanodeService(omKeyLocationInfo,
        cluster);
    containerData = dn.getDatanodeStateMachine()
        .getContainer().getContainerSet()
        .getContainer(omKeyLocationInfo.getContainerID())
        .getContainerData();
    keyValueContainerData = assertInstanceOf(KeyValueContainerData.class, containerData);
    try (DBHandle db = BlockUtils.getDB(keyValueContainerData, conf)) {

      // modify the bcsid for the container in the ROCKS DB thereby inducing
      // corruption
      db.getStore().getMetadataTable()
          .put(keyValueContainerData.getBcsIdKey(), 0L);
    }
    // after the restart, there will be a mismatch in BCSID of what is recorded
    // in the and what is there in RockSDB and hence the container would be
    // marked unhealthy
    index = cluster.getHddsDatanodeIndex(dn.getDatanodeDetails());
    cluster.restartHddsDatanode(dn.getDatanodeDetails(), true);
    // Make sure the container is marked unhealthy
    assertSame(cluster.getHddsDatanodes().get(index)
            .getDatanodeStateMachine()
            .getContainer().getContainerSet().getContainer(containerID)
            .getContainerState(),
        ContainerProtos.ContainerDataProto.State.UNHEALTHY);
  }
}
