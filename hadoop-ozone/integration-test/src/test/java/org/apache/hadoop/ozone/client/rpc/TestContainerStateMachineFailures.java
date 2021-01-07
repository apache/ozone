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

package org.apache.hadoop.ozone.client.rpc;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerNotOpenException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.ContainerStateMachine;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.UNHEALTHY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import static org.hamcrest.core.Is.is;
import org.junit.AfterClass;
import org.junit.Assert;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests the containerStateMachine failure handling.
 */

public class TestContainerStateMachineFailures {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static OzoneClient client;
  private static ObjectStore objectStore;
  private static String volumeName;
  private static String bucketName;
  private static XceiverClientManager xceiverClientManager;

  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setStreamBufferFlushDelay(false);
    conf.setFromObject(clientConfig);

    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 30, TimeUnit.SECONDS);
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

    conf.setLong(OzoneConfigKeys.DFS_RATIS_SNAPSHOT_THRESHOLD_KEY, 1);
    conf.setQuietMode(false);
    cluster =
        MiniOzoneCluster.newBuilder(conf).setNumDatanodes(10).setHbInterval(200)
            .build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.ONE, 60000);
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    xceiverClientManager = new XceiverClientManager(conf);
    volumeName = "testcontainerstatemachinefailures";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testContainerStateMachineFailures() throws Exception {
    OzoneOutputStream key =
            objectStore.getVolume(volumeName).getBucket(bucketName)
                    .createKey("ratis", 1024, ReplicationType.RATIS,
                            ReplicationFactor.ONE, new HashMap<>());
    byte[] testData = "ratis".getBytes();
    // First write and flush creates a container in the datanode
    key.write(testData);
    key.flush();
    key.write(testData);
    KeyOutputStream groupOutputStream =
            (KeyOutputStream) key.getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
            groupOutputStream.getLocationInfoList();
    Assert.assertEquals(1, locationInfoList.size());
    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);
    HddsDatanodeService dn = TestHelper.getDatanodeService(omKeyLocationInfo,
            cluster);
    // delete the container dir
    FileUtil.fullyDelete(new File(dn.getDatanodeStateMachine()
                    .getContainer().getContainerSet()
                    .getContainer(omKeyLocationInfo.getContainerID()).
                    getContainerData().getContainerPath()));
    try {
      // there is only 1 datanode in the pipeline, the pipeline will be closed
      // and allocation to new pipeline will fail as there is no other dn in
      // the cluster
      key.close();
    } catch (IOException ioe) {
    }
    long containerID = omKeyLocationInfo.getContainerID();

    // Make sure the container is marked unhealthy
    Assert.assertTrue(
            dn.getDatanodeStateMachine()
                    .getContainer().getContainerSet()
                    .getContainer(containerID)
                    .getContainerState()
                    == ContainerProtos.ContainerDataProto.State.UNHEALTHY);
    OzoneContainer ozoneContainer;

    // restart the hdds datanode, container should not in the regular set
    OzoneConfiguration config = dn.getConf();
    final String dir = config.get(OzoneConfigKeys.
            DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR)
            + UUID.randomUUID();
    config.set(OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR, dir);
    int index = cluster.getHddsDatanodeIndex(dn.getDatanodeDetails());
    cluster.restartHddsDatanode(dn.getDatanodeDetails(), false);
    ozoneContainer = cluster.getHddsDatanodes().get(index)
            .getDatanodeStateMachine().getContainer();
    Assert.assertNull(ozoneContainer.getContainerSet().
                    getContainer(containerID));
  }

  @Test
  public void testUnhealthyContainer() throws Exception {
    OzoneOutputStream key =
            objectStore.getVolume(volumeName).getBucket(bucketName)
                    .createKey("ratis", 1024, ReplicationType.RATIS,
                            ReplicationFactor.ONE, new HashMap<>());
    // First write and flush creates a container in the datanode
    key.write("ratis".getBytes());
    key.flush();
    key.write("ratis".getBytes());
    KeyOutputStream groupOutputStream = (KeyOutputStream) key
            .getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
            groupOutputStream.getLocationInfoList();
    Assert.assertEquals(1, locationInfoList.size());
    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);
    HddsDatanodeService dn = TestHelper.getDatanodeService(omKeyLocationInfo,
            cluster);
    ContainerData containerData =
            dn.getDatanodeStateMachine()
                    .getContainer().getContainerSet()
                    .getContainer(omKeyLocationInfo.getContainerID())
                    .getContainerData();
    Assert.assertTrue(containerData instanceof KeyValueContainerData);
    KeyValueContainerData keyValueContainerData =
            (KeyValueContainerData) containerData;
    // delete the container db file
    FileUtil.fullyDelete(new File(keyValueContainerData.getChunksPath()));
    try {
      // there is only 1 datanode in the pipeline, the pipeline will be closed
      // and allocation to new pipeline will fail as there is no other dn in
      // the cluster
      key.close();
    } catch (IOException ioe) {
    }

    long containerID = omKeyLocationInfo.getContainerID();

    // Make sure the container is marked unhealthy
    Assert.assertTrue(
            dn.getDatanodeStateMachine()
                    .getContainer().getContainerSet().getContainer(containerID)
                    .getContainerState()
                    == ContainerProtos.ContainerDataProto.State.UNHEALTHY);
    // Check metadata in the .container file
    File containerFile = new File(keyValueContainerData.getMetadataPath(),
            containerID + OzoneConsts.CONTAINER_EXTENSION);

    keyValueContainerData = (KeyValueContainerData) ContainerDataYaml
            .readContainerFile(containerFile);
    assertThat(keyValueContainerData.getState(), is(UNHEALTHY));

    OzoneConfiguration config = dn.getConf();
    final String dir = config.get(OzoneConfigKeys.
            DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR)
            + UUID.randomUUID();
    config.set(OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR, dir);
    int index = cluster.getHddsDatanodeIndex(dn.getDatanodeDetails());
    // restart the hdds datanode and see if the container is listed in the
    // in the missing container set and not in the regular set
    cluster.restartHddsDatanode(dn.getDatanodeDetails(), false);
    // make sure the container state is still marked unhealthy after restart
    keyValueContainerData = (KeyValueContainerData) ContainerDataYaml
            .readContainerFile(containerFile);
    assertThat(keyValueContainerData.getState(), is(UNHEALTHY));

    OzoneContainer ozoneContainer;
    HddsDatanodeService dnService = cluster.getHddsDatanodes().get(index);
    ozoneContainer = dnService
            .getDatanodeStateMachine().getContainer();
    HddsDispatcher dispatcher = (HddsDispatcher) ozoneContainer
            .getDispatcher();
    ContainerProtos.ContainerCommandRequestProto.Builder request =
            ContainerProtos.ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.CloseContainer);
    request.setContainerID(containerID);
    request.setCloseContainer(
            ContainerProtos.CloseContainerRequestProto.getDefaultInstance());
    request.setDatanodeUuid(dnService.getDatanodeDetails().getUuidString());
    Assert.assertEquals(ContainerProtos.Result.CONTAINER_UNHEALTHY,
            dispatcher.dispatch(request.build(), null)
                    .getResult());
  }

  @Test
  public void testApplyTransactionFailure() throws Exception {
    OzoneOutputStream key =
            objectStore.getVolume(volumeName).getBucket(bucketName)
                    .createKey("ratis", 1024, ReplicationType.RATIS,
                            ReplicationFactor.ONE, new HashMap<>());
    // First write and flush creates a container in the datanode
    key.write("ratis".getBytes());
    key.flush();
    key.write("ratis".getBytes());
    KeyOutputStream groupOutputStream = (KeyOutputStream) key.
            getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
            groupOutputStream.getLocationInfoList();
    Assert.assertEquals(1, locationInfoList.size());
    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);
    HddsDatanodeService dn = TestHelper.getDatanodeService(omKeyLocationInfo,
            cluster);
    int index = cluster.getHddsDatanodeIndex(dn.getDatanodeDetails());
    ContainerData containerData = dn.getDatanodeStateMachine()
                    .getContainer().getContainerSet()
                    .getContainer(omKeyLocationInfo.getContainerID())
                    .getContainerData();
    Assert.assertTrue(containerData instanceof KeyValueContainerData);
    KeyValueContainerData keyValueContainerData =
            (KeyValueContainerData) containerData;
    key.close();
    ContainerStateMachine stateMachine =
        (ContainerStateMachine) TestHelper.getStateMachine(cluster.
            getHddsDatanodes().get(index), omKeyLocationInfo.getPipeline());
    SimpleStateMachineStorage storage =
            (SimpleStateMachineStorage) stateMachine.getStateMachineStorage();
    stateMachine.takeSnapshot();
    Path parentPath = storage.findLatestSnapshot().getFile().getPath();
    // Since the snapshot threshold is set to 1, since there are
    // applyTransactions, we should see snapshots
    Assert.assertTrue(parentPath.getParent().toFile().listFiles().length > 0);
    FileInfo snapshot = storage.findLatestSnapshot().getFile();
    Assert.assertNotNull(snapshot);
    long containerID = omKeyLocationInfo.getContainerID();
    // delete the container db file
    FileUtil.fullyDelete(new File(keyValueContainerData.getContainerPath()));
    Pipeline pipeline = cluster.getStorageContainerLocationClient()
            .getContainerWithPipeline(containerID).getPipeline();
    XceiverClientSpi xceiverClient =
            xceiverClientManager.acquireClient(pipeline);
    ContainerProtos.ContainerCommandRequestProto.Builder request =
            ContainerProtos.ContainerCommandRequestProto.newBuilder();
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    request.setCmdType(ContainerProtos.Type.CloseContainer);
    request.setContainerID(containerID);
    request.setCloseContainer(
            ContainerProtos.CloseContainerRequestProto.getDefaultInstance());
    // close container transaction will fail over Ratis and will initiate
    // a pipeline close action

    try {
      xceiverClient.sendCommand(request.build());
      Assert.fail("Expected exception not thrown");
    } catch (IOException e) {
      // Exception should be thrown
    }
    // Make sure the container is marked unhealthy
    Assert.assertTrue(dn.getDatanodeStateMachine()
                    .getContainer().getContainerSet().getContainer(containerID)
                    .getContainerState()
                    == ContainerProtos.ContainerDataProto.State.UNHEALTHY);
    try {
      // try to take a new snapshot, ideally it should just fail
      stateMachine.takeSnapshot();
    } catch (IOException ioe) {
      Assert.assertTrue(ioe instanceof StateMachineException);
    }

    if (snapshot.getPath().toFile().exists()) {
      // Make sure the latest snapshot is same as the previous one
      try {
        FileInfo latestSnapshot = storage.findLatestSnapshot().getFile();
        Assert.assertTrue(snapshot.getPath().equals(latestSnapshot.getPath()));
      } catch (Throwable e) {
        Assert.assertFalse(snapshot.getPath().toFile().exists());
      }
    }
    
    // when remove pipeline, group dir including snapshot will be deleted
    LambdaTestUtils.await(5000, 500,
        () -> (!snapshot.getPath().toFile().exists()));
  }

  @Test
  public void testApplyTransactionIdempotencyWithClosedContainer()
          throws Exception {
    OzoneOutputStream key =
            objectStore.getVolume(volumeName).getBucket(bucketName)
                    .createKey("ratis", 1024, ReplicationType.RATIS,
                            ReplicationFactor.ONE, new HashMap<>());
    // First write and flush creates a container in the datanode
    key.write("ratis".getBytes());
    key.flush();
    key.write("ratis".getBytes());
    KeyOutputStream groupOutputStream = (KeyOutputStream) key.getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
            groupOutputStream.getLocationInfoList();
    Assert.assertEquals(1, locationInfoList.size());
    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);
    HddsDatanodeService dn = TestHelper.getDatanodeService(omKeyLocationInfo,
            cluster);
    ContainerData containerData = dn.getDatanodeStateMachine()
                    .getContainer().getContainerSet()
                    .getContainer(omKeyLocationInfo.getContainerID())
                    .getContainerData();
    Assert.assertTrue(containerData instanceof KeyValueContainerData);
    key.close();
    ContainerStateMachine stateMachine =
            (ContainerStateMachine) TestHelper.getStateMachine(dn,
                    omKeyLocationInfo.getPipeline());
    SimpleStateMachineStorage storage =
            (SimpleStateMachineStorage) stateMachine.getStateMachineStorage();
    Path parentPath = storage.findLatestSnapshot().getFile().getPath();
    stateMachine.takeSnapshot();
    Assert.assertTrue(parentPath.getParent().toFile().listFiles().length > 0);
    FileInfo snapshot = storage.findLatestSnapshot().getFile();
    Assert.assertNotNull(snapshot);
    long containerID = omKeyLocationInfo.getContainerID();
    Pipeline pipeline = cluster.getStorageContainerLocationClient()
            .getContainerWithPipeline(containerID).getPipeline();
    XceiverClientSpi xceiverClient =
            xceiverClientManager.acquireClient(pipeline);
    ContainerProtos.ContainerCommandRequestProto.Builder request =
            ContainerProtos.ContainerCommandRequestProto.newBuilder();
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    request.setCmdType(ContainerProtos.Type.CloseContainer);
    request.setContainerID(containerID);
    request.setCloseContainer(
            ContainerProtos.CloseContainerRequestProto.getDefaultInstance());
    try {
      xceiverClient.sendCommand(request.build());
    } catch (IOException e) {
      Assert.fail("Exception should not be thrown");
    }
    Assert.assertTrue(
            TestHelper.getDatanodeService(omKeyLocationInfo, cluster)
                    .getDatanodeStateMachine()
                    .getContainer().getContainerSet().getContainer(containerID)
                    .getContainerState()
                    == ContainerProtos.ContainerDataProto.State.CLOSED);
    Assert.assertTrue(stateMachine.isStateMachineHealthy());
    try {
      stateMachine.takeSnapshot();
    } catch (IOException ioe) {
      Assert.fail("Exception should not be thrown");
    }
    FileInfo latestSnapshot = storage.findLatestSnapshot().getFile();
    Assert.assertFalse(snapshot.getPath().equals(latestSnapshot.getPath()));
  }

  // The test injects multiple write chunk requests along with closed container
  // request thereby inducing a situation where a writeStateMachine call
  // gets executed when the closed container apply completes thereby
  // failing writeStateMachine call. In any case, our stateMachine should
  // not be marked unhealthy and pipeline should not fail if container gets
  // closed here.
  @Test
  public void testWriteStateMachineDataIdempotencyWithClosedContainer()
          throws Exception {
    OzoneOutputStream key =
            objectStore.getVolume(volumeName).getBucket(bucketName)
                    .createKey("ratis-1", 1024, ReplicationType.RATIS,
                            ReplicationFactor.ONE, new HashMap<>());
    // First write and flush creates a container in the datanode
    key.write("ratis".getBytes());
    key.flush();
    key.write("ratis".getBytes());
    KeyOutputStream groupOutputStream = (KeyOutputStream) key
            .getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
            groupOutputStream.getLocationInfoList();
    Assert.assertEquals(1, locationInfoList.size());
    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);
    HddsDatanodeService dn = TestHelper.getDatanodeService(omKeyLocationInfo,
            cluster);
    ContainerData containerData =
            dn.getDatanodeStateMachine()
                    .getContainer().getContainerSet()
                    .getContainer(omKeyLocationInfo.getContainerID())
                    .getContainerData();
    Assert.assertTrue(containerData instanceof KeyValueContainerData);
    key.close();
    ContainerStateMachine stateMachine =
            (ContainerStateMachine) TestHelper.getStateMachine(dn,
                    omKeyLocationInfo.getPipeline());
    SimpleStateMachineStorage storage =
            (SimpleStateMachineStorage) stateMachine.getStateMachineStorage();
    Path parentPath = storage.findLatestSnapshot().getFile().getPath();
    stateMachine.takeSnapshot();
    // Since the snapshot threshold is set to 1, since there are
    // applyTransactions, we should see snapshots
    Assert.assertTrue(parentPath.getParent().toFile().listFiles().length > 0);
    FileInfo snapshot = storage.findLatestSnapshot().getFile();
    Assert.assertNotNull(snapshot);
    long containerID = omKeyLocationInfo.getContainerID();
    Pipeline pipeline = cluster.getStorageContainerLocationClient()
            .getContainerWithPipeline(containerID).getPipeline();
    XceiverClientSpi xceiverClient =
            xceiverClientManager.acquireClient(pipeline);
    CountDownLatch latch = new CountDownLatch(100);
    int count = 0;
    AtomicInteger failCount = new AtomicInteger(0);
    Runnable r1 = () -> {
      try {
        ContainerProtos.ContainerCommandRequestProto.Builder request =
                ContainerProtos.ContainerCommandRequestProto.newBuilder();
        request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
        request.setCmdType(ContainerProtos.Type.CloseContainer);
        request.setContainerID(containerID);
        request.setCloseContainer(
                ContainerProtos.CloseContainerRequestProto.
                        getDefaultInstance());
        xceiverClient.sendCommand(request.build());
      } catch (IOException e) {
        failCount.incrementAndGet();
      }
    };
    Runnable r2 = () -> {
      try {
        xceiverClient.sendCommand(ContainerTestHelper
                .getWriteChunkRequest(pipeline, omKeyLocationInfo.getBlockID(),
                        1024, new Random().nextInt(), null));
        latch.countDown();
      } catch (IOException e) {
        latch.countDown();
        if (!(HddsClientUtils
                .checkForException(e) instanceof ContainerNotOpenException)) {
          failCount.incrementAndGet();
        }
      }
    };

    List<Thread> threadList = new ArrayList<>();

    for (int i = 0; i < 100; i++) {
      count++;
      Thread r = new Thread(r2);
      r.start();
      threadList.add(r);
    }

    Thread closeContainerThread = new Thread(r1);
    closeContainerThread.start();
    threadList.add(closeContainerThread);
    latch.await(600, TimeUnit.SECONDS);
    for (int i = 0; i < 101; i++) {
      threadList.get(i).join();
    }

    if (failCount.get() > 0) {
      fail("testWriteStateMachineDataIdempotencyWithClosedContainer failed");
    }
    Assert.assertTrue(
            TestHelper.getDatanodeService(omKeyLocationInfo, cluster)
                    .getDatanodeStateMachine()
                    .getContainer().getContainerSet().getContainer(containerID)
                    .getContainerState()
                    == ContainerProtos.ContainerDataProto.State.CLOSED);
    Assert.assertTrue(stateMachine.isStateMachineHealthy());
    try {
      stateMachine.takeSnapshot();
    } catch (IOException ioe) {
      Assert.fail("Exception should not be thrown");
    }
    FileInfo latestSnapshot = storage.findLatestSnapshot().getFile();
    Assert.assertFalse(snapshot.getPath().equals(latestSnapshot.getPath()));

  }
}
