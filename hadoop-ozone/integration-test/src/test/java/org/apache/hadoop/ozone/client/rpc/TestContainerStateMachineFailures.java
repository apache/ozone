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
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.QUASI_CLOSED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.UNHEALTHY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerNotOpenException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.ContainerStateMachine;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeUsage;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.StatemachineImplTestUtil;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests the containerStateMachine failure handling.
 */
public class TestContainerStateMachineFailures {

  private static MiniOzoneCluster cluster;
  private static OzoneClient client;
  private static ObjectStore objectStore;
  private static String volumeName;
  private static String bucketName;
  private static XceiverClientManager xceiverClientManager;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setStreamBufferFlushDelay(false);
    conf.setFromObject(clientConfig);

    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 2000,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 200, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 30, TimeUnit.SECONDS);
    conf.set(OzoneConfigKeys.OZONE_SCM_CLOSE_CONTAINER_WAIT_DURATION, "2s");
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_SCRUB_INTERVAL, "2s");
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT, "5s");

    RatisClientConfig ratisClientConfig =
        conf.getObject(RatisClientConfig.class);
    ratisClientConfig.setWriteRequestTimeout(Duration.ofSeconds(20));
    ratisClientConfig.setWatchRequestTimeout(Duration.ofSeconds(20));
    conf.setFromObject(ratisClientConfig);

    DatanodeRatisServerConfig ratisServerConfig =
        conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setRequestTimeOut(Duration.ofSeconds(3));
    ratisServerConfig.setWatchTimeOut(Duration.ofSeconds(10));
    conf.setFromObject(ratisServerConfig);

    RatisClientConfig.RaftConfig raftClientConfig =
        conf.getObject(RatisClientConfig.RaftConfig.class);
    raftClientConfig.setRpcRequestTimeout(Duration.ofSeconds(3));
    raftClientConfig.setRpcWatchRequestTimeout(Duration.ofSeconds(20));
    conf.setFromObject(raftClientConfig);

    conf.setLong(OzoneConfigKeys.HDDS_RATIS_SNAPSHOT_THRESHOLD_KEY, 1);
    conf.setQuietMode(false);
    cluster =
        MiniOzoneCluster.newBuilder(conf).setNumDatanodes(10)
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

  @Test
  public void testContainerStateMachineCloseOnMissingPipeline()
      throws Exception {
    // This integration test is a bit of a hack to see if the highly
    // improbable event where the Datanode does not have the pipeline
    // in its Ratis channel but still receives a close container command
    // for a container that is open or in closing state.
    // Bugs in code can lead to this sequence of events but for this test
    // to inject this state, it removes the pipeline by directly calling
    // the underlying method.

    OzoneOutputStream key =
        objectStore.getVolume(volumeName).getBucket(bucketName)
            .createKey("testQuasiClosed1", 1024,
                ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
                    ReplicationFactor.THREE), new HashMap<>());
    key.write("ratis".getBytes(UTF_8));
    key.flush();

    KeyOutputStream groupOutputStream = (KeyOutputStream) key.
        getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
        groupOutputStream.getLocationInfoList();
    assertEquals(1, locationInfoList.size());

    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);

    Set<HddsDatanodeService> datanodeSet =
        TestHelper.getDatanodeServices(cluster,
            omKeyLocationInfo.getPipeline());

    long containerID = omKeyLocationInfo.getContainerID();

    for (HddsDatanodeService dn : datanodeSet) {
      XceiverServerRatis wc = (XceiverServerRatis)
          dn.getDatanodeStateMachine().getContainer().getWriteChannel();
      if (wc == null) {
        // Test applicable only for RATIS based channel.
        return;
      }
      wc.notifyGroupRemove(RaftGroupId
          .valueOf(omKeyLocationInfo.getPipeline().getId().getId()));
      SCMCommand<?> command = new CloseContainerCommand(
          containerID, omKeyLocationInfo.getPipeline().getId());
      command.setTerm(
          cluster
              .getStorageContainerManager()
              .getScmContext()
              .getTermOfLeader());
      cluster.getStorageContainerManager().getScmNodeManager()
          .addDatanodeCommand(dn.getDatanodeDetails().getID(), command);
    }


    for (HddsDatanodeService dn : datanodeSet) {
      LambdaTestUtils.await(20000, 1000,
          () -> (dn.getDatanodeStateMachine()
              .getContainer().getContainerSet()
              .getContainer(containerID)
              .getContainerState().equals(QUASI_CLOSED)));
    }
    key.close();
  }

  @Test
  @Flaky("HDDS-12215")
  public void testContainerStateMachineRestartWithDNChangePipeline()
      throws Exception {
    try (OzoneOutputStream key = objectStore.getVolume(volumeName).getBucket(bucketName)
        .createKey("testDNRestart", 1024, ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
            ReplicationFactor.THREE), new HashMap<>())) {
      key.write("ratis".getBytes(UTF_8));
      key.flush();

      KeyOutputStream groupOutputStream = (KeyOutputStream) key.
          getOutputStream();
      List<OmKeyLocationInfo> locationInfoList =
          groupOutputStream.getLocationInfoList();
      assertEquals(1, locationInfoList.size());

      OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);
      Pipeline pipeline = omKeyLocationInfo.getPipeline();
      List<HddsDatanodeService> datanodes =
          new ArrayList<>(TestHelper.getDatanodeServices(cluster,
              pipeline));

      DatanodeDetails dn = datanodes.get(0).getDatanodeDetails();

      // Delete all data volumes.
      cluster.getHddsDatanode(dn).getDatanodeStateMachine().getContainer().getVolumeSet().getVolumesList()
          .stream().forEach(v -> {
            try {
              FileUtils.deleteDirectory(v.getStorageDir());
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });

      // Delete datanode.id datanodeIdFile.
      File datanodeIdFile = new File(HddsServerUtil.getDatanodeIdFilePath(cluster.getHddsDatanode(dn).getConf()));
      boolean deleted = datanodeIdFile.delete();
      assertTrue(deleted);
      cluster.restartHddsDatanode(dn, false);
      GenericTestUtils.waitFor(() -> {
        try {
          key.write("ratis".getBytes(UTF_8));
          key.flush();
          return groupOutputStream.getLocationInfoList().size() > 1;
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }, 1000, 30000);
    }
  }

  @Test
  public void testContainerStateMachineFailures() throws Exception {
    OzoneOutputStream key =
        objectStore.getVolume(volumeName).getBucket(bucketName)
            .createKey("ratis", 1024,
                ReplicationConfig.fromTypeAndFactor(
                    ReplicationType.RATIS,
                    ReplicationFactor.ONE), new HashMap<>());
    byte[] testData = "ratis".getBytes(UTF_8);
    // First write and flush creates a container in the datanode
    key.write(testData);
    key.flush();
    key.write(testData);
    KeyOutputStream groupOutputStream =
        (KeyOutputStream) key.getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
        groupOutputStream.getLocationInfoList();
    assertEquals(1, locationInfoList.size());
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
    assertSame(dn.getDatanodeStateMachine()
        .getContainer().getContainerSet()
        .getContainer(containerID)
        .getContainerState(), UNHEALTHY);
    OzoneContainer ozoneContainer;

    // restart the hdds datanode, container should not in the regular set
    OzoneConfiguration config = dn.getConf();
    final String dir = config.get(OzoneConfigKeys.
        HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR)
        + UUID.randomUUID();
    config.set(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR, dir);
    int index = cluster.getHddsDatanodeIndex(dn.getDatanodeDetails());
    cluster.restartHddsDatanode(dn.getDatanodeDetails(), false);
    ozoneContainer = cluster.getHddsDatanodes().get(index)
        .getDatanodeStateMachine().getContainer();
    assertNull(ozoneContainer.getContainerSet().
                    getContainer(containerID));
  }

  @Test
  public void testUnhealthyContainer() throws Exception {
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
    KeyOutputStream groupOutputStream = (KeyOutputStream) key
        .getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
        groupOutputStream.getLocationInfoList();
    assertEquals(1, locationInfoList.size());
    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);
    HddsDatanodeService dn = TestHelper.getDatanodeService(omKeyLocationInfo,
        cluster);
    ContainerData containerData =
        dn.getDatanodeStateMachine()
            .getContainer().getContainerSet()
            .getContainer(omKeyLocationInfo.getContainerID())
            .getContainerData();
    KeyValueContainerData keyValueContainerData =
        assertInstanceOf(KeyValueContainerData.class, containerData);
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
    assertSame(dn.getDatanodeStateMachine()
        .getContainer().getContainerSet().getContainer(containerID)
        .getContainerState(), UNHEALTHY);
    // Check metadata in the .container file
    File containerFile = new File(keyValueContainerData.getMetadataPath(),
        containerID + OzoneConsts.CONTAINER_EXTENSION);

    keyValueContainerData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    assertEquals(keyValueContainerData.getState(), UNHEALTHY);

    OzoneConfiguration config = dn.getConf();
    final String dir = config.get(OzoneConfigKeys.
        HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR)
        + UUID.randomUUID();
    config.set(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR, dir);
    int index = cluster.getHddsDatanodeIndex(dn.getDatanodeDetails());
    // restart the hdds datanode and see if the container is listed in the
    // in the missing container set and not in the regular set
    cluster.restartHddsDatanode(dn.getDatanodeDetails(), true);
    // make sure the container state is still marked unhealthy after restart
    keyValueContainerData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    assertEquals(keyValueContainerData.getState(), UNHEALTHY);

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
    assertEquals(ContainerProtos.Result.CONTAINER_UNHEALTHY,
            dispatcher.dispatch(request.build(), null)
                    .getResult());
  }

  @Test
  public void testApplyTransactionFailure() throws Exception {
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
    KeyOutputStream groupOutputStream = (KeyOutputStream) key.
        getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
        groupOutputStream.getLocationInfoList();
    assertEquals(1, locationInfoList.size());
    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);
    HddsDatanodeService dn = TestHelper.getDatanodeService(omKeyLocationInfo,
        cluster);
    int index = cluster.getHddsDatanodeIndex(dn.getDatanodeDetails());
    ContainerData containerData = dn.getDatanodeStateMachine()
        .getContainer().getContainerSet()
        .getContainer(omKeyLocationInfo.getContainerID())
        .getContainerData();
    KeyValueContainerData keyValueContainerData =
        assertInstanceOf(KeyValueContainerData.class, containerData);
    key.close();
    ContainerStateMachine stateMachine =
        (ContainerStateMachine) TestHelper.getStateMachine(cluster.
            getHddsDatanodes().get(index), omKeyLocationInfo.getPipeline());
    SimpleStateMachineStorage storage =
        (SimpleStateMachineStorage) stateMachine.getStateMachineStorage();
    stateMachine.takeSnapshot();
    final FileInfo snapshot = getSnapshotFileInfo(storage);
    final Path parentPath = snapshot.getPath();
    // Since the snapshot threshold is set to 1, since there are
    // applyTransactions, we should see snapshots
    assertThat(parentPath.getParent().toFile().listFiles().length).isGreaterThan(0);
    assertNotNull(snapshot);
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
      assertThrows(IOException.class, () -> xceiverClient.sendCommand(request.build()));
    } finally {
      xceiverClientManager.releaseClient(xceiverClient, false);
    }
    // Make sure the container is marked unhealthy
    assertSame(dn.getDatanodeStateMachine()
        .getContainer().getContainerSet().getContainer(containerID)
        .getContainerState(), UNHEALTHY);
    try {
      // try to take a new snapshot, ideally it should just fail
      stateMachine.takeSnapshot();
    } catch (IOException ioe) {
      assertInstanceOf(StateMachineException.class, ioe);
    }

    if (snapshot.getPath().toFile().exists()) {
      // Make sure the latest snapshot is same as the previous one
      try {
        final FileInfo latestSnapshot = getSnapshotFileInfo(storage);
        assertEquals(snapshot.getPath(), latestSnapshot.getPath());
      } catch (Throwable e) {
        assertFalse(snapshot.getPath().toFile().exists());
      }
    }

    // when remove pipeline, group dir including snapshot will be deleted
    LambdaTestUtils.await(10000, 500,
        () -> (!snapshot.getPath().toFile().exists()));
  }

  @Test
  @Flaky("HDDS-6115")
  void testApplyTransactionIdempotencyWithClosedContainer()
      throws Exception {
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
    ContainerData containerData = dn.getDatanodeStateMachine()
        .getContainer().getContainerSet()
        .getContainer(omKeyLocationInfo.getContainerID())
        .getContainerData();
    assertInstanceOf(KeyValueContainerData.class, containerData);
    key.close();
    ContainerStateMachine stateMachine =
        (ContainerStateMachine) TestHelper.getStateMachine(dn,
            omKeyLocationInfo.getPipeline());
    SimpleStateMachineStorage storage =
        (SimpleStateMachineStorage) stateMachine.getStateMachineStorage();
    final FileInfo snapshot = getSnapshotFileInfo(storage);
    final Path parentPath = snapshot.getPath();
    stateMachine.takeSnapshot();
    assertThat(parentPath.getParent().toFile().listFiles().length).isGreaterThan(0);
    assertNotNull(snapshot);
    long markIndex1 = StatemachineImplTestUtil.findLatestSnapshot(storage)
        .getIndex();
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
    xceiverClient.sendCommand(request.build());
    assertSame(
        TestHelper.getDatanodeService(omKeyLocationInfo, cluster)
            .getDatanodeStateMachine()
            .getContainer().getContainerSet().getContainer(containerID)
            .getContainerState(),
        ContainerProtos.ContainerDataProto.State.CLOSED);
    assertTrue(stateMachine.isStateMachineHealthy());
    try {
      stateMachine.takeSnapshot();
    } finally {
      xceiverClientManager.releaseClient(xceiverClient, false);
    }
    // This is just an attempt to wait for an asynchronous call from Ratis API
    // to updateIncreasingly to finish as part of flaky test issue "HDDS-6115"
    // This doesn't solve the problem completely but reduce the failure ratio.
    GenericTestUtils.waitFor((() -> {
      try {
        return markIndex1 != StatemachineImplTestUtil
            .findLatestSnapshot(storage).getIndex();
      } catch (IOException e) {
        // No action needed. The test case is going to fail at assertion.
        return true;
      }
    }), 1000, 30000);
    final FileInfo latestSnapshot = getSnapshotFileInfo(storage);
    assertNotEquals(snapshot.getPath(), latestSnapshot.getPath());
  }

  // The test injects multiple write chunk requests along with closed container
  // request thereby inducing a situation where a writeStateMachine call
  // gets executed when the closed container apply completes thereby
  // failing writeStateMachine call. In any case, our stateMachine should
  // not be marked unhealthy and pipeline should not fail if container gets
  // closed here.
  @Test
  @Flaky("HDDS-13482")
  void testWriteStateMachineDataIdempotencyWithClosedContainer()
      throws Exception {
    OzoneOutputStream key =
        objectStore.getVolume(volumeName).getBucket(bucketName)
            .createKey("ratis-1", 1024,
                ReplicationConfig.fromTypeAndFactor(
                    ReplicationType.RATIS,
                    ReplicationFactor.ONE), new HashMap<>());
    // First write and flush creates a container in the datanode
    key.write("ratis".getBytes(UTF_8));
    key.flush();
    key.write("ratis".getBytes(UTF_8));
    KeyOutputStream groupOutputStream = (KeyOutputStream) key
        .getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
        groupOutputStream.getLocationInfoList();
    assertEquals(1, locationInfoList.size());
    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);
    HddsDatanodeService dn = TestHelper.getDatanodeService(omKeyLocationInfo,
        cluster);
    ContainerData containerData =
        dn.getDatanodeStateMachine()
            .getContainer().getContainerSet()
            .getContainer(omKeyLocationInfo.getContainerID())
            .getContainerData();
    assertInstanceOf(KeyValueContainerData.class, containerData);
    key.close();
    ContainerStateMachine stateMachine =
        (ContainerStateMachine) TestHelper.getStateMachine(dn,
            omKeyLocationInfo.getPipeline());
    SimpleStateMachineStorage storage =
        (SimpleStateMachineStorage) stateMachine.getStateMachineStorage();
    final FileInfo snapshot = getSnapshotFileInfo(storage);
    final Path parentPath = snapshot.getPath();
    stateMachine.takeSnapshot();
    // Since the snapshot threshold is set to 1, since there are
    // applyTransactions, we should see snapshots
    assertThat(parentPath.getParent().toFile().listFiles().length).isGreaterThan(0);
    assertNotNull(snapshot);
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
        ByteString data = ByteString.copyFromUtf8("hello");
        ContainerProtos.ContainerCommandRequestProto.Builder writeChunkRequest =
            ContainerTestHelper.newWriteChunkRequestBuilder(pipeline,
                omKeyLocationInfo.getBlockID(), data.size());
        writeChunkRequest.setWriteChunk(writeChunkRequest.getWriteChunkBuilder()
            .setData(data));
        xceiverClient.sendCommand(writeChunkRequest.build());
        latch.countDown();
      } catch (IOException e) {
        latch.countDown();
        if (!(HddsClientUtils
            .checkForException(e) instanceof ContainerNotOpenException)) {
          failCount.incrementAndGet();
        }
        String message = e.getMessage();
        assertThat(message).doesNotContain("hello");
        assertThat(message).contains(HddsUtils.REDACTED.toStringUtf8());
      }
    };

    try {
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
      assertTrue(latch.await(600, TimeUnit.SECONDS));
      for (int i = 0; i < 101; i++) {
        threadList.get(i).join();
      }

      if (failCount.get() > 0) {
        fail(
            "testWriteStateMachineDataIdempotencyWithClosedContainer " +
                "failed");
      }
      assertSame(
          TestHelper.getDatanodeService(omKeyLocationInfo, cluster)
              .getDatanodeStateMachine()
              .getContainer().getContainerSet().getContainer(containerID)
              .getContainerState(),
          ContainerProtos.ContainerDataProto.State.CLOSED);
      assertTrue(stateMachine.isStateMachineHealthy());
      stateMachine.takeSnapshot();

      final FileInfo latestSnapshot = getSnapshotFileInfo(storage);
      assertNotEquals(snapshot.getPath(), latestSnapshot.getPath());

      r2.run();
    } finally {
      xceiverClientManager.releaseClient(xceiverClient, false);
    }
  }

  @Test
  @Flaky("HDDS-14101")
  void testContainerStateMachineSingleFailureRetry()
      throws Exception {
    try (OzoneOutputStream key = objectStore.getVolume(volumeName).getBucket(bucketName)
        .createKey("ratis1", 1024,
            ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
                ReplicationFactor.THREE), new HashMap<>())) {

      key.write("ratis".getBytes(UTF_8));
      key.flush();
      key.write("ratis".getBytes(UTF_8));
      key.write("ratis".getBytes(UTF_8));

      KeyOutputStream groupOutputStream = (KeyOutputStream) key.
          getOutputStream();
      List<OmKeyLocationInfo> locationInfoList =
          groupOutputStream.getLocationInfoList();
      assertEquals(1, locationInfoList.size());

      OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);
      ContainerSet containerSet = cluster.getHddsDatanode(omKeyLocationInfo.getPipeline().getLeaderNode())
          .getDatanodeStateMachine().getContainer().getContainerSet();

      induceFollowerFailure(omKeyLocationInfo, 2);
      key.flush();
      // wait for container close for failure in flush for both followers applyTransaction failure
      GenericTestUtils.waitFor(() -> containerSet.getContainer(omKeyLocationInfo.getContainerID()).getContainerData()
              .getState().equals(ContainerProtos.ContainerDataProto.State.CLOSED), 100, 30000);
      key.write("ratis".getBytes(UTF_8));
      key.flush();
    }

    validateData("ratis1", 2, "ratisratisratisratis");
  }

  @Test
  @Flaky("HDDS-14101")
  void testContainerStateMachineDualFailureRetry()
      throws Exception {
    OzoneOutputStream key =
        objectStore.getVolume(volumeName).getBucket(bucketName)
            .createKey("ratis2", 1024,
                ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
                    ReplicationFactor.THREE), new HashMap<>());

    key.write("ratis".getBytes(UTF_8));
    key.flush();
    key.write("ratis".getBytes(UTF_8));
    key.write("ratis".getBytes(UTF_8));

    KeyOutputStream groupOutputStream = (KeyOutputStream) key.
        getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
        groupOutputStream.getLocationInfoList();
    assertEquals(1, locationInfoList.size());

    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);

    induceFollowerFailure(omKeyLocationInfo, 1);

    key.flush();
    key.write("ratis".getBytes(UTF_8));
    key.flush();
    key.close();
    validateData("ratis1", 2, "ratisratisratisratis");
  }

  @Test
  void testContainerStateMachineAllNodeFailure()
      throws Exception {
    // mark all dn volume as full to induce failure
    List<Pair<VolumeUsage, Long>> increasedVolumeSpace = new ArrayList<>();
    cluster.getHddsDatanodes().forEach(
        dn -> {
          List<StorageVolume> volumesList = dn.getDatanodeStateMachine().getContainer().getVolumeSet().getVolumesList();
          volumesList.forEach(sv -> {
            final VolumeUsage volumeUsage = sv.getVolumeUsage();
            if (volumeUsage != null) {
              final long available = sv.getCurrentUsage().getAvailable();
              increasedVolumeSpace.add(Pair.of(volumeUsage, available));
              volumeUsage.incrementUsedSpace(available);
            }
          });
        }
    );

    long startTime = Time.monotonicNow();
    ReplicationConfig replicationConfig = ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
        ReplicationFactor.THREE);
    try (OzoneOutputStream key = objectStore.getVolume(volumeName).getBucket(bucketName).createKey(
        "testkey1", 1024, replicationConfig, new HashMap<>())) {

      key.write("ratis".getBytes(UTF_8));
      key.flush();
      fail();
    } catch (IOException ex) {
      assertThat(ex.getMessage()).contains("Retry request failed. retries get failed due to exceeded" +
          " maximum allowed retries number: 5");
    } finally {
      increasedVolumeSpace.forEach(e -> e.getLeft().decrementUsedSpace(e.getRight()));
      // test execution is less than 2 sec but to be safe putting 30 sec as without fix, taking more than 60 sec
      assertThat(Time.monotonicNow() - startTime)
          .describedAs("Operation took longer than expected")
          .isLessThan(30000);
    }

    // previous pipeline gets closed due to disk full failure, so created a new pipeline and write should succeed,
    // and this ensures later test case can pass (should not fail due to pipeline unavailability as timeout is 200ms
    // for pipeline creation which can fail in testcase later on)
    Pipeline pipeline = cluster.getStorageContainerManager().getPipelineManager().createPipeline(replicationConfig);
    cluster.getStorageContainerManager().getPipelineManager().waitPipelineReady(pipeline.getId(), 60000);

    try (OzoneOutputStream key = objectStore.getVolume(volumeName).getBucket(bucketName).createKey(
        "testkey2", 1024, replicationConfig, new HashMap<>())) {

      key.write("ratis".getBytes(UTF_8));
      key.flush();
    }
  }

  private void induceFollowerFailure(OmKeyLocationInfo omKeyLocationInfo,
                                     int failureCount) {
    DatanodeID leader = omKeyLocationInfo.getPipeline().getLeaderId();
    Set<HddsDatanodeService> datanodeSet =
        TestHelper.getDatanodeServices(cluster,
            omKeyLocationInfo.getPipeline());
    int count = 0;
    for (HddsDatanodeService dn : datanodeSet) {
      DatanodeID dnId = dn.getDatanodeDetails().getID();
      if (!dnId.equals(leader)) {
        count++;
        long containerID = omKeyLocationInfo.getContainerID();
        Container container = dn
            .getDatanodeStateMachine()
            .getContainer()
            .getContainerSet()
            .getContainer(containerID);
        if (container != null) {
          ContainerData containerData =
              container
                  .getContainerData();
          KeyValueContainerData keyValueContainerData =
              assertInstanceOf(KeyValueContainerData.class, containerData);
          FileUtil.fullyDelete(new File(keyValueContainerData.getChunksPath()));
        }

        if (count == failureCount) {
          break;
        }
      }
    }
  }

  private void validateData(String key, int locationCount, String payload) throws Exception {
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(key)
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(omKeyArgs);

    assertEquals(locationCount,
        keyInfo.getLatestVersionLocations().getLocationListCount());
    byte[] buffer = new byte[Math.toIntExact(keyInfo.getDataSize())];
    try (OzoneInputStream o = objectStore.getVolume(volumeName)
        .getBucket(bucketName).readKey(key)) {
      IOUtils.readFully(o, buffer);
    }
    String response = new String(buffer, StandardCharsets.UTF_8);
    assertEquals(payload, response);
  }

  static FileInfo getSnapshotFileInfo(SimpleStateMachineStorage storage)
      throws IOException {
    return StatemachineImplTestUtil.findLatestSnapshot(storage).getFile();
  }
}
