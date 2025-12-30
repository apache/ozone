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

package org.apache.hadoop.ozone.container.common;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.createEndpoint;
import static org.apache.hadoop.ozone.container.upgrade.UpgradeUtils.defaultLayoutVersionProto;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CloseContainerCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus.Status;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeleteBlocksCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicateContainerCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.VersionInfo;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.Storage.StorageState;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.states.endpoint.HeartbeatEndpointTask;
import org.apache.hadoop.ozone.container.common.states.endpoint.RegisterEndpointTask;
import org.apache.hadoop.ozone.container.common.states.endpoint.VersionEndpointTask;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeChoosingPolicyFactory;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.replication.ReplicationServer.ReplicationConfig;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests the endpoints.
 */
public class TestEndPoint {
  private static InetSocketAddress serverAddress;
  private static RPC.Server scmServer;
  private static ScmTestMock scmServerImpl;
  @TempDir
  private static File testDir;
  private static OzoneConfiguration ozoneConf;
  private static VolumeChoosingPolicy volumeChoosingPolicy;
  private static DatanodeDetails dnDetails;

  @TempDir
  private File tempDir;

  @AfterAll
  public static void tearDown() throws Exception {
    if (scmServer != null) {
      scmServer.stop();
    }
  }

  @BeforeAll
  static void setUp() throws Exception {
    serverAddress = SCMTestUtils.getReuseableAddress();
    ozoneConf = SCMTestUtils.getConf(testDir);
    scmServerImpl = new ScmTestMock();
    dnDetails = randomDatanodeDetails();
    DatanodeLayoutStorage layoutStorage = new DatanodeLayoutStorage(ozoneConf,
        UUID.randomUUID().toString(),
        HDDSLayoutFeature.DATANODE_SCHEMA_V3.layoutVersion());
    layoutStorage.initialize();
    scmServer = SCMTestUtils.startScmRpcServer(ozoneConf,
        scmServerImpl, serverAddress, 10);
    volumeChoosingPolicy = VolumeChoosingPolicyFactory.getPolicy(ozoneConf);
  }

  /**
   * This test asserts that we are able to make a version call to SCM server
   * and gets back the expected values.
   */
  @Test
  public void testGetVersion() throws Exception {
    try (EndpointStateMachine rpcEndPoint =
        createEndpoint(SCMTestUtils.getConf(tempDir),
            serverAddress, 1000)) {
      SCMVersionResponseProto responseProto = rpcEndPoint.getEndPoint()
          .getVersion(null);
      assertNotNull(responseProto);
      assertEquals(VersionInfo.DESCRIPTION_KEY, responseProto.getKeys(0).getKey());
      assertEquals(VersionInfo.getLatestVersion().getDescription(), responseProto.getKeys(0).getValue());
    }
  }

  /**
   * We make getVersion RPC call, but via the VersionEndpointTask which is
   * how the state machine would make the call.
   */
  @Test
  public void testGetVersionTask() throws Exception {
    ozoneConf.setFromObject(new ReplicationConfig().setPort(0));
    try (EndpointStateMachine rpcEndPoint = createEndpoint(ozoneConf,
        serverAddress, 1000)) {
      ozoneConf.setBoolean(
          OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED,
          true);
      ozoneConf.setBoolean(
          OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_RANDOM_PORT, true);
      OzoneContainer ozoneContainer = new OzoneContainer(dnDetails,
          ozoneConf, ContainerTestUtils.getMockContext(dnDetails, ozoneConf),
          volumeChoosingPolicy);
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.GETVERSION);
      VersionEndpointTask versionTask = new VersionEndpointTask(rpcEndPoint,
          ozoneConf, ozoneContainer);
      EndpointStateMachine.EndPointStates newState = versionTask.call();

      // if version call worked the endpoint should automatically move to the
      // next state.
      assertEquals(EndpointStateMachine.EndPointStates.REGISTER, newState);

      // Now rpcEndpoint should remember the version it got from SCM
      assertNotNull(rpcEndPoint.getVersion());
    }
  }


  /**
   * Writing data to tmp dir and checking if it has been cleaned.
   * Add some entries to DatanodeStore.MetadataTable and check
   * that they have been deleted during tmp dir cleanup.
   */
  @Test
  public void testDeletedContainersClearedOnStartup() throws Exception {
    ozoneConf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_IPC_RANDOM_PORT,
        true);
    ozoneConf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_RANDOM_PORT,
        true);
    ozoneConf.setFromObject(new ReplicationConfig().setPort(0));
    OzoneContainer ozoneContainer = createVolume(ozoneConf);
    try (EndpointStateMachine rpcEndPoint = createEndpoint(ozoneConf,
        serverAddress, 1000)) {
      HddsVolume hddsVolume = (HddsVolume) ozoneContainer.getVolumeSet()
          .getVolumesList().get(0);
      KeyValueContainer kvContainer = addContainer(ozoneConf, hddsVolume);
      // For testing, we are moving the container under the tmp directory,
      // in order to delete during datanode startup or shutdown
      KeyValueContainerUtil.moveToDeletedContainerDir(
          kvContainer.getContainerData(), hddsVolume);
      Path containerTmpPath = KeyValueContainerUtil.getTmpDirectoryPath(
          kvContainer.getContainerData(), hddsVolume);
      assertTrue(containerTmpPath.toFile().exists());

      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.GETVERSION);

      // versionTask.call() cleans the tmp dir and removes container from DB
      VersionEndpointTask versionTask = new VersionEndpointTask(rpcEndPoint,
          ozoneConf, ozoneContainer);
      EndpointStateMachine.EndPointStates newState = versionTask.call();

      assertEquals(EndpointStateMachine.EndPointStates.REGISTER, newState);

      // assert that tmp dir is empty
      File[] leftoverContainers =
          hddsVolume.getDeletedContainerDir().listFiles();
      assertNotNull(leftoverContainers);
      assertEquals(0, leftoverContainers.length);
    } finally {
      ozoneContainer.stop();
    }
  }

  @Test
  public void testCheckVersionResponse() throws Exception {
    ozoneConf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_IPC_RANDOM_PORT,
        true);
    ozoneConf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_RANDOM_PORT,
        true);
    ozoneConf.setBoolean(
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_RANDOM_PORT, true);
    ozoneConf.setFromObject(new ReplicationConfig().setPort(0));
    try (EndpointStateMachine rpcEndPoint = createEndpoint(ozoneConf,
        serverAddress, 1000)) {
      LogCapturer logCapturer = LogCapturer.captureLogs(VersionEndpointTask.class);
      OzoneContainer ozoneContainer = new OzoneContainer(dnDetails, ozoneConf,
          ContainerTestUtils.getMockContext(dnDetails, ozoneConf),
          volumeChoosingPolicy);
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.GETVERSION);
      VersionEndpointTask versionTask = new VersionEndpointTask(rpcEndPoint,
          ozoneConf, ozoneContainer);
      EndpointStateMachine.EndPointStates newState = versionTask.call();

      // if version call worked the endpoint should automatically move to the
      // next state.
      assertEquals(EndpointStateMachine.EndPointStates.REGISTER, newState);

      // Now rpcEndpoint should remember the version it got from SCM
      assertNotNull(rpcEndPoint.getVersion());

      // Now change server cluster ID, so datanode cluster ID will be
      // different from SCM server response cluster ID.
      String newClusterId = UUID.randomUUID().toString();
      scmServerImpl.setClusterId(newClusterId);
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.GETVERSION);
      newState = versionTask.call();
      assertEquals(EndpointStateMachine.EndPointStates.SHUTDOWN, newState);
      List<HddsVolume> volumesList = StorageVolumeUtil.getHddsVolumesList(
          ozoneContainer.getVolumeSet().getFailedVolumesList());
      assertEquals(1, volumesList.size());
      assertThat(logCapturer.getOutput())
          .contains("org.apache.hadoop.ozone.common" +
              ".InconsistentStorageStateException: Mismatched ClusterIDs");
      assertEquals(0, ozoneContainer.getVolumeSet().getVolumesList().size());
      assertEquals(1, ozoneContainer.getVolumeSet().getFailedVolumesList().size());
    }
  }

  /**
   * This test checks that dnlayout version file contains proper
   * clusterID identifying the scm cluster the datanode is part of.
   * Dnlayout version file set upon call to version endpoint.
   */
  @Test
  public void testDnLayoutVersionFile() throws Exception {
    ozoneConf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_RANDOM_PORT,
        true);
    try (EndpointStateMachine rpcEndPoint = createEndpoint(ozoneConf,
        serverAddress, 1000)) {
      OzoneContainer ozoneContainer = new OzoneContainer(dnDetails, ozoneConf,
          ContainerTestUtils.getMockContext(dnDetails, ozoneConf),
          volumeChoosingPolicy);
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.GETVERSION);
      VersionEndpointTask versionTask = new VersionEndpointTask(rpcEndPoint,
          ozoneConf, ozoneContainer);
      versionTask.call();

      // After the version call, the datanode layout file should
      // have its clusterID field set to the clusterID of the scm
      DatanodeLayoutStorage layout
          = new DatanodeLayoutStorage(ozoneConf,
          "na_expect_storage_initialized");
      assertEquals(scmServerImpl.getClusterId(), layout.getClusterID());

      // Delete storage volume info
      File storageDir = ozoneContainer.getVolumeSet()
          .getVolumesList().get(0).getStorageDir();
      FileUtils.forceDelete(storageDir);

      // Format volume VERSION file with
      // different clusterId than SCM clusterId.
      ozoneContainer.getVolumeSet().getVolumesList()
          .get(0).format("different_cluster_id");
      // Update layout clusterId and persist it.
      layout.setClusterId("different_cluster_id");
      layout.persistCurrentState();

      // As the volume level clusterId didn't match with SCM clusterId
      // Even after the version call, the datanode layout file should
      // not update its clusterID field.
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.GETVERSION);
      versionTask.call();
      DatanodeLayoutStorage layout1
          = new DatanodeLayoutStorage(ozoneConf,
          "na_expect_storage_initialized");

      assertEquals("different_cluster_id", layout1.getClusterID());
      assertNotEquals(scmServerImpl.getClusterId(), layout1.getClusterID());

      // another call() with OzoneContainer already started should not write the file
      FileUtils.forceDelete(layout1.getVersionFile());
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.GETVERSION);
      versionTask.call();
      assertEquals(StorageState.NOT_INITIALIZED, new DatanodeLayoutStorage(ozoneConf, "any").getState());

      FileUtils.forceDelete(storageDir);
    }
  }

  /**
   * This test makes a call to end point where there is no SCM server. We
   * expect that versionTask should be able to handle it.
   */
  @Test
  public void testGetVersionToInvalidEndpoint() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf(tempDir);
    InetSocketAddress nonExistentServerAddress = SCMTestUtils
        .getReuseableAddress();
    try (EndpointStateMachine rpcEndPoint = createEndpoint(conf,
        nonExistentServerAddress, 1000)) {
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.GETVERSION);
      DatanodeDetails datanodeDetails = randomDatanodeDetails();
      OzoneContainer ozoneContainer = new OzoneContainer(datanodeDetails,
          conf, ContainerTestUtils.getMockContext(datanodeDetails, ozoneConf),
          volumeChoosingPolicy);
      VersionEndpointTask versionTask = new VersionEndpointTask(rpcEndPoint,
          conf, ozoneContainer);
      EndpointStateMachine.EndPointStates newState = versionTask.call();

      // This version call did NOT work, so endpoint should remain in the same
      // state.
      assertEquals(EndpointStateMachine.EndPointStates.GETVERSION, newState);
    }
  }

  /**
   * This test makes a getVersionRPC call, but the DummyStorageServer is
   * going to respond little slowly. We will assert that we are still in the
   * GETVERSION state after the timeout.
   */
  @Test
  public void testGetVersionAssertRpcTimeOut() throws Exception {
    final long rpcTimeout = 1000;
    final long tolerance = 100;
    OzoneConfiguration conf = SCMTestUtils.getConf(tempDir);

    try (EndpointStateMachine rpcEndPoint = createEndpoint(conf,
        serverAddress, (int) rpcTimeout)) {
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.GETVERSION);
      DatanodeDetails datanodeDetails = randomDatanodeDetails();
      OzoneContainer ozoneContainer = new OzoneContainer(datanodeDetails, conf,
          ContainerTestUtils.getMockContext(datanodeDetails, ozoneConf),
          volumeChoosingPolicy);
      VersionEndpointTask versionTask = new VersionEndpointTask(rpcEndPoint,
          conf, ozoneContainer);

      scmServerImpl.setRpcResponseDelay(1500);
      long start = Time.monotonicNow();
      EndpointStateMachine.EndPointStates newState = versionTask.call();
      long end = Time.monotonicNow();
      scmServerImpl.setRpcResponseDelay(0);
      assertThat(end - start).isLessThanOrEqualTo(rpcTimeout + tolerance);
      assertEquals(EndpointStateMachine.EndPointStates.GETVERSION, newState);
    }
  }

  @Test
  public void testRegister() throws Exception {
    DatanodeDetails nodeToRegister = randomDatanodeDetails();
    try (EndpointStateMachine rpcEndPoint = createEndpoint(
        SCMTestUtils.getConf(tempDir), serverAddress, 1000)) {
      SCMRegisteredResponseProto responseProto = rpcEndPoint.getEndPoint()
          .register(nodeToRegister.getExtendedProtoBufMessage(), HddsTestUtils
                  .createNodeReport(
                      Arrays.asList(getStorageReports(
                          nodeToRegister.getID())),
                      Arrays.asList(getMetadataStorageReports(
                          nodeToRegister.getID()))),
              HddsTestUtils.getRandomContainerReports(10),
              HddsTestUtils.getRandomPipelineReports(),
              defaultLayoutVersionProto());
      assertNotNull(responseProto);
      assertEquals(nodeToRegister.getUuidString(), responseProto.getDatanodeUUID());
      assertNotNull(responseProto.getClusterID());
      assertEquals(10, scmServerImpl.getContainerCountsForDatanode(nodeToRegister));
      assertEquals(1, scmServerImpl.getNodeReportsCount(nodeToRegister));
    }
  }

  private StorageReportProto getStorageReports(DatanodeID id) {
    String storagePath = testDir.getAbsolutePath() + "/data-" + id;
    return HddsTestUtils.createStorageReport(id, storagePath, 100, 10, 90,
        null);
  }

  private MetadataStorageReportProto getMetadataStorageReports(DatanodeID id) {
    String storagePath = testDir.getAbsolutePath() + "/metadata-" + id;
    return HddsTestUtils.createMetadataStorageReport(storagePath, 100, 10, 90,
        null);
  }

  private RegisterEndpointTask getRegisterEndpointTask(boolean clearDatanodeDetails, OzoneConfiguration conf,
                                                       EndpointStateMachine rpcEndPoint) throws Exception {
    OzoneContainer ozoneContainer = mock(OzoneContainer.class);
    DatanodeID datanodeID = DatanodeID.randomID();
    when(ozoneContainer.getNodeReport()).thenReturn(HddsTestUtils
        .createNodeReport(Arrays.asList(getStorageReports(datanodeID)),
            Arrays.asList(getMetadataStorageReports(datanodeID))));
    ContainerController controller = mock(ContainerController.class);
    when(controller.getContainerReport()).thenReturn(
        HddsTestUtils.getRandomContainerReports(10));
    when(ozoneContainer.getController()).thenReturn(controller);
    when(ozoneContainer.getPipelineReport()).thenReturn(
        HddsTestUtils.getRandomPipelineReports());
    HDDSLayoutVersionManager versionManager =
        mock(HDDSLayoutVersionManager.class);
    when(versionManager.getMetadataLayoutVersion())
        .thenReturn(maxLayoutVersion());
    when(versionManager.getSoftwareLayoutVersion())
        .thenReturn(maxLayoutVersion());
    RegisterEndpointTask endpointTask =
        new RegisterEndpointTask(rpcEndPoint, ozoneContainer,
            mock(StateContext.class), versionManager);
    if (!clearDatanodeDetails) {
      DatanodeDetails datanodeDetails = randomDatanodeDetails();
      endpointTask.setDatanodeDetails(datanodeDetails);
    }
    return endpointTask;
  }

  private EndpointStateMachine registerTaskHelper(InetSocketAddress scmAddress,
      int rpcTimeout, boolean clearDatanodeDetails
  ) throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf(tempDir);
    EndpointStateMachine rpcEndPoint = createEndpoint(conf, scmAddress, rpcTimeout);
    rpcEndPoint.setState(EndpointStateMachine.EndPointStates.REGISTER);
    RegisterEndpointTask endpointTask = getRegisterEndpointTask(clearDatanodeDetails, conf, rpcEndPoint);
    endpointTask.call();
    return rpcEndPoint;
  }

  @Test
  public void testRegisterTask() throws Exception {
    try (EndpointStateMachine rpcEndpoint =
        registerTaskHelper(serverAddress, 1000, false)) {
      // Successful register should move us to Heartbeat state.
      assertEquals(EndpointStateMachine.EndPointStates.HEARTBEAT, rpcEndpoint.getState());
    }
  }

  @Test
  public void testRegisterToInvalidEndpoint() throws Exception {
    InetSocketAddress address = SCMTestUtils.getReuseableAddress();
    try (EndpointStateMachine rpcEndpoint =
        registerTaskHelper(address, 1000, false)) {
      assertEquals(EndpointStateMachine.EndPointStates.REGISTER, rpcEndpoint.getState());
    }
  }

  @Test
  public void testRegisterNoContainerID() throws Exception {
    InetSocketAddress address = SCMTestUtils.getReuseableAddress();
    try (EndpointStateMachine rpcEndpoint =
        registerTaskHelper(address, 1000, true)) {
      // No Container ID, therefore we tell the datanode that we would like to
      // shutdown.
      assertEquals(EndpointStateMachine.EndPointStates.SHUTDOWN, rpcEndpoint.getState());
    }
  }

  @Test
  public void testRegisterRpcTimeout() throws Exception {
    final int rpcTimeout = 1000;
    final int tolerance = 200;
    scmServerImpl.setRpcResponseDelay(rpcTimeout + tolerance * 2);
    OzoneConfiguration conf = SCMTestUtils.getConf(tempDir);

    try (EndpointStateMachine rpcEndPoint = createEndpoint(conf, serverAddress, rpcTimeout)) {
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.REGISTER);
      RegisterEndpointTask endpointTask = getRegisterEndpointTask(false, conf, rpcEndPoint);
      long start = Time.monotonicNow();
      endpointTask.call();
      long end = Time.monotonicNow();
      assertThat(end - start)
          .isGreaterThanOrEqualTo(rpcTimeout)
          .isLessThanOrEqualTo(rpcTimeout + tolerance);
    } finally {
      scmServerImpl.setRpcResponseDelay(0);
    }
  }

  @Test
  public void testHeartbeat() throws Exception {
    DatanodeDetails dataNode = randomDatanodeDetails();
    try (EndpointStateMachine rpcEndPoint =
        createEndpoint(SCMTestUtils.getConf(tempDir),
            serverAddress, 1000)) {
      SCMHeartbeatRequestProto request = SCMHeartbeatRequestProto.newBuilder()
          .setDatanodeDetails(dataNode.getProtoBufMessage())
          .setNodeReport(HddsTestUtils.createNodeReport(
              Arrays.asList(getStorageReports(dataNode.getID())),
              Arrays.asList(getMetadataStorageReports(dataNode.getID()))))
          .build();

      SCMHeartbeatResponseProto responseProto = rpcEndPoint.getEndPoint()
          .sendHeartbeat(request);
      assertNotNull(responseProto);
      assertEquals(0, responseProto.getCommandsCount());
    }
  }

  @Test
  public void testHeartbeatWithCommandStatusReport(@TempDir File endPointTempDir) throws Exception {
    DatanodeDetails dataNode = randomDatanodeDetails();
    try (EndpointStateMachine rpcEndPoint =
        createEndpoint(SCMTestUtils.getConf(endPointTempDir),
            serverAddress, 1000)) {
      // Add some scmCommands for heartbeat response
      addScmCommands();

      SCMHeartbeatRequestProto request = SCMHeartbeatRequestProto.newBuilder()
          .setDatanodeDetails(dataNode.getProtoBufMessage())
          .setNodeReport(HddsTestUtils.createNodeReport(
              Arrays.asList(getStorageReports(dataNode.getID())),
              Arrays.asList(getMetadataStorageReports(dataNode.getID()))))
          .build();

      SCMHeartbeatResponseProto responseProto = rpcEndPoint.getEndPoint()
          .sendHeartbeat(request);
      assertNotNull(responseProto);
      assertEquals(3, responseProto.getCommandsCount());
      assertEquals(0, scmServerImpl.getCommandStatusReportCount());

      // Send heartbeat again from heartbeat endpoint task
      final StateContext stateContext = heartbeatTaskHelper(
          serverAddress, 3000);
      Map<Long, CommandStatus> map = stateContext.getCommandStatusMap();
      assertNotNull(map);
      assertEquals(1, map.size(), "Should have 1 objects");
      assertThat(map).containsKey(3L);
      assertEquals(Type.deleteBlocksCommand, map.get(3L).getType());
      assertEquals(Status.PENDING, map.get(3L).getStatus());

      scmServerImpl.clearScmCommandRequests();
    }
  }

  private void addScmCommands() {
    SCMCommandProto closeCommand = SCMCommandProto.newBuilder()
        .setCloseContainerCommandProto(
            CloseContainerCommandProto.newBuilder().setCmdId(1)
                .setContainerID(1)
                .setPipelineID(PipelineID.randomId().getProtobuf())
                .build())
        .setCommandType(Type.closeContainerCommand)
        .build();
    SCMCommandProto replicationCommand = SCMCommandProto.newBuilder()
        .setReplicateContainerCommandProto(
            ReplicateContainerCommandProto.newBuilder()
                .setCmdId(2)
                .setContainerID(2)
                .build())
        .setCommandType(Type.replicateContainerCommand)
        .build();
    SCMCommandProto deleteBlockCommand = SCMCommandProto.newBuilder()
        .setDeleteBlocksCommandProto(
            DeleteBlocksCommandProto.newBuilder()
                .setCmdId(3)
                .addDeletedBlocksTransactions(
                    DeletedBlocksTransaction.newBuilder()
                        .setContainerID(45)
                        .setCount(1)
                        .setTxID(23)
                        .build())
                .build())
        .setCommandType(Type.deleteBlocksCommand)
        .build();
    scmServerImpl.addScmCommandRequest(closeCommand);
    scmServerImpl.addScmCommandRequest(deleteBlockCommand);
    scmServerImpl.addScmCommandRequest(replicationCommand);
  }

  private StateContext heartbeatTaskHelper(
      InetSocketAddress scmAddress,
      int rpcTimeout
  ) throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf(tempDir);
    // Mini Ozone cluster will not come up if the port is not true, since
    // Ratis will exit if the server port cannot be bound. We can remove this
    // hard coding once we fix the Ratis default behaviour.
    conf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_RANDOM_PORT, true);

    // Create a datanode state machine for stateConext used by endpoint task
    try (DatanodeStateMachine stateMachine = new DatanodeStateMachine(
        randomDatanodeDetails(), conf);
        EndpointStateMachine rpcEndPoint =
            createEndpoint(conf, scmAddress, rpcTimeout)) {
      HddsProtos.DatanodeDetailsProto datanodeDetailsProto =
          randomDatanodeDetails().getProtoBufMessage();
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.HEARTBEAT);

      final StateContext stateContext =
          new StateContext(conf, DatanodeStateMachine.DatanodeStates.RUNNING,
              stateMachine, "");

      HeartbeatEndpointTask endpointTask =
          new HeartbeatEndpointTask(rpcEndPoint, conf, stateContext,
              stateMachine.getLayoutVersionManager());
      endpointTask.setDatanodeDetailsProto(datanodeDetailsProto);
      endpointTask.call();
      assertNotNull(endpointTask.getDatanodeDetailsProto());

      assertEquals(EndpointStateMachine.EndPointStates.HEARTBEAT, rpcEndPoint.getState());
      return stateContext;
    }
  }

  @Test
  public void testHeartbeatTask() throws Exception {
    heartbeatTaskHelper(serverAddress, 1000);
  }

  @Test
  public void testHeartbeatTaskToInvalidNode() throws Exception {
    InetSocketAddress invalidAddress = SCMTestUtils.getReuseableAddress();
    heartbeatTaskHelper(invalidAddress, 1000);
  }

  @Test
  public void testHeartbeatTaskRpcTimeOut() throws Exception {
    final long rpcTimeout = 1000;
    final long tolerance = 200;
    scmServerImpl.setRpcResponseDelay(1500);
    long start = Time.monotonicNow();
    InetSocketAddress invalidAddress = SCMTestUtils.getReuseableAddress();
    heartbeatTaskHelper(invalidAddress, 1000);
    long end = Time.monotonicNow();
    scmServerImpl.setRpcResponseDelay(0);
    // 6s is introduced by DeleteBlocksCommandHandler#stop
    assertThat(end - start).isLessThanOrEqualTo(rpcTimeout + tolerance + 6000);
  }

  private OzoneContainer createVolume(OzoneConfiguration conf)
      throws IOException {
    OzoneContainer ozoneContainer = new OzoneContainer(dnDetails, conf,
        ContainerTestUtils.getMockContext(dnDetails, ozoneConf),
        volumeChoosingPolicy);

    String clusterId = scmServerImpl.getClusterId();

    MutableVolumeSet volumeSet = ozoneContainer.getVolumeSet();
    ContainerTestUtils.createDbInstancesForTestIfNeeded(volumeSet,
        clusterId, clusterId, conf);
    // VolumeSet for this test, contains only 1 volume
    assertEquals(1, volumeSet.getVolumesList().size());
    StorageVolume volume = volumeSet.getVolumesList().get(0);

    // Check instanceof and typecast
    assertInstanceOf(HddsVolume.class, volume);
    return ozoneContainer;
  }

  private KeyValueContainer addContainer(
      OzoneConfiguration conf, HddsVolume hddsVolume)
      throws IOException {
    String clusterId = scmServerImpl.getClusterId();
    KeyValueContainer container = ContainerTestUtils.
        addContainerToVolumeDir(hddsVolume, clusterId,
            conf, OzoneConsts.SCHEMA_V3);
    File containerDBFile = container.getContainerDBFile();
    assertTrue(container.getContainerFile().exists());
    assertTrue(containerDBFile.exists());
    return container;
  }
}
