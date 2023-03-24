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
package org.apache.hadoop.ozone.container.common;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CloseContainerCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus.Status;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeleteBlocksCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicateContainerCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.VersionInfo;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
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
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.replication.ReplicationServer.ReplicationConfig;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.Time;

import static org.apache.hadoop.hdds.DFSConfigKeysLegacy.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.ozone.container.upgrade.UpgradeUtils.defaultLayoutVersionProto;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.createEndpoint;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the endpoints.
 */
public class TestEndPoint {
  private static InetSocketAddress serverAddress;
  private static RPC.Server scmServer;
  private static ScmTestMock scmServerImpl;
  private static File testDir;

  @AfterAll
  public static void tearDown() throws Exception {
    if (scmServer != null) {
      scmServer.stop();
    }
    FileUtil.fullyDelete(testDir);
  }

  @BeforeAll
  public static void setUp() throws Exception {
    serverAddress = SCMTestUtils.getReuseableAddress();
    scmServerImpl = new ScmTestMock();
    scmServer = SCMTestUtils.startScmRpcServer(SCMTestUtils.getConf(),
        scmServerImpl, serverAddress, 10);
    testDir = PathUtils.getTestDir(TestEndPoint.class);
  }

  /**
   * This test asserts that we are able to make a version call to SCM server
   * and gets back the expected values.
   */
  @Test
  public void testGetVersion() throws Exception {
    try (EndpointStateMachine rpcEndPoint =
        createEndpoint(SCMTestUtils.getConf(),
            serverAddress, 1000)) {
      SCMVersionResponseProto responseProto = rpcEndPoint.getEndPoint()
          .getVersion(null);
      Assertions.assertNotNull(responseProto);
      Assertions.assertEquals(VersionInfo.DESCRIPTION_KEY,
          responseProto.getKeys(0).getKey());
      Assertions.assertEquals(VersionInfo.getLatestVersion().getDescription(),
          responseProto.getKeys(0).getValue());
    }
  }

  /**
   * We make getVersion RPC call, but via the VersionEndpointTask which is
   * how the state machine would make the call.
   */
  @Test
  public void testGetVersionTask() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf();
    conf.setFromObject(new ReplicationConfig().setPort(0));
    try (EndpointStateMachine rpcEndPoint = createEndpoint(conf,
        serverAddress, 1000)) {
      DatanodeDetails datanodeDetails = randomDatanodeDetails();
      conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_DATASTREAM_ENABLED,
          true);
      conf.setBoolean(
          OzoneConfigKeys.DFS_CONTAINER_RATIS_DATASTREAM_RANDOM_PORT, true);
      OzoneContainer ozoneContainer = new OzoneContainer(
          datanodeDetails, conf, getContext(datanodeDetails));
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.GETVERSION);
      VersionEndpointTask versionTask = new VersionEndpointTask(rpcEndPoint,
          conf, ozoneContainer);
      EndpointStateMachine.EndPointStates newState = versionTask.call();

      // if version call worked the endpoint should automatically move to the
      // next state.
      Assertions.assertEquals(EndpointStateMachine.EndPointStates.REGISTER,
          newState);

      // Now rpcEndpoint should remember the version it got from SCM
      Assertions.assertNotNull(rpcEndPoint.getVersion());
    }
  }


  /**
   * Writing data to tmp dir and checking if it has been cleaned.
   * Add some entries to DatanodeStore.MetadataTable and check
   * that they have been deleted during tmp dir cleanup.
   */
  @Test
  public void testTmpDirCleanup() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf();
    conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT,
        true);
    conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT,
        true);
    conf.setFromObject(new ReplicationConfig().setPort(0));
    try (EndpointStateMachine rpcEndPoint = createEndpoint(conf,
        serverAddress, 1000)) {
      DatanodeDetails datanodeDetails = randomDatanodeDetails();
      OzoneContainer ozoneContainer = new OzoneContainer(
          datanodeDetails, conf, getContext(datanodeDetails));
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.GETVERSION);

      String clusterId = scmServerImpl.getClusterId();

      MutableVolumeSet volumeSet = ozoneContainer.getVolumeSet();
      ContainerTestUtils.createDbInstancesForTestIfNeeded(volumeSet,
          clusterId, clusterId, conf);
      // VolumeSet for this test, contains only 1 volume
      Assertions.assertEquals(1, volumeSet.getVolumesList().size());
      StorageVolume volume = volumeSet.getVolumesList().get(0);

      // Check instanceof and typecast
      Assertions.assertTrue(volume instanceof HddsVolume);
      HddsVolume hddsVolume = (HddsVolume) volume;

      // Write some data before calling versionTask.call()
      // Create a container and move it under the tmp delete dir.
      KeyValueContainer container = ContainerTestUtils.
          setUpTestContainerUnderTmpDir(hddsVolume, clusterId,
              conf, OzoneConsts.SCHEMA_V3);
      File containerDBFile = container.getContainerDBFile();

      Assertions.assertTrue(container.getContainerFile().exists());
      Assertions.assertTrue(containerDBFile.exists());

      KeyValueContainerData containerData = container.getContainerData();

      // Get DBHandle
      try (DBHandle dbHandle = BlockUtils.getDB(containerData, conf)) {

        DatanodeStoreSchemaThreeImpl store = (DatanodeStoreSchemaThreeImpl)
            dbHandle.getStore();
        Table<String, Long> metadataTable = store.getMetadataTable();

        // DB MetadataTable is empty
        Assertions.assertEquals(0, metadataTable.getEstimatedKeyCount());

        // Add some keys to MetadataTable
        metadataTable.put(containerData.getBytesUsedKey(), 0L);
        metadataTable.put(containerData.getBlockCountKey(), 0L);
        metadataTable.put(containerData.getPendingDeleteBlockCountKey(), 0L);

        // The new keys should have been added in the MetadataTable
        Assertions.assertEquals(3, metadataTable.getEstimatedKeyCount());

        // versionTask.call() cleans the tmp dir and removes container from DB
        VersionEndpointTask versionTask = new VersionEndpointTask(rpcEndPoint,
            conf, ozoneContainer);
        EndpointStateMachine.EndPointStates newState = versionTask.call();

        Assertions.assertEquals(EndpointStateMachine.EndPointStates.REGISTER,
            newState);

        // assert that tmp dir is empty
        Assertions.assertFalse(KeyValueContainerUtil.ContainerDeleteDirectory
            .getDeleteLeftovers(hddsVolume).hasNext());
        // All DB keys have been deleted, MetadataTable should be empty
        Assertions.assertEquals(0, metadataTable.getEstimatedKeyCount());
      }
    }
  }

  @Test
  public void testCheckVersionResponse() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf();
    conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT,
        true);
    conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT,
        true);
    conf.setBoolean(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_DATASTREAM_RANDOM_PORT, true);
    conf.setFromObject(new ReplicationConfig().setPort(0));
    try (EndpointStateMachine rpcEndPoint = createEndpoint(conf,
        serverAddress, 1000)) {
      GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
          .captureLogs(VersionEndpointTask.LOG);
      DatanodeDetails datanodeDetails = randomDatanodeDetails();
      OzoneContainer ozoneContainer = new OzoneContainer(
          datanodeDetails, conf, getContext(datanodeDetails));
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.GETVERSION);
      VersionEndpointTask versionTask = new VersionEndpointTask(rpcEndPoint,
          conf, ozoneContainer);
      EndpointStateMachine.EndPointStates newState = versionTask.call();

      // if version call worked the endpoint should automatically move to the
      // next state.
      Assertions.assertEquals(EndpointStateMachine.EndPointStates.REGISTER,
          newState);

      // Now rpcEndpoint should remember the version it got from SCM
      Assertions.assertNotNull(rpcEndPoint.getVersion());

      // Now change server cluster ID, so datanode cluster ID will be
      // different from SCM server response cluster ID.
      String newClusterId = UUID.randomUUID().toString();
      scmServerImpl.setClusterId(newClusterId);
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.GETVERSION);
      newState = versionTask.call();
      Assertions.assertEquals(EndpointStateMachine.EndPointStates.SHUTDOWN,
          newState);
      List<HddsVolume> volumesList = StorageVolumeUtil.getHddsVolumesList(
          ozoneContainer.getVolumeSet().getFailedVolumesList());
      Assertions.assertEquals(1, volumesList.size());
      Assertions.assertTrue(logCapturer.getOutput()
          .contains("org.apache.hadoop.ozone.common" +
              ".InconsistentStorageStateException: Mismatched ClusterIDs"));
      Assertions.assertEquals(0,
          ozoneContainer.getVolumeSet().getVolumesList().size());
      Assertions.assertEquals(1,
          ozoneContainer.getVolumeSet().getFailedVolumesList().size());
    }
  }

  /**
   * This test makes a call to end point where there is no SCM server. We
   * expect that versionTask should be able to handle it.
   */
  @Test
  public void testGetVersionToInvalidEndpoint() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf();
    InetSocketAddress nonExistentServerAddress = SCMTestUtils
        .getReuseableAddress();
    try (EndpointStateMachine rpcEndPoint = createEndpoint(conf,
        nonExistentServerAddress, 1000)) {
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.GETVERSION);
      DatanodeDetails datanodeDetails = randomDatanodeDetails();
      OzoneContainer ozoneContainer = new OzoneContainer(
          datanodeDetails, conf, getContext(datanodeDetails));
      VersionEndpointTask versionTask = new VersionEndpointTask(rpcEndPoint,
          conf, ozoneContainer);
      EndpointStateMachine.EndPointStates newState = versionTask.call();

      // This version call did NOT work, so endpoint should remain in the same
      // state.
      Assertions.assertEquals(EndpointStateMachine.EndPointStates.GETVERSION,
          newState);
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
    OzoneConfiguration conf = SCMTestUtils.getConf();

    try (EndpointStateMachine rpcEndPoint = createEndpoint(conf,
        serverAddress, (int) rpcTimeout)) {
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.GETVERSION);
      DatanodeDetails datanodeDetails = randomDatanodeDetails();
      OzoneContainer ozoneContainer = new OzoneContainer(
          datanodeDetails, conf, getContext(datanodeDetails));
      VersionEndpointTask versionTask = new VersionEndpointTask(rpcEndPoint,
          conf, ozoneContainer);

      scmServerImpl.setRpcResponseDelay(1500);
      long start = Time.monotonicNow();
      EndpointStateMachine.EndPointStates newState = versionTask.call();
      long end = Time.monotonicNow();
      scmServerImpl.setRpcResponseDelay(0);
      Assertions.assertTrue(end - start <= rpcTimeout + tolerance);
      Assertions.assertEquals(EndpointStateMachine.EndPointStates.GETVERSION,
          newState);
    }
  }

  @Test
  public void testRegister() throws Exception {
    DatanodeDetails nodeToRegister = randomDatanodeDetails();
    try (EndpointStateMachine rpcEndPoint = createEndpoint(
        SCMTestUtils.getConf(), serverAddress, 1000)) {
      SCMRegisteredResponseProto responseProto = rpcEndPoint.getEndPoint()
          .register(nodeToRegister.getExtendedProtoBufMessage(), HddsTestUtils
                  .createNodeReport(
                      Arrays.asList(getStorageReports(
                          nodeToRegister.getUuid())),
                      Arrays.asList(getMetadataStorageReports(
                          nodeToRegister.getUuid()))),
              HddsTestUtils.getRandomContainerReports(10),
              HddsTestUtils.getRandomPipelineReports(),
              defaultLayoutVersionProto());
      Assertions.assertNotNull(responseProto);
      Assertions.assertEquals(nodeToRegister.getUuidString(),
          responseProto.getDatanodeUUID());
      Assertions.assertNotNull(responseProto.getClusterID());
      Assertions.assertEquals(10, scmServerImpl.
          getContainerCountsForDatanode(nodeToRegister));
      Assertions.assertEquals(1,
          scmServerImpl.getNodeReportsCount(nodeToRegister));
    }
  }

  private StorageReportProto getStorageReports(UUID id) {
    String storagePath = testDir.getAbsolutePath() + "/data-" + id;
    return HddsTestUtils.createStorageReport(id, storagePath, 100, 10, 90,
        null);
  }

  private MetadataStorageReportProto getMetadataStorageReports(UUID id) {
    String storagePath = testDir.getAbsolutePath() + "/metadata-" + id;
    return HddsTestUtils.createMetadataStorageReport(storagePath, 100, 10, 90,
        null);
  }

  private EndpointStateMachine registerTaskHelper(InetSocketAddress scmAddress,
      int rpcTimeout, boolean clearDatanodeDetails
  ) throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf();
    EndpointStateMachine rpcEndPoint =
        createEndpoint(conf,
            scmAddress, rpcTimeout);
    rpcEndPoint.setState(EndpointStateMachine.EndPointStates.REGISTER);
    OzoneContainer ozoneContainer = mock(OzoneContainer.class);
    UUID datanodeID = UUID.randomUUID();
    when(ozoneContainer.getNodeReport()).thenReturn(HddsTestUtils
        .createNodeReport(Arrays.asList(getStorageReports(datanodeID)),
            Arrays.asList(getMetadataStorageReports(datanodeID))));
    ContainerController controller = Mockito.mock(ContainerController.class);
    when(controller.getContainerReport()).thenReturn(
        HddsTestUtils.getRandomContainerReports(10));
    when(ozoneContainer.getController()).thenReturn(controller);
    when(ozoneContainer.getPipelineReport()).thenReturn(
        HddsTestUtils.getRandomPipelineReports());
    HDDSLayoutVersionManager versionManager =
        Mockito.mock(HDDSLayoutVersionManager.class);
    when(versionManager.getMetadataLayoutVersion())
        .thenReturn(maxLayoutVersion());
    when(versionManager.getSoftwareLayoutVersion())
        .thenReturn(maxLayoutVersion());
    RegisterEndpointTask endpointTask =
        new RegisterEndpointTask(rpcEndPoint, conf, ozoneContainer,
            mock(StateContext.class), versionManager);
    if (!clearDatanodeDetails) {
      DatanodeDetails datanodeDetails = randomDatanodeDetails();
      endpointTask.setDatanodeDetails(datanodeDetails);
    }
    endpointTask.call();
    return rpcEndPoint;
  }

  @Test
  public void testRegisterTask() throws Exception {
    try (EndpointStateMachine rpcEndpoint =
        registerTaskHelper(serverAddress, 1000, false)) {
      // Successful register should move us to Heartbeat state.
      Assertions.assertEquals(EndpointStateMachine.EndPointStates.HEARTBEAT,
          rpcEndpoint.getState());
    }
  }

  @Test
  public void testRegisterToInvalidEndpoint() throws Exception {
    InetSocketAddress address = SCMTestUtils.getReuseableAddress();
    try (EndpointStateMachine rpcEndpoint =
        registerTaskHelper(address, 1000, false)) {
      Assertions.assertEquals(EndpointStateMachine.EndPointStates.REGISTER,
          rpcEndpoint.getState());
    }
  }

  @Test
  public void testRegisterNoContainerID() throws Exception {
    InetSocketAddress address = SCMTestUtils.getReuseableAddress();
    try (EndpointStateMachine rpcEndpoint =
        registerTaskHelper(address, 1000, true)) {
      // No Container ID, therefore we tell the datanode that we would like to
      // shutdown.
      Assertions.assertEquals(EndpointStateMachine.EndPointStates.SHUTDOWN,
          rpcEndpoint.getState());
    }
  }

  @Test
  public void testRegisterRpcTimeout() throws Exception {
    final long rpcTimeout = 1000;
    final long tolerance = 200;
    scmServerImpl.setRpcResponseDelay(1500);
    long start = Time.monotonicNow();
    registerTaskHelper(serverAddress, 1000, false).close();
    long end = Time.monotonicNow();
    scmServerImpl.setRpcResponseDelay(0);
    Assertions.assertTrue(end - start <= rpcTimeout + tolerance);
  }

  @Test
  public void testHeartbeat() throws Exception {
    DatanodeDetails dataNode = randomDatanodeDetails();
    try (EndpointStateMachine rpcEndPoint =
        createEndpoint(SCMTestUtils.getConf(),
            serverAddress, 1000)) {
      SCMHeartbeatRequestProto request = SCMHeartbeatRequestProto.newBuilder()
          .setDatanodeDetails(dataNode.getProtoBufMessage())
          .setNodeReport(HddsTestUtils.createNodeReport(
              Arrays.asList(getStorageReports(dataNode.getUuid())),
              Arrays.asList(getMetadataStorageReports(dataNode.getUuid()))))
          .build();

      SCMHeartbeatResponseProto responseProto = rpcEndPoint.getEndPoint()
          .sendHeartbeat(request);
      Assertions.assertNotNull(responseProto);
      Assertions.assertEquals(0, responseProto.getCommandsCount());
    }
  }

  @Test
  public void testHeartbeatWithCommandStatusReport() throws Exception {
    DatanodeDetails dataNode = randomDatanodeDetails();
    try (EndpointStateMachine rpcEndPoint =
        createEndpoint(SCMTestUtils.getConf(),
            serverAddress, 1000)) {
      // Add some scmCommands for heartbeat response
      addScmCommands();

      SCMHeartbeatRequestProto request = SCMHeartbeatRequestProto.newBuilder()
          .setDatanodeDetails(dataNode.getProtoBufMessage())
          .setNodeReport(HddsTestUtils.createNodeReport(
              Arrays.asList(getStorageReports(dataNode.getUuid())),
              Arrays.asList(getMetadataStorageReports(dataNode.getUuid()))))
          .build();

      SCMHeartbeatResponseProto responseProto = rpcEndPoint.getEndPoint()
          .sendHeartbeat(request);
      Assertions.assertNotNull(responseProto);
      Assertions.assertEquals(3, responseProto.getCommandsCount());
      Assertions.assertEquals(0, scmServerImpl.getCommandStatusReportCount());

      // Send heartbeat again from heartbeat endpoint task
      final StateContext stateContext = heartbeatTaskHelper(
          serverAddress, 3000);
      Map<Long, CommandStatus> map = stateContext.getCommandStatusMap();
      Assertions.assertNotNull(map);
      Assertions.assertEquals(1, map.size(), "Should have 1 objects");
      Assertions.assertTrue(map.containsKey(3L));
      Assertions.assertEquals(Type.deleteBlocksCommand, map.get(3L).getType());
      Assertions.assertEquals(Status.PENDING, map.get(3L).getStatus());

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
    OzoneConfiguration conf = SCMTestUtils.getConf();
    conf.set(DFS_DATANODE_DATA_DIR_KEY, testDir.getAbsolutePath());
    conf.set(OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    // Mini Ozone cluster will not come up if the port is not true, since
    // Ratis will exit if the server port cannot be bound. We can remove this
    // hard coding once we fix the Ratis default behaviour.
    conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT, true);

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
              stateMachine);

      HeartbeatEndpointTask endpointTask =
          new HeartbeatEndpointTask(rpcEndPoint, conf, stateContext,
              stateMachine.getLayoutVersionManager());
      endpointTask.setDatanodeDetailsProto(datanodeDetailsProto);
      endpointTask.call();
      Assertions.assertNotNull(endpointTask.getDatanodeDetailsProto());

      Assertions.assertEquals(EndpointStateMachine.EndPointStates.HEARTBEAT,
          rpcEndPoint.getState());
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
    Assertions.assertTrue(end - start <= rpcTimeout + tolerance + 6000);
  }

  private StateContext getContext(DatanodeDetails datanodeDetails) {
    DatanodeStateMachine stateMachine = Mockito.mock(
        DatanodeStateMachine.class);
    StateContext context = Mockito.mock(StateContext.class);
    Mockito.when(stateMachine.getDatanodeDetails()).thenReturn(datanodeDetails);
    Mockito.when(context.getParent()).thenReturn(stateMachine);
    return context;
  }

}
