/*
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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.states.DatanodeState;
import org.apache.hadoop.ozone.container.common.states.datanode.InitDatanodeState;
import org.apache.hadoop.ozone.container.common.states.datanode.RunningDatanodeState;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_RPC_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests the datanode state machine class and its states.
 */
public class TestDatanodeStateMachine {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDatanodeStateMachine.class);
  // Changing it to 1, as current code checks for multiple scm directories,
  // and fail if exists
  private static final int SCM_SERVER_COUNT = 1;
  private List<String> serverAddresses;
  private List<RPC.Server> scmServers;
  private List<ScmTestMock> mockServers;
  private ExecutorService executorService;
  private OzoneConfiguration conf;
  @TempDir
  private File testRoot;

  @BeforeEach
  void setUp() throws Exception {
    conf = SCMTestUtils.getConf(testRoot);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_RPC_TIMEOUT, 500,
        TimeUnit.MILLISECONDS);
    conf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_RANDOM_PORT, true);
    conf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_IPC_RANDOM_PORT, true);
    conf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED,
        true);
    conf.setBoolean(
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_RANDOM_PORT, true);
    serverAddresses = new ArrayList<>();
    scmServers = new ArrayList<>();
    mockServers = new ArrayList<>();
    for (int x = 0; x < SCM_SERVER_COUNT; x++) {
      int port = SCMTestUtils.getReuseableAddress().getPort();
      String address = "127.0.0.1";
      serverAddresses.add(address + ":" + port);
      ScmTestMock mock = new ScmTestMock();
      scmServers.add(SCMTestUtils.startScmRpcServer(conf, mock,
          new InetSocketAddress(address, port), 10));
      mockServers.add(mock);
    }

    conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES,
        serverAddresses.toArray(new String[0]));

    executorService = HadoopExecutors.newCachedThreadPool(
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("TestDataNodeStateMachineThread-%d").build());
  }

  @AfterEach
  public void tearDown() throws Exception {
    try {
      if (executorService != null) {
        executorService.shutdown();
        try {
          if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
          }

          if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
            LOG.error("Unable to shutdown properly.");
          }
        } catch (InterruptedException e) {
          LOG.error("Error attempting to shutdown.", e);
          executorService.shutdownNow();
        }
      }
      for (RPC.Server s : scmServers) {
        s.stop();
      }
    } catch (Exception e) {
      //ignore all exception from the shutdown
    }
  }

  /**
   * Assert that starting statemachine executes the Init State.
   */
  @Test
  public void testStartStopDatanodeStateMachine() throws IOException,
      InterruptedException, TimeoutException {
    try (DatanodeStateMachine stateMachine =
        new DatanodeStateMachine(getNewDatanodeDetails(), conf)) {
      stateMachine.startDaemon();
      SCMConnectionManager connectionManager =
          stateMachine.getConnectionManager();
      GenericTestUtils.waitFor(
          () -> {
            int size = connectionManager.getValues().size();
            LOG.info("connectionManager.getValues().size() is {}", size);
            return size == 1;
          }, 1000, 30000);

      stateMachine.stopDaemon();
      assertTrue(stateMachine.isDaemonStopped());
    }
  }

  /**
   * This test explores the state machine by invoking each call in sequence just
   * like as if the state machine would call it. Because this is a test we are
   * able to verify each of the assumptions.
   * <p>
   * Here is what happens at High level.
   * <p>
   * 1. We start the datanodeStateMachine in the INIT State.
   * <p>
   * 2. We invoke the INIT state task.
   * <p>
   * 3. That creates a set of RPC endpoints that are ready to connect to SCMs.
   * <p>
   * 4. We assert that we have moved to the running state for the
   * DatanodeStateMachine.
   * <p>
   * 5. We get the task for the Running State - Executing that running state,
   * makes the first network call in of the state machine. The Endpoint is in
   * the GETVERSION State and we invoke the task.
   * <p>
   * 6. We assert that this call was a success by checking that each of the
   * endponts now have version response that it got from the SCM server that it
   * was talking to and also each of the mock server serviced one RPC call.
   * <p>
   * 7. Since the Register is done now, next calls to get task will return
   * HeartbeatTask, which sends heartbeats to SCM. We assert that we get right
   * task from sub-system below.
   *
   * @throws IOException
   */
  @Test
  public void testDatanodeStateContext() throws IOException,
      InterruptedException, ExecutionException, TimeoutException {
    // There is no mini cluster started in this test,
    // create a ID file so that state machine could load a fake datanode ID.
    File idPath = new File(
        conf.get(ScmConfigKeys.OZONE_SCM_DATANODE_ID_DIR),
        OzoneConsts.OZONE_SCM_DATANODE_ID_FILE_DEFAULT);
    idPath.delete();
    DatanodeDetails datanodeDetails = getNewDatanodeDetails();
    DatanodeDetails.Port port = DatanodeDetails.newStandalonePort(
        OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT_DEFAULT);
    datanodeDetails.setPort(port);
    ContainerUtils.writeDatanodeDetailsTo(datanodeDetails, idPath, conf);
    try (DatanodeStateMachine stateMachine =
             new DatanodeStateMachine(datanodeDetails, conf)) {
      HddsDatanodeService hddsDatanodeService = new HddsDatanodeService();
      hddsDatanodeService.setDatanodeStateMachine(stateMachine);
      stateMachine.getContainer().setHddsDatanodeService(hddsDatanodeService);
      DatanodeStateMachine.DatanodeStates currentState =
          stateMachine.getContext().getState();
      assertEquals(DatanodeStateMachine.DatanodeStates.INIT,
          currentState);

      DatanodeState<DatanodeStateMachine.DatanodeStates> task =
          stateMachine.getContext().getTask();
      assertEquals(InitDatanodeState.class, task.getClass());

      task.execute(executorService);
      DatanodeStateMachine.DatanodeStates newState =
          task.await(2, TimeUnit.SECONDS);

      for (EndpointStateMachine endpoint :
          stateMachine.getConnectionManager().getValues()) {
        // We assert that each of the is in State GETVERSION.
        assertEquals(EndpointStateMachine.EndPointStates.GETVERSION,
            endpoint.getState());
      }

      // The Datanode has moved into Running State, since endpoints are created.
      // We move to running state when we are ready to issue RPC calls to SCMs.
      assertEquals(DatanodeStateMachine.DatanodeStates.RUNNING,
          newState);

      // If we had called context.execute instead of calling into each state
      // this would have happened automatically.
      stateMachine.getContext().setState(newState);
      task = stateMachine.getContext().getTask();
      assertEquals(RunningDatanodeState.class, task.getClass());

      DatanodeLayoutStorage layoutStorage = new DatanodeLayoutStorage(conf,
          UUID.randomUUID().toString(),
          HDDSLayoutFeature.DATANODE_SCHEMA_V3.layoutVersion());
      layoutStorage.initialize();

      // This execute will invoke getVersion calls against all SCM endpoints
      // that we know of.

      task.execute(executorService);
      newState = task.await(10, TimeUnit.SECONDS);

      // Wait for GetVersion call (called by task.execute) to finish. After
      // Earlier task.execute called into GetVersion. Wait for the execution
      // to finish and the endPointState to move to REGISTER state.
      GenericTestUtils.waitFor(() -> {
        for (EndpointStateMachine endpoint :
            stateMachine.getConnectionManager().getValues()) {
          if (endpoint.getState() !=
              EndpointStateMachine.EndPointStates.REGISTER) {
            return false;
          }
        }
        return true;
      }, 1000, 50000);

      // If we are in running state, we should be in running.
      assertEquals(DatanodeStateMachine.DatanodeStates.RUNNING,
          newState);

      for (EndpointStateMachine endpoint :
          stateMachine.getConnectionManager().getValues()) {

        // Since the earlier task.execute called into GetVersion, the
        // endPointState Machine should move to REGISTER state.
        assertEquals(EndpointStateMachine.EndPointStates.REGISTER,
            endpoint.getState());

        // We assert that each of the end points have gotten a version from the
        // SCM Server.
        assertNotNull(endpoint.getVersion());
      }

      // We can also assert that all mock servers have received only one RPC
      // call at this point of time.
      for (ScmTestMock mock : mockServers) {
        assertEquals(1, mock.getRpcCount());
      }

      // This task is the Running task, but running task executes tasks based
      // on the state of Endpoints, hence this next call will be a Register at
      // the endpoint RPC level.
      task = stateMachine.getContext().getTask();
      task.execute(executorService);
      newState = task.await(2, TimeUnit.SECONDS);

      // If we are in running state, we should be in running.
      assertEquals(DatanodeStateMachine.DatanodeStates.RUNNING,
          newState);

      for (ScmTestMock mock : mockServers) {
        assertEquals(2, mock.getRpcCount());
      }

      // This task is the Running task, but running task executes tasks based
      // on the state of Endpoints, hence this next call will be a
      // HeartbeatTask at the endpoint RPC level.
      task = stateMachine.getContext().getTask();
      task.execute(executorService);
      newState = task.await(2, TimeUnit.SECONDS);

      // If we are in running state, we should be in running.
      assertEquals(DatanodeStateMachine.DatanodeStates.RUNNING,
          newState);


      for (ScmTestMock mock : mockServers) {
        assertEquals(1, mock.getHeartbeatCount());
      }
    }
  }

  @Test
  public void testDatanodeStateMachineWithIdWriteFail() throws Exception {

    File idPath = new File(
        conf.get(ScmConfigKeys.OZONE_SCM_DATANODE_ID_DIR),
        OzoneConsts.OZONE_SCM_DATANODE_ID_FILE_DEFAULT);
    idPath.delete();
    DatanodeDetails datanodeDetails = getNewDatanodeDetails();
    DatanodeDetails.Port port = DatanodeDetails.newStandalonePort(
        OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT_DEFAULT);
    datanodeDetails.setPort(port);

    try (DatanodeStateMachine stateMachine =
             new DatanodeStateMachine(datanodeDetails, conf)) {
      DatanodeStateMachine.DatanodeStates currentState =
          stateMachine.getContext().getState();
      assertEquals(DatanodeStateMachine.DatanodeStates.INIT,
          currentState);

      DatanodeState<DatanodeStateMachine.DatanodeStates> task =
          stateMachine.getContext().getTask();
      assertEquals(InitDatanodeState.class, task.getClass());

      //Set the idPath to read only, state machine will fail to write
      // datanodeId file and set the state to shutdown.
      idPath.getParentFile().mkdirs();
      idPath.getParentFile().setReadOnly();

      task.execute(executorService);
      DatanodeStateMachine.DatanodeStates newState =
          task.await(2, TimeUnit.SECONDS);

      //As, we have changed the permission of idPath to readable, writing
      // will fail and it will set the state to shutdown.
      assertEquals(DatanodeStateMachine.DatanodeStates.SHUTDOWN,
          newState);

      //Setting back to writable.
      idPath.getParentFile().setWritable(true);
    }
  }

  /**
   * Test state transition with a list of invalid scm configurations,
   * and verify the state transits to SHUTDOWN each time.
   */
  @Test
  public void testDatanodeStateMachineWithInvalidConfiguration()
      throws Exception {
    List<Map.Entry<String, String>> confList =
        new ArrayList<>();

    // Invalid ozone.scm.names
    /** Empty **/
    confList.add(Maps.immutableEntry(
        ScmConfigKeys.OZONE_SCM_NAMES, ""));
    /** Invalid schema **/
    confList.add(Maps.immutableEntry(
        ScmConfigKeys.OZONE_SCM_NAMES, "x..y"));
    /** Invalid port **/
    confList.add(Maps.immutableEntry(
        ScmConfigKeys.OZONE_SCM_NAMES, "scm:xyz"));
    /** Port out of range **/
    confList.add(Maps.immutableEntry(
        ScmConfigKeys.OZONE_SCM_NAMES, "scm:123456"));

    confList.forEach((entry) -> {
      OzoneConfiguration perTestConf = new OzoneConfiguration(conf);
      perTestConf.setStrings(entry.getKey(), entry.getValue());
      LOG.info("Test with {} = {}", entry.getKey(), entry.getValue());
      try (DatanodeStateMachine stateMachine = new DatanodeStateMachine(
          getNewDatanodeDetails(), perTestConf)) {
        DatanodeStateMachine.DatanodeStates currentState =
            stateMachine.getContext().getState();
        assertEquals(DatanodeStateMachine.DatanodeStates.INIT,
            currentState);
        DatanodeState<DatanodeStateMachine.DatanodeStates> task =
            stateMachine.getContext().getTask();
        task.execute(executorService);
        DatanodeStateMachine.DatanodeStates newState =
            task.await(2, TimeUnit.SECONDS);
        assertEquals(DatanodeStateMachine.DatanodeStates.SHUTDOWN,
            newState);
      } catch (Exception e) {
        fail("Unexpected exception found");
      }
    });
  }

  private DatanodeDetails getNewDatanodeDetails() {
    DatanodeDetails.Port containerPort = DatanodeDetails.newStandalonePort(0);
    DatanodeDetails.Port ratisPort = DatanodeDetails.newRatisPort(0);
    DatanodeDetails.Port restPort = DatanodeDetails.newRestPort(0);
    DatanodeDetails.Port streamPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.RATIS_DATASTREAM, 0);
    return DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort)
        .addPort(streamPort)
        .build();
  }
}
