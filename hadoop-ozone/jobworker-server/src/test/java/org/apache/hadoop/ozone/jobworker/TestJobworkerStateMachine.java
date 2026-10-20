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

package org.apache.hadoop.ozone.jobworker;

import static org.apache.hadoop.hdds.server.ServerUtils.executorServiceShutdownGraceful;
import static org.apache.hadoop.ozone.conf.OMJobworkerConfiguration.getGrpcPortKey;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.MockJobworkerDetails;
import org.apache.hadoop.ozone.jobworker.states.InitJobworkerState;
import org.apache.hadoop.ozone.jobworker.states.JobworkerStateHandler;
import org.apache.hadoop.ozone.jobworker.states.RunningJobworkerState;
import org.apache.hadoop.ozone.jobworker.utils.JobworkerGrpcRequestHandlerMock;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the jobworker state machine class and its states.
 */
public class TestJobworkerStateMachine {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestJobworkerStateMachine.class);
  private static final String OM_SERVICE_ID = "omServiceId";
  private static final int OM_SERVER_COUNT = 3;
  private static final int OM_GROUP_COUNT = 2;
  private List<Server> omServers;
  private List<JobworkerGrpcRequestHandlerMock> mockServers;
  private ExecutorService executorService;
  private OzoneConfiguration conf;
  private static final ImmutableMap<String, String> OM_SERVICE1_HOST_ADDRESS;
  private static final ImmutableMap<String, String> OM_SERVICE2_HOST_ADDRESS;

  static {
    OM_SERVICE1_HOST_ADDRESS = ImmutableMap.of(
        OZONE_OM_ADDRESS_KEY + ".omServiceId1.om1", "localhost",
        OZONE_OM_ADDRESS_KEY + ".omServiceId1.om2", "localhost",
        OZONE_OM_ADDRESS_KEY + ".omServiceId1.om3", "localhost"
    );
    OM_SERVICE2_HOST_ADDRESS = ImmutableMap.of(
        OZONE_OM_ADDRESS_KEY + ".omServiceId2.om1", "localhost",
        OZONE_OM_ADDRESS_KEY + ".omServiceId2.om2", "localhost",
        OZONE_OM_ADDRESS_KEY + ".omServiceId2.om3", "localhost"
    );
  }

  private static Stream<Arguments> invalidConfigProvider() {
    return Stream.of(
        Arguments.of("Empty OM service ID", ImmutableMap.of(
            OZONE_OM_ADDRESS_KEY, "",
            JobworkerConfiguration.OZONE_JOBWORKER_OM_SERVICE_IDS_KEY, ""
        )),

        Arguments.of("Bad address in HA config", ImmutableMap.<String, String>builder()
            .put(JobworkerConfiguration.OZONE_JOBWORKER_OM_SERVICE_IDS_KEY, "omServiceId1")
            .put(OZONE_OM_NODES_KEY + ".omServiceId1", "om1,om2,om3")
            .putAll(OM_SERVICE1_HOST_ADDRESS)
            .put(getGrpcPortKey() + ".omServiceId1.om1", "xyz")
            .put(getGrpcPortKey() + ".omServiceId1.om2", "1234")
            .put(getGrpcPortKey() + ".omServiceId1.om3", "1235")
            .build()
        ),

        Arguments.of("Cannot resolve address", ImmutableMap.of(
            OZONE_OM_ADDRESS_KEY, "om1234:1234")
        ),

        Arguments.of("Cannot resolve address in HA config", ImmutableMap.of(
            JobworkerConfiguration.OZONE_JOBWORKER_OM_SERVICE_IDS_KEY, "omServiceId1",
            OZONE_OM_NODES_KEY + ".omServiceId1", "om1,om2,om3",
            OZONE_OM_ADDRESS_KEY + ".omServiceId1.om1", "localhost:1234",
            OZONE_OM_ADDRESS_KEY + ".omServiceId1.om2", "localhost:1235",
            OZONE_OM_ADDRESS_KEY + ".omServiceId1.om3", "om1234:1236")
        ),

        Arguments.of("Missing all OZONE_OM_ADDRESS_KEY", ImmutableMap.of(
            JobworkerConfiguration.OZONE_JOBWORKER_OM_SERVICE_IDS_KEY, "omServiceId1",
            OZONE_OM_NODES_KEY + ".omServiceId1", "om1,om2,om3")
        ),

        Arguments.of("Missing some of OZONE_OM_ADDRESS_KEY", ImmutableMap.of(
            JobworkerConfiguration.OZONE_JOBWORKER_OM_SERVICE_IDS_KEY, "omServiceId1",
            OZONE_OM_NODES_KEY + ".omServiceId1", "om1,om2,om3",
            OZONE_OM_ADDRESS_KEY + ".omServiceId1.om1", "localhost",
            OZONE_OM_ADDRESS_KEY + ".omServiceId1.om2", "localhost")
        ),

        Arguments.of("Missing omServiceId2 OZONE_OM_ADDRESS_KEY", ImmutableMap.<String, String>builder()
            .put(JobworkerConfiguration.OZONE_JOBWORKER_OM_SERVICE_IDS_KEY, "omServiceId1, omServiceId2")
            .put(OZONE_OM_NODES_KEY + ".omServiceId1", "om1,om2,om3")
            .putAll(OM_SERVICE1_HOST_ADDRESS)
            .put(OZONE_OM_NODES_KEY + ".omServiceId2", "om1,om2,om3")
            .build()
        ),

        Arguments.of("Duplicate Address in an OM Group", ImmutableMap.<String, String>builder()
            .put(JobworkerConfiguration.OZONE_JOBWORKER_OM_SERVICE_IDS_KEY, "omServiceId1")
            .put(OZONE_OM_NODES_KEY + ".omServiceId1", "om1,om2,om3")
            .putAll(OM_SERVICE1_HOST_ADDRESS)
            .put(getGrpcPortKey() + ".omServiceId1.om1", "1233")
            .put(getGrpcPortKey() + ".omServiceId1.om2", "1234")
            .put(getGrpcPortKey() + ".omServiceId1.om3", "1233") // Duplicate Address
            .build()
        ),

        Arguments.of("Duplicate Address in different OM Group", ImmutableMap.<String, String>builder()
            .put(JobworkerConfiguration.OZONE_JOBWORKER_OM_SERVICE_IDS_KEY, "omServiceId1, omServiceId2")
            .put(OZONE_OM_NODES_KEY + ".omServiceId1", "om1,om2,om3")
            .putAll(OM_SERVICE1_HOST_ADDRESS)
            .put(getGrpcPortKey() + ".omServiceId1.om1", "1233")
            .put(getGrpcPortKey() + ".omServiceId1.om2", "1234")
            .put(getGrpcPortKey() + ".omServiceId1.om3", "1235")
            .put(OZONE_OM_NODES_KEY + ".omServiceId2", "om1,om2,om3")
            .putAll(OM_SERVICE2_HOST_ADDRESS)
            .put(getGrpcPortKey() + ".omServiceId2.om1", "1236")
            .put(getGrpcPortKey() + ".omServiceId2.om2", "1237")
            .put(getGrpcPortKey() + ".omServiceId2.om3", "1233") // Duplicate Address
            .build()
        )
    );
  }

  @BeforeEach
  public void setUp() throws Exception {
    conf = new OzoneConfiguration();

    omServers = new ArrayList<>();
    mockServers = new ArrayList<>();

    ArrayList<String> omServiceIds = new ArrayList<>();
    for (int i = 0; i < OM_GROUP_COUNT; i++) {
      omServiceIds.add(OM_SERVICE_ID + i);
    }
    conf.set(JobworkerConfiguration.OZONE_JOBWORKER_OM_SERVICE_IDS_KEY, String.join(",", omServiceIds));

    for (int i = 0; i < OM_GROUP_COUNT; i++) {
      List<String> serverAddresses = new ArrayList<>();
      List<Integer> serverPort = new ArrayList<>();
      String omServiceId = OM_SERVICE_ID + i;
      for (int x = 0; x < OM_SERVER_COUNT; x++) {
        int port = 0; // Use ephemeral port for test
        String address = "127.0.0.1";
        JobworkerGrpcRequestHandlerMock mock = new JobworkerGrpcRequestHandlerMock();
        Server server = ServerBuilder.forPort(port)
            .addService(mock)
            .build()
            .start();
        port = server.getPort(); // Get the actual port assigned
        serverAddresses.add(address);
        serverPort.add(port);
        omServers.add(server);
        mockServers.add(mock);
      }

      ArrayList<String> omIds = new ArrayList<>();
      for (int x = 0; x < OM_SERVER_COUNT; x++) {
        String omId = "om" + x;
        omIds.add(omId);
        conf.setStrings(OZONE_OM_ADDRESS_KEY + "." + omServiceId + "." + omId,
            serverAddresses.get(x));
        conf.setInt(getGrpcPortKey() + "." + omServiceId + "." + omId,
            serverPort.get(x));
      }
      conf.setStrings(OZONE_OM_NODES_KEY + "." + omServiceId, String.join(", ", omIds));
    }

    executorService = HadoopExecutors.newCachedThreadPool(
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("TestJobworkerStateMachineThread-%d").build());
  }

  @AfterEach
  public void tearDown() throws Exception {
    try {
      if (executorService != null) {
        executorServiceShutdownGraceful(executorService);
      }
      for (Server s : omServers) {
        s.shutdown();
        s.awaitTermination(5, TimeUnit.SECONDS);
      }
    } catch (Exception e) {
      // Ignore all exceptions from the shutdown
    }
  }

  @Test
  public void testStartStopJobworkerStateMachine() throws IOException,
      InterruptedException, TimeoutException {
    JobworkerStopService stopService = () -> {
    };

    try (JobworkerStateMachine stateMachine =
             new JobworkerStateMachine(MockJobworkerDetails.randomJobworkerDetails(), conf, stopService)) {
      stateMachine.startDaemon();
      JobworkerConnectionManager connectionManager =
          stateMachine.getConnectionManager();
      GenericTestUtils.waitFor(
          () -> {
            int size = connectionManager.getAllEndpoints().size();
            LOG.info("connectionManager.getValues().size() is {}", size);
            return size == OM_SERVER_COUNT * OM_GROUP_COUNT;
          }, 1000, 30000);

      stateMachine.stopDaemon();
      assertTrue(stateMachine.isDaemonStopped());
    }
  }

  /**
   * This test explores the state machine by invoking each call in sequence just
   * like as if the state machine would call it.
   */
  @Test
  public void testJobworkerStateContext() throws IOException,
      InterruptedException, ExecutionException, TimeoutException {
    JobworkerStopService stopService = () -> {
    };
    try (JobworkerStateMachine stateMachine = new JobworkerStateMachine(
        MockJobworkerDetails.randomJobworkerDetails(), conf, stopService)) {

      JobworkerStates currentState =
          stateMachine.getContext().getState();

      // The first JobworkerStateMachine should always be INIT
      assertEquals(JobworkerStates.INIT, currentState);
      JobworkerStateHandler<JobworkerStates> task =
          stateMachine.getTask();
      assertEquals(InitJobworkerState.class, task.getClass());
      task.execute(executorService);
      JobworkerStates newState = task.await(2, TimeUnit.SECONDS);
      assertEquals(OM_SERVER_COUNT * OM_GROUP_COUNT, stateMachine.getConnectionManager().getAllEndpoints().size());

      // Set to the next State for JobworkerStateMachine just likeJobworkerStateContext.execute
      stateMachine.getContext().setState(newState);
      // The next state of JobworkerStateMachine.INIT should be RUNNING
      assertEquals(JobworkerStates.RUNNING, newState);
      task = stateMachine.getTask();
      assertEquals(RunningJobworkerState.class, task.getClass());
      // The first EndpointStates States should always be GETVERSION
      for (JobworkerEndpointStateMachine endpoint :
          stateMachine.getConnectionManager().getAllEndpoints()) {
        assertEquals(JobworkerEndpointStateMachine.EndpointStates.GETVERSION, endpoint.getState());
      }

      // This will invoke getVersion against all OM endpoints
      task.execute(executorService);
      newState = task.await(5, TimeUnit.SECONDS);
      GenericTestUtils.waitFor(() -> {
        for (JobworkerEndpointStateMachine endpoint :
            stateMachine.getConnectionManager().getAllEndpoints()) {
          if (endpoint.getState() !=
              JobworkerEndpointStateMachine.EndpointStates.REGISTER) {
            return false;
          }
        }
        return true;
      }, 1000, 30000);
      // We should have 1 getVersion call on the OM side now
      for (JobworkerGrpcRequestHandlerMock mock : mockServers) {
        assertEquals(1, mock.getVersionCallCount());
      }
      for (JobworkerEndpointStateMachine endpoint :
          stateMachine.getConnectionManager().getAllEndpoints()) {
        assertNotNull(endpoint.getVersion());
      }
      // JobworkerStateMachine will be maintained in RUNNING State
      assertEquals(JobworkerStates.RUNNING, newState);

      // Let all the Jobworker register to All the OM
      task = stateMachine.getTask();
      task.execute(executorService);
      newState = task.await(5, TimeUnit.SECONDS);
      for (JobworkerEndpointStateMachine endpoint :
          stateMachine.getConnectionManager().getAllEndpoints()) {
        assertEquals(JobworkerEndpointStateMachine.EndpointStates.HEARTBEAT, endpoint.getState());
      }
      // JobworkerStateMachine will be maintained in RUNNING State
      assertEquals(JobworkerStates.RUNNING, newState);
      // We should have 1 register call on the OM side now
      for (JobworkerGrpcRequestHandlerMock mock : mockServers) {
        assertEquals(1, mock.getRegisterCallCount());
      }

      // This task is the Running task, but the running task executes tasks based
      // on the state of Endpoints, hence this next call will be a
      // HeartbeatTask at the endpoint RPC level.
      task = stateMachine.getTask();
      task.execute(executorService);
      newState = task.await(5, TimeUnit.SECONDS);
      for (JobworkerEndpointStateMachine endpoint :
          stateMachine.getConnectionManager().getAllEndpoints()) {
        // EndpointStates will be maintained in HEARTBEAT State
        assertEquals(JobworkerEndpointStateMachine.EndpointStates.HEARTBEAT, endpoint.getState());
      }
      // JobworkerStateMachine will be maintained in RUNNING State
      assertEquals(JobworkerStates.RUNNING, newState);
      // We should have 1 heartbeat call now
      for (JobworkerGrpcRequestHandlerMock mock : mockServers) {
        assertThat(mock.getHeartbeatCallCount()).isGreaterThanOrEqualTo(1);
      }
    }
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("invalidConfigProvider")
  void testJobworkerStateMachineWithInvalidConfiguration(String description,
                                                         Map<String, String> configMap) {
    OzoneConfiguration perTestConf = new OzoneConfiguration();
    configMap.forEach(perTestConf::set);

    JobworkerStopService stopService = () -> {
    };

    try (JobworkerStateMachine stateMachine = new JobworkerStateMachine(
        MockJobworkerDetails.randomJobworkerDetails(), perTestConf, stopService)) {

      assertEquals(JobworkerStates.INIT,
          stateMachine.getContext().getState());

      JobworkerStateHandler<JobworkerStates> task =
          stateMachine.getTask();
      task.execute(executorService);

      JobworkerStates newState =
          task.await(5, TimeUnit.SECONDS);
      assertEquals(JobworkerStates.SHUTDOWN, newState);

    } catch (Exception e) {
      fail("Unexpected exception found: " + e.getMessage(), e);
    }
  }
}
