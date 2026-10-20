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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.MockJobworkerDetails;
import org.apache.hadoop.ozone.jobworker.protocolPB.JobworkerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.jobworker.report.JobworkerReportManager;
import org.apache.hadoop.ozone.jobworker.states.endpoint.HeartbeatEndpointTask;
import org.apache.hadoop.ozone.jobworker.states.endpoint.RegisterEndpointTask;
import org.apache.hadoop.ozone.jobworker.states.endpoint.VersionEndpointTask;
import org.apache.hadoop.ozone.jobworker.utils.JobworkerGrpcRequestHandlerMock;
import org.apache.hadoop.ozone.jobworker.volume.JobworkerVolumeSet;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the Jobworker endpoints.
 */
public class TestJobworkerEndpoint {

  private static String serverAddress;
  private static Server omServer;
  private static JobworkerGrpcRequestHandlerMock mockHandler;
  private static OzoneConfiguration ozoneConf;
  private static JobworkerDetails jobworkerDetails;

  @BeforeAll
  static void setUp() throws Exception {
    ozoneConf = new OzoneConfiguration();
    jobworkerDetails = MockJobworkerDetails.randomJobworkerDetails();

    int port = 0; // Use ephemeral port for test
    String address = "127.0.0.1";
    mockHandler = new JobworkerGrpcRequestHandlerMock();
    omServer = ServerBuilder.forPort(port)
        .addService(mockHandler)
        .build()
        .start();
    port = omServer.getPort(); // Get the actual port assigned
    serverAddress = address + ":" + port;

    // Configure OM addresses in the config
    ozoneConf.setStrings(JobworkerConfiguration.OZONE_JOBWORKER_OM_SERVICE_IDS_KEY, "omServiceId1");
    ozoneConf.setStrings(OMConfigKeys.OZONE_OM_NODES_KEY + ".omServiceId1", "om1");
    ozoneConf.setStrings(OMConfigKeys.OZONE_OM_ADDRESS_KEY + ".omServiceId1.om1", serverAddress);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    if (omServer != null) {
      omServer.shutdown();
      omServer.awaitTermination();
    }
  }

  @BeforeEach
  public void reset() throws Exception {
    mockHandler.resetCallCount();
    mockHandler.resetDelayMs();
  }

  private static InetSocketAddress getSocketAddressFromString(String address) {
    String[] parts = address.split(":");
    String host = parts[0];
    int port = Integer.parseInt(parts[1]);
    return new InetSocketAddress(host, port);
  }

  @Test
  public void testGetVersionTask() throws Exception {
    // Create a connection to the real gRPC server
    try (JobworkerEndpointStateMachine endpoint = createEndpoint(serverAddress, ozoneConf)) {
      try {
        endpoint.setState(JobworkerEndpointStateMachine.EndpointStates.GETVERSION);
        VersionEndpointTask versionTask =
            new VersionEndpointTask(endpoint, mock(JobworkerVolumeSet.class));
        JobworkerEndpointStateMachine.EndpointStates newState = versionTask.call();

        // if version call worked, the endpoint should automatically move to the
        // next state.
        assertEquals(JobworkerEndpointStateMachine.EndpointStates.REGISTER,
            newState);
        assertNotNull(endpoint.getVersion());
        assertEquals(1, mockHandler.getVersionCallCount());
      } finally {
        endpoint.close();
      }
    }
  }

  @Test
  public void testGetVersionToInvalidEndpoint() throws Exception {
    String nonExistentServerAddress = "localhost:1";
    try (JobworkerEndpointStateMachine endpoint = createEndpoint(nonExistentServerAddress, ozoneConf)) {
      try {
        endpoint.setState(JobworkerEndpointStateMachine.EndpointStates.GETVERSION);
        VersionEndpointTask versionTask = new VersionEndpointTask(endpoint, mock(JobworkerVolumeSet.class));
        JobworkerEndpointStateMachine.EndpointStates newState = versionTask.call();
        // This version call did NOT work, so endpoint should remain in the same
        // state.
        assertEquals(JobworkerEndpointStateMachine.EndpointStates.GETVERSION, newState);
      } finally {
        endpoint.close();
      }
    }
  }

  @Test
  public void testGetVersionAssertRpcTimeOut() throws Exception {
    final long rpcTimeout = 500;
    final long tolerance = 100;
    // Set the delay in the handler
    mockHandler.setGetVersionDelayMs(5000);

    // Create custom configuration with the short timeout
    OzoneConfiguration timeoutConf = new OzoneConfiguration(ozoneConf);
    JobworkerConfiguration jwConfig = timeoutConf.getObject(JobworkerConfiguration.class);
    jwConfig.setRpcTimeout(Duration.ofMillis(rpcTimeout));
    timeoutConf.setFromObject(jwConfig);

    try (JobworkerEndpointStateMachine endpoint = createEndpoint(serverAddress, timeoutConf)) {
      try {
        endpoint.setState(JobworkerEndpointStateMachine.EndpointStates.GETVERSION);
        VersionEndpointTask versionTask = new VersionEndpointTask(endpoint, mock(JobworkerVolumeSet.class));

        long start = Time.monotonicNow();
        JobworkerEndpointStateMachine.EndpointStates newState = versionTask.call();
        long end = Time.monotonicNow();
        assertTrue(end - start <= rpcTimeout + tolerance);
        // This version call did NOT work, so endpoint should remain in the same state.
        assertEquals(JobworkerEndpointStateMachine.EndpointStates.GETVERSION,
            newState);
      } finally {
        endpoint.close();
      }
    }
  }

  @Test
  public void testRegisterTask() throws Exception {
    try (JobworkerEndpointStateMachine endpoint = createEndpoint(serverAddress, ozoneConf)) {
      JobworkerStateContext context = mock(JobworkerStateContext.class);

      try {
        endpoint.setState(JobworkerEndpointStateMachine.EndpointStates.REGISTER);
        RegisterEndpointTask endpointTask = RegisterEndpointTask.newBuilder()
            .setConfig(ozoneConf)
            .setEndpointStateMachine(endpoint)
            .setContext(context)
            .setJobworkerDetails(jobworkerDetails)
            .build();

        JobworkerEndpointStateMachine.EndpointStates newState = endpointTask.call();

        // Successful register should move us to Heartbeat state.
        assertEquals(JobworkerEndpointStateMachine.EndpointStates.HEARTBEAT,
            newState);

        assertEquals(1, mockHandler.getRegisterCallCount());
      } finally {
        endpoint.close();
      }
    }
  }

  @Test
  public void testRegisterRpcTimeOut() throws Exception {
    final long rpcTimeout = 500;
    final long tolerance = 100;
    // Set the delay in the handler
    mockHandler.setRegisterDelayMs(5000);
    // Create custom configuration with the short timeout
    OzoneConfiguration timeoutConf = new OzoneConfiguration(ozoneConf);
    JobworkerConfiguration jwConfig = new JobworkerConfiguration();
    jwConfig.setRpcTimeout(Duration.ofMillis(rpcTimeout));
    timeoutConf.setFromObject(jwConfig);

    try (JobworkerEndpointStateMachine endpoint = createEndpoint(serverAddress, timeoutConf)) {
      try {
        endpoint.setState(JobworkerEndpointStateMachine.EndpointStates.REGISTER);
        VersionEndpointTask versionTask =
            new VersionEndpointTask(endpoint, mock(JobworkerVolumeSet.class));
        long start = Time.monotonicNow();
        JobworkerEndpointStateMachine.EndpointStates newState = versionTask.call();
        long end = Time.monotonicNow();
        assertTrue(end - start <= rpcTimeout + tolerance);
        // This version call did NOT work, so endpoint should remain in the same state.
        assertEquals(JobworkerEndpointStateMachine.EndpointStates.REGISTER,
            newState);
      } finally {
        endpoint.close();
      }
    }
  }

  @Test
  public void testRegisterToInvalidEndpoint() throws Exception {
    String invalidAddress = "localhost:1";
    try (JobworkerEndpointStateMachine endpoint = createEndpoint(invalidAddress, ozoneConf)) {
      JobworkerStateContext context = mock(JobworkerStateContext.class);

      try {
        endpoint.setState(JobworkerEndpointStateMachine.EndpointStates.REGISTER);
        RegisterEndpointTask endpointTask = RegisterEndpointTask.newBuilder()
            .setConfig(ozoneConf)
            .setEndpointStateMachine(endpoint)
            .setContext(context)
            .setJobworkerDetails(jobworkerDetails)
            .build();
        JobworkerEndpointStateMachine.EndpointStates newState = endpointTask.call();

        // Failed register should keep us in the REGISTER state
        assertEquals(JobworkerEndpointStateMachine.EndpointStates.REGISTER, newState);
      } finally {
        endpoint.close();
      }
    }
  }

  @Test
  public void testHeartbeatTask() throws Exception {
    try (JobworkerEndpointStateMachine endpoint = createEndpoint(serverAddress, ozoneConf)) {
      JobworkerStateContext context = mock(JobworkerStateContext.class);
      JobworkerReportManager jobworkerReportManager = mock(JobworkerReportManager.class);
      JobworkerStateMachine stateMachine = mock(JobworkerStateMachine.class);
      when(jobworkerReportManager.getLimitedCountAvailableReports(any(), any())).thenReturn(new ArrayList<>());
      when(stateMachine.getReportManager()).thenReturn(jobworkerReportManager);
      when(context.getParent()).thenReturn(stateMachine);
      when(context.getJobworkerDetails()).thenReturn(jobworkerDetails);
      try {
        endpoint.setState(JobworkerEndpointStateMachine.EndpointStates.HEARTBEAT);
        HeartbeatEndpointTask endpointTask = HeartbeatEndpointTask.newBuilder()
            .setContext(context)
            .setJobworkerDetails(jobworkerDetails)
            .setEndpointStateMachine(endpoint)
            .build();
        JobworkerEndpointStateMachine.EndpointStates newState = endpointTask.call();

        // Successful heartbeat should keep us in the HEARTBEAT state
        assertEquals(JobworkerEndpointStateMachine.EndpointStates.HEARTBEAT, newState);
        assertEquals(1, mockHandler.getHeartbeatCallCount());
      } finally {
        endpoint.close();
      }
    }
  }

  @Test
  public void testHeartbeatTaskToInvalidNode() throws Exception {
    String invalidAddress = "localhost:1";
    try (JobworkerEndpointStateMachine endpoint = createEndpoint(invalidAddress, ozoneConf)) {
      JobworkerStateContext context = mock(JobworkerStateContext.class);
      JobworkerStateMachine stateMachine = mock(JobworkerStateMachine.class);
      when(context.getParent()).thenReturn(stateMachine);
      when(context.getJobworkerDetails()).thenReturn(jobworkerDetails);

      try {
        endpoint.setState(JobworkerEndpointStateMachine.EndpointStates.HEARTBEAT);

        HeartbeatEndpointTask endpointTask = HeartbeatEndpointTask.newBuilder()
            .setContext(context)
            .setEndpointStateMachine(endpoint)
            .setJobworkerDetails(jobworkerDetails)
            .build();
        JobworkerEndpointStateMachine.EndpointStates newState = endpointTask.call();

        // Failed heartbeat should keep us in the HEARTBEAT state
        assertEquals(JobworkerEndpointStateMachine.EndpointStates.HEARTBEAT,
            newState);
      } finally {
        endpoint.close();
      }
    }
  }

  @Test
  public void testHeartbeatTaskRpcTimeOut() throws Exception {
    final long rpcTimeout = 500;
    final long tolerance = 100;
    // Set the delay in the handler
    mockHandler.setHeartbeatDelayMs(5000);

    // Create custom configuration with the short timeout
    OzoneConfiguration timeoutConf = new OzoneConfiguration(ozoneConf);
    JobworkerConfiguration jwConf = timeoutConf.getObject(JobworkerConfiguration.class);
    jwConf.setRpcTimeout(Duration.ofMillis(rpcTimeout));
    timeoutConf.setFromObject(jwConf);

    try (JobworkerEndpointStateMachine endpoint = createEndpoint(serverAddress, timeoutConf)) {
      JobworkerStateContext context = mock(JobworkerStateContext.class);
      JobworkerStateMachine stateMachine = mock(JobworkerStateMachine.class);
      when(context.getParent()).thenReturn(stateMachine);
      when(context.getJobworkerDetails()).thenReturn(jobworkerDetails);

      try {
        endpoint.setState(JobworkerEndpointStateMachine.EndpointStates.HEARTBEAT);

        HeartbeatEndpointTask endpointTask = HeartbeatEndpointTask.newBuilder()
            .setContext(context)
            .setEndpointStateMachine(endpoint)
            .setJobworkerDetails(jobworkerDetails)
            .build();

        long start = Time.monotonicNow();
        JobworkerEndpointStateMachine.EndpointStates newState = endpointTask.call();
        long end = Time.monotonicNow();
        assertTrue(end - start <= rpcTimeout + tolerance);
        assertEquals(JobworkerEndpointStateMachine.EndpointStates.HEARTBEAT,
            newState);
      } finally {
        endpoint.close();
      }
    }
  }

  @Test
  public void testExceedMaximumInboundLength() throws Exception {
    JobworkerDetails largeJobworkerDetails = MockJobworkerDetails.randomJobworkerDetails();

    OzoneConfiguration limitedConf = new OzoneConfiguration(ozoneConf);
    JobworkerConfiguration jwConfig = limitedConf.getObject(JobworkerConfiguration.class);
    jwConfig.setGrpcMaximumInboundLength(1);
    limitedConf.setFromObject(jwConfig);

    try (JobworkerEndpointStateMachine endpoint = createEndpoint(serverAddress, limitedConf)) {
      JobworkerStateContext context = mock(JobworkerStateContext.class);
      when(context.getJobworkerDetails()).thenReturn(largeJobworkerDetails);

      try {
        endpoint.setState(JobworkerEndpointStateMachine.EndpointStates.HEARTBEAT);

        HeartbeatEndpointTask endpointTask = HeartbeatEndpointTask.newBuilder()
            .setContext(context)
            .setEndpointStateMachine(endpoint)
            .setJobworkerDetails(largeJobworkerDetails)
            .build();

        // This call should not throw exception but should handle the error and stay in HEARTBEAT state
        JobworkerEndpointStateMachine.EndpointStates newState = endpointTask.call();
        assertEquals(JobworkerEndpointStateMachine.EndpointStates.HEARTBEAT, newState);
        assertTrue(endpoint.getMissedCount() > 0);
      } finally {
        endpoint.close();
      }
    }
  }

  private JobworkerEndpointStateMachine createEndpoint(
      String omAddress, OzoneConfiguration localConf) {
    InetSocketAddress socketAddress = getSocketAddressFromString(omAddress);
    JobworkerProtocolClientSideTranslatorPB client =
        new JobworkerProtocolClientSideTranslatorPB(
            socketAddress.getHostName(), socketAddress.getPort(), localConf);
    JobworkerEndpointStateMachine endpointStateMachine = new JobworkerEndpointStateMachine(
        socketAddress, client, localConf,
        "test-", "omServiceId1");
    endpointStateMachine.setOmServiceId("omServiceId1");
    return endpointStateMachine;
  }
}
