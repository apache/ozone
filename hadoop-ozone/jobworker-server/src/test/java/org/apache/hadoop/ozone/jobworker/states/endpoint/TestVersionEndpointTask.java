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

package org.apache.hadoop.ozone.jobworker.states.endpoint;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionResponse;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.jobworker.JobworkerEndpointStateMachine;
import org.apache.hadoop.ozone.jobworker.JobworkerEndpointStateMachine.EndpointStates;
import org.apache.hadoop.ozone.jobworker.protocol.JobworkerProtocol;
import org.apache.hadoop.ozone.jobworker.volume.JobworkerVolumeSet;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for VersionEndpointTask.
 */
public class TestVersionEndpointTask {

  private JobworkerEndpointStateMachine endpointStateMachine;
  private JobworkerProtocol protocol;
  private GenericTestUtils.LogCapturer logCapturer;
  private static final String CLUSTER_ID_1 = "cluster1";
  private static final String OM_SERVICE_ID_1 = "omServiceId1";

  @BeforeEach
  public void setUp() throws Exception {
    // Reset static fields before each test
    resetStaticFields();
    logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(VersionEndpointTask.LOG);

    protocol = mock(JobworkerProtocol.class);
    endpointStateMachine = mock(JobworkerEndpointStateMachine.class);

    when(endpointStateMachine.getState()).thenReturn(EndpointStates.GETVERSION);
    when(endpointStateMachine.getEndPoint()).thenReturn(protocol);
    InetSocketAddress address = new InetSocketAddress("localhost", 9999);
    when(endpointStateMachine.getAddress()).thenReturn(address);
    when(endpointStateMachine.getConfiguredOmServiceId()).thenReturn(OM_SERVICE_ID_1);
  }

  private void resetStaticFields() throws Exception {
    Class<?> clazz = VersionEndpointTask.class;
    Field verifiedClusterIdField = clazz.getDeclaredField("verifiedClusterId");
    verifiedClusterIdField.setAccessible(true);
    verifiedClusterIdField.set(null, null);
    Field mappingField = clazz.getDeclaredField("OM_SERVICE_ID_MAPPING");
    mappingField.setAccessible(true);
    Map<String, String> map = (ConcurrentHashMap<String, String>) mappingField.get(null);
    map.clear();
  }

  /**
   * Create a mock OM version response with the specified values.
   */
  private GetOMVersionResponse createVersionResponse(String clusterId, String omServiceId, String omId) {
    return GetOMVersionResponse.newBuilder()
        .setSoftwareVersion(1)
        .addKeys(HddsProtos.KeyValue.newBuilder()
            .setKey(OzoneConsts.CLUSTER_ID)
            .setValue(clusterId)
            .build())
        .addKeys(HddsProtos.KeyValue.newBuilder()
            .setKey(OzoneConsts.OM_SERVICE_ID)
            .setValue(omServiceId)
            .build())
        .addKeys(HddsProtos.KeyValue.newBuilder()
            .setKey(OzoneConsts.OM_ID)
            .setValue(omId)
            .build())
        .build();
  }

  @Test
  public void testSuccessfulVersionRequest() throws Exception {
    GetOMVersionResponse versionResponse = createVersionResponse(
        CLUSTER_ID_1, OM_SERVICE_ID_1, "om1");
    when(protocol.getOMVersion(any(GetOMVersionRequest.class)))
        .thenReturn(versionResponse);
    VersionEndpointTask task = new VersionEndpointTask(endpointStateMachine, mock(JobworkerVolumeSet.class));
    task.call();

    // The next states should be REGISTER
    verify(endpointStateMachine).setState(EndpointStates.REGISTER);
  }

  @Test
  public void testMultipleOMsSameClusterId() throws Exception {
    // First OM response
    GetOMVersionResponse versionResponse1 = createVersionResponse(
        CLUSTER_ID_1, OM_SERVICE_ID_1, "om1");
    when(protocol.getOMVersion(any(GetOMVersionRequest.class)))
        .thenReturn(versionResponse1);

    // Create and run the task for first OM
    VersionEndpointTask task1 = new VersionEndpointTask(endpointStateMachine, mock(JobworkerVolumeSet.class));
    task1.call();

    // Create a second mock endpoint for a different OM in same cluster
    JobworkerEndpointStateMachine endpoint2 = mock(JobworkerEndpointStateMachine.class);
    JobworkerProtocol protocol2 = mock(JobworkerProtocol.class);
    when(endpoint2.getState()).thenReturn(EndpointStates.GETVERSION);
    when(endpoint2.getEndPoint()).thenReturn(protocol2);
    when(endpoint2.getConfiguredOmServiceId()).thenReturn(OM_SERVICE_ID_1);

    // Second OM response (same cluster)
    GetOMVersionResponse versionResponse2 = createVersionResponse(
        CLUSTER_ID_1, OM_SERVICE_ID_1, "om2");
    when(protocol2.getOMVersion(any(GetOMVersionRequest.class)))
        .thenReturn(versionResponse2);

    // This should succeed as they have the same cluster ID
    VersionEndpointTask task2 = new VersionEndpointTask(endpoint2, mock(JobworkerVolumeSet.class));
    assertDoesNotThrow(task2::call);
    assertTrue(logCapturer.getOutput().contains("First OM reported cluster ID: " + CLUSTER_ID_1));
    assertFalse(logCapturer.getOutput().contains("A jobworker can only serve one cluster"));
    assertFalse(logCapturer.getOutput().contains("Conflicting OMServiceId mapping detected"));
    verify(endpoint2).setState(EndpointStates.REGISTER);
  }

  @Test
  public void testMultipleOMsDifferentClusterId() throws Exception {
    // First OM response
    GetOMVersionResponse versionResponse1 = createVersionResponse(
        CLUSTER_ID_1, OM_SERVICE_ID_1, "om1");
    when(protocol.getOMVersion(any(GetOMVersionRequest.class)))
        .thenReturn(versionResponse1);

    // Create and run the task for first OM
    VersionEndpointTask task1 = new VersionEndpointTask(endpointStateMachine, mock(JobworkerVolumeSet.class));
    task1.call();

    // Create a second mock endpoint for a different OM in different cluster
    JobworkerEndpointStateMachine endpoint2 = mock(JobworkerEndpointStateMachine.class);
    JobworkerProtocol protocol2 = mock(JobworkerProtocol.class);
    when(endpoint2.getState()).thenReturn(EndpointStates.GETVERSION);
    when(endpoint2.getEndPoint()).thenReturn(protocol2);
    when(endpoint2.getConfiguredOmServiceId()).thenReturn("omServiceId2");

    // Second OM response (different cluster)
    GetOMVersionResponse versionResponse2 = createVersionResponse(
        "cluster2", "omServiceId2", "om2");
    when(protocol2.getOMVersion(any(GetOMVersionRequest.class)))
        .thenReturn(versionResponse2);

    // This should fail with IllegalStateException due to cluster ID mismatch
    VersionEndpointTask task2 = new VersionEndpointTask(endpoint2, mock(JobworkerVolumeSet.class));
    task2.call();
    assertTrue(logCapturer.getOutput().contains("First OM reported cluster ID: " + CLUSTER_ID_1));
    assertTrue(logCapturer.getOutput().contains("A jobworker can only serve one cluster"));
    verify(endpoint2).setState(EndpointStates.SHUTDOWN);
  }

  @Test
  public void testOMServiceIdMismatch() throws Exception {
    // First OM response
    GetOMVersionResponse versionResponse1 = createVersionResponse(
        CLUSTER_ID_1, OM_SERVICE_ID_1, "om1");
    when(protocol.getOMVersion(any(GetOMVersionRequest.class)))
        .thenReturn(versionResponse1);

    // Create and run the task for first OM
    VersionEndpointTask task1 = new VersionEndpointTask(endpointStateMachine, mock(JobworkerVolumeSet.class));
    task1.call();

    // Create a second mock endpoint with the same configured OMServiceId
    // but different reported OMServiceId
    JobworkerEndpointStateMachine endpoint2 = mock(JobworkerEndpointStateMachine.class);
    JobworkerProtocol protocol2 = mock(JobworkerProtocol.class);
    when(endpoint2.getState()).thenReturn(EndpointStates.GETVERSION);
    when(endpoint2.getEndPoint()).thenReturn(protocol2);
    when(endpoint2.getConfiguredOmServiceId()).thenReturn(OM_SERVICE_ID_1); // Same configured ID

    // Different reported OMServiceId but same cluster
    GetOMVersionResponse versionResponse2 = createVersionResponse(
        CLUSTER_ID_1, "omServiceId2", "om2"); // Different reported ID
    when(protocol2.getOMVersion(any(GetOMVersionRequest.class)))
        .thenReturn(versionResponse2);

    // This should fail with IllegalStateException due to OMServiceId mismatch
    VersionEndpointTask task2 = new VersionEndpointTask(endpoint2, mock(JobworkerVolumeSet.class));
    task2.call();
    assertTrue(logCapturer.getOutput().contains("First OM reported cluster ID: " + CLUSTER_ID_1));
    assertTrue(logCapturer.getOutput().contains("Conflicting OMServiceId mapping detected"));
    verify(endpoint2).setState(EndpointStates.SHUTDOWN);
  }

  @Test
  public void testFailureRequest() throws Exception {
    when(protocol.getOMVersion(any(GetOMVersionRequest.class)))
        .thenThrow(new IOException("Connection refused"));
    VersionEndpointTask task = new VersionEndpointTask(endpointStateMachine, mock(JobworkerVolumeSet.class));
    task.call();

    // The state should not change
    verify(endpointStateMachine, never()).setState(EndpointStates.GETVERSION);
  }
}
