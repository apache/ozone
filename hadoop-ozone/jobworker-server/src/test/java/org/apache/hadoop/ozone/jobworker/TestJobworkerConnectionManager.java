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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the JobworkerConnectionManager class.
 */
public class TestJobworkerConnectionManager {

  private OzoneConfiguration conf;
  private JobworkerConnectionManager connectionManager;
  private static final String THREAD_NAME_PREFIX = "test-";
  private static final String OM_SERVICE_ID = "omServiceId1";

  @BeforeEach
  public void setUp() {
    conf = new OzoneConfiguration();
    connectionManager = new JobworkerConnectionManager(conf);
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (connectionManager != null) {
      connectionManager.close();
    }
  }

  @Test
  public void testAddOMEndpoint() throws Exception {
    InetSocketAddress omAddress = new InetSocketAddress("localhost", 9862);
    connectionManager.addOMEndpoint(omAddress, THREAD_NAME_PREFIX, OM_SERVICE_ID);
    Collection<JobworkerEndpointStateMachine> endpoints = connectionManager.getAllEndpoints();

    assertNotNull(endpoints);
    assertEquals(1, endpoints.size());
    // Verify the endpoint's properties
    JobworkerEndpointStateMachine endpoint = endpoints.iterator().next();
    assertEquals(omAddress, endpoint.getAddress());
    assertEquals(OM_SERVICE_ID, endpoint.getConfiguredOmServiceId());
    // Verify trying to add the same endpoint throws an exception
    assertThrows(IllegalArgumentException.class, () ->
        connectionManager.addOMEndpoint(omAddress, THREAD_NAME_PREFIX, OM_SERVICE_ID));
  }

  @Test
  public void testClose() throws Exception {
    // Add some endpoints
    InetSocketAddress address1 = new InetSocketAddress("localhost", 9862);
    InetSocketAddress address2 = new InetSocketAddress("localhost", 9863);
    connectionManager.addOMEndpoint(address1, THREAD_NAME_PREFIX, OM_SERVICE_ID);
    connectionManager.addOMEndpoint(address2, THREAD_NAME_PREFIX, OM_SERVICE_ID);
    for (JobworkerEndpointStateMachine endpoint : connectionManager.getAllEndpoints()) {
      assertEquals(JobworkerEndpointStateMachine.EndpointStates.GETVERSION, endpoint.getState());
      assertFalse(endpoint.getExecutorService().isShutdown());
    }

    connectionManager.close();

    for (JobworkerEndpointStateMachine endpoint : connectionManager.getAllEndpoints()) {
      assertEquals(JobworkerEndpointStateMachine.EndpointStates.SHUTDOWN, endpoint.getState());
      assertTrue(endpoint.getExecutorService().isShutdown());
    }
  }
}
