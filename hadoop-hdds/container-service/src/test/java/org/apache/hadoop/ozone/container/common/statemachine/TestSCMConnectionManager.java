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

package org.apache.hadoop.ozone.container.common.statemachine;

import static org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine.EndPointStates.HEARTBEAT;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.net.HostAndPort;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for SCMConnectionManager.
 */
public class TestSCMConnectionManager {

  @Test
  public void testRemoveSCMServerDoesNotMarkEndpointShutdown()
      throws Exception {
    try (SCMConnectionManager connectionManager =
             new SCMConnectionManager(new OzoneConfiguration())) {
      final HostAndPort address = new HostAndPort("127.0.0.1", 9861);
      connectionManager.addSCMServer(address, "");
      EndpointStateMachine endpoint =
          connectionManager.getValues().iterator().next();
      endpoint.setState(HEARTBEAT);

      connectionManager.removeSCMServer(address);

      Assertions.assertTrue(connectionManager.getValues().isEmpty());
      Assertions.assertEquals(HEARTBEAT, endpoint.getState());
    }
  }
}
