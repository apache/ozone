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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.net.HostAndPort;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for SCMConnectionManager.
 */
public class TestSCMConnectionManager {

  private static final InetSocketAddress NEW_IP = newIp();

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

  @Test
  public void refreshRebuildsEndpointWhenIpChanges() throws Exception {
    try (SCMConnectionManager cm =
             new SCMConnectionManager(new OzoneConfiguration())) {
      final HostAndPort address = spy(new HostAndPort("127.0.0.1", 9861));
      cm.addSCMServer(address, "");
      final EndpointStateMachine original = cm.getValues().iterator().next();
      doReturn(NEW_IP).when(address).resolveLatest();

      Assertions.assertTrue(cm.refreshSCMServer(address, ""));
      Assertions.assertNotSame(original, cm.getValues().iterator().next());
      Assertions.assertEquals(NEW_IP, address.getAddress());
    }
  }

  @Test
  public void refreshBuildFailureLeavesEndpointAndAddressUnchanged()
      throws Exception {
    try (FailingConnectionManager cm =
             new FailingConnectionManager(new OzoneConfiguration())) {
      final HostAndPort address = spy(new HostAndPort("127.0.0.1", 9861));
      cm.addSCMServer(address, "");
      final EndpointStateMachine original = cm.getValues().iterator().next();
      final InetSocketAddress before = address.getAddress();
      doReturn(NEW_IP).when(address).resolveLatest();
      cm.failBuild = true;

      // Build fails after DNS returned a new IP: the live endpoint and the cached address must both
      // stay unchanged, otherwise the DN dials the stale proxy forever while getAddress() reports the
      // new IP -- the "stuck until restart" state this feature removes.
      Assertions.assertThrows(IOException.class, () -> cm.refreshSCMServer(address, ""));
      Assertions.assertSame(original, cm.getValues().iterator().next());
      Assertions.assertEquals(before, address.getAddress());
    }
  }

  @Test
  public void refreshAbandonedWhenEndpointRemovedDuringResolve() throws Exception {
    try (SCMConnectionManager cm =
             new SCMConnectionManager(new OzoneConfiguration())) {
      final HostAndPort address = spy(new HostAndPort("127.0.0.1", 9861));
      cm.addSCMServer(address, "");
      final InetSocketAddress before = address.getAddress();
      // resolveLatest runs in the unlocked window between the endpoint snapshot and the write lock;
      // removing the endpoint right there exercises the lost-race guard deterministically.
      doAnswer(inv -> {
        cm.removeSCMServer(address);
        return NEW_IP;
      }).when(address).resolveLatest();

      Assertions.assertFalse(cm.refreshSCMServer(address, ""));
      Assertions.assertTrue(cm.getValues().isEmpty());
      Assertions.assertEquals(before, address.getAddress());
    }
  }

  private static InetSocketAddress newIp() {
    try {
      return new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 0, 7}), 9861);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /** SCMConnectionManager whose endpoint build can be forced to fail, to exercise rollback. */
  private static final class FailingConnectionManager extends SCMConnectionManager {
    private boolean failBuild;

    FailingConnectionManager(ConfigurationSource conf) {
      super(conf);
    }

    @Override
    EndpointStateMachine buildScmEndpoint(HostAndPort address, InetSocketAddress dialAddress,
        String threadNamePrefix) throws IOException {
      if (failBuild) {
        throw new IOException("simulated build failure");
      }
      return super.buildScmEndpoint(address, dialAddress, threadNamePrefix);
    }
  }
}
