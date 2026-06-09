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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
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
      InetSocketAddress address = new InetSocketAddress("127.0.0.1", 9861);
      connectionManager.addSCMServer(address, "");
      EndpointStateMachine endpoint =
          connectionManager.getValues().iterator().next();
      endpoint.setState(HEARTBEAT);

      connectionManager.removeSCMServer(address);

      Assertions.assertTrue(connectionManager.getValues().isEmpty());
      Assertions.assertEquals(HEARTBEAT, endpoint.getState());
    }
  }

  /**
   * resolveLatestAddress() returns null when no preserved hostname is
   * available -- legacy code path -- so re-resolution is a no-op for that
   * endpoint. Operator must restart the DN to pick up a new IP.
   */
  @Test
  public void testResolveLatestAddressReturnsNullWithoutHostAndPort() {
    InetSocketAddress address = new InetSocketAddress("127.0.0.1", 9861);
    EndpointStateMachine endpoint = new EndpointStateMachine(
        address, /*hostAndPort=*/ null, /*endPoint=*/ null,
        new OzoneConfiguration(), "");
    Assertions.assertNull(endpoint.resolveLatestAddress());
    Assertions.assertNull(endpoint.getHostAndPort());
  }

  /**
   * When the cached IP matches what DNS currently returns for the
   * preserved hostname, resolveLatestAddress() returns null (no swap
   * needed). Uses "localhost" because it reliably resolves to a loopback
   * address in any test environment.
   */
  @Test
  public void testResolveLatestAddressReturnsNullWhenIpUnchanged()
      throws Exception {
    InetAddress loopback = InetAddress.getByName("localhost");
    InetSocketAddress address = new InetSocketAddress(loopback, 9861);
    EndpointStateMachine endpoint = new EndpointStateMachine(
        address, "localhost:9861", null, new OzoneConfiguration(), "");
    InetSocketAddress refreshed = endpoint.resolveLatestAddress();
    Assertions.assertNull(refreshed,
        "localhost re-resolves to the same loopback address; refresh "
            + "must report no change so the endpoint is not torn down "
            + "needlessly.");
  }

  /**
   * When the cached IP differs from what DNS currently returns for the
   * preserved hostname, resolveLatestAddress() returns the freshly-
   * resolved address. Simulates the "SCM pod was rescheduled to a new
   * IP" scenario by constructing the endpoint with a deliberately stale
   * cached IP.
   */
  @Test
  public void testResolveLatestAddressReturnsNewAddressOnIpChange()
      throws Exception {
    // Pretend localhost previously resolved to 127.0.0.99 (stale IP).
    // In real Kubernetes this would be the now-defunct pod IP.
    InetSocketAddress staleAddress = new InetSocketAddress(
        InetAddress.getByAddress(new byte[] {127, 0, 0, 99}), 9861);
    EndpointStateMachine endpoint = new EndpointStateMachine(
        staleAddress, "localhost:9861", null,
        new OzoneConfiguration(), "");
    InetSocketAddress refreshed = endpoint.resolveLatestAddress();
    Assertions.assertNotNull(refreshed,
        "localhost re-resolves to loopback (typically 127.0.0.1), "
            + "which differs from the stale 127.0.0.99 we cached; "
            + "refresh must report the change so the endpoint can "
            + "swap to the live address.");
    Assertions.assertEquals(9861, refreshed.getPort());
    Assertions.assertNotEquals(staleAddress.getAddress(),
        refreshed.getAddress());
  }

  /**
   * refreshSCMServer() swaps an endpoint's address atomically in the
   * connection manager when the cached IP is stale. The replacement
   * endpoint starts in GETVERSION state -- the version handshake must
   * be re-run because the new SCM pod is effectively a fresh process.
   */
  @Test
  public void testRefreshSCMServerSwapsEndpointOnIpChange() throws Exception {
    try (SCMConnectionManager connectionManager =
             new SCMConnectionManager(new OzoneConfiguration())) {
      InetSocketAddress staleAddress = new InetSocketAddress(
          InetAddress.getByAddress(new byte[] {127, 0, 0, 99}), 9861);
      connectionManager.addSCMServer(staleAddress, "localhost:9861", "");

      InetSocketAddress refreshed = connectionManager.refreshSCMServer(
          staleAddress, "");

      Assertions.assertNotNull(refreshed);
      Assertions.assertEquals(1, connectionManager.getNumOfConnections());
      EndpointStateMachine swapped =
          connectionManager.getValues().iterator().next();
      Assertions.assertEquals(refreshed, swapped.getAddress());
      Assertions.assertEquals("localhost:9861", swapped.getHostAndPort());
    }
  }

  /**
   * Regression for the rollback gap Copilot flagged on
   * {@code refreshSCMServer}: if building the replacement endpoint
   * throws (transient DNS blip during proxy construction, peer not
   * yet accepting on the new IP, NetUtils refusing the resolved
   * address), the connection manager must NOT have already removed
   * the stale endpoint -- otherwise the peer disappears from
   * {@code scmMachines} and no future heartbeat has anywhere to dial.
   * <p>
   * The fix builds the replacement BEFORE removing the stale entry.
   * This test injects a build that throws and asserts the stale
   * endpoint is still registered after the call returns its
   * IOException.
   */
  @Test
  public void testRefreshSCMServerLeavesStaleEndpointOnBuildFailure()
      throws Exception {
    final IOException simulated =
        new IOException("simulated transient build failure");
    try (SCMConnectionManager connectionManager =
        new SCMConnectionManager(new OzoneConfiguration()) {
          private boolean firstBuildDone;

          @Override
          EndpointStateMachine buildScmEndpoint(InetSocketAddress address,
              String hostAndPort, String threadNamePrefix)
              throws IOException {
            if (!firstBuildDone) {
              firstBuildDone = true;
              return super.buildScmEndpoint(address, hostAndPort,
                  threadNamePrefix);
            }
            throw simulated;
          }
        }) {
      InetSocketAddress staleAddress = new InetSocketAddress(
          InetAddress.getByAddress(new byte[] {127, 0, 0, 99}), 9861);
      connectionManager.addSCMServer(staleAddress, "localhost:9861", "");
      EndpointStateMachine before =
          connectionManager.getValues().iterator().next();

      IOException thrown = Assertions.assertThrows(IOException.class,
          () -> connectionManager.refreshSCMServer(staleAddress, ""));
      Assertions.assertSame(simulated, thrown,
          "the underlying build failure must be propagated, not "
              + "swallowed by an enclosing recovery branch");

      Assertions.assertEquals(1, connectionManager.getNumOfConnections(),
          "stale endpoint must remain registered when buildScmEndpoint "
              + "throws -- otherwise the peer disappears from the "
              + "connection manager and no heartbeat can recover it");
      Assertions.assertSame(before,
          connectionManager.getValues().iterator().next(),
          "the SAME EndpointStateMachine instance must still be "
              + "registered (not a half-constructed replacement)");
    }
  }

  /**
   * refreshSCMServer() against an endpoint whose cached IP already matches
   * DNS is a no-op -- the existing endpoint stays in place untouched. This
   * prevents needless tearing-down of healthy connections when the
   * heartbeat task asks to refresh after a transient blip.
   */
  @Test
  public void testRefreshSCMServerNoopWhenIpUnchanged() throws Exception {
    try (SCMConnectionManager connectionManager =
             new SCMConnectionManager(new OzoneConfiguration())) {
      InetAddress loopback = InetAddress.getByName("localhost");
      InetSocketAddress address = new InetSocketAddress(loopback, 9861);
      connectionManager.addSCMServer(address, "localhost:9861", "");
      EndpointStateMachine before =
          connectionManager.getValues().iterator().next();

      InetSocketAddress refreshed =
          connectionManager.refreshSCMServer(address, "");

      Assertions.assertNull(refreshed);
      Assertions.assertEquals(1, connectionManager.getNumOfConnections());
      Assertions.assertSame(before,
          connectionManager.getValues().iterator().next(),
          "Endpoint instance must not be torn down when IP is unchanged.");
    }
  }
}
