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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Verifies the swap-mechanism + real-network-handshake half of the
 * DataNode → SCM DNS-refresh-on-failure recovery path.
 * <p>
 * Stands up a real SCM-side RPC server (via {@link ScmTestMock}) bound to a
 * loopback address on an OS-assigned port, then primes
 * {@link SCMConnectionManager} with a deliberately stale entry: cached at
 * {@code 127.0.0.99:port} (no listener, unreachable) but with the preserved
 * hostname {@code localhost:port}. This is the steady state an Ozone
 * DataNode falls into when a Kubernetes SCM pod has been rescheduled to a
 * new IP -- the cached InetSocketAddress points to the gone-away IP, but
 * DNS for the hostname now resolves elsewhere.
 * <p>
 * The test calls {@link SCMConnectionManager#refreshSCMServer} directly
 * and asserts that:
 * <ol>
 *   <li>the swap occurred,</li>
 *   <li>the cached endpoint now points at the live SCM,</li>
 *   <li>a real heartbeat through the swapped endpoint round-trips
 *       successfully (mock SCM observes the count incrementing).</li>
 * </ol>
 * Together these prove the swap-mechanism path: address re-resolution,
 * atomic endpoint swap, fresh RPC proxy construction, real network
 * handshake, server-side handler invocation.
 * <p>
 * The complementary trigger-chain integration -- heartbeat IOException
 * → catch block → {@code maybeRefreshScmAddress} → flag/threshold check
 * → {@code refreshSCMServer} -- is covered by
 * {@code TestHeartbeatEndpointTaskDnsRefresh}. Together the two tests
 * prove the full production recovery flow without a single flaky
 * timing-dependent assertion.
 */
public class TestSCMConnectionManagerDnsRefreshE2E {

  private RPC.Server scmServer;
  private SCMConnectionManager connectionManager;

  @AfterEach
  public void tearDown() throws Exception {
    if (connectionManager != null) {
      connectionManager.close();
    }
    if (scmServer != null) {
      scmServer.stop();
    }
  }

  @Test
  @Timeout(value = 30, unit = java.util.concurrent.TimeUnit.SECONDS)
  public void testRefreshSwapsEndpointAndHeartbeatSucceeds() throws Exception {
    // Step 1: stand up a real mock SCM RPC server on a loopback address,
    // OS-assigned port. This is the "live" SCM after the pod restart.
    OzoneConfiguration conf = new OzoneConfiguration();
    ScmTestMock scmServerImpl = new ScmTestMock();
    scmServer = SCMTestUtils.startScmRpcServer(conf, scmServerImpl);
    InetSocketAddress liveAddr = scmServer.getListenerAddress();
    int port = liveAddr.getPort();

    // The hostname we'll preserve. localhost reliably resolves to a
    // loopback address in any test environment, and the server is
    // bound to a loopback address, so a dial of localhost:port
    // succeeds.
    String hostAndPort = "localhost:" + port;

    // Step 2: prime the connection manager with a deliberately stale
    // cached InetSocketAddress (127.0.0.99 has no listener; any RPC
    // attempt would fail). Crucially, the entry still carries the
    // preserved hostname "localhost:port", which is the input to DNS
    // re-resolution. This is exactly the state a DN falls into after
    // an SCM pod is rescheduled in Kubernetes.
    InetSocketAddress staleAddr = new InetSocketAddress(
        InetAddress.getByAddress(new byte[] {127, 0, 0, 99}), port);
    connectionManager = new SCMConnectionManager(conf);
    connectionManager.addSCMServer(staleAddr, hostAndPort, "");

    assertEquals(1, connectionManager.getNumOfConnections());

    // Step 3: call the recovery path. refreshSCMServer must:
    //   - re-resolve "localhost" via DNS,
    //   - notice the resolved IP differs from the stale 127.0.0.99,
    //   - tear down the unreachable endpoint and build a fresh one
    //     bound to the live address.
    InetSocketAddress refreshed = connectionManager.refreshSCMServer(
        staleAddr, "");

    assertNotNull(refreshed,
        "refreshSCMServer must report a swap when the cached IP "
            + "127.0.0.99 differs from what DNS now returns for localhost");
    assertEquals(port, refreshed.getPort(),
        "port must be preserved across the swap; only the IP changes");
    assertEquals(1, connectionManager.getNumOfConnections(),
        "swap is in-place: still exactly one endpoint, just bound to a "
            + "fresh address");

    EndpointStateMachine swapped =
        connectionManager.getValues().iterator().next();
    assertEquals(refreshed, swapped.getAddress(),
        "the cached endpoint must now hold the freshly-resolved address");
    assertEquals(hostAndPort, swapped.getHostAndPort(),
        "preserved hostname survives the swap so future refreshes still "
            + "work after another pod restart");

    // Step 4: the live test of the recovery -- send a real heartbeat
    // through the swapped endpoint and verify the mock SCM saw it.
    // If any step in the chain (address swap, proxy construction,
    // socket dial, server handler) is broken, this RPC throws and
    // the test fails.
    int beforeCount = scmServerImpl.getRpcCount();
    SCMHeartbeatRequestProto request = SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(
            org.apache.hadoop.hdds.protocol.MockDatanodeDetails
                .randomDatanodeDetails().getProtoBufMessage())
        .build();
    SCMHeartbeatResponseProto response =
        swapped.getEndPoint().sendHeartbeat(request);
    assertNotNull(response, "heartbeat response must come back from "
        + "the live SCM via the swapped endpoint");
    assertTrue(scmServerImpl.getRpcCount() > beforeCount,
        "mock SCM must observe at least one new RPC -- proves the "
            + "end-to-end network path through the swapped endpoint is "
            + "actually live");
  }
}
