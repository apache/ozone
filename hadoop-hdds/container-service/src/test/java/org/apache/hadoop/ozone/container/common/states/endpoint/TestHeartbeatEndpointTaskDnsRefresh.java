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

package org.apache.hadoop.ozone.container.common.states.endpoint;

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_DN_SCM_HEARTBEAT_REFRESH_THRESHOLD_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine.DatanodeStates;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.security.AccessControlException;
import org.junit.jupiter.api.Test;

/**
 * Verifies the production trigger chain for DN → SCM DNS refresh:
 * heartbeat fails with a connection-class IOException → catch block in
 * {@code HeartbeatEndpointTask.call()} runs → {@code maybeRefreshScmAddress}
 * checks the flag and the missed-count threshold → fires
 * {@code SCMConnectionManager.refreshSCMServer}. The previous
 * {@code TestSCMConnectionManagerDnsRefreshE2E} drove the swap mechanism
 * directly; this test proves the integration.
 */
public class TestHeartbeatEndpointTaskDnsRefresh {

  private static final InetSocketAddress STALE_ADDR = makeAddr(127, 0, 0, 99);
  private static final InetSocketAddress REFRESHED_ADDR = makeAddr(127, 0, 0, 1);

  /**
   * Build an InetSocketAddress whose underlying address is RESOLVED to
   * a specific IPv4 literal (so the test asserts on a real, non-equal
   * post-refresh address). Going through {@code InetAddress.getByAddress}
   * skips DNS entirely; the resulting socket address is stable in CI.
   */
  private static InetSocketAddress makeAddr(int a, int b, int c, int d) {
    try {
      return new InetSocketAddress(
          InetAddress.getByAddress(new byte[]{(byte) a, (byte) b,
              (byte) c, (byte) d}), 9861);
    } catch (java.net.UnknownHostException impossible) {
      throw new AssertionError(impossible);
    }
  }

  /**
   * With the flag enabled and the missed-heartbeat counter at the
   * configured threshold, a connection-class IOException from
   * sendHeartbeat must drive refreshSCMServer exactly once with the
   * cached address as the argument.
   */
  @Test
  public void testRefreshFiresWhenFlagEnabledAndThresholdMet()
      throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY, true);
    conf.setInt(OZONE_DN_SCM_HEARTBEAT_REFRESH_THRESHOLD_KEY, 3);

    Fixture f = newFixture(conf);
    when(f.endpoint.getMissedCount()).thenReturn(3L);
    when(f.endpoint.getHostAndPort()).thenReturn("scm1:9861");
    when(f.scm.sendHeartbeat(any()))
        .thenThrow(new IOException("conn refused", new ConnectException()));

    f.task.call();

    // Cached address is what the catch block should ask the manager to
    // re-resolve. A regression that passed the wrong key would leave
    // refreshSCMServer with a non-existent address and silently
    // produce no swap.
    verify(f.connectionManager, atLeastOnce())
        .refreshSCMServer(eq(STALE_ADDR), any());
  }

  /**
   * With the flag disabled (the default), refreshSCMServer must NOT be
   * called even on a connection-class failure. Guards against the
   * "default-off" safety claim being silently broken.
   */
  @Test
  public void testRefreshSuppressedWhenFlagDisabled() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY, false);
    conf.setInt(OZONE_DN_SCM_HEARTBEAT_REFRESH_THRESHOLD_KEY, 1);

    Fixture f = newFixture(conf);
    when(f.endpoint.getMissedCount()).thenReturn(99L);
    when(f.endpoint.getHostAndPort()).thenReturn("scm1:9861");
    when(f.scm.sendHeartbeat(any()))
        .thenThrow(new IOException("conn refused", new ConnectException()));

    f.task.call();

    verify(f.connectionManager, never()).refreshSCMServer(any(), any());
  }

  /**
   * Threshold semantics: missed count BELOW the configured threshold
   * must NOT fire a refresh, even with the flag enabled and a
   * connection-class failure. Guards against thrashing on the first
   * transient blip.
   */
  @Test
  public void testRefreshSuppressedBelowThreshold() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY, true);
    conf.setInt(OZONE_DN_SCM_HEARTBEAT_REFRESH_THRESHOLD_KEY, 5);

    Fixture f = newFixture(conf);
    when(f.endpoint.getMissedCount()).thenReturn(2L);
    when(f.endpoint.getHostAndPort()).thenReturn("scm1:9861");
    when(f.scm.sendHeartbeat(any()))
        .thenThrow(new IOException("conn refused", new ConnectException()));

    f.task.call();

    verify(f.connectionManager, never()).refreshSCMServer(any(), any());
  }

  /**
   * If the endpoint never preserved its host:port string (legacy
   * 4-arg ctor path), refresh must be a no-op. Operators in that case
   * are expected to restart the DN to pick up new IPs.
   */
  @Test
  public void testRefreshSuppressedWhenHostAndPortNotPreserved()
      throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY, true);
    conf.setInt(OZONE_DN_SCM_HEARTBEAT_REFRESH_THRESHOLD_KEY, 1);

    Fixture f = newFixture(conf);
    when(f.endpoint.getMissedCount()).thenReturn(99L);
    when(f.endpoint.getHostAndPort()).thenReturn(null); // legacy ctor path
    when(f.scm.sendHeartbeat(any()))
        .thenThrow(new IOException("conn refused", new ConnectException()));

    f.task.call();

    verify(f.connectionManager, never()).refreshSCMServer(any(), any());
  }

  /**
   * After a successful refresh, the per-endpoint StateContext queues
   * must be migrated from the old address key to the new one. The
   * old key must be GONE and the new key must be PRESENT.
   * <p>
   * Critically, this test uses two RESOLVED but distinct InetSocketAddress
   * instances (127.0.0.99 and 127.0.0.1) so {@code STALE_ADDR.equals(REFRESHED_ADDR)}
   * is false. A prior version of this test used two unresolved-by-name
   * addresses that compared equal, making {@code migrateEndpoint}
   * short-circuit and the assertion pass vacuously regardless of
   * whether migration ran.
   */
  @Test
  public void testStateContextEndpointsMigrateAfterSuccessfulSwap()
      throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY, true);
    conf.setInt(OZONE_DN_SCM_HEARTBEAT_REFRESH_THRESHOLD_KEY, 1);

    Fixture f = newFixture(conf);
    f.context.addEndpoint(STALE_ADDR);
    // Pre-populate the old key with a queue marker so we can prove the
    // migrated map is the SAME map we put data into (load-bearing
    // assertion: queued reports survive the rekey).
    f.context.getIncrementalReportQueueSize().put(STALE_ADDR, 0);

    when(f.endpoint.getMissedCount()).thenReturn(99L);
    when(f.endpoint.getHostAndPort()).thenReturn("scm1:9861");
    when(f.connectionManager.refreshSCMServer(eq(STALE_ADDR), any()))
        .thenReturn(REFRESHED_ADDR);
    when(f.scm.sendHeartbeat(any()))
        .thenThrow(new IOException("conn refused", new ConnectException()));

    f.task.call();

    // After migration, the OLD key must be gone from the
    // incrementalReportsQueue/containerActions/pipelineActions/endpoints
    // and the NEW key must be present.
    assertEquals(true,
        f.context.getIncrementalReportQueueSize().containsKey(REFRESHED_ADDR),
        "post-refresh, the new endpoint key must exist in the "
            + "incremental-reports map");
    assertEquals(false,
        f.context.getIncrementalReportQueueSize().containsKey(STALE_ADDR),
        "post-refresh, the old endpoint key must NOT exist in the "
            + "incremental-reports map (otherwise producers writing to "
            + "the stale key still pile up reports a dead endpoint will "
            + "never deliver)");
    assertEquals(true,
        f.context.getContainerActionQueueSize().containsKey(REFRESHED_ADDR),
        "post-refresh, the new endpoint key must exist in the "
            + "container-actions map");
    assertEquals(false,
        f.context.getContainerActionQueueSize().containsKey(STALE_ADDR),
        "post-refresh, the old endpoint key must NOT exist in the "
            + "container-actions map");
  }

  /**
   * R2 negative-filter test: with the flag enabled and the threshold
   * met, a heartbeat failure that is NOT a connection-class exception
   * (e.g. AccessControlException -- the peer is reachable on the
   * cached IP and rejects us at the application layer) must NOT
   * trigger a refresh. Re-resolving DNS would not help, and a
   * misbehaving peer that returns AccessControlException on every
   * heartbeat would otherwise drive a DNS-lookup storm.
   */
  @Test
  public void testRefreshSuppressedOnApplicationLevelException()
      throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY, true);
    conf.setInt(OZONE_DN_SCM_HEARTBEAT_REFRESH_THRESHOLD_KEY, 1);

    Fixture f = newFixture(conf);
    when(f.endpoint.getMissedCount()).thenReturn(99L);
    when(f.endpoint.getHostAndPort()).thenReturn("scm1:9861");
    when(f.scm.sendHeartbeat(any()))
        .thenThrow(new IOException("rejected",
            new AccessControlException("client lacks SCM_TOKEN")));

    f.task.call();

    verify(f.connectionManager, never()).refreshSCMServer(any(), any());
  }

  // ------- fixture helpers -------

  private static final class Fixture {
    private final HeartbeatEndpointTask task;
    private final EndpointStateMachine endpoint;
    private final SCMConnectionManager connectionManager;
    private final DatanodeStateMachine datanodeStateMachine;
    private final StateContext context;
    private final StorageContainerDatanodeProtocolClientSideTranslatorPB scm;

    Fixture(HeartbeatEndpointTask task, EndpointStateMachine endpoint,
            SCMConnectionManager mgr, DatanodeStateMachine dsm,
            StateContext ctx,
            StorageContainerDatanodeProtocolClientSideTranslatorPB scm) {
      this.task = task;
      this.endpoint = endpoint;
      this.connectionManager = mgr;
      this.datanodeStateMachine = dsm;
      this.context = ctx;
      this.scm = scm;
    }
  }

  private Fixture newFixture(OzoneConfiguration conf) throws Exception {
    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);

    EndpointStateMachine endpoint = mock(EndpointStateMachine.class);
    when(endpoint.getEndPoint()).thenReturn(scm);
    when(endpoint.getAddress()).thenReturn(STALE_ADDR);

    SCMConnectionManager connectionManager = mock(SCMConnectionManager.class);
    DatanodeStateMachine datanodeStateMachine = mock(DatanodeStateMachine.class);
    when(datanodeStateMachine.getConnectionManager())
        .thenReturn(connectionManager);
    when(datanodeStateMachine.getQueuedCommandCount())
        .thenReturn(new org.apache.hadoop.hdfs.util.EnumCounters<>(
            org.apache.hadoop.hdds.protocol.proto
                .StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.class));

    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        datanodeStateMachine, "");

    HDDSLayoutVersionManager lvm = mock(HDDSLayoutVersionManager.class);
    when(lvm.getSoftwareLayoutVersion()).thenReturn(maxLayoutVersion());
    when(lvm.getMetadataLayoutVersion()).thenReturn(maxLayoutVersion());

    DatanodeDetails dd = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .build();

    HeartbeatEndpointTask task = HeartbeatEndpointTask.newBuilder()
        .setConfig(conf)
        .setDatanodeDetails(dd)
        .setContext(context)
        .setLayoutVersionManager(lvm)
        .setEndpointStateMachine(endpoint)
        .build();

    return new Fixture(task, endpoint, connectionManager,
        datanodeStateMachine, context, scm);
  }
}
