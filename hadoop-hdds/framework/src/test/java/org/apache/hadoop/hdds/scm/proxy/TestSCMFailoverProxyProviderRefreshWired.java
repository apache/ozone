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

package org.apache.hadoop.hdds.scm.proxy;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NODES_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.ratis.ServerNotLeaderException;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Wired-path tests for {@link SCMFailoverProxyProviderBase#getRetryPolicy}'s
 * interaction with the connection-class filter and
 * {@link SCMFailoverProxyProviderBase#refreshProxyAddressIfChanged}.
 * Complements {@code TestConnectionFailureUtils} (helper-in-isolation)
 * and {@code TestSCMFailoverProxyProviderRefresh} (per-instance refresh)
 * by exercising the actual retry policy whose return value drives the
 * RetryInvocationHandler in production. It also verifies wired
 * suggested-leader failover target selection.
 */
public class TestSCMFailoverProxyProviderRefreshWired {

  private static final String SCM_SERVICE_ID = "scmservice";
  private static final String SCM_NODE_1 = "scm1";
  private static final String SCM_NODE_2 = "scm2";
  private static final String SCM_NODE_3 = "scm3";

  private OzoneConfiguration conf;

  @BeforeEach
  public void setUp() {
    // A 2-node SCM HA config so the failover ring has a second node to
    // advance to. With a single non-HA entry, SCMNodeInfo.buildNodeInfo
    // yields one dummy node and performFailover can never move, which
    // would make the pinning assertion below vacuous. See TestSCMNodeInfo
    // for the canonical HA config shape.
    conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_SERVICE_IDS_KEY, SCM_SERVICE_ID);
    conf.set(OZONE_SCM_NODES_KEY + "." + SCM_SERVICE_ID,
        SCM_NODE_1 + "," + SCM_NODE_2);
    conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_ADDRESS_KEY,
        SCM_SERVICE_ID, SCM_NODE_1), "localhost");
    conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_ADDRESS_KEY,
        SCM_SERVICE_ID, SCM_NODE_2), "localhost");
  }

  /**
   * A counting subclass that records each call to
   * {@code refreshProxyAddressIfChanged} so the test can assert exactly
   * when the wiring fires.
   */
  private static final class CountingProvider
      extends SCMBlockLocationFailoverProxyProvider {
    private int refreshCalls;

    CountingProvider(OzoneConfiguration c) {
      super(c);
    }

    @Override
    boolean refreshProxyAddressIfChanged(String nodeId) {
      refreshCalls++;
      return false;
    }
  }

  @Test
  public void testSocketTimeoutTriggersRefreshHook() throws Exception {
    conf.setBoolean(OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY, true);
    CountingProvider provider = new CountingProvider(conf);
    RetryPolicy policy = provider.getRetryPolicy();
    policy.shouldRetry(new SocketTimeoutException("EC2 silent drop"),
        0, 0, false);
    assertEquals(1, provider.refreshCalls,
        "SocketTimeoutException must invoke the refresh hook exactly once");
  }

  @Test
  public void testConnectExceptionTriggersRefreshHook() throws Exception {
    conf.setBoolean(OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY, true);
    CountingProvider provider = new CountingProvider(conf);
    RetryPolicy policy = provider.getRetryPolicy();
    policy.shouldRetry(
        new IOException("connection refused", new ConnectException()),
        0, 0, false);
    assertEquals(1, provider.refreshCalls);
  }

  @Test
  public void testApplicationLevelErrorDoesNotTriggerRefresh() throws Exception {
    conf.setBoolean(OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY, true);
    CountingProvider provider = new CountingProvider(conf);
    RetryPolicy policy = provider.getRetryPolicy();
    policy.shouldRetry(new ServerNotLeaderException("not the leader"),
        0, 0, false);
    assertEquals(0, provider.refreshCalls,
        "ServerNotLeaderException is application-level; refresh must NOT fire");
  }

  @Test
  public void testFailoverToIpv6SuggestedLeader() {
    SCMBlockLocationFailoverProxyProvider provider = newThreeNodeProvider();
    SCMProxyInfo leaderProxy = proxyInfoOf(provider, SCM_NODE_3);
    int port = leaderProxy.getAddress().getPort();
    provider.replaceProxyInfoForTest(SCM_NODE_3,
        new SCMProxyInfo(leaderProxy.getServiceId(), SCM_NODE_3,
            new InetSocketAddress("2001:db8::1", port)));

    // The cached address expands to 2001:db8:0:0:0:0:0:1, so the compressed hint only
    // matches through the canonical comparison.
    failoverWith(provider, notLeader("[2001:db8::1]:" + port));

    assertEquals(SCM_NODE_3, provider.getCurrentProxySCMNodeId(),
        "suggested leader must override the next round-robin SCM");
  }

  @Test
  public void testFailoverToIpv4SuggestedLeader() {
    SCMBlockLocationFailoverProxyProvider provider = newThreeNodeProvider();
    SCMProxyInfo leaderProxy = proxyInfoOf(provider, SCM_NODE_3);
    // Give scm3 a distinct IPv4 address so its authority is unambiguous among the
    // localhost nodes.
    int port = leaderProxy.getAddress().getPort();
    provider.replaceProxyInfoForTest(SCM_NODE_3,
        new SCMProxyInfo(leaderProxy.getServiceId(), SCM_NODE_3,
            new InetSocketAddress("127.0.0.2", port)));

    failoverWith(provider, notLeader("127.0.0.2:" + port));

    assertEquals(SCM_NODE_3, provider.getCurrentProxySCMNodeId(),
        "an IPv4 suggested leader must keep matching");
  }

  @Test
  public void testFailoverToSuggestedLeaderMatchesWithoutResolution() {
    SCMBlockLocationFailoverProxyProvider provider = newThreeNodeProvider();
    SCMProxyInfo leaderProxy = proxyInfoOf(provider, SCM_NODE_3);
    // An address that was never resolved: matching it can only be done on the host text,
    // and getAddress() is null.
    int port = leaderProxy.getAddress().getPort();
    provider.replaceProxyInfoForTest(SCM_NODE_3,
        new SCMProxyInfo(leaderProxy.getServiceId(), SCM_NODE_3,
            InetSocketAddress.createUnresolved("scm3.example.com", port)));

    failoverWith(provider, notLeader("scm3.example.com:" + port));

    assertEquals(SCM_NODE_3, provider.getCurrentProxySCMNodeId(),
        "suggested leader must match by authority without DNS resolution");
  }

  @Test
  public void testSuggestedLeaderDistinguishesIpv6ScopeIds() {
    SCMBlockLocationFailoverProxyProvider provider = newThreeNodeProvider();
    // Every node gets the same IPv6 address and port; only the scope tells scm3 apart, so a
    // scope-blind comparison matches one of the other two whatever the iteration order.
    int port = proxyInfoOf(provider, SCM_NODE_3).getAddress().getPort();
    for (SCMProxyInfo proxyInfo : new ArrayList<>(provider.getSCMProxyInfoList())) {
      String host = SCM_NODE_3.equals(proxyInfo.getNodeId()) ? "fe80::1%2" : "fe80::1%1";
      provider.replaceProxyInfoForTest(proxyInfo.getNodeId(),
          new SCMProxyInfo(proxyInfo.getServiceId(), proxyInfo.getNodeId(),
              new InetSocketAddress(host, port)));
    }

    failoverWith(provider, notLeader("[fe80::1%2]:" + port));

    assertEquals(SCM_NODE_3, provider.getCurrentProxySCMNodeId(),
        "a differing IPv6 scope must not be treated as the same SCM");
  }

  @Test
  public void testSuggestedLeaderDistinguishesIpv6ZoneNameCase() {
    SCMBlockLocationFailoverProxyProvider provider = newThreeNodeProvider();
    // As above, but the scope is an interface name. Interface names are case-sensitive, so a
    // case-insensitive host comparison matches one of the ETH0 nodes instead of scm3.
    int port = proxyInfoOf(provider, SCM_NODE_3).getAddress().getPort();
    for (SCMProxyInfo proxyInfo : new ArrayList<>(provider.getSCMProxyInfoList())) {
      String host = SCM_NODE_3.equals(proxyInfo.getNodeId()) ? "fe80::1%eth0" : "fe80::1%ETH0";
      provider.replaceProxyInfoForTest(proxyInfo.getNodeId(),
          new SCMProxyInfo(proxyInfo.getServiceId(), proxyInfo.getNodeId(),
              InetSocketAddress.createUnresolved(host, port)));
    }

    failoverWith(provider, notLeader("[fe80::1%eth0]:" + port));

    assertEquals(SCM_NODE_3, provider.getCurrentProxySCMNodeId(),
        "a differing IPv6 zone name must not be treated as the same SCM");
  }

  @Test
  public void testSuggestedLeaderMatchesResolvedAddressOfHostname() throws Exception {
    SCMBlockLocationFailoverProxyProvider provider = newThreeNodeProvider();
    SCMProxyInfo leaderProxy = proxyInfoOf(provider, SCM_NODE_3);
    int port = leaderProxy.getAddress().getPort();
    // scm3 is configured by hostname but already resolved to an IPv6 address, which is what
    // Ratis reports in the hint. getByAddress attaches the hostname without any lookup.
    InetAddress resolved = InetAddress.getByAddress("scm3.example.com",
        InetAddress.getByName("2001:db8::1").getAddress());
    provider.replaceProxyInfoForTest(SCM_NODE_3,
        new SCMProxyInfo(leaderProxy.getServiceId(), SCM_NODE_3,
            new InetSocketAddress(resolved, port)));

    failoverWith(provider, notLeader("[2001:db8::1]:" + port));

    assertEquals(SCM_NODE_3, provider.getCurrentProxySCMNodeId(),
        "a hint carrying the resolved IP must match a hostname-configured SCM");
  }

  @Test
  public void testUnparseableSuggestedLeaderKeepsRoundRobin() {
    SCMBlockLocationFailoverProxyProvider provider = newThreeNodeProvider();
    int port = proxyInfoOf(provider, SCM_NODE_3).getAddress().getPort();
    // The message parser's non-bracketed alternative accepts a malformed authority such as
    // [a]b:9863, which no longer parses as host and port on the client side.
    String malformed = "[a]b:" + port;
    ServerNotLeaderException parsed =
        new ServerNotLeaderException(notLeader(malformed).getMessage());
    assertEquals(malformed, parsed.getSuggestedLeader());

    LogCapturer log = LogCapturer.captureLogs(SCMBlockLocationFailoverProxyProvider.class);
    try {
      failoverWith(provider, parsed);
    } finally {
      log.stopCapturing();
    }

    assertThat(log.getOutput()).contains("Ignoring unparseable suggested leader " + malformed);
    assertEquals(SCM_NODE_2, provider.getCurrentProxySCMNodeId(),
        "an unparseable suggested leader must fall back to round-robin failover");
  }

  private SCMBlockLocationFailoverProxyProvider newThreeNodeProvider() {
    conf.set(OZONE_SCM_NODES_KEY + "." + SCM_SERVICE_ID,
        SCM_NODE_1 + "," + SCM_NODE_2 + "," + SCM_NODE_3);
    conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_ADDRESS_KEY,
        SCM_SERVICE_ID, SCM_NODE_3), "localhost");
    return new SCMBlockLocationFailoverProxyProvider(conf);
  }

  private static SCMProxyInfo proxyInfoOf(
      SCMBlockLocationFailoverProxyProvider provider, String nodeId) {
    return provider.getSCMProxyInfoList().stream()
        .filter(proxyInfo -> nodeId.equals(proxyInfo.getNodeId()))
        .findFirst().get();
  }

  private static ServerNotLeaderException notLeader(String suggestedLeader) {
    return new ServerNotLeaderException(RaftPeerId.valueOf(SCM_NODE_1),
        suggestedLeader, "localhost", "SCM");
  }

  private static void failoverWith(SCMBlockLocationFailoverProxyProvider provider,
      ServerNotLeaderException e) {
    provider.performFailoverToAssignedLeader(null, e);
    provider.performFailover(null);
  }

  @Test
  public void testFlagDisabledSuppressesRefresh() throws Exception {
    conf.setBoolean(OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY, false);
    CountingProvider provider = new CountingProvider(conf);
    RetryPolicy policy = provider.getRetryPolicy();
    policy.shouldRetry(new ConnectException("refused"), 0, 0, false);
    assertEquals(0, provider.refreshCalls,
        "with the flag off the refresh hook must never fire");
  }

  /**
   * After advancing to the second SCM node, a connection failure whose
   * DNS refresh succeeds must PIN the provider on that second node: the
   * next performFailover stays put instead of round-robining back to the
   * first node. A single-node ring cannot observe this (there is nowhere
   * to advance), which is why setUp() configures two HA nodes.
   */
  @Test
  public void testRefreshSuccessPinsCurrentNodeId() throws Exception {
    conf.setBoolean(OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY, true);
    SCMBlockLocationFailoverProxyProvider provider =
        new SCMBlockLocationFailoverProxyProvider(conf) {
          @Override
          boolean refreshProxyAddressIfChanged(String nodeId) {
            return true;
          }
        };

    String firstNode = provider.getCurrentProxySCMNodeId();
    // Round-robin advance to the second node.
    provider.performFailover(null);
    String secondNode = provider.getCurrentProxySCMNodeId();
    assertNotEquals(firstNode, secondNode,
        "2-node HA ring must advance to a distinct second node");

    RetryPolicy policy = provider.getRetryPolicy();
    // Connection failure + successful refresh pins updatedLeaderNodeID to
    // the current (second) node, so the next performFailover stays put.
    // If the pin regressed, performFailover would round-robin back to the
    // first node and the assertion below would fail.
    policy.shouldRetry(new ConnectException("refused"), 0, 1, false);
    provider.performFailover(null);

    assertEquals(secondNode, provider.getCurrentProxySCMNodeId(),
        "after a successful refresh, performFailover must stay on the "
            + "second node rather than round-robining back to the first");
  }
}
