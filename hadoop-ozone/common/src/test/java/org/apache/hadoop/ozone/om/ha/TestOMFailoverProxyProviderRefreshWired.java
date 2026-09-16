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

package org.apache.hadoop.ozone.om.ha;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.StringJoiner;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Wired-path tests for {@code OMFailoverProxyProviderBase.shouldRetry}'s
 * interaction with the new connection-class filter and refresh hook.
 * These complement {@code TestConnectionFailureUtils} (helper-in-isolation)
 * and {@code TestOMProxyInfoDnsRefresh} (per-instance refresh) by
 * exercising the actual retry policy whose return value drives the
 * RetryInvocationHandler in production.
 * <p>
 * The "load-bearing" assertion is that a {@link SocketTimeoutException}
 * -- the AWS EC2 / EKS silent-drop case the PR is sold on -- routed
 * through {@code shouldRetry} actually triggers the per-node DNS refresh
 * on the current OM. {@code TestConnectionFailureUtils} proves the
 * filter classifies it correctly in isolation; this test proves the
 * filter is wired.
 */
public class TestOMFailoverProxyProviderRefreshWired {

  private static final String OM_SERVICE_ID = "om-svc-refresh-wired";
  private OzoneConfiguration conf;

  @BeforeEach
  public void setUp() {
    conf = new OzoneConfiguration();
    StringJoiner ids = new StringJoiner(",");
    for (int i = 1; i <= 3; i++) {
      String nodeId = "om-" + i;
      conf.set(ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY, OM_SERVICE_ID,
          nodeId), "localhost:" + (9860 + i));
      ids.add(nodeId);
    }
    conf.set(ConfUtils.addKeySuffixes(OZONE_OM_NODES_KEY, OM_SERVICE_ID),
        ids.toString());
  }

  /**
   * A counting subclass that records each call to
   * {@code maybeRefreshCurrentOmAddress} so the test can assert
   * exactly when the wiring fires.
   */
  private static final class CountingProvider
      extends HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> {
    private int refreshCalls;

    CountingProvider(OzoneConfiguration c) throws IOException {
      super(c, UserGroupInformation.getCurrentUser(), OM_SERVICE_ID,
          OzoneManagerProtocolPB.class);
    }

    @Override
    synchronized boolean maybeRefreshCurrentOmAddress() {
      refreshCalls++;
      return false;
    }
  }

  /**
   * SocketTimeoutException through {@code shouldRetry} -- the AWS
   * silent-drop scenario -- must invoke the refresh hook when the
   * flag is on. Round 1 personas flagged this exception type as
   * missing from the original filter; Round 2 added it to
   * ConnectionFailureUtils. This test proves the wiring.
   */
  @Test
  public void testSocketTimeoutTriggersRefreshHook() throws Exception {
    conf.setBoolean(OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY, true);
    CountingProvider p = new CountingProvider(conf);
    RetryPolicy policy = p.getRetryPolicy(10);
    RetryPolicy.RetryAction action = policy.shouldRetry(
        new SocketTimeoutException("EC2 silent drop"), 0, 0, false);
    assertEquals(RetryPolicy.RetryAction.RetryDecision.FAILOVER_AND_RETRY,
        action.action);
    assertEquals(1, p.refreshCalls,
        "SocketTimeoutException must invoke the refresh hook exactly once");
  }

  /**
   * ConnectException (the OpenStack fast-RST scenario) must also
   * invoke the refresh hook. Together with the SocketTimeout test,
   * this proves the filter covers both K8s failure shapes the JIRA
   * description names.
   */
  @Test
  public void testConnectExceptionTriggersRefreshHook() throws Exception {
    conf.setBoolean(OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY, true);
    CountingProvider p = new CountingProvider(conf);
    RetryPolicy policy = p.getRetryPolicy(10);
    policy.shouldRetry(
        new IOException("connection refused", new ConnectException()), 0, 0, false);
    assertEquals(1, p.refreshCalls);
  }

  /**
   * Application-level errors (an OMException not wrapped in a
   * connection-class) must NOT invoke the refresh hook. Re-resolving
   * DNS would not help and would amplify load.
   */
  @Test
  public void testApplicationLevelErrorDoesNotTriggerRefresh()
      throws Exception {
    conf.setBoolean(OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY, true);
    CountingProvider p = new CountingProvider(conf);
    RetryPolicy policy = p.getRetryPolicy(10);
    policy.shouldRetry(new OMException("not the leader",
        ResultCodes.INTERNAL_ERROR), 0, 0, false);
    assertEquals(0, p.refreshCalls,
        "OMException is application-level; refresh hook must NOT fire");
  }

  /**
   * Flag-off invariant: even on a connection-class exception, the
   * refresh hook must NOT be invoked when the resolve-needed flag is
   * false. Guards the "default-off" safety claim of the PR.
   */
  @Test
  public void testFlagDisabledSuppressesRefresh() throws Exception {
    conf.setBoolean(OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY, false);
    CountingProvider p = new CountingProvider(conf);
    RetryPolicy policy = p.getRetryPolicy(10);
    policy.shouldRetry(new ConnectException("refused"), 0, 0, false);
    assertEquals(0, p.refreshCalls,
        "with the flag off the refresh hook must never fire, even for "
            + "connection-class exceptions");
  }

  /**
   * Verifies the C2 "retry-same-proxy" pin: when a refresh succeeds,
   * the next failover must STAY on the just-refreshed nodeId rather
   * than advancing to the next peer in the failover ring.
   * <p>
   * Round 3 found that the prior version of this test was vacuous:
   * with both currentProxyIndex and nextProxyIndex initialised to 0
   * from construction, performFailover was a no-op (currentProxyIndex
   * = nextProxyIndex = 0), and the assertion held REGARDLESS of
   * whether the pin code at OMFailoverProxyProviderBase.shouldRetry
   * actually invoked setNextOmProxy. To make the pin observably
   * load-bearing, this test PRE-ADVANCES nextProxyIndex by triggering
   * a non-refresh shouldRetry first (an OMException, which is not a
   * connection-class failure). That sets nextProxyIndex to (current+1).
   * Then a second shouldRetry with a connection-class exception fires
   * the refresh-success path, which MUST pull nextProxyIndex back to
   * the current node. If the pin code is broken, the post-test
   * currentProxyOMNodeId will be the NEXT node, not the original.
   */
  @Test
  public void testRefreshSuccessPinsCurrentNodeId() throws Exception {
    conf.setBoolean(OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY, true);
    HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> p =
        new HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB>(
            conf, UserGroupInformation.getCurrentUser(), OM_SERVICE_ID,
            OzoneManagerProtocolPB.class) {
          @Override
          boolean maybeRefreshCurrentOmAddress() {
            return true; // pretend the swap happened
          }
        };

    String beforeNode = p.getCurrentProxyOMNodeId();
    RetryPolicy policy = p.getRetryPolicy(10);

    // Pre-advance nextProxyIndex by triggering a non-refresh failover
    // (selectNextOmProxy increments nextProxyIndex). We use a wrapper
    // exception that does NOT pass isConnectionFailure so the refresh
    // hook is NOT invoked here.
    policy.shouldRetry(new IOException("not-a-connection-failure"),
        0, 0, false);
    // Sanity: a subsequent performFailover would now move us off the
    // original node, because nextProxyIndex was advanced.

    // Now trigger the connection-failure path with refresh enabled.
    // The pin MUST pull nextProxyIndex back to the original node.
    RetryPolicy.RetryAction action = policy.shouldRetry(
        new ConnectException("refused"), 0, 1, false);
    assertTrue(
        action.action == RetryPolicy.RetryAction.RetryDecision.FAILOVER_AND_RETRY,
        "refresh-success path returns FAILOVER_AND_RETRY so the retry "
            + "framework re-dials with the new IP");
    p.performFailover(null);
    assertEquals(beforeNode, p.getCurrentProxyOMNodeId(),
        "after a successful refresh, performFailover must STAY on the "
            + "original nodeId even though a prior shouldRetry advanced "
            + "nextProxyIndex -- otherwise the freshly-fixed peer is "
            + "bypassed for up to N-1 retries");
    assertNotNull(beforeNode);
  }
}
