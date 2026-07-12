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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link OMProxyInfo#refreshAddressIfChanged()} correctly
 * detects DNS changes -- the Kubernetes pod-IP-change recovery path on
 * the Client → OM RPC route.
 */
public class TestOMProxyInfoDnsRefresh {

  /**
   * When DNS for the configured hostname now returns the same IP that
   * is already cached, refresh is a no-op. Returns false; cached
   * address and proxy are untouched. Critically, the cached proxy must
   * NOT be discarded -- a regression that nulled {@code proxy}
   * unconditionally would tear down a healthy connection on every
   * application-level failure.
   */
  @Test
  public void testRefreshIsNoopWhenIpUnchanged() throws Exception {
    Object originalProxy = new Object();
    OMProxyInfo<Object> info = OMProxyInfo.newInstance(
        originalProxy, "svc", "om1", "localhost:9862");
    InetSocketAddress before = info.getAddress();

    boolean swapped = info.refreshAddressIfChanged();

    assertFalse(swapped, "no swap when DNS resolves to the same IP");
    assertSame(before, info.getAddress(),
        "cached address must not be replaced when IP is unchanged");
    assertSame(originalProxy, info.getProxy(),
        "cached proxy must NOT be discarded on a no-op refresh");
  }

  /**
   * To drive the change-detection path we construct an OMProxyInfo
   * pointing at "localhost", then inject a deliberately stale IP via
   * the test hook. Re-resolving "localhost" then yields the live
   * loopback IP, the cached stale IP differs, and the swap fires.
   */
  @Test
  public void testRefreshSwapsAddressOnIpChange() throws Exception {
    OMProxyInfo<Object> info = OMProxyInfo.newInstance(
        /*proxy=*/ null, "svc", "om1", "localhost:9862");

    InetSocketAddress staleAddr = new InetSocketAddress(
        InetAddress.getByAddress(new byte[] {127, 0, 0, 99}), 9862);
    info.setCachedAddressForTest(staleAddr);

    boolean swapped = info.refreshAddressIfChanged();
    assertTrue(swapped, "swap must fire when DNS returns a different IP "
        + "than the stale 127.0.0.99 we forced into the cache");
    assertNotEquals(staleAddr.getAddress(), info.getAddress().getAddress(),
        "cached address must hold the freshly-resolved IP after swap");
    assertNull(info.getProxy(),
        "cached proxy must be discarded so the next dial uses the new IP");
  }

  /**
   * createProxyIfNeeded rebuilds the proxy from the freshly-resolved
   * address after a swap. The lambda asserts the parameter equals the
   * post-refresh address -- a regression that passes a stale or null
   * address to the factory would fire here.
   */
  @Test
  public void testProxyRebuildsAfterRefreshUsesNewAddress() throws Exception {
    OMProxyInfo<Object> info = OMProxyInfo.newInstance(
        new Object(), "svc", "om1", "localhost:9862");

    InetSocketAddress staleAddr = new InetSocketAddress(
        InetAddress.getByAddress(new byte[] {127, 0, 0, 99}), 9862);
    info.setCachedAddressForTest(staleAddr);
    assertTrue(info.refreshAddressIfChanged());
    assertNull(info.getProxy());

    InetSocketAddress expectedNewAddress = info.getAddress();
    Object freshProxy = new Object();
    InetSocketAddress[] dialedWith = new InetSocketAddress[1];
    info.createProxyIfNeeded(addr -> {
      dialedWith[0] = addr;
      return freshProxy;
    });

    assertSame(expectedNewAddress, dialedWith[0],
        "factory must be invoked with the freshly-resolved address, "
            + "not the stale one or null");
    assertSame(freshProxy, info.getProxy());
  }

  /**
   * dtService must update alongside rpcAddr on a successful swap.
   * Stale dtService after refresh would silently break post-refresh
   * authentication.
   * <p>
   * The earlier shape of this test only asserted that {@code dtService}
   * was non-null after refresh -- vacuous, because the constructor had
   * already built a correct value from the initial "localhost"
   * resolution, and {@code setCachedAddressForTest} only mutates
   * {@code rpcAddr}. The assertion would pass even if the refresh code
   * forgot to rebuild {@code dtService} at all.
   * <p>
   * This shape makes the assertion load-bearing by deliberately
   * staling {@code dtService} (and {@code rpcAddr}) before refresh,
   * then asserting the post-refresh {@code dtService} matches the value
   * {@link SecurityUtil#buildTokenService} would produce for the live
   * address. A regression that skipped the swap inside
   * {@code refreshAddressIfChanged} would leave the stale sentinel in
   * place and the assertion would fail.
   */
  @Test
  public void testRefreshUpdatesDelegationTokenService() throws Exception {
    OMProxyInfo<Object> info = OMProxyInfo.newInstance(
        new Object(), "svc", "om1", "localhost:9862");
    InetSocketAddress staleAddr = new InetSocketAddress(
        InetAddress.getByAddress(new byte[] {127, 0, 0, 99}), 9862);
    Text staleDtService = new Text("stale-sentinel:9862");
    info.setCachedAddressForTest(staleAddr);
    info.setCachedDtServiceForTest(staleDtService);
    assertSame(staleDtService, info.getDelegationTokenService(),
        "test setup: dtService must be the stale sentinel before "
            + "refresh, otherwise the post-refresh assertion is "
            + "vacuous.");

    assertTrue(info.refreshAddressIfChanged());

    Text refreshedDtService = info.getDelegationTokenService();
    assertNotNull(refreshedDtService,
        "dtService must be rebuilt after a successful swap");
    assertNotEquals(staleDtService, refreshedDtService,
        "dtService must be replaced with a value derived from the live "
            + "address; if the refresh code forgot to rebuild dtService "
            + "the stale sentinel would still be present.");
    assertEquals(SecurityUtil.buildTokenService(info.getAddress()),
        refreshedDtService,
        "dtService must equal SecurityUtil.buildTokenService applied "
            + "to the live address.");
  }
}
