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

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link SCMFailoverProxyProviderBase#refreshProxyAddressIfChanged}
 * correctly detects DNS changes and swaps in a fresh {@link SCMProxyInfo}
 * when the SCM peer's IP has shifted under a stable hostname (the
 * Kubernetes pod-IP-change recovery scenario).
 */
public class TestSCMFailoverProxyProviderRefresh {

  /**
   * Build a provider whose only SCM entry deliberately points at a
   * stale IP (127.0.0.99). Re-resolving the preserved hostname
   * "localhost" yields a different IP (typically 127.0.0.1), so the
   * refresh helper must swap in a fresh SCMProxyInfo.
   */
  @Test
  public void testRefreshSwapsAddressOnIpChange() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    // Single SCM, no service id, hostname "localhost".
    conf.set(OZONE_SCM_NAMES, "localhost");
    conf.set(OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, "localhost:9863");

    SCMBlockLocationFailoverProxyProvider provider =
        new SCMBlockLocationFailoverProxyProvider(conf);

    // Replace the cached entry with a deliberately-stale IP. This
    // simulates the state we'd be in if the SCM pod had been
    // rescheduled to a new IP after the provider was constructed.
    SCMProxyInfo cached = provider.getSCMProxyInfoList().iterator().next();
    String nodeId = cached.getNodeId();
    InetSocketAddress staleAddr = new InetSocketAddress(
        InetAddress.getByAddress(new byte[] {127, 0, 0, 99}),
        cached.getAddress().getPort());
    provider.replaceProxyInfoForTest(nodeId,
        new SCMProxyInfo(cached.getServiceId(), nodeId, staleAddr,
            cached.getHostAndPort()));

    boolean swapped = provider.refreshProxyAddressIfChanged(nodeId);
    assertTrue(swapped, "refresh must report a swap when DNS now "
        + "resolves localhost to an IP different from the stale 127.0.0.99");

    SCMProxyInfo updated = provider.getSCMProxyInfoList().iterator().next();
    assertNotEquals(staleAddr.getAddress(), updated.getAddress().getAddress(),
        "after refresh, cached entry must hold a fresh IP");
    assertEquals(staleAddr.getPort(), updated.getAddress().getPort(),
        "port must be preserved across the swap");
    assertEquals("localhost:9863", updated.getHostAndPort(),
        "host:port string must survive the swap so future refreshes work");
  }

  /**
   * When DNS still returns the cached IP, refreshProxyAddressIfChanged
   * is a no-op. This guards against tearing down a healthy proxy on
   * every transient blip when the IP is genuinely unchanged.
   */
  @Test
  public void testRefreshNoopWhenIpUnchanged() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    // Numeric loopback so re-resolution is deterministic: a literal IP
    // parses back to itself, independent of how "localhost" happens to
    // resolve (IPv4 vs IPv6, or multi-A/AAAA ordering) between the two
    // lookups. That ambiguity could otherwise surface as a spurious swap
    // and flake this no-op assertion.
    conf.set(OZONE_SCM_NAMES, "127.0.0.1");
    conf.set(OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, "127.0.0.1:9863");

    SCMBlockLocationFailoverProxyProvider provider =
        new SCMBlockLocationFailoverProxyProvider(conf);

    SCMProxyInfo before = provider.getSCMProxyInfoList().iterator().next();
    String nodeId = before.getNodeId();

    boolean swapped = provider.refreshProxyAddressIfChanged(nodeId);
    assertFalse(swapped, "no swap expected when DNS resolves to the "
        + "same IP that's already cached");
  }

  /**
   * If the entry has no preserved host:port string, refresh is
   * unsupported (legacy code path) and returns false. Belts-and-braces
   * sanity check.
   */
  @Test
  public void testRefreshNoopWithoutHostAndPort() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_NAMES, "localhost");
    conf.set(OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, "localhost:9863");

    SCMBlockLocationFailoverProxyProvider provider =
        new SCMBlockLocationFailoverProxyProvider(conf);

    SCMProxyInfo cached = provider.getSCMProxyInfoList().iterator().next();
    String nodeId = cached.getNodeId();
    // Replace with the legacy three-arg ctor (hostAndPort = null).
    provider.replaceProxyInfoForTest(nodeId,
        new SCMProxyInfo(cached.getServiceId(), nodeId,
            NetUtils.createSocketAddr("localhost:9863")));

    assertFalse(provider.refreshProxyAddressIfChanged(nodeId));
    assertNotNull(provider.getSCMProxyInfoList().iterator().next());
  }
}
