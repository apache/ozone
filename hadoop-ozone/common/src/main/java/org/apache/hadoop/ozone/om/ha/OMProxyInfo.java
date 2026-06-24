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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.FailoverProxyProvider.ProxyInfo;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ProxyInfo} with additional info such as {@link #nodeId} and {@link #rpcAddr}.
 */
public final class OMProxyInfo<T> extends ProxyInfo<T> {
  private static final Logger LOG = LoggerFactory.getLogger(OMProxyInfo.class);

  private final String nodeId;
  /**
   * The original "host:port" config string. Stable for the lifetime of
   * this OMProxyInfo; used as the source of truth for re-resolving DNS
   * when the cached IP becomes stale (Kubernetes pod-IP-change recovery).
   */
  private final String rpcAddrStr;
  /**
   * The currently-resolved address. Initialized at construction by
   * resolving {@link #rpcAddrStr}, and may be replaced atomically by
   * {@link #refreshAddressIfChanged()} when the failover provider
   * detects that the OM node has been rescheduled to a new IP.
   * <p>
   * Mutable but always read/written under the OMProxyInfo's monitor.
   */
  private InetSocketAddress rpcAddr;
  /**
   * Token-service name derived from {@link #rpcAddr}. Updated alongside
   * {@link #rpcAddr} on a successful refresh.
   */
  private Text dtService;

  public static <T> OMProxyInfo<T> newInstance(T proxy, String serviceID, String nodeID, String rpcAddress) {
    if (nodeID == null) {
      nodeID = OzoneConsts.OM_DEFAULT_NODE_ID;
    }
    final String info = "nodeId=" + nodeID + ",nodeAddress=" + rpcAddress;
    return new OMProxyInfo<T>(proxy, serviceID, nodeID, rpcAddress, info);
  }

  private OMProxyInfo(T proxy, String serviceID, String nodeID, String rpcAddress, String proxyInfo) {
    super(proxy, proxyInfo);
    this.nodeId = Objects.requireNonNull(nodeID, "nodeID == null");
    this.rpcAddrStr = Objects.requireNonNull(rpcAddress, "rpcAddress == null");
    this.rpcAddr = NetUtils.createSocketAddr(rpcAddrStr);
    if (rpcAddr.isUnresolved()) {
      LOG.warn("OzoneManager address {} for serviceID {} remains unresolved " +
              "for node ID {} Check your ozone-site.xml file to ensure ozone " +
              "manager addresses are configured properly.",
          rpcAddress, serviceID, nodeId);
      this.dtService = null;
    } else {
      // This issue will be a problem with docker/kubernetes world where one of
      // the container is killed, and that OM address will be unresolved.
      // For now skip the unresolved OM address setting it to the token
      // service field.
      this.dtService = SecurityUtil.buildTokenService(rpcAddr);
    }
  }

  public String getNodeId() {
    return nodeId;
  }

  public String getAddressString() {
    return rpcAddrStr;
  }

  public synchronized InetSocketAddress getAddress() {
    return rpcAddr;
  }

  /**
   * Test-only: inject a deliberately stale cached address to drive
   * the DNS-refresh code path without standing up a real OM.
   * <p>
   * Rejects null because {@link #refreshAddressIfChanged()} dereferences
   * {@code rpcAddr.getAddress()} unconditionally; a null injection would
   * surface as a confusing NPE downstream rather than as a test bug
   * here.
   */
  @VisibleForTesting
  synchronized void setCachedAddressForTest(InetSocketAddress address) {
    this.rpcAddr = Objects.requireNonNull(address,
        "cached address must be non-null");
  }

  /**
   * Test-only: inject a deliberately stale delegation-token service
   * identifier so the refresh path's {@link #dtService} swap is
   * load-bearing. Without this hook, a test that calls
   * {@link #setCachedAddressForTest} alone would leave {@code dtService}
   * already correctly derived from the original constructor-time
   * resolution, and the assertion "dtService is rebuilt on refresh"
   * would pass even if the refresh path forgot to update it.
   */
  @VisibleForTesting
  synchronized void setCachedDtServiceForTest(Text service) {
    this.dtService = Objects.requireNonNull(service,
        "dtService must be non-null");
  }

  public synchronized Text getDelegationTokenService() {
    return dtService;
  }

  public synchronized T getProxy() {
    return proxy;
  }

  public synchronized void createProxyIfNeeded(CheckedFunction<InetSocketAddress, T, IOException> createProxy) {
    if (proxy == null) {
      try {
        proxy = createProxy.apply(getAddress());
      } catch (IOException ioe) {
        throw new IllegalStateException("Failed to create OM proxy for " + this, ioe);
      }
    }
  }

  /**
   * Re-resolve {@link #rpcAddrStr} via DNS and, if the resolved IP has
   * changed, replace the cached {@link #rpcAddr} (and the derived
   * delegation-token service) and discard the cached proxy so that the
   * next {@link #createProxyIfNeeded} call dials the new IP. The stale
   * RPC proxy is closed via {@link RPC#stopProxy} so the underlying
   * Hadoop {@code Client} connection thread and authenticated SASL
   * session against the gone-away peer are not leaked.
   * <p>
   * Returns true when a swap occurred. Off the failure path this is a
   * no-op (returns false): unchanged IP, unresolved lookup, or
   * malformed host string.
   * <p>
   * The DNS lookup and the {@code RPC.stopProxy} call are performed
   * outside the entry monitor so that a slow / dead resolver or a
   * blocking proxy teardown does not freeze concurrent readers of
   * {@link #getAddress()} / {@link #getProxy()}.
   */
  public boolean refreshAddressIfChanged() {
    final InetSocketAddress refreshed;
    try {
      refreshed = NetUtils.createSocketAddr(rpcAddrStr);
    } catch (IllegalArgumentException ex) {
      // Pass the exception (not just getMessage()) so SLF4J emits the
      // stack trace -- malformed address parsing failures need the
      // full chain for operator diagnosis.
      LOG.warn("Failed to re-resolve OM address {}", rpcAddrStr, ex);
      return false;
    }
    if (refreshed.isUnresolved()) {
      LOG.warn("OM hostname {} re-resolved to an unresolved address; "
          + "leaving cached entry in place.", rpcAddrStr);
      return false;
    }
    // Compute the new delegation-token service identifier OUTSIDE the
    // entry monitor. SecurityUtil.buildTokenService is unlikely to throw
    // for a resolved address, but if it ever did inside the swap block
    // we'd be left with rpcAddr=new but dtService=old and proxy=non-null
    // pointing at the old IP -- and the equality short-circuit at the
    // top of the synchronized block below would skip every subsequent
    // refresh attempt because rpcAddr already matches the new IP.
    // Building first means a throw here aborts the whole refresh with
    // no state change.
    final Text newDtService = SecurityUtil.buildTokenService(refreshed);
    final T staleProxy;
    final InetSocketAddress old;
    synchronized (this) {
      // Null-safe IP comparison. The constructor accepts (with a warn)
      // an unresolved rpcAddr -- in that case rpcAddr.getAddress() is
      // null, and a successful re-resolution is genuinely a change so
      // we MUST proceed to swap rather than NPE on .equals().
      InetAddress cachedIp = rpcAddr.getAddress();
      InetAddress refreshedIp = refreshed.getAddress();
      if (cachedIp != null && refreshedIp != null
          && refreshedIp.equals(cachedIp)) {
        return false;
      }
      old = rpcAddr;
      staleProxy = this.proxy;
      this.rpcAddr = refreshed;
      this.dtService = newDtService;
      this.proxy = null;
    }
    if (staleProxy != null) {
      try {
        RPC.stopProxy(staleProxy);
      } catch (RuntimeException stopEx) {
        // Pass the exception (not just getMessage()) so SLF4J emits the
        // stack trace -- proxy-stop failures during connection teardown
        // are otherwise hard to diagnose.
        LOG.warn("Failed to stop stale OM proxy for nodeId {}",
            nodeId, stopEx);
      }
    }
    LOG.info("DNS re-resolution: OM nodeId {} address {} -> {} "
        + "(hostname {}).", nodeId, old, refreshed, rpcAddrStr);
    return true;
  }

  /**
   * A {@link OMProxyInfo} map with a particular order.
   * <p>
   * The map structure (the {@code proxies} list and the {@code ordering}
   * map) is built once at construction and wrapped in unmodifiable
   * views, so the structure itself is immutable and safe to share
   * without external synchronization.
   * <p>
   * Per-entry mutable state -- specifically each {@link OMProxyInfo}'s
   * {@code rpcAddr}, {@code dtService}, and cached {@code proxy} field,
   * which DNS-refresh-on-failure may swap -- is guarded by that
   * entry's own monitor. Callers must reach mutable per-entry state
   * only through the synchronized accessors ({@link #getAddress()},
   * {@link #getProxy()}, {@link #getDelegationTokenService()},
   * {@link #createProxyIfNeeded}, {@link #refreshAddressIfChanged}).
   */
  public static class OrderedMap<P> {
    /** A list of proxies in a particular order. */
    private final List<OMProxyInfo<P>> proxies;
    /**
     * The ordering of the nodes.
     * <p>
     * Invariant 1: Given a nodeId, let Integer i = ordering.get(nodeId);
     *              If i != null, then nodeId.equals(info.getNodeId()) == true, where info = proxies.get(i).
     *              Otherwise, i == null, then nodeId.equals(info.getNodeId()) == false for any info in proxies.
     * <p>
     * Invariant 2: Given 0 <= i < proxies.size(), let nodeId = proxies.get(i).getNodeId().
     *              Then, ordering.get(nodeId) == i.
     * <p>
     * Invariant 3: Let p = proxies.iterator() and o = ordering.entrySet().iterator().
     *              Then, p.hasNext() == o.hasNext().
     *              If p.hasNext() == true, then p.next().getNodeId().equals(o.next().getKey()).
     *
     * @see #assertInvariants()
     */
    private final Map<String, Integer> ordering;

    public OrderedMap(List<OMProxyInfo<P>> proxies) {
      this.proxies = Collections.unmodifiableList(proxies);

      final Map<String, Integer> map = new LinkedHashMap<>(); // use LinkedHashMap to preserve iteration ordering
      for (int i = 0; i < proxies.size(); i++) {
        final String nid = proxies.get(i).getNodeId();
        final Integer previous = map.put(nid, i);
        Preconditions.assertNull(previous, () -> "Duplicate nodeId " + nid + " in " + proxies);
      }
      this.ordering = Collections.unmodifiableMap(map);

      assertInvariants();
    }

    private void assertInvariants() {
      // assert Invariant 1
      for (String nodeId : getNodeIds()) {
        final Integer i = indexOf(nodeId);
        Objects.requireNonNull(i, () -> "nodeId " + nodeId + " not found");
        final OMProxyInfo<P> info = get(i);
        Objects.requireNonNull(info, () -> "info not found for index " + i);
        Preconditions.assertSame(nodeId, info.getNodeId(), "nodeId");
      }
      // assert Invariant 2
      for (int index = 0; index < proxies.size(); index++) {
        final String nodeId = getNodeId(index);
        final Integer i = ordering.get(nodeId);
        Objects.requireNonNull(i, () -> "nodeId " + nodeId + " not found");
        Preconditions.assertSame(index, i.intValue(), "index");
      }
      // assert Invariant 3
      final Iterator<OMProxyInfo<P>> p = proxies.iterator();
      final Iterator<Map.Entry<String, Integer>> o = ordering.entrySet().iterator();
      for (int i = 0; p.hasNext(); i++) {
        Preconditions.assertTrue(o.hasNext());
        final Map.Entry<String, Integer> e = o.next();
        Preconditions.assertSame(p.next().getNodeId(), e.getKey(), "nodeId");
        Preconditions.assertSame(i, e.getValue().intValue(), "index");
      }
      Preconditions.assertTrue(!o.hasNext());
      Preconditions.assertSame(proxies.size(), ordering.size(), "size");
    }

    public Set<String> getNodeIds() {
      return ordering.keySet();
    }

    List<OMProxyInfo<P>> getProxies() {
      return proxies;
    }

    int size() {
      return proxies.size();
    }

    String getNodeId(int i) {
      final OMProxyInfo<P> proxy = get(i);
      return proxy != null ? proxy.getNodeId() : null;
    }

    OMProxyInfo<P> get(int i) {
      return i >= 0 && i < proxies.size() ? proxies.get(i) : null;
    }

    public OMProxyInfo<P> get(String nodeID) {
      final Integer i = indexOf(nodeID);
      return i != null ? get(i) : null;
    }

    Integer indexOf(String nodeID) {
      return ordering.get(nodeID);
    }

    boolean contains(String nodeId, String address) {
      if (nodeId == null || address == null) {
        return false;
      }
      final OMProxyInfo<P> p = get(nodeId);
      return p != null && address.equals(p.getAddressString());
    }

    @Override
    public String toString() {
      return proxies.toString();
    }
  }
}
