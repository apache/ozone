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

import static java.util.Collections.unmodifiableList;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmRpcRetryCount;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmRpcRetryInterval;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmRpcTimeOutInMilliseconds;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.management.ObjectName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io_.retry.RetryPolicies;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.protocolPB.ReconDatanodeProtocolPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SCMConnectionManager - Acts as a class that manages the membership
 * information of the SCMs that we are working with.
 */
public class SCMConnectionManager
    implements Closeable, SCMConnectionManagerMXBean {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMConnectionManager.class);

  private final ReadWriteLock mapLock;
  private final Map<InetSocketAddress, EndpointStateMachine> scmMachines;

  private final int rpcTimeout;
  private final ConfigurationSource conf;
  private ObjectName jmxBean;

  public SCMConnectionManager(ConfigurationSource conf) {
    this.mapLock = new ReentrantReadWriteLock();
    Long timeOut = getScmRpcTimeOutInMilliseconds(conf);
    this.rpcTimeout = timeOut.intValue();
    this.scmMachines = new HashMap<>();
    this.conf = conf;
    jmxBean = MBeans.register("HddsDatanode",
        "SCMConnectionManager",
        this);
  }

  /**
   * Returns Config.
   *
   * @return ozoneConfig.
   */
  public ConfigurationSource getConf() {
    return conf;
  }

  /**
   * Get RpcTimeout.
   *
   * @return - Return RPC timeout.
   */
  public int getRpcTimeout() {
    return rpcTimeout;
  }

  /**
   * Takes a read lock.
   */
  public void readLock() {
    this.mapLock.readLock().lock();
  }

  /**
   * Releases the read lock.
   */
  public void readUnlock() {
    this.mapLock.readLock().unlock();
  }

  /**
   * Takes the write lock.
   */
  public void writeLock() {
    this.mapLock.writeLock().lock();
  }

  /**
   * Releases the write lock.
   */
  public void writeUnlock() {
    this.mapLock.writeLock().unlock();
  }

  /**
   * adds a new SCM machine to the target set.
   *
   * @param address - Address of the SCM machine to send heartbeat to.
   * @throws IOException
   */
  public void addSCMServer(InetSocketAddress address,
      String threadNamePrefix) throws IOException {
    addSCMServer(address, null, threadNamePrefix);
  }

  /**
   * Adds a new SCM machine and remembers the original host:port string
   * so the DN can re-resolve DNS on connection failure (e.g. after the
   * SCM peer is rescheduled to a new pod IP in Kubernetes).
   *
   * @param address     resolved RPC address used to build the proxy
   * @param hostAndPort original "host:port" string, or null to disable
   *                    DNS re-resolution for this endpoint
   * @param threadNamePrefix prefix for the endpoint's task thread
   */
  public void addSCMServer(InetSocketAddress address, String hostAndPort,
      String threadNamePrefix) throws IOException {
    writeLock();
    try {
      if (scmMachines.containsKey(address)) {
        LOG.warn("Trying to add an existing SCM Machine to Machines group. " +
            "Ignoring the request.");
        return;
      }
      EndpointStateMachine endPoint =
          buildScmEndpoint(address, hostAndPort, threadNamePrefix);
      scmMachines.put(address, endPoint);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Build (but do NOT register) a fresh active-SCM endpoint bound to
   * {@code address}. The caller is responsible for registering it in
   * {@code scmMachines} (and tearing down any previous endpoint it
   * replaces). Factored out of {@link #addSCMServer} so
   * {@link #refreshSCMServer} can construct the replacement BEFORE
   * removing the stale entry, preserving the peer in {@code scmMachines}
   * if proxy construction throws (transient DNS failure, peer not yet
   * accepting on the new IP, etc.).
   */
  @VisibleForTesting
  EndpointStateMachine buildScmEndpoint(InetSocketAddress address,
      String hostAndPort, String threadNamePrefix) throws IOException {
    Configuration hadoopConfig =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(this.conf);
    RPC.setProtocolEngine(
        hadoopConfig,
        StorageContainerDatanodeProtocolPB.class,
        ProtobufRpcEngine.class);
    long version =
        RPC.getProtocolVersion(StorageContainerDatanodeProtocolPB.class);

    RetryPolicy retryPolicy =
        RetryPolicies.retryUpToMaximumCountWithFixedSleep(
            getScmRpcRetryCount(conf), getScmRpcRetryInterval(conf),
            TimeUnit.MILLISECONDS);

    StorageContainerDatanodeProtocolPB rpcProxy = RPC.getProtocolProxy(
        StorageContainerDatanodeProtocolPB.class, version,
        address, UserGroupInformation.getCurrentUser(), hadoopConfig,
        NetUtils.getDefaultSocketFactory(hadoopConfig), getRpcTimeout(),
        retryPolicy).getProxy();

    StorageContainerDatanodeProtocolClientSideTranslatorPB rpcClient =
        new StorageContainerDatanodeProtocolClientSideTranslatorPB(
            rpcProxy);

    EndpointStateMachine endPoint = new EndpointStateMachine(address,
        hostAndPort, rpcClient, this.conf, threadNamePrefix);
    endPoint.setPassive(false);
    return endPoint;
  }

  /**
   * Re-resolve the SCM hostname for the endpoint at {@code oldAddress} and,
   * if the resolved IP has changed, atomically replace the endpoint with a
   * fresh one bound to the new address.
   * <p>
   * Returns the new {@link InetSocketAddress} on a successful swap, or null
   * if no swap occurred (endpoint not found, hostname not preserved at
   * construction, IP unchanged, or DNS lookup failed). Callers receive
   * enough information to update any external maps keyed by the old
   * address.
   * <p>
   * The replacement is built via the existing {@link #addSCMServer} path,
   * so the new endpoint starts in {@code GETVERSION} state and re-walks
   * version → register → heartbeat. This is the correct behavior: a peer
   * that has been rescheduled is effectively a fresh process.
   */
  public InetSocketAddress refreshSCMServer(InetSocketAddress oldAddress,
      String threadNamePrefix) throws IOException {
    // PHASE A (read lock): snapshot the endpoint reference and the
    // preserved hostAndPort string. We cannot hold any lock across
    // the DNS lookup that follows, so we capture only what we need
    // and release the lock immediately.
    final EndpointStateMachine snapshotEndpoint;
    final String hostAndPort;
    readLock();
    try {
      snapshotEndpoint = scmMachines.get(oldAddress);
      if (snapshotEndpoint == null) {
        return null;
      }
      // Recon endpoints (added via addReconServer) speak a different
      // protocol than active SCM endpoints. The current refresh path
      // only knows how to rebuild SCM endpoints, so refusing to
      // refresh a passive endpoint avoids silently downgrading a
      // Recon endpoint to an SCM-protocol one. Recon's cached IP is
      // also a much narrower problem in practice (Recon is rarely
      // pod-rescheduled the way SCM-HA peers are).
      if (snapshotEndpoint.isPassive()) {
        return null;
      }
      hostAndPort = snapshotEndpoint.getHostAndPort();
      if (hostAndPort == null) {
        return null;
      }
    } finally {
      readUnlock();
    }

    // PHASE B (no lock): perform the DNS lookup. NetUtils.createSocketAddr
    // can block on a slow / dead resolver. Holding any lock across this
    // would stall every concurrent reader and writer of the connection
    // manager -- including parallel heartbeat dispatch.
    InetSocketAddress resolved = snapshotEndpoint.resolveLatestAddress();
    if (resolved == null) {
      return null;
    }

    // PHASE C (write lock): re-check that the snapshot is still current
    // (another refresh / removeSCMServer may have raced ahead while we
    // were resolving), enforce the collision invariant, build the
    // replacement, and commit the swap atomically. Capture the stale
    // endpoint reference for teardown OUTSIDE the lock.
    final EndpointStateMachine staleEndpoint;
    final InetSocketAddress refreshed;
    writeLock();
    try {
      EndpointStateMachine current = scmMachines.get(oldAddress);
      if (current != snapshotEndpoint) {
        // Lost the race: someone else removed or replaced this entry
        // while we were resolving DNS. Abandon the swap; whatever
        // raced ahead is now responsible for the peer's state.
        return null;
      }
      // Refuse the swap if the freshly-resolved address collides with
      // another already-registered SCM peer key (e.g. transient kube-dns
      // returning peer-B's IP for peer-A's hostname). Without this guard,
      // the put below would silently overwrite peer-B's
      // EndpointStateMachine, leaking its executor and orphaning its
      // task thread, while peer-A's task ends up dialing peer-B's IP
      // with peer-A's host context. Leave the stale endpoint in place;
      // the next heartbeat retries DNS.
      if (!resolved.equals(oldAddress)
          && scmMachines.containsKey(resolved)) {
        LOG.warn("DNS re-resolution: refused to swap endpoint {} -> {} "
            + "because the new address collides with an already-registered "
            + "SCM peer. Leaving stale endpoint in place.",
            oldAddress, resolved);
        return null;
      }
      // Build the replacement BEFORE removing the stale entry so a
      // failure to construct the new proxy (transient DNS, peer not
      // yet accepting on the new IP, NetUtils refusing the address)
      // leaves the existing endpoint registered. Otherwise the peer
      // would disappear from scmMachines entirely and the next
      // heartbeat cycle would have nothing to dial -- much worse than
      // the pre-PR behaviour of dialing the stale IP.
      //
      // buildScmEndpoint also invokes RPC.getProtocolProxy under the
      // write lock; this is intentional. Proxy construction is
      // synchronous-but-not-network-blocking (it builds an invocation
      // handler; the actual socket connect is lazy on first call).
      // The cost of running it under writeLock is bounded; the cost
      // of dropping the lock here is a complex three-phase commit
      // dance to defend against a second concurrent refresh racing
      // ahead. The trade-off favours holding the lock for this step.
      EndpointStateMachine replacement;
      try {
        replacement = buildScmEndpoint(resolved, hostAndPort,
            threadNamePrefix);
      } catch (IOException buildEx) {
        LOG.warn("DNS re-resolution: failed to build replacement SCM "
                + "endpoint for {} -> {} (host {}); leaving stale endpoint "
                + "in place. Cause: {}", oldAddress, resolved, hostAndPort,
            buildEx.getMessage());
        throw buildEx;
      }
      scmMachines.put(resolved, replacement);
      if (!resolved.equals(oldAddress)) {
        scmMachines.remove(oldAddress);
      }
      staleEndpoint = snapshotEndpoint;
      refreshed = resolved;
    } finally {
      writeUnlock();
    }

    // PHASE D (no lock): best-effort teardown of the stale endpoint.
    // close() blocks on RPC.stopProxy / socket teardown -- holding
    // writeLock during that would stall every concurrent reader
    // (other endpoints' heartbeats calling getValues()) and writer.
    // close() failures don't affect registration (already committed),
    // so a RuntimeException here only impacts cleanup.
    try {
      staleEndpoint.close();
    } catch (RuntimeException closeEx) {
      LOG.warn("Failed to close stale endpoint {}", oldAddress, closeEx);
    }
    LOG.info("DNS re-resolution: SCM endpoint {} -> {} (host {}).",
        oldAddress, refreshed, hostAndPort);
    return refreshed;
  }

  /**
   * Adds a new Recon server to the set of endpoints.
   *
   * @param address Recon address.
   * @throws IOException
   */
  public void addReconServer(InetSocketAddress address,
      String threadNamePrefix) throws IOException {
    LOG.info("Adding Recon Server : {}", address.toString());
    writeLock();
    try {
      if (scmMachines.containsKey(address)) {
        LOG.warn("Trying to add an existing SCM Machine to Machines group. " +
            "Ignoring the request.");
        return;
      }
      Configuration hadoopConfig =
          LegacyHadoopConfigurationSource.asHadoopConfiguration(this.conf);
      RPC.setProtocolEngine(hadoopConfig, ReconDatanodeProtocolPB.class,
          ProtobufRpcEngine.class);
      long version =
          RPC.getProtocolVersion(ReconDatanodeProtocolPB.class);

      RetryPolicy retryPolicy =
          RetryPolicies.retryUpToMaximumCountWithFixedSleep(
              getScmRpcRetryCount(conf), getScmRpcRetryInterval(conf),
              TimeUnit.MILLISECONDS);
      ReconDatanodeProtocolPB rpcProxy = RPC.getProtocolProxy(
          ReconDatanodeProtocolPB.class, version,
          address, UserGroupInformation.getCurrentUser(), hadoopConfig,
          NetUtils.getDefaultSocketFactory(hadoopConfig), getRpcTimeout(),
          retryPolicy).getProxy();

      StorageContainerDatanodeProtocolClientSideTranslatorPB rpcClient =
          new StorageContainerDatanodeProtocolClientSideTranslatorPB(rpcProxy);

      EndpointStateMachine endPoint =
          new EndpointStateMachine(address, rpcClient, conf, threadNamePrefix);
      endPoint.setPassive(true);
      scmMachines.put(address, endPoint);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Removes a  SCM machine for the target set.
   *
   * @param address - Address of the SCM machine to send heartbeat to.
   * @throws IOException
   */
  public void removeSCMServer(InetSocketAddress address) throws IOException {
    writeLock();
    try {
      EndpointStateMachine endPoint = scmMachines.remove(address);
      if (endPoint == null) {
        LOG.warn("Trying to remove a non-existent SCM machine. " +
            "Ignoring the request.");
        return;
      }
      // This is a normal reconfiguration removal. Do not set the endpoint to
      // SHUTDOWN, as an in-flight task may report that state as a DN fatal
      // shutdown.
      endPoint.close();
    } finally {
      writeUnlock();
    }
  }

  /**
   * Returns all known RPCEndpoints.
   *
   * @return - List of RPC Endpoints.
   */
  public Collection<EndpointStateMachine> getValues() {
    readLock();
    try {
      return unmodifiableList(new ArrayList<>(scmMachines.values()));
    } finally {
      readUnlock();
    }
  }

  @Override
  public void close() throws IOException {
    getValues().forEach(endpointStateMachine
        -> IOUtils.cleanupWithLogger(LOG, endpointStateMachine));
    if (jmxBean != null) {
      MBeans.unregister(jmxBean);
      jmxBean = null;
    }
  }

  @Override
  public List<EndpointStateMachineMBean> getSCMServers() {
    readLock();
    try {
      return unmodifiableList(new ArrayList<>(scmMachines.values()));
    } finally {
      readUnlock();
    }
  }

  /**
   * @return the number of connections (both SCM and Recon)
   */
  public int getNumOfConnections() {
    readLock();
    try {
      return scmMachines.size();
    } finally {
      readUnlock();
    }
  }
}
