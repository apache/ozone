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

import static org.apache.hadoop.ozone.om.ha.OMFailoverProxyProviderBase.getLeaderNotReadyException;
import static org.apache.hadoop.ozone.om.ha.OMFailoverProxyProviderBase.getNotLeaderException;
import static org.apache.hadoop.ozone.om.ha.OMFailoverProxyProviderBase.getReadException;
import static org.apache.hadoop.ozone.om.ha.OMFailoverProxyProviderBase.getReadIndexException;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io_.retry.RetryProxy;
import org.apache.hadoop.ipc_.Client.ConnectionId;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.RpcNoSuchProtocolException;
import org.apache.hadoop.ipc_.RpcProxy;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.helpers.ReadConsistency;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ReadConsistencyHint;
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link org.apache.hadoop.io.retry.FailoverProxyProvider} implementation
 * that supports reading from follower OM(s) (i.e. non-leader OMs also includes
 * OM listeners).
 * <p>
 * This constructs a wrapper proxy might send the read request to follower
 * OM(s), if follower read is enabled. It will try to send read requests
 * to the first OM node. If RPC failed, it will try to failover to the next OM node.
 * It will fail back to the leader OM after it has exhausted all the OMs.
 * TODO: Currently the logic does not prioritize forwarding to followers since
 *  it requires an extra RPC latency to check the OM role info.
 *  In the future, we can try to try to pick the followers before forwarding
 *  the request to the leader (similar to ObserverReadProxyProvider).
 * <p>
 * Read and write requests will still be sent to leader OM if reading from
 * follower is disabled.
 */
public class HadoopRpcOMFollowerReadFailoverProxyProvider implements FailoverProxyProvider<OzoneManagerProtocolPB> {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopRpcOMFollowerReadFailoverProxyProvider.class);

  /** The inner proxy provider used for leader-based failover. */
  private final HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> leaderProxy;

  /** The combined proxy which redirects to other proxies as necessary. */
  private final ProxyInfo<OzoneManagerProtocolPB> combinedProxy;

  /**
   * Whether reading from follower is enabled. If this is false, all read
   * requests will still go to OM leader.
   */
  private volatile boolean useFollowerRead;

  /**
   * The current index of the underlying leader-based proxy provider's omNodesInOrder currently being used.
   * Should only be accessed in synchronized methods.
   */
  private int currentIndex = -1;

  /** The last proxy that has been used. Only used for testing. */
  private volatile OMProxyInfo<OzoneManagerProtocolPB> lastProxy = null;

  /** The read consistency hint used when follower read is enabled. */
  private final ReadConsistencyHint followerReadConsistency;
  /** The read consistency hint used when follower read is disabled or when follower read fails. */
  private final ReadConsistencyHint leaderReadConsistency;

  public HadoopRpcOMFollowerReadFailoverProxyProvider(
      HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> leaderProxy
  ) {
    this(leaderProxy, ReadConsistency.LINEARIZABLE_ALLOW_FOLLOWER, ReadConsistency.DEFAULT, true);
  }

  public HadoopRpcOMFollowerReadFailoverProxyProvider(
      HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> leaderProxy,
      ReadConsistency followerReadConsistencyType,
      ReadConsistency leaderReadConsistencyType,
      boolean useFollowerRead) {
    Preconditions.assertTrue(followerReadConsistencyType.allowFollowerRead(),
        "Invalid follower read consistency " + followerReadConsistencyType);
    Preconditions.assertTrue(!leaderReadConsistencyType.allowFollowerRead(),
        "Invalid leader read consistency " + leaderReadConsistencyType);
    this.leaderProxy = leaderProxy;
    // Create a wrapped proxy containing all the proxies. Since this combined
    // proxy is just redirecting to other proxies, all invocations can share it.
    final String combinedInfo = "[" + leaderProxy.getOMProxies().stream()
        .map(a -> a.proxyInfo)
        .reduce((a, b) -> a + ", " + b).orElse("") + "]";
    combinedProxy = new ProxyInfo<>(new FollowerReadProxy(), combinedInfo);
    this.useFollowerRead = useFollowerRead;
    this.followerReadConsistency = followerReadConsistencyType.getHint();
    this.leaderReadConsistency = leaderReadConsistencyType.getHint();
  }

  @Override
  public Class<OzoneManagerProtocolPB> getInterface() {
    return OzoneManagerProtocolPB.class;
  }

  @Override
  public ProxyInfo<OzoneManagerProtocolPB> getProxy() {
    return combinedProxy;
  }

  @Override
  public void performFailover(OzoneManagerProtocolPB currProxy) {
    // Since FollowerReadProxy might use or fall back to leader-based failover logic,
    // we should delegate the failover logic to the leader's failover.
    leaderProxy.performFailover(currProxy);
  }

  public RetryPolicy getRetryPolicy(int maxFailovers) {
    // We use the OMFailoverProxyProviderBase's RetryPolicy instead of using our own retry policy
    // for a few reasons
    // 1. We want to ensure that the retry policy behavior remains the same when we use the leader proxy
    //    (when follower read is disabled or using write request)
    // 2. The FollowerReadProxy is also written so that the thrown exception is handled by the
    //    OMFailoverProxyProviderbase's RetryPolicy
    return leaderProxy.getRetryPolicy(maxFailovers);
  }

  /**
   * Create a client that applies the default consistency hint once, before entering the retry loop.
   */
  public OzoneManagerProtocolPB newProxy(int maxFailovers) {
    OzoneManagerProtocolPB retryProxy = (OzoneManagerProtocolPB) RetryProxy.create(
        OzoneManagerProtocolPB.class, this, getRetryPolicy(maxFailovers));
    return new ReadConsistencyProxy(retryProxy);
  }

  private OMRequest applyReadConsistency(OMRequest request) throws ServiceException {
    if (request == null) {
      // Keep invalid requests non-retriable, including calls made directly to the routing proxy.
      throw new ServiceException(new RpcNoSuchProtocolException("OMRequest == null"));
    }
    if (!request.hasReadConsistencyHint()) {
      ReadConsistencyHint hint = useFollowerRead && OmUtils.shouldSendToFollower(request)
          ? followerReadConsistency : leaderReadConsistency;
      if (hint != null) {
        return request.toBuilder().setReadConsistencyHint(hint).build();
      }
    }
    return request;
  }

  private class ReadConsistencyProxy implements OzoneManagerProtocolPB, RpcProxy {
    private final OzoneManagerProtocolPB retryProxy;

    ReadConsistencyProxy(OzoneManagerProtocolPB retryProxy) {
      this.retryProxy = retryProxy;
    }

    @Override
    public OMResponse submitRequest(RpcController controller, OMRequest request) throws ServiceException {
      return retryProxy.submitRequest(controller, applyReadConsistency(request));
    }

    @Override
    public ConnectionId getConnectionId() {
      return RPC.getConnectionIdForProxy(retryProxy);
    }

    @Override
    public void close() throws IOException {
      HadoopRpcOMFollowerReadFailoverProxyProvider.this.close();
    }
  }

  @VisibleForTesting
  public ProxyInfo<OzoneManagerProtocolPB> getLastProxy() {
    return lastProxy;
  }

  /**
   * Return the currently used proxy. If there is none, first calls
   * {@link #changeProxy(OMProxyInfo)} to initialize one.
   */
  @VisibleForTesting
  public OMProxyInfo<OzoneManagerProtocolPB> getCurrentProxy() {
    return changeProxy(null);
  }

  /**
   * Move to the next proxy in the proxy list. If the OMProxyInfo supplied by
   * the caller does not match the current proxy, the call is ignored; this is
   * to handle concurrent calls (to avoid changing the proxy multiple times).
   * The service state of the newly selected proxy will be updated before
   * returning.
   *
   * @param initial The expected current proxy
   * @return The new proxy that should be used.
   */
  private synchronized OMProxyInfo<OzoneManagerProtocolPB> changeProxy(OMProxyInfo<OzoneManagerProtocolPB> initial) {
    OMProxyInfo<OzoneManagerProtocolPB> currentProxy = leaderProxy.getOMProxyMap().get(currentIndex);
    if (currentProxy != initial) {
      // Must have been a concurrent modification; ignore the move request
      return currentProxy;
    }
    final OMProxyInfo.OrderedMap<OzoneManagerProtocolPB> omProxies = leaderProxy.getOMProxyMap();
    currentIndex = (currentIndex + 1) % omProxies.size();
    final String currentOmNodeId = omProxies.getNodeId(currentIndex);
    currentProxy = leaderProxy.createOMProxyIfNeeded(currentOmNodeId);
    LOG.debug("Changed current proxy from {} to {}",
        initial == null ? "none" : initial.proxyInfo,
        currentProxy.proxyInfo);
    return currentProxy;
  }

  /**
   * A protocol implementation that redirects incoming requests to followers.
   * <p>
   * If follower reads are enabled, attempt to send read operations to the
   * current proxy which can be either a leader or follower. If the current
   * proxy's OM node fails, adjust the current proxy and return on the next one.
   * <p>
   * Write requests are always forwarded to the leader.
   */
  private class FollowerReadProxy implements OzoneManagerProtocolPB, RpcProxy {

    @Override
    public OMResponse submitRequest(RpcController controller, OMRequest omRequest) throws ServiceException {
      lastProxy = null;
      omRequest = applyReadConsistency(omRequest);
      boolean isFollowerReadEligible = useFollowerRead && OmUtils.shouldSendToFollower(omRequest);

      if (isFollowerReadEligible) {
        int failedCount = 0;
        for (int i = 0; useFollowerRead && i < leaderProxy.getOMProxyMap().size(); i++) {
          OMProxyInfo<OzoneManagerProtocolPB> current = getCurrentProxy();
          LOG.debug("Attempting to service submitRequest with cmdType {} using proxy {}",
              omRequest.getCmdType(), current.proxyInfo);
          try {
            final OMResponse response = current.getProxy().submitRequest(controller, omRequest);
            lastProxy = current;
            LOG.debug("Invocation of submitRequest with cmdType {} using {} was successful",
                omRequest.getCmdType(), current.proxyInfo);
            return response;
          } catch (Throwable failure) {
            LOG.debug("Invocation of submitRequest with cmdType {} using proxy {} failed",
                omRequest.getCmdType(), current.proxyInfo, failure);
            if (!(failure instanceof Exception)) {
              throw toServiceException(failure);
            }
            Exception e = (Exception) failure;
            if (e instanceof InterruptedIOException ||
                e instanceof InterruptedException) {
              // If interrupted, do not retry.
              LOG.warn("Invocation returned interrupted exception on [{}];",
                  current.proxyInfo, e);
              throw toServiceException(e);
            }

            if (e instanceof ServiceException) {
              OMNotLeaderException notLeaderException =
                  getNotLeaderException(e);
              if (notLeaderException != null) {
                // We should disable follower read here since this means
                // the OM follower does not support / disable follower read or something is misconfigured
                LOG.debug("Encountered OMNotLeaderException from {}. " +
                    "Disable OM follower read and retry OM leader directly.", current.proxyInfo);
                useFollowerRead = false;
                // Break here instead of throwing exception so that it is not counted
                // as a failover
                break;
              }

              OMLeaderNotReadyException leaderNotReadyException =
                  getLeaderNotReadyException(e);
              if (leaderNotReadyException != null) {
                LOG.debug("Encountered OMLeaderNotReadyException from {}. " +
                    "Directly throw the exception to trigger retry", current.proxyInfo);
                // Throw here to trigger retry since we already communicate to the leader
                // If we break here instead, we will retry the same leader again without waiting
                throw toServiceException(e);
              }

              ReadIndexException readIndexException = getReadIndexException(e);
              if (readIndexException != null) {
                // This should trigger failover in the following shouldFailover
                LOG.debug("Encountered ReadIndexException from {}. ", current.proxyInfo);
              }

              ReadException readException = getReadException(e);
              if (readException != null) {
                // This should trigger failover in the following shouldFailover
                LOG.debug("Encountered ReadException from {}. ", current.proxyInfo);
              }
            }

            if (!leaderProxy.shouldFailover(e)) {
              // We reuse the leader proxy provider failover since we want to ensure
              // if the follower read proxy decides that the exception should be failed,
              // the leader proxy provider failover retry policy (i.e. OMFailoverProxyProviderBase#getRetryPolicy)
              // should also fail the call.
              // Otherwise, if the follower read proxy decides the exception should be failed, but
              // the leader decides to failover to the its next proxy, the follower read proxy remains
              // unchanged and the next read calls might query the same failing OM node and
              // fail indefinitely.
              LOG.debug("Invocation with cmdType {} returned exception on [{}] that cannot be retried; " +
                      "{} failure(s) so far",
                  omRequest.getCmdType(), current.proxyInfo, failedCount, e);
              throw toServiceException(e);
            } else {
              failedCount++;
              LOG.warn(
                  "Invocation with cmdType {} returned exception on [{}]; {} failure(s) so far",
                  omRequest.getCmdType(), current.proxyInfo, failedCount, e);
              changeProxy(current);
            }
          }
        }

        // Only log message if there are actual follower failures.
        // Getting here with failedCount = 0 could
        // be that there is simply no Follower node running at all.
        if (failedCount > 0) {
          // If we get here, it means all followers have failed.
          LOG.warn("{} nodes have failed for submitRequest with cmdType {}."
                  + " Falling back to leader.", failedCount,
              omRequest.getCmdType());
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Read falling back to leader without follower read "
                + "fail, is there no follower node running?");
          }
        }
      }

      // Either all followers have failed, follower reads are disabled,
      // or this is a write request. In any case, forward the request to
      // the leader OM.
      LOG.debug("Using leader-based failoverProxy to service submitRequest");
      final OMProxyInfo<OzoneManagerProtocolPB> currentLeaderProxy = leaderProxy.getProxy();
      try {
        OMResponse response = currentLeaderProxy.getProxy().submitRequest(controller, omRequest);
        lastProxy = currentLeaderProxy;
        return response;
      } catch (Throwable e) {
        LOG.debug("Exception thrown from leader-based failoverProxy", e);
        // This exception will be handled by the OMFailoverProxyProviderBase#getRetryPolicy
        // (see getRetryPolicy). This ensures that the leader-only failover should still work.
        throw toServiceException(e);
      }
    }

    @Override
    public void close() throws IOException {
      // The provider owns the underlying OM proxies.
    }

    @Override
    public ConnectionId getConnectionId() {
      // Read the proxy through the synchronized accessor instead of the
      // inherited public field. With DNS-refresh-on-failure, OMProxyInfo
      // mutates the proxy field under its monitor, so a direct
      // unsynchronized field read can return a stale reference long
      // after the refresh has installed the replacement (no happens-
      // before edge between the writer's swap and an unsynchronized
      // reader). Reference reads are atomic per JLS so this is a
      // visibility hazard, not a tearing one -- but the outcome is the
      // same: a stale proxy whose underlying connection has been
      // stopped is dialed instead of the live replacement.
      return RPC.getConnectionIdForProxy(useFollowerRead
          ? getCurrentProxy().getProxy() : leaderProxy.getProxy().getProxy());
    }
  }

  @Override
  public synchronized void close() throws IOException {
    // All the proxies are stored in the underlying leaderProxy
    // so we invoke close on the underlying leaderProxy
    leaderProxy.close();
  }

  @VisibleForTesting
  public boolean isUseFollowerRead() {
    return useFollowerRead;
  }

  @VisibleForTesting
  public List<OMProxyInfo<OzoneManagerProtocolPB>> getOMProxies() {
    return leaderProxy.getOMProxies();
  }

  public synchronized void changeInitialProxyForTest(String initialOmNodeId) {
    final OMProxyInfo<OzoneManagerProtocolPB> currentProxy = leaderProxy.getOMProxyMap().get(currentIndex);
    if (currentProxy != null && currentProxy.getNodeId().equals(initialOmNodeId)) {
      return;
    }

    Integer indexOfTargetNodeId = leaderProxy.getOMProxyMap().indexOf(initialOmNodeId);
    if (indexOfTargetNodeId == null) {
      return;
    }

    currentIndex = indexOfTargetNodeId;
    leaderProxy.createOMProxyIfNeeded(initialOmNodeId);
  }

  /**
   * Preserve ServiceException instances and wrap other failures in the protocol's declared exception type.
   */
  private static ServiceException toServiceException(Throwable e) {
    return e instanceof ServiceException ? (ServiceException) e : new ServiceException(e);
  }

}
