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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc_.Client.ConnectionId;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.RpcInvocationHandler;
import org.apache.hadoop.ipc_.RpcNoSuchProtocolException;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.exceptions.OMReadException;
import org.apache.hadoop.ozone.om.exceptions.OMReadIndexException;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
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
  private final HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> failoverProxy;

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

  public HadoopRpcOMFollowerReadFailoverProxyProvider(
      HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> failoverProxy) {
    this.failoverProxy = failoverProxy;
    // Create a wrapped proxy containing all the proxies. Since this combined
    // proxy is just redirecting to other proxies, all invocations can share it.
    final String combinedInfo = "[" + failoverProxy.getOMProxies().stream()
        .map(a -> a.proxyInfo)
        .reduce((a, b) -> a + ", " + b).orElse("") + "]";
    OzoneManagerProtocolPB wrappedProxy = (OzoneManagerProtocolPB) Proxy.newProxyInstance(
        FollowerReadInvocationHandler.class.getClassLoader(),
        new Class<?>[] {OzoneManagerProtocolPB.class}, new FollowerReadInvocationHandler());
    combinedProxy = new ProxyInfo<>(wrappedProxy, combinedInfo);
    this.useFollowerRead = true;
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
    // Since FollowerReadInvocationHandler might user or fallback to leader-based failover logic,
    // we should delegate the failover logic to the leader's failover.
    failoverProxy.performFailover(currProxy);
  }

  public RetryPolicy getRetryPolicy(int maxFailovers) {
    // We use the OMFailoverProxyProviderBase's RetryPolicy instead of using our own retry policy
    // for a few reasons
    // 1. We want to ensure that the retry policy behavior remains the same when we use the leader proxy
    //    (when follower read is disabled or using write request)
    // 2. The FollowerInvocationHandler is also written so that the thrown exception is handled by the
    //    OMFailoverProxyProviderbase's RetryPolicy
    return failoverProxy.getRetryPolicy(maxFailovers);
  }

  /**
   * Parse the OM request from the request args.
   *
   * @return parsed OM request.
   */
  private static OMRequest parseOMRequest(Object[] args) throws ServiceException {
    String error = null;
    if (args == null) {
      error = "args == null";
    } else if (args.length < 2) {
      error = "args.length == " + args.length + " < 2";
    } else if (args[1] == null) {
      error = "args[1] == null";
    } else if (!(args[1] instanceof OMRequest)) {
      error = "Non-OMRequest: " + args[1].getClass();
    }
    if (error != null) {
      // Throws a non-retriable exception to prevent retry and failover
      // See the HddsUtils#shouldNotFailoverOnRpcException used in
      // OMFailoverProxyProviderBase#shouldFailover
      throwServiceException(new RpcNoSuchProtocolException("Failed to parseOMRequest: " + error));
    }
    return (OMRequest) args[1];
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
    OMProxyInfo<OzoneManagerProtocolPB> currentProxy = failoverProxy.getOMProxyMap().get(currentIndex);
    if (currentProxy != initial) {
      // Must have been a concurrent modification; ignore the move request
      return currentProxy;
    }
    final OMProxyInfo.OrderedMap<OzoneManagerProtocolPB> omProxies = failoverProxy.getOMProxyMap();
    currentIndex = (currentIndex + 1) % omProxies.size();
    final String currentOmNodeId = omProxies.getNodeId(currentIndex);
    currentProxy = failoverProxy.createOMProxyIfNeeded(currentOmNodeId);
    LOG.debug("Changed current proxy from {} to {}",
        initial == null ? "none" : initial.proxyInfo,
        currentProxy.proxyInfo);
    return currentProxy;
  }

  /**
   * An InvocationHandler to handle incoming requests. This class's invoke
   * method contains the primary logic for redirecting to followers.
   * <p>
   * If follower reads are enabled, attempt to send read operations to the
   * current proxy which can be either a leader or follower. If the current
   * proxy's OM node fails, adjust the current proxy and return on the next one.
   * <p>
   * Write requests are always forwarded to the leader.
   */
  private class FollowerReadInvocationHandler implements RpcInvocationHandler {

    @Override
    public Object invoke(Object proxy, final Method method, final Object[] args)
        throws Throwable {
      lastProxy = null;
      if (method.getDeclaringClass() == Object.class) {
        // If the method is not a OzoneManagerProtocolPB method (e.g. Object#toString()),
        // we should invoke the method on the current proxy
        return method.invoke(this, args);
      }
      OMRequest omRequest = parseOMRequest(args);
      if (useFollowerRead && OmUtils.shouldSendToFollower(omRequest)) {
        int failedCount = 0;
        for (int i = 0; useFollowerRead && i < failoverProxy.getOMProxyMap().size(); i++) {
          OMProxyInfo<OzoneManagerProtocolPB> current = getCurrentProxy();
          LOG.debug("Attempting to service {} with cmdType {} using proxy {}",
              method.getName(), omRequest.getCmdType(), current.proxyInfo);
          try {
            final Object retVal = method.invoke(current.getProxy(), args);
            lastProxy = current;
            LOG.debug("Invocation of {} with cmdType {} using {} was successful",
                method.getName(), omRequest.getCmdType(), current.proxyInfo);
            return retVal;
          } catch (InvocationTargetException ite) {
            LOG.debug("Invocation of {} with cmdType {} using proxy {} failed", method.getName(),
                omRequest.getCmdType(), current.proxyInfo, ite);
            if (!(ite.getCause() instanceof Exception)) {
              throwServiceException(ite.getCause());
            }
            Exception e = (Exception) ite.getCause();
            if (e instanceof InterruptedIOException ||
                e instanceof InterruptedException) {
              // If interrupted, do not retry.
              LOG.warn("Invocation returned interrupted exception on [{}];",
                  current.proxyInfo, e);
              throwServiceException(e);
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
                throw e;
              }

              OMReadIndexException readIndexException = getReadIndexException(e);
              if (readIndexException != null) {
                // This should trigger failover in the following shouldFailover
                LOG.debug("Encountered OMReadIndexException from {}. ", current.proxyInfo);
              }

              OMReadException readException = getReadException(e);
              if (readException != null) {
                // This should trigger failover in the following shouldFailover
                LOG.debug("Encountered OMReadException from {}. ", current.proxyInfo);
              }
            }

            if (!failoverProxy.shouldFailover(e)) {
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
              throw e;
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
          LOG.warn("{} nodes have failed for read request {} with cmdType {}."
                  + " Falling back to leader.", failedCount,
              omRequest.getCmdType(), method.getName());
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
      LOG.debug("Using leader-based failoverProxy to service {}", method.getName());
      final OMProxyInfo<OzoneManagerProtocolPB> leaderProxy = failoverProxy.getProxy();
      Object retVal = null;
      try {
        retVal = method.invoke(leaderProxy.getProxy(), args);
      } catch (InvocationTargetException e) {
        LOG.debug("Exception thrown from leader-based failoverProxy", e.getCause());
        // This exception will be handled by the OMFailoverProxyProviderBase#getRetryPolicy
        // (see getRetryPolicy). This ensures that the leader-only failover should still work.
        throwServiceException(e.getCause());
      }
      lastProxy = leaderProxy;
      return retVal;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public ConnectionId getConnectionId() {
      return RPC.getConnectionIdForProxy(useFollowerRead
          ? getCurrentProxy().proxy : failoverProxy.getProxy().getProxy());
    }
  }

  @Override
  public synchronized void close() throws IOException {
    // All the proxies are stored in the underlying failoverProxy
    // so we invoke close on the underlying failoverProxy
    failoverProxy.close();
  }

  @VisibleForTesting
  public boolean isUseFollowerRead() {
    return useFollowerRead;
  }

  @VisibleForTesting
  public List<OMProxyInfo<OzoneManagerProtocolPB>> getOMProxies() {
    return failoverProxy.getOMProxies();
  }

  public synchronized void changeInitialProxyForTest(String initialOmNodeId) {
    final OMProxyInfo<OzoneManagerProtocolPB> currentProxy = failoverProxy.getOMProxyMap().get(currentIndex);
    if (currentProxy != null && currentProxy.getNodeId().equals(initialOmNodeId)) {
      return;
    }

    int indexOfTargetNodeId = failoverProxy.getOMProxyMap().indexOf(initialOmNodeId);
    if (indexOfTargetNodeId == -1) {
      return;
    }

    currentIndex = indexOfTargetNodeId;
    failoverProxy.createOMProxyIfNeeded(initialOmNodeId);
  }

  /**
   * Throw the passed {@link Throwable} wrapped in {@link ServiceException}.
   * This is required to prevent {@link java.lang.reflect.UndeclaredThrowableException} to be thrown
   * since {@link OzoneManagerProtocolPB#submitRequest(RpcController, OMRequest)} only
   * throws {@link ServiceException}.
   * @param e exception to wrap in {@link ServiceException}.
   * @throws ServiceException the exception that wraps the passed throwable.
   */
  private static void throwServiceException(Throwable e) throws ServiceException {
    throw e instanceof ServiceException ? (ServiceException) e : new ServiceException(e);
  }

}
