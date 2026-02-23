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
import com.google.protobuf.ServiceException;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.exceptions.OMReadException;
import org.apache.hadoop.ozone.om.exceptions.OMReadIndexException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A failover proxy provider base abstract class.
 * Provides common methods for failover proxy provider
 * implementations. Failover proxy provider allows clients to configure
 * multiple OMs to connect to. In case of OM failover, client can try
 * connecting to another OM node from the list of proxies.
 */
public abstract class OMFailoverProxyProviderBase<T> implements
    FailoverProxyProvider<T>, Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMFailoverProxyProviderBase.class);

  private final ConfigurationSource conf;
  private final Class<T> protocolClass;

  private final OMProxyInfo.OrderedMap<T> omProxies;

  // These are used to identify the current and next OM node
  // Note that these fields need to be modified atomically (e.g. using synchronized)
  private int currentProxyIndex;
  private int nextProxyIndex;

  // OMFailoverProxyProvider, on encountering certain exception, tries each OM
  // once in a round robin fashion. After that it waits for configured time
  // before attempting to contact all the OMs again. For other exceptions
  // such as LeaderNotReadyException, the same OM is contacted again with a
  // linearly increasing wait time.
  private final Set<String> attemptedOMs = new HashSet<>();
  private String lastAttemptedOM;
  private int numAttemptsOnSameOM = 0;
  private final long waitBetweenRetries;
  private final Set<String> accessControlExceptionOMs = new HashSet<>();
  private boolean performFailoverDone;

  private final UserGroupInformation ugi;

  public OMFailoverProxyProviderBase(ConfigurationSource configuration,
                                     UserGroupInformation ugi,
                                     String omServiceId,
                                     Class<T> protocol) throws IOException {
    this.conf = configuration;
    this.protocolClass = protocol;
    this.performFailoverDone = true;
    this.ugi = ugi;

    waitBetweenRetries = conf.getLong(
        OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_KEY,
        OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT);

    this.omProxies = new OMProxyInfo.OrderedMap<>(initOmProxiesFromConfigs(conf, omServiceId));
    nextProxyIndex = 0;
    currentProxyIndex = 0;
  }

  /**
   * Initialize the OM proxies from the configuration and the OM service ID.
   *
   * @param config configuration containing OM node information
   * @param omSvcId OM service ID
   * @throws IOException if any exception occurs while trying to initialize the proxy.
   */
  protected abstract List<OMProxyInfo<T>> initOmProxiesFromConfigs(ConfigurationSource config, String omSvcId)
      throws IOException;

  /**
   * Get the protocol proxy for provided address.
   * @param omAddress An instance of {@link InetSocketAddress} which contains the address to connect
   * @return the proxy connection to the address and the set of methods supported by the server at the address
   * @throws IOException if any error occurs while trying to get the proxy
   */
  protected T createOMProxy(InetSocketAddress omAddress) throws IOException {
    Configuration hadoopConf =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(getConf());

    RPC.setProtocolEngine(hadoopConf, getInterface(), ProtobufRpcEngine.class);

    // Ensure we do not attempt retry on the same OM in case of exceptions
    RetryPolicy connectionRetryPolicy = RetryPolicies.failoverOnNetworkException(0);

    return RPC.getProtocolProxy(
        getInterface(),
        RPC.getProtocolVersion(protocolClass),
        omAddress,
        ugi,
        hadoopConf,
        NetUtils.getDefaultSocketFactory(hadoopConf),
        (int) OmUtils.getOMClientRpcTimeOut(getConf()),
        connectionRetryPolicy
    ).getProxy();
  }

  protected synchronized boolean shouldFailover(Exception ex) {
    Throwable unwrappedException = HddsUtils.getUnwrappedException(ex);
    if (unwrappedException instanceof AccessControlException ||
        unwrappedException instanceof SecretManager.InvalidToken) {
      // Retry all available OMs once before failing with
      // AccessControlException.
      final String nextProxyOMNodeId = omProxies.getNodeId(nextProxyIndex);
      if (accessControlExceptionOMs.contains(nextProxyOMNodeId)) {
        accessControlExceptionOMs.clear();
        return false;
      } else {
        accessControlExceptionOMs.add(nextProxyOMNodeId);
        return !accessControlExceptionOMs.containsAll(omProxies.getNodeIds());
      }
    } else if (HddsUtils.shouldNotFailoverOnRpcException(unwrappedException)) {
      return false;
    } else if (ex instanceof StateMachineException) {
      StateMachineException smEx = (StateMachineException) ex;
      Throwable cause = smEx.getCause();
      if (cause instanceof OMException) {
        OMException omEx = (OMException) cause;
        // Do not failover if the operation was blocked because the OM was
        // prepared.
        return omEx.getResult() !=
           OMException.ResultCodes.NOT_SUPPORTED_OPERATION_WHEN_PREPARED;
      }
    }
    return true;
  }

  @VisibleForTesting
  public synchronized String getCurrentProxyOMNodeId() {
    return omProxies.getNodeId(currentProxyIndex);
  }

  @VisibleForTesting
  public synchronized String getNextProxyOMNodeId() {
    return omProxies.getNodeId(nextProxyIndex);
  }

  @VisibleForTesting
  public RetryPolicy getRetryPolicy(int maxFailovers) {
    // Client will attempt up to maxFailovers number of failovers between
    // available OMs before throwing exception.
    RetryPolicy retryPolicy = new RetryPolicy() {
      @Override
      public RetryAction shouldRetry(Exception exception, int retries,
          int failovers, boolean isIdempotentOrAtMostOnce)
          throws Exception {

        String omNodeId = getCurrentProxyOMNodeId();

        if (LOG.isDebugEnabled()) {
          if (exception.getCause() != null) {
            LOG.debug("RetryProxy: OM {}: {}: {}", omNodeId,
                exception.getCause().getClass().getSimpleName(),
                exception.getCause().getMessage());
          } else {
            LOG.debug("RetryProxy: OM {}: {}", omNodeId,
                exception.getMessage());
          }
        }

        if (exception instanceof ServiceException) {
          OMNotLeaderException notLeaderException =
              getNotLeaderException(exception);
          if (notLeaderException != null) {
            // Prepare the next OM to be tried. This will help with calculation
            // of the wait times needed get creating the retryAction.
            String suggestedLeaderAddress =
                notLeaderException.getSuggestedLeaderAddress();
            String suggestedNodeId =
                notLeaderException.getSuggestedLeaderNodeId();
            if (suggestedLeaderAddress != null &&
                suggestedNodeId != null &&
                omProxies.contains(suggestedNodeId, suggestedLeaderAddress)) {
              setNextOmProxy(suggestedNodeId);
              return getRetryAction(RetryDecision.FAILOVER_AND_RETRY,
                  failovers);
            }

            selectNextOmProxy();
            return getRetryAction(RetryDecision.FAILOVER_AND_RETRY, failovers);
          }

          OMLeaderNotReadyException leaderNotReadyException =
              getLeaderNotReadyException(exception);
          if (leaderNotReadyException != null) {
            // Retry on same OM again as leader OM is not ready.
            // Failing over to same OM so that wait time between retries is
            // incremented
            setNextOmProxy(omNodeId);
            return getRetryAction(RetryDecision.FAILOVER_AND_RETRY, failovers);
          }
        }

        if (!shouldFailover(exception)) {
          return RetryAction.FAIL; // do not retry
        }

        // Prepare the next OM to be tried. This will help with calculation
        // of the wait times needed get creating the retryAction.
        selectNextOmProxy();
        return getRetryAction(RetryDecision.FAILOVER_AND_RETRY, failovers);
      }

      private RetryAction getRetryAction(RetryDecision fallbackAction,
          int failovers) {
        if (failovers < maxFailovers) {
          return new RetryAction(fallbackAction, getWaitTime());
        } else {
          LOG.error("Failed to connect to OMs: {}. Attempted {} failovers.",
              omProxies.getNodeIds(), maxFailovers);
          return RetryAction.FAIL;
        }
      }
    };

    return retryPolicy;
  }

  @Override
  public final Class<T> getInterface() {
    return protocolClass;
  }

  /**
   * Called whenever an error warrants failing over. It is determined by the
   * retry policy. This method is supposed to called only once in a
   * multithreaded environment. This where the failover occurs.
   * performFailOver updates the currentProxyOmNodeId
   * When 2 or more threads run in parallel, the
   * RetryInvocationHandler will check the expectedFailOverCount
   * and not execute performFailOver() for one of them. So the other thread(s)
   * shall not call performFailOver(), instead it will call getProxy().
   */
  @Override
  public synchronized void performFailover(T currentProxy) {
    if (LOG.isDebugEnabled()) {
      final String c = omProxies.getNodeId(currentProxyIndex);
      final String n = omProxies.getNodeId(nextProxyIndex);
      LOG.debug("Failing over OM from {}:{} to {}:{}", c, currentProxyIndex, n, nextProxyIndex);
    }
    currentProxyIndex = nextProxyIndex;
    performFailoverDone = true;
  }

  /**
   * Set the next leaderOMNodeId returned through OMResponse if it does
   * not match the current leaderOMNodeId cached by the proxy provider.
   */
  public void setNextOmProxy(String newLeaderOMNodeId) {
    if (newLeaderOMNodeId == null) {
      LOG.debug("No suggested leader nodeId. Performing failover to next peer" +
          " node");
      selectNextOmProxy();
    } else {
      if (updateLeaderOMNodeId(newLeaderOMNodeId)) {
        LOG.debug("Failing over OM proxy to nodeId: {}", newLeaderOMNodeId);
      }
    }
  }

  /**
   * Selects the next OM Leader to try.
   */
  public synchronized void selectNextOmProxy() {
    if (performFailoverDone) {
      performFailoverDone = false;
      int newProxyIndex = incrementNextProxyIndex();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Incrementing OM proxy index to {}, nodeId: {}",
            newProxyIndex, omProxies.getNodeId(newProxyIndex));
      }
    }
  }

  /**
   * Update the proxy index to the next proxy in the list.
   * @return the new proxy index
   */
  private synchronized int incrementNextProxyIndex() {
    // Before failing over to next proxy, add the proxy OM (which has
    // returned an exception) to the list of attemptedOMs.
    lastAttemptedOM = omProxies.getNodeId(nextProxyIndex);
    attemptedOMs.add(lastAttemptedOM);

    nextProxyIndex = (nextProxyIndex + 1) % omProxies.size();
    return nextProxyIndex;
  }

  /**
   * Failover to the OM proxy specified by the new leader OMNodeId.
   * @param newLeaderOMNodeId OMNodeId to failover to.
   * @return true if failover is successful, false otherwise.
   */
  private synchronized boolean updateLeaderOMNodeId(String newLeaderOMNodeId) {
    final String nextProxyOMNodeId = omProxies.getNodeId(nextProxyIndex);
    if (!nextProxyOMNodeId.equals(newLeaderOMNodeId)) {
      final Integer i = omProxies.indexOf(newLeaderOMNodeId);
      if (i != null) {
        lastAttemptedOM = nextProxyOMNodeId;
        nextProxyIndex = i;
        return true;
      }
    } else {
      lastAttemptedOM = nextProxyOMNodeId;
    }
    return false;
  }

  /**
   * Get the wait time based on
   * 1. Is the same OM being retried based on response from OM.
   * 2. Is this a new OM that is being used.
   * 3. Were all the OMs visited once and retries need to be resumed after a
   * delay.
   * @return delay in milliseconds
   */
  public synchronized long getWaitTime() {
    if (omProxies.getNodeId(nextProxyIndex).equals(lastAttemptedOM)) {
      // Clear attemptedOMs list as the same OM has been selected again.
      attemptedOMs.clear();

      // The same OM will be contacted again. So wait and then retry.
      numAttemptsOnSameOM++;
      return (waitBetweenRetries * numAttemptsOnSameOM);
    }
    // Reset numAttemptsOnSameOM as we failed over to a different OM.
    numAttemptsOnSameOM = 0;

    // OMs are being contacted in Round Robin way. Check if all the OMs have
    // been contacted in this attempt.
    for (String omNodeID : omProxies.getNodeIds()) {
      if (!attemptedOMs.contains(omNodeID)) {
        return 0;
      }
    }
    // This implies all the OMs have been contacted once. Return true and
    // clear the list. The OMs will be retried in a Round Robin fashion again
    // after a delay.
    attemptedOMs.clear();
    return waitBetweenRetries;
  }

  public List<OMProxyInfo<T>> getOMProxies() {
    return omProxies.getProxies();
  }

  public OMProxyInfo.OrderedMap<T> getOMProxyMap() {
    return omProxies;
  }

  /**
   * Unwrap the exception and return the wrapped OMLeaderNotReadyException if any.
   *
   * @param exception exception to unwrap.
   * @return the unwrapped OMLeaderNotReadyException or null if the wrapped
   *         exception is not OMLeaderNotReadyException.
   */
  public static OMLeaderNotReadyException getLeaderNotReadyException(
      Exception exception) {
    Throwable cause = exception.getCause();
    if (cause instanceof RemoteException) {
      IOException ioException =
          ((RemoteException) cause).unwrapRemoteException();
      if (ioException instanceof OMLeaderNotReadyException) {
        return (OMLeaderNotReadyException) ioException;
      }
    }
    return null;
  }

  /**
   * Unwrap the exception and return the wrapped OMNotLeaderException if any.
   *
   * @param exception exception to unwrap.
   * @return the unwrapped OMNotLeaderException or null if the wrapped
   *         exception is not OMNotLeaderException.
   */
  public static OMNotLeaderException getNotLeaderException(
      Exception exception) {
    Throwable cause = exception.getCause();
    if (cause instanceof RemoteException) {
      IOException ioException =
          ((RemoteException) cause).unwrapRemoteException();
      if (ioException instanceof OMNotLeaderException) {
        return (OMNotLeaderException) ioException;
      }
    }
    return null;
  }

  /**
   * Unwrap the exception and return the wrapped ReadIndexException if any.
   *
   * @param exception exception to unwrap.
   * @return the unwrapped OMReadIndexException or null if the wrapped
   *         exception is not OMReadIndexException.
   */
  public static OMReadIndexException getReadIndexException(Exception exception) {
    Throwable cause = exception.getCause();
    if (cause instanceof RemoteException) {
      IOException ioException =
          ((RemoteException) cause).unwrapRemoteException();
      if (ioException instanceof OMReadIndexException) {
        return (OMReadIndexException) ioException;
      }
    }
    return null;
  }

  /**
   * Unwrap the exception and return the wrapped ReadException if any.
   *
   * @param exception exception to unwrap.
   * @return the unwrapped OMReadException or null if the wrapped
   *         exception is not OMReadException.
   */
  public static OMReadException getReadException(Exception exception) {
    Throwable cause = exception.getCause();
    if (cause instanceof RemoteException) {
      IOException ioException =
          ((RemoteException) cause).unwrapRemoteException();
      if (ioException instanceof OMReadException) {
        return (OMReadException) ioException;
      }
    }
    return null;
  }

  protected ConfigurationSource getConf() {
    return conf;
  }
}
