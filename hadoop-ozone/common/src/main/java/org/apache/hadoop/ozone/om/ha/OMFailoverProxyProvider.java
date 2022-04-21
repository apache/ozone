/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;

import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;

/**
 * A failover proxy provider implementation which allows clients to configure
 * multiple OMs to connect to. In case of OM failover, client can try
 * connecting to another OM node from the list of proxies.
 */
public class OMFailoverProxyProvider<T> implements
    FailoverProxyProvider<T>, Closeable {

  public static final Logger LOG =
      LoggerFactory.getLogger(OMFailoverProxyProvider.class);

  private final String omServiceId;
  private final ConfigurationSource conf;
  private final Class<T> protocolClass;
  private final long omVersion;
  private final UserGroupInformation ugi;
  private final Text delegationTokenService;

  // Map of OMNodeID to its proxy
  private Map<String, ProxyInfo<T>> omProxies;
  private Map<String, OMProxyInfo> omProxyInfos;
  private List<String> omNodeIDList;

  private String nextProxyOMNodeId;
  private String currentProxyOMNodeId;
  private int nextProxyIndex;
  private int currentProxyIndex;

  private List<String> retryExceptions = new ArrayList<>();

  // OMFailoverProxyProvider, on encountering certain exception, tries each OM
  // once in a round robin fashion. After that it waits for configured time
  // before attempting to contact all the OMs again. For other exceptions
  // such as LeaderNotReadyException, the same OM is contacted again with a
  // linearly increasing wait time.
  private Set<String> attemptedOMs = new HashSet<>();
  private String lastAttemptedOM;
  private int numAttemptsOnSameOM = 0;
  private final long waitBetweenRetries;
  private Set<String> accessControlExceptionOMs = new HashSet<>();
  private boolean performFailoverDone;

  public OMFailoverProxyProvider(ConfigurationSource configuration,
      UserGroupInformation ugi, String omServiceId, Class<T> protocol)
      throws IOException {
    this.conf = configuration;
    this.omVersion = RPC.getProtocolVersion(protocol);
    this.ugi = ugi;
    this.omServiceId = omServiceId;
    this.protocolClass = protocol;
    this.performFailoverDone = true;
    loadOMClientConfigs(conf, this.omServiceId);
    this.delegationTokenService = computeDelegationTokenService();

    nextProxyIndex = 0;
    nextProxyOMNodeId = omNodeIDList.get(nextProxyIndex);
    currentProxyIndex = 0;
    currentProxyOMNodeId = nextProxyOMNodeId;

    waitBetweenRetries = conf.getLong(
        OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_KEY,
        OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT);
  }

  protected void loadOMClientConfigs(ConfigurationSource config, String omSvcId)
      throws IOException {
    this.omProxies = new HashMap<>();
    this.omProxyInfos = new HashMap<>();
    this.omNodeIDList = new ArrayList<>();

    Collection<String> omNodeIds = OmUtils.getActiveOMNodeIds(config,
        omSvcId);

    for (String nodeId : OmUtils.emptyAsSingletonNull(omNodeIds)) {

      String rpcAddrKey = ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
          omSvcId, nodeId);
      String rpcAddrStr = OmUtils.getOmRpcAddress(config, rpcAddrKey);
      if (rpcAddrStr == null) {
        continue;
      }

      OMProxyInfo omProxyInfo = new OMProxyInfo(omSvcId, nodeId,
          rpcAddrStr);

      if (omProxyInfo.getAddress() != null) {


        // For a non-HA OM setup, nodeId might be null. If so, we assign it
        // the default value
        if (nodeId == null) {
          nodeId = OzoneConsts.OM_DEFAULT_NODE_ID;
        }
        // ProxyInfo will be set during first time call to server.
        omProxies.put(nodeId, null);
        omProxyInfos.put(nodeId, omProxyInfo);
        omNodeIDList.add(nodeId);
      } else {
        LOG.error("Failed to create OM proxy for {} at address {}",
            nodeId, rpcAddrStr);
      }
    }

    if (omProxies.isEmpty()) {
      throw new IllegalArgumentException("Could not find any configured " +
          "addresses for OM. Please configure the system with "
          + OZONE_OM_ADDRESS_KEY);
    }
  }

  @VisibleForTesting
  public synchronized String getCurrentProxyOMNodeId() {
    return currentProxyOMNodeId;
  }

  private T createOMProxy(InetSocketAddress omAddress) throws IOException {
    Configuration hadoopConf =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(conf);
    RPC.setProtocolEngine(hadoopConf, protocolClass, ProtobufRpcEngine.class);

    // FailoverOnNetworkException ensures that the IPC layer does not attempt
    // retries on the same OM in case of connection exception. This retry
    // policy essentially results in TRY_ONCE_THEN_FAIL.
    RetryPolicy connectionRetryPolicy = RetryPolicies
        .failoverOnNetworkException(0);

    return (T) RPC.getProtocolProxy(protocolClass, omVersion,
        omAddress, ugi, hadoopConf, NetUtils.getDefaultSocketFactory(
            hadoopConf), (int) OmUtils.getOMClientRpcTimeOut(conf),
        connectionRetryPolicy).getProxy();

  }

  /**
   * Get the proxy object which should be used until the next failover event
   * occurs. RPC proxy object is intialized lazily.
   * @return the OM proxy object to invoke methods upon
   */
  @Override
  public synchronized ProxyInfo<T> getProxy() {
    ProxyInfo currentProxyInfo = omProxies.get(currentProxyOMNodeId);
    if (currentProxyInfo == null) {
      currentProxyInfo = createOMProxy(currentProxyOMNodeId);
    }
    return currentProxyInfo;
  }

  /**
   * Creates proxy object.
   */
  protected ProxyInfo createOMProxy(String nodeId) {
    OMProxyInfo omProxyInfo = omProxyInfos.get(nodeId);
    InetSocketAddress address = omProxyInfo.getAddress();
    ProxyInfo proxyInfo;
    try {
      T proxy = createOMProxy(address);
      // Create proxyInfo here, to make it work with all Hadoop versions.
      proxyInfo = new ProxyInfo<>(proxy, omProxyInfo.toString());
      omProxies.put(nodeId, proxyInfo);
    } catch (IOException ioe) {
      LOG.error("{} Failed to create RPC proxy to OM at {}",
          this.getClass().getSimpleName(), address, ioe);
      throw new RuntimeException(ioe);
    }
    return proxyInfo;
  }

  @VisibleForTesting
  public RetryPolicy getRetryPolicy(int maxFailovers) {
    // Client will attempt upto maxFailovers number of failovers between
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
            // TODO: NotLeaderException should include the host
            //  address of the suggested leader along with the nodeID.
            //  Failing over just based on nodeID is not very robust.

            // Prepare the next OM to be tried. This will help with calculation
            // of the wait times needed get creating the retryAction.
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
              getOMProxyInfos(), maxFailovers);
          return RetryAction.FAIL;
        }
      }
    };

    return retryPolicy;
  }

  public Text getCurrentProxyDelegationToken() {
    return delegationTokenService;
  }

  protected Text computeDelegationTokenService() {
    // For HA, this will return "," separated address of all OM's.
    List<String> addresses = new ArrayList<>();

    for (Map.Entry<String, OMProxyInfo> omProxyInfoSet :
        omProxyInfos.entrySet()) {
      Text dtService = omProxyInfoSet.getValue().getDelegationTokenService();

      // During client object creation when one of the OM configured address
      // in unreachable, dtService can be null.
      if (dtService != null) {
        addresses.add(dtService.toString());
      }
    }

    if (!addresses.isEmpty()) {
      Collections.sort(addresses);
      return new Text(String.join(",", addresses));
    } else {
      // If all OM addresses are unresolvable, set dt service to null. Let
      // this fail in later step when during connection setup.
      return null;
    }
  }

  @Override
  public Class<T> getInterface() {
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
    LOG.debug("Failing over OM from {}:{} to {}:{}",
        currentProxyOMNodeId, currentProxyIndex,
        nextProxyOMNodeId, nextProxyIndex);
    currentProxyOMNodeId = nextProxyOMNodeId;
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
            newProxyIndex, omNodeIDList.get(newProxyIndex));
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
    lastAttemptedOM = nextProxyOMNodeId;
    attemptedOMs.add(nextProxyOMNodeId);

    nextProxyIndex = (nextProxyIndex + 1) % omProxies.size();
    nextProxyOMNodeId = omNodeIDList.get(nextProxyIndex);
    return nextProxyIndex;
  }

  /**
   * Failover to the OM proxy specified by the new leader OMNodeId.
   * @param newLeaderOMNodeId OMNodeId to failover to.
   * @return true if failover is successful, false otherwise.
   */
  private synchronized boolean updateLeaderOMNodeId(String newLeaderOMNodeId) {
    if (!nextProxyOMNodeId.equals(newLeaderOMNodeId)) {
      if (omProxies.containsKey(newLeaderOMNodeId)) {
        lastAttemptedOM = nextProxyOMNodeId;
        nextProxyOMNodeId = newLeaderOMNodeId;
        nextProxyIndex = omNodeIDList.indexOf(nextProxyOMNodeId);
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
    if (nextProxyOMNodeId.equals(lastAttemptedOM)) {
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
    for (String omNodeID : omProxyInfos.keySet()) {
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

  private synchronized boolean shouldFailover(Exception ex) {
    Throwable unwrappedException = HddsUtils.getUnwrappedException(ex);
    if (unwrappedException instanceof AccessControlException ||
        unwrappedException instanceof SecretManager.InvalidToken) {
      // Retry all available OMs once before failing with
      // AccessControlException.
      if (accessControlExceptionOMs.contains(nextProxyOMNodeId)) {
        accessControlExceptionOMs.clear();
        return false;
      } else {
        accessControlExceptionOMs.add(nextProxyOMNodeId);
        if (accessControlExceptionOMs.containsAll(omNodeIDList)) {
          return false;
        }
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

  /**
   * Close all the proxy objects which have been opened over the lifetime of
   * the proxy provider.
   */
  @Override
  public synchronized void close() throws IOException {
    for (ProxyInfo<T> proxyInfo : omProxies.values()) {
      if (proxyInfo != null) {
        RPC.stopProxy(proxyInfo.proxy);
      }
    }
  }

  @VisibleForTesting
  public List<ProxyInfo> getOMProxies() {
    return new ArrayList<ProxyInfo>(omProxies.values());
  }

  @VisibleForTesting
  public Map<String, ProxyInfo<T>> getOMProxyMap() {
    return omProxies;
  }

  @VisibleForTesting
  public List<OMProxyInfo> getOMProxyInfos() {
    return new ArrayList<OMProxyInfo>(omProxyInfos.values());
  }

  /**
   * Check if exception is OMLeaderNotReadyException.
   *
   * @param exception
   * @return OMLeaderNotReadyException
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
   * Check if exception is a OMNotLeaderException.
   *
   * @return OMNotLeaderException.
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

  @VisibleForTesting
  protected void setProxiesForTesting(
      Map<String, ProxyInfo<T>> testOMProxies,
      Map<String, OMProxyInfo> testOMProxyInfos,
      List<String> testOMNodeIDList) {
    this.omProxies = testOMProxies;
    this.omProxyInfos = testOMProxyInfos;
    this.omNodeIDList = testOMNodeIDList;
  }
}

