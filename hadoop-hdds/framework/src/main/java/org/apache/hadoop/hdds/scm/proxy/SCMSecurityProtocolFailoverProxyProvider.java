/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.proxy;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolPB;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision.FAILOVER_AND_RETRY;

/**
 * Failover proxy provider for SCMSecurityProtocol server.
 */
public class SCMSecurityProtocolFailoverProxyProvider implements
    FailoverProxyProvider<SCMSecurityProtocolPB>, Closeable {

  public static final Logger LOG =
      LoggerFactory.getLogger(SCMSecurityProtocolFailoverProxyProvider.class);

  // scmNodeId -> ProxyInfo<rpcProxy>
  private final Map<String,
      ProxyInfo<SCMSecurityProtocolPB>> scmProxies;

  // scmNodeId -> SCMProxyInfo
  private final Map<String, SCMProxyInfo> scmProxyInfoMap;

  private List<String> scmNodeIds;

  private String currentProxySCMNodeId;
  private int currentProxyIndex;

  private final ConfigurationSource conf;
  private final SCMClientConfig scmClientConfig;
  private final long scmVersion;

  private String scmServiceId;

  private final int maxRetryCount;
  private final long retryInterval;

  private final UserGroupInformation ugi;

  /**
   * Construct fail-over proxy provider for SCMSecurityProtocol Server.
   * @param conf
   * @param userGroupInformation
   */
  public SCMSecurityProtocolFailoverProxyProvider(ConfigurationSource conf,
      UserGroupInformation userGroupInformation) {
    Preconditions.checkNotNull(userGroupInformation);
    this.ugi = userGroupInformation;
    this.conf = conf;
    this.scmVersion = RPC.getProtocolVersion(SCMSecurityProtocolPB.class);

    this.scmProxies = new HashMap<>();
    this.scmProxyInfoMap = new HashMap<>();
    loadConfigs();

    this.currentProxyIndex = 0;
    currentProxySCMNodeId = scmNodeIds.get(currentProxyIndex);
    scmClientConfig = conf.getObject(SCMClientConfig.class);
    this.maxRetryCount = scmClientConfig.getRetryCount();
    this.retryInterval = scmClientConfig.getRetryInterval();
  }

  protected void loadConfigs() {
    List<SCMNodeInfo> scmNodeInfoList = SCMNodeInfo.buildNodeInfo(conf);
    scmNodeIds = new ArrayList<>();

    for (SCMNodeInfo scmNodeInfo : scmNodeInfoList) {
      if (scmNodeInfo.getScmSecurityAddress() == null) {
        throw new ConfigurationException("SCM Client Address could not " +
            "be obtained from config. Config is not properly defined");
      } else {
        InetSocketAddress scmSecurityAddress =
            NetUtils.createSocketAddr(scmNodeInfo.getScmSecurityAddress());

        scmServiceId = scmNodeInfo.getServiceId();
        String scmNodeId = scmNodeInfo.getNodeId();

        scmNodeIds.add(scmNodeId);
        SCMProxyInfo scmProxyInfo = new SCMProxyInfo(scmServiceId, scmNodeId,
            scmSecurityAddress);
        scmProxyInfoMap.put(scmNodeId, scmProxyInfo);
      }
    }
  }

  @Override
  public synchronized ProxyInfo<SCMSecurityProtocolPB> getProxy() {
    ProxyInfo currentProxyInfo = scmProxies.get(getCurrentProxySCMNodeId());
    if (currentProxyInfo == null) {
      currentProxyInfo = createSCMProxy(getCurrentProxySCMNodeId());
    }
    return currentProxyInfo;
  }

  /**
   * Creates proxy object.
   */
  private ProxyInfo createSCMProxy(String nodeId) {
    ProxyInfo proxyInfo;
    SCMProxyInfo scmProxyInfo = scmProxyInfoMap.get(nodeId);
    InetSocketAddress address = scmProxyInfo.getAddress();
    try {
      SCMSecurityProtocolPB scmProxy = createSCMProxy(address);
      // Create proxyInfo here, to make it work with all Hadoop versions.
      proxyInfo = new ProxyInfo<>(scmProxy, scmProxyInfo.toString());
      scmProxies.put(nodeId, proxyInfo);
      return proxyInfo;
    } catch (IOException ioe) {
      LOG.error("{} Failed to create RPC proxy to SCM at {}",
          this.getClass().getSimpleName(), address, ioe);
      throw new RuntimeException(ioe);
    }
  }

  private SCMSecurityProtocolPB createSCMProxy(InetSocketAddress scmAddress)
      throws IOException {
    Configuration hadoopConf =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(conf);
    RPC.setProtocolEngine(hadoopConf, SCMSecurityProtocolPB.class,
        ProtobufRpcEngine.class);

    // FailoverOnNetworkException ensures that the IPC layer does not attempt
    // retries on the same SCM in case of connection exception. This retry
    // policy essentially results in TRY_ONCE_THEN_FAIL.

    RetryPolicy connectionRetryPolicy = RetryPolicies
        .failoverOnNetworkException(0);

    return RPC.getProtocolProxy(SCMSecurityProtocolPB.class,
        scmVersion, scmAddress, ugi,
        hadoopConf, NetUtils.getDefaultSocketFactory(hadoopConf),
        (int)scmClientConfig.getRpcTimeOut(), connectionRetryPolicy).getProxy();
  }


  @Override
  public void performFailover(SCMSecurityProtocolPB currentProxy) {
    if (LOG.isDebugEnabled()) {
      int currentIndex = getCurrentProxyIndex();
      LOG.debug("Failing over SCM Security proxy to index: {}, nodeId: {}",
          currentIndex, scmNodeIds.get(currentIndex));
    }
  }

  /**
   * Performs fail-over to the next proxy.
   */
  public void performFailoverToNextProxy() {
    int newProxyIndex = incrementProxyIndex();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Incrementing SCM Security proxy index to {}, nodeId: {}",
          newProxyIndex, scmNodeIds.get(newProxyIndex));
    }
  }

  /**
   * Update the proxy index to the next proxy in the list.
   * @return the new proxy index
   */
  private synchronized int incrementProxyIndex() {
    currentProxyIndex = (currentProxyIndex + 1) % scmProxies.size();
    currentProxySCMNodeId = scmNodeIds.get(currentProxyIndex);
    return currentProxyIndex;
  }

  public RetryPolicy getRetryPolicy() {
    // Client will attempt up to maxFailovers number of failovers between
    // available SCMs before throwing exception.
    RetryPolicy retryPolicy = new RetryPolicy() {
      @Override
      public RetryAction shouldRetry(Exception exception, int retries,
          int failovers, boolean isIdempotentOrAtMostOnce)
          throws Exception {

        if (LOG.isDebugEnabled()) {
          if (exception.getCause() != null) {
            LOG.debug("RetryProxy: SCM Security Server {}: {}: {}",
                getCurrentProxySCMNodeId(),
                exception.getCause().getClass().getSimpleName(),
                exception.getCause().getMessage());
          } else {
            LOG.debug("RetryProxy: SCM {}: {}", getCurrentProxySCMNodeId(),
                exception.getMessage());
          }
        }

        // For AccessControl Exception where Client is not authentica
        if (HAUtils.isAccessControlException(exception)) {
          return RetryAction.FAIL;
        }

        // Perform fail over to next proxy, as right now we don't have any
        // suggested leader ID from server, we fail over to next one.
        // TODO: Act based on server response if leader id is passed.
        performFailoverToNextProxy();
        return getRetryAction(FAILOVER_AND_RETRY, failovers);
      }

      private RetryAction getRetryAction(RetryDecision fallbackAction,
          int failovers) {
        if (failovers < maxRetryCount) {
          return new RetryAction(fallbackAction, getRetryInterval());
        } else {
          return RetryAction.FAIL;
        }
      }
    };

    return retryPolicy;
  }


  @Override
  public Class< SCMSecurityProtocolPB > getInterface() {
    return SCMSecurityProtocolPB.class;
  }

  @Override
  public void close() throws IOException {
    for (Map.Entry<String, ProxyInfo<SCMSecurityProtocolPB>> proxy :
        scmProxies.entrySet()) {
      if (proxy.getValue() != null) {
        RPC.stopProxy(proxy.getValue());
      }
      scmProxies.remove(proxy.getKey());
    }
  }

  public synchronized String getCurrentProxySCMNodeId() {
    return currentProxySCMNodeId;
  }

  public synchronized int getCurrentProxyIndex() {
    return currentProxyIndex;
  }

  private long getRetryInterval() {
    return retryInterval;
  }
}
