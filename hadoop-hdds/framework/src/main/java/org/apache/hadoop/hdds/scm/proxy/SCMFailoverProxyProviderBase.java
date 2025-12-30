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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.ratis.ServerNotLeaderException;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;

/**
 * A failover proxy provider base abstract class.
 * Provides common methods for failover proxy provider
 * implementations. Failover proxy provider allows clients to configure
 * multiple SCMs to connect to. In case of SCM failover, client can try
 * connecting to another SCM node from the list of proxies.
 */
public abstract class SCMFailoverProxyProviderBase<T> implements FailoverProxyProvider<T> {

  private final SCMClientConfig scmClientConfig;

  private final Class<T> protocolClass;

  // scmNodeId -> ProxyInfo<rpcProxy>
  private final Map<String, ProxyInfo<T>> scmProxies;
  // scmNodeId -> SCMProxyInfo
  private final Map<String, SCMProxyInfo> scmProxyInfoMap;
  private List<String> scmNodeIds;

  // As SCM Client is shared across threads, performFailOver()
  // updates the currentProxySCMNodeId based on the updateLeaderNodeId which is
  // updated in shouldRetry(). When 2 or more threads run in parallel, the
  // RetryInvocationHandler will check the expectedFailOverCount
  // and not execute performFailOver() for one of them. So the other thread(s)
  // shall not call performFailOver(), it will call getProxy() which uses
  // currentProxySCMNodeId and returns the proxy.
  private volatile String currentProxySCMNodeId;
  private volatile int currentProxyIndex;

  private final ConfigurationSource conf;
  private final long scmVersion;

  private final int maxRetryCount;
  private final long retryInterval;

  private final UserGroupInformation ugi;

  private String updatedLeaderNodeID = null;

  /**
   * Construct SCMFailoverProxyProviderBase.
   * If userGroupInformation is not null, use the passed ugi, else obtain
   * from {@link UserGroupInformation#getCurrentUser()}
   */
  public SCMFailoverProxyProviderBase(Class<T> protocol, ConfigurationSource conf,
      UserGroupInformation userGroupInformation) {
    this.protocolClass = protocol;
    this.conf = conf;

    if (userGroupInformation == null) {
      try {
        this.ugi = UserGroupInformation.getCurrentUser();
      } catch (IOException ex) {
        getLogger().error("Unable to fetch user credentials from UGI", ex);
        throw new RuntimeException(ex);
      }
    } else {
      this.ugi = userGroupInformation;
    }
    this.scmVersion = RPC.getProtocolVersion(protocol);

    this.scmProxies = new HashMap<>();
    this.scmProxyInfoMap = new HashMap<>();
    loadConfigs();

    this.currentProxyIndex = 0;
    currentProxySCMNodeId = scmNodeIds.get(currentProxyIndex);

    scmClientConfig = conf.getObject(SCMClientConfig.class);
    this.maxRetryCount = scmClientConfig.getRetryCount();
    this.retryInterval = scmClientConfig.getRetryInterval();

    getLogger().info("Created fail-over proxy for protocol {} with {} nodes: {}", protocol.getSimpleName(),
        scmNodeIds.size(), scmProxyInfoMap.values());
  }

  /**
   * Get the logger implementation for the specific protocol's failover proxy provider.
   */
  protected abstract Logger getLogger();

  /**
   * Get the specific protocol address from {@link SCMNodeInfo}.
   * @param scmNodeInfo SCM node info which contains different protocols' address.
   * @return protocol address.
   */
  protected abstract String getProtocolAddress(SCMNodeInfo scmNodeInfo);

  /**
   * Get the SCM node ID the current proxy is pointing to.
   * This can be overridden with a single SCM node ID to disable SCM failover.
   * See {@link SingleSecretKeyProtocolProxyProvider}
   * @return current proxy's SCM Node ID.
   */
  protected synchronized String getCurrentProxySCMNodeId() {
    return currentProxySCMNodeId;
  }

  @VisibleForTesting
  protected synchronized void loadConfigs() {
    List<SCMNodeInfo> scmNodeInfoList = SCMNodeInfo.buildNodeInfo(conf);
    scmNodeIds = new ArrayList<>();


    for (SCMNodeInfo scmNodeInfo : scmNodeInfoList) {
      String protocolAddress = getProtocolAddress(scmNodeInfo);
      if (protocolAddress == null) {
        throw new ConfigurationException(protocolClass.getSimpleName() + " SCM Address could not " +
            "be obtained from config. Config is not properly defined");
      } else {
        InetSocketAddress protocolAddr = NetUtils.createSocketAddr(protocolAddress);

        String scmServiceId = scmNodeInfo.getServiceId();
        String scmNodeId = scmNodeInfo.getNodeId();
        scmNodeIds.add(scmNodeId);
        SCMProxyInfo scmProxyInfo = new SCMProxyInfo(scmServiceId, scmNodeId, protocolAddr);
        scmProxyInfoMap.put(scmNodeId, scmProxyInfo);
      }
    }
  }

  @VisibleForTesting
  public synchronized void changeCurrentProxy(String nodeId) {
    currentProxyIndex = scmNodeIds.indexOf(nodeId);
    currentProxySCMNodeId = nodeId;
    nextProxyIndex();
  }

  @Override
  public synchronized ProxyInfo<T> getProxy() {
    ProxyInfo<T> currentProxyInfo = scmProxies.get(getCurrentProxySCMNodeId());
    if (currentProxyInfo == null) {
      currentProxyInfo = createSCMProxy(getCurrentProxySCMNodeId());
    }
    return currentProxyInfo;
  }

  public synchronized List<T> getProxies() {
    for (SCMProxyInfo scmProxyInfo : scmProxyInfoMap.values()) {
      if (scmProxies.get(scmProxyInfo.getNodeId()) == null) {
        scmProxies.put(scmProxyInfo.getNodeId(),
            createSCMProxy(scmProxyInfo.getNodeId()));
      }
    }
    return scmProxies.values().stream()
        .map(proxyInfo -> proxyInfo.proxy).collect(Collectors.toList());
  }

  @Override
  public synchronized void performFailover(T newLeader) {
    if (updatedLeaderNodeID != null) {
      currentProxySCMNodeId = updatedLeaderNodeID;
    } else {
      nextProxyIndex();
    }
    getLogger().debug("Failing over to next proxy. {}", getCurrentProxySCMNodeId());
  }

  public synchronized void performFailoverToAssignedLeader(String newLeader,
                                                           Exception e) {
    ServerNotLeaderException snle =
        (ServerNotLeaderException) SCMHAUtils.getServerNotLeaderException(e);
    if (snle != null && snle.getSuggestedLeader() != null) {
      Optional<SCMProxyInfo> matchedProxyInfo =
          scmProxyInfoMap.values().stream().filter(
              proxyInfo -> NetUtils.getHostPortString(proxyInfo.getAddress())
                  .equals(snle.getSuggestedLeader())).findFirst();
      if (matchedProxyInfo.isPresent()) {
        newLeader = matchedProxyInfo.get().getNodeId();
        getLogger().debug("Performing failover to suggested leader {}, nodeId {}",
            snle.getSuggestedLeader(), newLeader);
      } else {
        getLogger().debug("Suggested leader {} does not match with any of the " +
                "proxyInfo address {}", snle.getSuggestedLeader(),
            Arrays.toString(scmProxyInfoMap.values().toArray()));
      }
    }
    assignLeaderToNode(newLeader);
  }

  @Override
  public Class<T> getInterface() {
    return protocolClass;
  }

  public List<String> getSCMNodeIds() {
    return Collections.unmodifiableList(scmNodeIds);
  }

  public Collection<SCMProxyInfo> getSCMProxyInfoList() {
    return Collections.unmodifiableCollection(scmProxyInfoMap.values());
  }

  @Override
  public synchronized void close() throws IOException {
    for (ProxyInfo<T> proxy : scmProxies.values()) {
      T scmProxy = proxy.proxy;
      if (scmProxy != null) {
        RPC.stopProxy(scmProxy);
      }
    }
  }

  private long getRetryInterval() {
    // TODO add exponential backup
    return retryInterval;
  }

  private synchronized void nextProxyIndex() {
    // round robin the next proxy
    currentProxyIndex = (currentProxyIndex + 1) % scmProxyInfoMap.size();
    currentProxySCMNodeId =  scmNodeIds.get(currentProxyIndex);
  }

  private synchronized void assignLeaderToNode(String newLeaderNodeId) {
    if (!currentProxySCMNodeId.equals(newLeaderNodeId)) {
      if (scmProxyInfoMap.containsKey(newLeaderNodeId)) {
        updatedLeaderNodeID = newLeaderNodeId;
        getLogger().debug("Updated LeaderNodeID {}", updatedLeaderNodeID);
      } else {
        updatedLeaderNodeID = null;
      }
    }
  }

  /**
   * Creates proxy object.
   */
  private ProxyInfo<T> createSCMProxy(String nodeId) {
    ProxyInfo<T> proxyInfo;
    SCMProxyInfo scmProxyInfo = scmProxyInfoMap.get(nodeId);
    InetSocketAddress address = scmProxyInfo.getAddress();
    try {
      T scmProxy = createSCMProxy(address);
      // Create proxyInfo here, to make it work with all Hadoop versions.
      proxyInfo = new ProxyInfo<>(scmProxy, scmProxyInfo.toString());
      scmProxies.put(nodeId, proxyInfo);
      return proxyInfo;
    } catch (IOException ioe) {
      getLogger().error("{} Failed to create RPC proxy to SCM at {}",
          this.getClass().getSimpleName(), address, ioe);
      throw new RuntimeException(ioe);
    }
  }

  private T createSCMProxy(InetSocketAddress scmAddress) throws IOException {
    Configuration hadoopConf =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(conf);
    RPC.setProtocolEngine(hadoopConf, protocolClass, ProtobufRpcEngine.class);
    // FailoverOnNetworkException ensures that the IPC layer does not attempt
    // retries on the same SCM in case of connection exception. This retry
    // policy essentially results in TRY_ONCE_THEN_FAIL.
    RetryPolicy connectionRetryPolicy = RetryPolicies.failoverOnNetworkException(0);
    return RPC.getProtocolProxy(
        protocolClass,
        scmVersion, scmAddress, ugi,
        hadoopConf, NetUtils.getDefaultSocketFactory(hadoopConf),
        (int)scmClientConfig.getRpcTimeOut(), connectionRetryPolicy).getProxy();
  }

  public RetryPolicy getRetryPolicy() {
    return new RetryPolicy() {
      @Override
      public RetryAction shouldRetry(Exception e, int retry,
                                     int failover, boolean b) {
        if (getLogger().isDebugEnabled()) {
          if (e.getCause() != null) {
            getLogger().debug("RetryProxy: SCM Server {}: {}: {}",
                getCurrentProxySCMNodeId(),
                e.getCause().getClass().getSimpleName(),
                e.getCause().getMessage());
          } else {
            getLogger().debug("RetryProxy: SCM {}: {}", getCurrentProxySCMNodeId(),
                e.getMessage());
          }
        }

        if (SCMHAUtils.checkRetriableWithNoFailoverException(e)) {
          setUpdatedLeaderNodeID();
        } else {
          performFailoverToAssignedLeader(null, e);
        }
        return SCMHAUtils.getRetryAction(failover, retry, e, maxRetryCount,
            getRetryInterval());
      }
    };
  }

  public synchronized void setUpdatedLeaderNodeID() {
    this.updatedLeaderNodeID = getCurrentProxySCMNodeId();
  }
}
