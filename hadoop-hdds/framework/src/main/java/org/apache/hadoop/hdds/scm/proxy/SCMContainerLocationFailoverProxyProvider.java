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

package org.apache.hadoop.hdds.scm.proxy;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.ratis.ServerNotLeaderException;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Failover proxy provider for StorageContainerLocationProtocolPB.
 */
public class SCMContainerLocationFailoverProxyProvider implements
    FailoverProxyProvider<StorageContainerLocationProtocolPB>, Closeable {
  public static final Logger LOG =
      LoggerFactory.getLogger(SCMContainerLocationFailoverProxyProvider.class);

  // scmNodeId -> ProxyInfo<rpcProxy>
  private final Map<String,
      ProxyInfo<StorageContainerLocationProtocolPB>> scmProxies;
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
  private final SCMClientConfig scmClientConfig;
  private final long scmVersion;

  private String scmServiceId;

  private final int maxRetryCount;
  private final long retryInterval;

  private final UserGroupInformation ugi;

  private String updatedLeaderNodeID = null;

  /**
   * Construct SCMContainerLocationFailoverProxyProvider.
   * If userGroupInformation is not null, use the passed ugi, else obtain
   * from {@link UserGroupInformation#getCurrentUser()}
   * @param conf
   * @param userGroupInformation
   */
  public SCMContainerLocationFailoverProxyProvider(ConfigurationSource conf,
      UserGroupInformation userGroupInformation) {
    this.conf = conf;

    if (userGroupInformation == null) {
      try {
        this.ugi = UserGroupInformation.getCurrentUser();
      } catch (IOException ex) {
        LOG.error("Unable to fetch user credentials from UGI", ex);
        throw new RuntimeException(ex);
      }
    } else {
      this.ugi = userGroupInformation;
    }
    this.scmVersion = RPC.getProtocolVersion(
        StorageContainerLocationProtocolPB.class);

    this.scmProxies = new HashMap<>();
    this.scmProxyInfoMap = new HashMap<>();
    loadConfigs();

    this.currentProxyIndex = 0;
    currentProxySCMNodeId = scmNodeIds.get(currentProxyIndex);
    scmClientConfig = conf.getObject(SCMClientConfig.class);
    this.maxRetryCount = scmClientConfig.getRetryCount();
    this.retryInterval = scmClientConfig.getRetryInterval();
  }

  @VisibleForTesting
  protected synchronized void loadConfigs() {
    List<SCMNodeInfo> scmNodeInfoList = SCMNodeInfo.buildNodeInfo(conf);

    scmNodeIds = new ArrayList<>();

    for (SCMNodeInfo scmNodeInfo : scmNodeInfoList) {
      if (scmNodeInfo.getScmClientAddress() == null) {
        throw new ConfigurationException("SCM Client Address could not " +
            "be obtained from config. Config is not properly defined");
      } else {
        InetSocketAddress scmClientAddress =
            NetUtils.createSocketAddr(scmNodeInfo.getScmClientAddress());

        scmServiceId = scmNodeInfo.getServiceId();
        String scmNodeId = scmNodeInfo.getNodeId();

        scmNodeIds.add(scmNodeId);
        SCMProxyInfo scmProxyInfo = new SCMProxyInfo(scmServiceId, scmNodeId,
            scmClientAddress);
        scmProxyInfoMap.put(scmNodeId, scmProxyInfo);
      }
    }
  }

  @VisibleForTesting
  public synchronized String getCurrentProxySCMNodeId() {
    return currentProxySCMNodeId;
  }

  @VisibleForTesting
  public synchronized void changeCurrentProxy(String nodeId) {
    currentProxyIndex = scmNodeIds.indexOf(nodeId);
    currentProxySCMNodeId = nodeId;
    nextProxyIndex();
  }

  @Override
  public synchronized ProxyInfo<StorageContainerLocationProtocolPB> getProxy() {
    ProxyInfo currentProxyInfo = scmProxies.get(getCurrentProxySCMNodeId());
    if (currentProxyInfo == null) {
      currentProxyInfo = createSCMProxy(getCurrentProxySCMNodeId());
    }
    return currentProxyInfo;
  }

  public synchronized List<StorageContainerLocationProtocolPB> getProxies() {
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
  public synchronized void performFailover(
      StorageContainerLocationProtocolPB newLeader) {
    if (updatedLeaderNodeID != null) {
      currentProxySCMNodeId = updatedLeaderNodeID;
    } else {
      nextProxyIndex();
    }
    LOG.debug("Failing over to next proxy. {}", getCurrentProxySCMNodeId());
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
        LOG.debug("Performing failover to suggested leader {}, nodeId {}",
            snle.getSuggestedLeader(), newLeader);
      } else {
        LOG.debug("Suggested leader {} does not match with any of the " +
                "proxyInfo adress {}", snle.getSuggestedLeader(),
            Arrays.toString(scmProxyInfoMap.values().toArray()));
      }
    }
    assignLeaderToNode(newLeader);
  }

  @Override
  public Class<StorageContainerLocationProtocolPB> getInterface() {
    return StorageContainerLocationProtocolPB.class;
  }

  @Override
  public synchronized void close() throws IOException {
    for (ProxyInfo<StorageContainerLocationProtocolPB>
        proxy : scmProxies.values()) {
      StorageContainerLocationProtocolPB scmProxy =
          proxy.proxy;
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
        LOG.debug("Updated LeaderNodeID {}", updatedLeaderNodeID);
      } else {
        updatedLeaderNodeID = null;
      }
    }
  }

  /**
   * Creates proxy object.
   */
  private ProxyInfo createSCMProxy(String nodeId) {
    ProxyInfo proxyInfo;
    SCMProxyInfo scmProxyInfo = scmProxyInfoMap.get(nodeId);
    InetSocketAddress address = scmProxyInfo.getAddress();
    try {
      StorageContainerLocationProtocolPB scmProxy = createSCMProxy(address);
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


  private StorageContainerLocationProtocolPB createSCMProxy(
      InetSocketAddress scmAddress) throws IOException {
    Configuration hadoopConf =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(conf);
    RPC.setProtocolEngine(hadoopConf, StorageContainerLocationProtocolPB.class,
        ProtobufRpcEngine.class);
    // FailoverOnNetworkException ensures that the IPC layer does not attempt
    // retries on the same OM in case of connection exception. This retry
    // policy essentially results in TRY_ONCE_THEN_FAIL.
    RetryPolicy connectionRetryPolicy = RetryPolicies
        .failoverOnNetworkException(0);
    return RPC.getProtocolProxy(
        StorageContainerLocationProtocolPB.class,
        scmVersion, scmAddress, ugi,
        hadoopConf, NetUtils.getDefaultSocketFactory(hadoopConf),
        (int)scmClientConfig.getRpcTimeOut(), connectionRetryPolicy).getProxy();
  }

  public RetryPolicy getRetryPolicy() {
    return new RetryPolicy() {
      @Override
      public RetryAction shouldRetry(Exception e, int retry,
                                     int failover, boolean b) {
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
