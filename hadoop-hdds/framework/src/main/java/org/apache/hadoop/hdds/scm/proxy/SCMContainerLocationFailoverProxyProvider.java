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
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Failover proxy provider for SCM container location.
 */
public class SCMContainerLocationFailoverProxyProvider implements
    FailoverProxyProvider<StorageContainerLocationProtocolPB>, Closeable {
  public static final Logger LOG =
      LoggerFactory.getLogger(SCMContainerLocationFailoverProxyProvider.class);

  private Map<String, ProxyInfo<StorageContainerLocationProtocolPB>> scmProxies;
  private Map<String, SCMProxyInfo> scmProxyInfoMap;
  private List<String> scmNodeIds;

  private String currentProxySCMNodeId;
  private int currentProxyIndex;

  private final ConfigurationSource conf;
  private final SCMClientConfig scmClientConfig;
  private final long scmVersion;

  private String scmServiceId;

  private final int maxRetryCount;
  private final long retryInterval;


  public SCMContainerLocationFailoverProxyProvider(ConfigurationSource conf) {
    this.conf = conf;
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
  protected void loadConfigs() {
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
        ProxyInfo< StorageContainerLocationProtocolPB > proxy
            = new ProxyInfo<>(null, scmProxyInfo.toString());
        scmProxies.put(scmNodeId, proxy);
        scmProxyInfoMap.put(scmNodeId, scmProxyInfo);
      }
    }
  }

  @VisibleForTesting
  public synchronized String getCurrentProxyOMNodeId() {
    return currentProxySCMNodeId;
  }

  @Override
  public synchronized ProxyInfo getProxy() {
    ProxyInfo currentProxyInfo = scmProxies.get(currentProxySCMNodeId);
    createSCMProxyIfNeeded(currentProxyInfo, currentProxySCMNodeId);
    return currentProxyInfo;
  }

  @Override
  public void performFailover(
      StorageContainerLocationProtocolPB newLeader) {
    // Should do nothing here.
    LOG.debug("Failing over to next proxy. {}", getCurrentProxyOMNodeId());
  }

  public void performFailoverToAssignedLeader(String newLeader) {
    if (newLeader == null) {
      // If newLeader is not assigned, it will fail over to next proxy.
      nextProxyIndex();
    } else {
      if (!assignLeaderToNode(newLeader)) {
        LOG.debug("Failing over OM proxy to nodeId: {}", newLeader);
        nextProxyIndex();
      }
    }
  }

  @Override
  public Class<
      StorageContainerLocationProtocolPB> getInterface() {
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

  public RetryPolicy.RetryAction getRetryAction(int failovers) {
    if (failovers < maxRetryCount) {
      return new RetryPolicy.RetryAction(
          RetryPolicy.RetryAction.RetryDecision.FAILOVER_AND_RETRY,
          getRetryInterval());
    } else {
      return RetryPolicy.RetryAction.FAIL;
    }
  }

  private synchronized long getRetryInterval() {
    // TODO add exponential backup
    return retryInterval;
  }

  private synchronized int nextProxyIndex() {
//    lastAttemptedLeader = currentProxySCMNodeId;

    // round robin the next proxy
    currentProxyIndex = (currentProxyIndex + 1) % scmProxies.size();
    currentProxySCMNodeId =  scmNodeIds.get(currentProxyIndex);
    return currentProxyIndex;
  }

  synchronized boolean assignLeaderToNode(String newLeaderNodeId) {
    if (!currentProxySCMNodeId.equals(newLeaderNodeId)) {
      if (scmProxies.containsKey(newLeaderNodeId)) {
//        lastAttemptedLeader = currentProxySCMNodeId;
        currentProxySCMNodeId = newLeaderNodeId;
        currentProxyIndex = scmNodeIds.indexOf(currentProxySCMNodeId);
        return true;
      }
    }
//    } else {
//      lastAttemptedLeader = currentProxySCMNodeId;
//    }
    return false;
  }

  /**
   * Creates proxy object if it does not already exist.
   */
  private void createSCMProxyIfNeeded(ProxyInfo proxyInfo,
                                      String nodeId) {
    if (proxyInfo.proxy == null) {
      InetSocketAddress address = scmProxyInfoMap.get(nodeId).getAddress();
      try {
        StorageContainerLocationProtocolPB proxy =
            createSCMProxy(address);
        try {
          proxyInfo.proxy = proxy;
        } catch (IllegalAccessError iae) {
          scmProxies.put(nodeId,
              new ProxyInfo<>(proxy, proxyInfo.proxyInfo));
        }
      } catch (IOException ioe) {
        LOG.error("{} Failed to create RPC proxy to SCM at {}",
            this.getClass().getSimpleName(), address, ioe);
        throw new RuntimeException(ioe);
      }
    }
  }

  private StorageContainerLocationProtocolPB createSCMProxy(
      InetSocketAddress scmAddress) throws IOException {
    Configuration hadoopConf =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(conf);
    RPC.setProtocolEngine(hadoopConf, StorageContainerLocationProtocolPB.class,
        ProtobufRpcEngine.class);
    return RPC.getProxy(
        StorageContainerLocationProtocolPB.class,
        scmVersion, scmAddress, UserGroupInformation.getCurrentUser(),
        hadoopConf, NetUtils.getDefaultSocketFactory(hadoopConf),
        (int)scmClientConfig.getRpcTimeOut());
  }

  public RetryPolicy getSCMContainerLocationRetryPolicy(
      String suggestedLeader) {
    RetryPolicy retryPolicy = new RetryPolicy() {
      @Override
      public RetryAction shouldRetry(Exception e, int retry,
                                     int failover, boolean b) {
        performFailoverToAssignedLeader(suggestedLeader);
        return getRetryAction(failover);
      }
    };
    return retryPolicy;
  }
}
