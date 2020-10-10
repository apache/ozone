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
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.hadoop.hdds.HddsUtils.getHostName;
import static org.apache.hadoop.hdds.HddsUtils.getPortNumberFromConfigKeys;
import static org.apache.hadoop.hdds.HddsUtils.getScmAddressForClients;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY;

/**
 * Failover proxy provider for SCM container location.
 */
public class SCMContainerLocationFailoverProxyProvider implements
    FailoverProxyProvider<StorageContainerLocationProtocolPB>, Closeable {
  public static final Logger LOG =
      LoggerFactory.getLogger(StorageContainerLocationProtocolPB.class);

  private Map<String, ProxyInfo<StorageContainerLocationProtocolPB>> scmProxies;
  private Map<String, SCMProxyInfo> scmProxyInfoMap;
  private List<String> scmNodeIDList;

  private String currentProxySCMNodeId;
  private int currentProxyIndex;

  private final ConfigurationSource conf;
  private final SCMClientConfig scmClientConfig;
  private final long scmVersion;

  private final String scmServiceId;

  private String lastAttemptedLeader;

  private final int maxRetryCount;
  private final long retryInterval;

  public static final String SCM_DUMMY_NODEID_PREFIX = "scm";

  public SCMContainerLocationFailoverProxyProvider(ConfigurationSource conf) {
    this.conf = conf;
    this.scmVersion = RPC.getProtocolVersion(
        StorageContainerLocationProtocolPB.class);
    this.scmServiceId = conf.getTrimmed(OZONE_SCM_SERVICE_IDS_KEY);
    this.scmProxies = new HashMap<>();
    this.scmProxyInfoMap = new HashMap<>();
    this.scmNodeIDList = new ArrayList<>();
    loadConfigs();

    this.currentProxyIndex = 0;
    currentProxySCMNodeId = scmNodeIDList.get(currentProxyIndex);
    scmClientConfig = conf.getObject(SCMClientConfig.class);
    this.maxRetryCount = scmClientConfig.getRetryCount();
    this.retryInterval = scmClientConfig.getRetryInterval();
  }

  @VisibleForTesting
  protected Collection<InetSocketAddress> getSCMAddressList() {
    Collection<String> scmAddressList =
        conf.getTrimmedStringCollection(OZONE_SCM_NAMES);
    Collection<InetSocketAddress> resultList = new ArrayList<>();
    if (!scmAddressList.isEmpty()) {
      final int port = getPortNumberFromConfigKeys(conf,
          ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY)
          .orElse(ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT);
      for (String scmAddress : scmAddressList) {
        LOG.info("SCM Address for proxy is {}", scmAddress);

        Optional<String> hostname = getHostName(scmAddress);
        if (hostname.isPresent()) {
          resultList.add(NetUtils.createSocketAddr(
              hostname.get() + ":" + port));
        }
      }
    }
    if (resultList.isEmpty()) {
      // fall back
      resultList.add(getScmAddressForClients(conf));
    }
    return resultList;
  }

  private void loadConfigs() {
    Collection<InetSocketAddress> scmAddressList = getSCMAddressList();
    int scmNodeIndex = 1;
    for (InetSocketAddress scmAddress : scmAddressList) {
      String nodeId = SCM_DUMMY_NODEID_PREFIX + scmNodeIndex;
      if (scmAddress == null) {
        LOG.error("Failed to create SCM proxy for {}.", nodeId);
        continue;
      }
      scmNodeIndex++;
      SCMProxyInfo scmProxyInfo = new SCMProxyInfo(
          scmServiceId, nodeId, scmAddress);
      ProxyInfo<StorageContainerLocationProtocolPB> proxy
          = new ProxyInfo<>(null, scmProxyInfo.toString());
      scmProxies.put(nodeId, proxy);
      scmProxyInfoMap.put(nodeId, scmProxyInfo);
      scmNodeIDList.add(nodeId);
    }

    if (scmProxies.isEmpty()) {
      throw new IllegalArgumentException("Could not find any configured " +
          "addresses for SCM. Please configure the system with "
          + OZONE_SCM_NAMES);
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
    lastAttemptedLeader = currentProxySCMNodeId;

    // round robin the next proxy
    currentProxyIndex = (currentProxyIndex + 1) % scmProxies.size();
    currentProxySCMNodeId =  scmNodeIDList.get(currentProxyIndex);
    return currentProxyIndex;
  }

  synchronized boolean assignLeaderToNode(String newLeaderNodeId) {
    if (!currentProxySCMNodeId.equals(newLeaderNodeId)) {
      if (scmProxies.containsKey(newLeaderNodeId)) {
        lastAttemptedLeader = currentProxySCMNodeId;
        currentProxySCMNodeId = newLeaderNodeId;
        currentProxyIndex = scmNodeIDList.indexOf(currentProxySCMNodeId);
        return true;
      }
    } else {
      lastAttemptedLeader = currentProxySCMNodeId;
    }
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
    RPC.setProtocolEngine(hadoopConf, StorageContainerLocationProtocol.class,
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
