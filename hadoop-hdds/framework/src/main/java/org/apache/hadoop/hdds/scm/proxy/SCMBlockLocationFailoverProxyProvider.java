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
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolPB;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction;
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

import static org.apache.hadoop.ozone.OzoneConsts.SCM_DUMMY_SERVICE_ID;

/**
 * Failover proxy provider for SCM block location.
 */
public class SCMBlockLocationFailoverProxyProvider implements
    FailoverProxyProvider<ScmBlockLocationProtocolPB>, Closeable {
  public static final Logger LOG =
      LoggerFactory.getLogger(SCMBlockLocationFailoverProxyProvider.class);

  private Map<String, ProxyInfo<ScmBlockLocationProtocolPB>> scmProxies;
  private Map<String, SCMProxyInfo> scmProxyInfoMap;
  private List<String> scmNodeIds;

  private String currentProxySCMNodeId;
  private int currentProxyIndex;

  private final ConfigurationSource conf;
  private final long scmVersion;

  private String scmServiceId;

  private String lastAttemptedLeader;

  private final int maxRetryCount;
  private final long retryInterval;


  public SCMBlockLocationFailoverProxyProvider(ConfigurationSource conf) {
    this.conf = conf;
    this.scmVersion = RPC.getProtocolVersion(ScmBlockLocationProtocolPB.class);

    // Set some constant for non-HA.
    if (scmServiceId == null) {
      scmServiceId = SCM_DUMMY_SERVICE_ID;
    }
    this.scmProxies = new HashMap<>();
    this.scmProxyInfoMap = new HashMap<>();

    loadConfigs();

    this.currentProxyIndex = 0;
    currentProxySCMNodeId = scmNodeIds.get(currentProxyIndex);

    SCMClientConfig config = conf.getObject(SCMClientConfig.class);
    this.maxRetryCount = config.getRetryCount();
    this.retryInterval = config.getRetryInterval();
  }

  private void loadConfigs() {

    scmNodeIds = new ArrayList<>();
    List<SCMNodeInfo> scmNodeInfoList = SCMNodeInfo.buildNodeInfo(conf);

    for (SCMNodeInfo scmNodeInfo : scmNodeInfoList) {
      if (scmNodeInfo.getBlockClientAddress() == null) {
        throw new ConfigurationException("SCM BlockClient Address could not " +
            "be obtained from config. Config is not properly defined");
      } else {
        InetSocketAddress scmBlockClientAddress =
            NetUtils.createSocketAddr(scmNodeInfo.getBlockClientAddress());

        scmServiceId = scmNodeInfo.getServiceId();
        String scmNodeId = scmNodeInfo.getNodeId();
        scmNodeIds.add(scmNodeId);
        SCMProxyInfo scmProxyInfo = new SCMProxyInfo(
            scmNodeInfo.getServiceId(), scmNodeInfo.getNodeId(),
            scmBlockClientAddress);
        ProxyInfo<ScmBlockLocationProtocolPB> proxy
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
  public void performFailover(ScmBlockLocationProtocolPB newLeader) {
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
  public Class<ScmBlockLocationProtocolPB> getInterface() {
    return ScmBlockLocationProtocolPB.class;
  }

  @Override
  public synchronized void close() throws IOException {
    for (ProxyInfo<ScmBlockLocationProtocolPB> proxy : scmProxies.values()) {
      ScmBlockLocationProtocolPB scmProxy = proxy.proxy;
      if (scmProxy != null) {
        RPC.stopProxy(scmProxy);
      }
    }
  }

  public RetryAction getRetryAction(int failovers) {
    if (failovers < maxRetryCount) {
      return new RetryAction(RetryAction.RetryDecision.FAILOVER_AND_RETRY,
          getRetryInterval());
    } else {
      return RetryAction.FAIL;
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
    currentProxySCMNodeId =  scmNodeIds.get(currentProxyIndex);
    return currentProxyIndex;
  }

  private synchronized boolean assignLeaderToNode(String newLeaderNodeId) {
    if (!currentProxySCMNodeId.equals(newLeaderNodeId)) {
      if (scmProxies.containsKey(newLeaderNodeId)) {
        lastAttemptedLeader = currentProxySCMNodeId;
        currentProxySCMNodeId = newLeaderNodeId;
        currentProxyIndex = scmNodeIds.indexOf(currentProxySCMNodeId);
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
        ScmBlockLocationProtocolPB proxy = createSCMProxy(address);
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

  private ScmBlockLocationProtocolPB createSCMProxy(
      InetSocketAddress scmAddress) throws IOException {
    Configuration hadoopConf =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(conf);
    RPC.setProtocolEngine(hadoopConf, ScmBlockLocationProtocolPB.class,
        ProtobufRpcEngine.class);
    return RPC.getProxy(ScmBlockLocationProtocolPB.class, scmVersion,
        scmAddress, UserGroupInformation.getCurrentUser(), hadoopConf,
        NetUtils.getDefaultSocketFactory(hadoopConf),
        (int)conf.getObject(SCMClientConfig.class).getRpcTimeOut());
  }

  public RetryPolicy getSCMBlockLocationRetryPolicy(String newLeader) {
    RetryPolicy retryPolicy = new RetryPolicy() {
      @Override
      public RetryAction shouldRetry(Exception e, int retry,
                                     int failover, boolean b) {
        performFailoverToAssignedLeader(newLeader);
        return getRetryAction(failover);
      }
    };
    return retryPolicy;
  }
}

