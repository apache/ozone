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
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolPB;
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

  private String scmServiceId;

  private final int maxRetryCount;
  private final long retryInterval;

  private final UserGroupInformation ugi;

  private String updatedLeaderNodeID = null;


  public SCMBlockLocationFailoverProxyProvider(ConfigurationSource conf) {
    this.conf = conf;
    this.scmVersion = RPC.getProtocolVersion(ScmBlockLocationProtocolPB.class);

    try {
      this.ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException ex) {
      LOG.error("Unable to fetch user credentials from UGI", ex);
      throw new RuntimeException(ex);
    }

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

  private synchronized void loadConfigs() {

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

  @VisibleForTesting
  public synchronized String getCurrentProxySCMNodeId() {
    return currentProxySCMNodeId;
  }

  @Override
  public synchronized ProxyInfo<ScmBlockLocationProtocolPB> getProxy() {
    String currentProxyNodeId = getCurrentProxySCMNodeId();
    ProxyInfo currentProxyInfo = scmProxies.get(currentProxyNodeId);
    if (currentProxyInfo == null) {
      currentProxyInfo = createSCMProxy(currentProxyNodeId);
    }
    return currentProxyInfo;
  }

  @Override
  public synchronized void performFailover(
      ScmBlockLocationProtocolPB newLeader) {
    //If leader node id is set, use that or else move to next proxy index.
    if (updatedLeaderNodeID != null) {
      currentProxySCMNodeId = updatedLeaderNodeID;
    } else {
      nextProxyIndex();
    }

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

  private synchronized long getRetryInterval() {
    // TODO add exponential backup
    return retryInterval;
  }

  private synchronized void nextProxyIndex() {
    // round robin the next proxy

    currentProxyIndex = (getCurrentProxyIndex() + 1) % scmProxyInfoMap.size();
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
      ScmBlockLocationProtocolPB scmProxy = createSCMProxy(address);
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

  private ScmBlockLocationProtocolPB createSCMProxy(
      InetSocketAddress scmAddress) throws IOException {
    Configuration hadoopConf =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(conf);
    RPC.setProtocolEngine(hadoopConf, ScmBlockLocationProtocolPB.class,
        ProtobufRpcEngine.class);
    // FailoverOnNetworkException ensures that the IPC layer does not attempt
    // retries on the same OM in case of connection exception. This retry
    // policy essentially results in TRY_ONCE_THEN_FAIL.
    RetryPolicy connectionRetryPolicy = RetryPolicies
        .failoverOnNetworkException(0);
    return RPC.getProtocolProxy(ScmBlockLocationProtocolPB.class, scmVersion,
        scmAddress, ugi, hadoopConf,
        NetUtils.getDefaultSocketFactory(hadoopConf),
        (int)conf.getObject(SCMClientConfig.class).getRpcTimeOut(),
        connectionRetryPolicy).getProxy();
  }

  public RetryPolicy getSCMBlockLocationRetryPolicy(String newLeader) {
    RetryPolicy retryPolicy = new RetryPolicy() {
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
    return retryPolicy;
  }

  public synchronized int getCurrentProxyIndex() {
    return currentProxyIndex;
  }

  public synchronized void setUpdatedLeaderNodeID() {
    this.updatedLeaderNodeID = getCurrentProxySCMNodeId();
  }
}

