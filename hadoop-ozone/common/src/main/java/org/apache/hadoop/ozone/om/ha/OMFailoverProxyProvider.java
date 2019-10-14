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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;

/**
 * A failover proxy provider implementation which allows clients to configure
 * multiple OMs to connect to. In case of OM failover, client can try
 * connecting to another OM node from the list of proxies.
 */
public class OMFailoverProxyProvider implements
    FailoverProxyProvider<OzoneManagerProtocolPB>, Closeable {

  public static final Logger LOG =
      LoggerFactory.getLogger(OMFailoverProxyProvider.class);

  // Map of OMNodeID to its proxy
  private Map<String, ProxyInfo<OzoneManagerProtocolPB>> omProxies;
  private Map<String, OMProxyInfo> omProxyInfos;
  private List<String> omNodeIDList;

  private String currentProxyOMNodeId;
  private int currentProxyIndex;

  private final Configuration conf;
  private final long omVersion;
  private final UserGroupInformation ugi;
  private final Text delegationTokenService;

  private final String omServiceId;

  public OMFailoverProxyProvider(OzoneConfiguration configuration,
      UserGroupInformation ugi, String omServiceId) throws IOException {
    this.conf = configuration;
    this.omVersion = RPC.getProtocolVersion(OzoneManagerProtocolPB.class);
    this.ugi = ugi;
    this.omServiceId = omServiceId;
    loadOMClientConfigs(conf, this.omServiceId);
    this.delegationTokenService = computeDelegationTokenService();

    currentProxyIndex = 0;
    currentProxyOMNodeId = omNodeIDList.get(currentProxyIndex);
  }

  public OMFailoverProxyProvider(OzoneConfiguration configuration,
      UserGroupInformation ugi) throws IOException {
    this(configuration, ugi, null);
  }

  private void loadOMClientConfigs(Configuration config, String omSvcId)
      throws IOException {
    this.omProxies = new HashMap<>();
    this.omProxyInfos = new HashMap<>();
    this.omNodeIDList = new ArrayList<>();

    Collection<String> omServiceIds = Collections.singletonList(omSvcId);

    for (String serviceId : OmUtils.emptyAsSingletonNull(omServiceIds)) {
      Collection<String> omNodeIds = OmUtils.getOMNodeIds(config, serviceId);

      for (String nodeId : OmUtils.emptyAsSingletonNull(omNodeIds)) {

        String rpcAddrKey = OmUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
            serviceId, nodeId);
        String rpcAddrStr = OmUtils.getOmRpcAddress(config, rpcAddrKey);
        if (rpcAddrStr == null) {
          continue;
        }

        OMProxyInfo omProxyInfo = new OMProxyInfo(nodeId, rpcAddrStr);

        if (omProxyInfo.getAddress() != null) {

          ProxyInfo<OzoneManagerProtocolPB> proxyInfo =
              new ProxyInfo(null, omProxyInfo.toString());

          // For a non-HA OM setup, nodeId might be null. If so, we assign it
          // a dummy value
          if (nodeId == null) {
            nodeId = OzoneConsts.OM_NODE_ID_DUMMY;
          }
          omProxies.put(nodeId, proxyInfo);
          omProxyInfos.put(nodeId, omProxyInfo);
          omNodeIDList.add(nodeId);
        } else {
          LOG.error("Failed to create OM proxy for {} at address {}",
              nodeId, rpcAddrStr);
        }
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

  private OzoneManagerProtocolPB createOMProxy(InetSocketAddress omAddress)
      throws IOException {
    RPC.setProtocolEngine(conf, OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);
    return RPC.getProxy(OzoneManagerProtocolPB.class, omVersion, omAddress, ugi,
        conf, NetUtils.getDefaultSocketFactory(conf),
        Client.getRpcTimeout(conf));
  }

  /**
   * Get the proxy object which should be used until the next failover event
   * occurs. RPC proxy object is intialized lazily.
   * @return the OM proxy object to invoke methods upon
   */
  @Override
  public synchronized ProxyInfo getProxy() {
    ProxyInfo currentProxyInfo = omProxies.get(currentProxyOMNodeId);
    createOMProxyIfNeeded(currentProxyInfo, currentProxyOMNodeId);
    return currentProxyInfo;
  }

  /**
   * Creates proxy object if it does not already exist.
   */
  private void createOMProxyIfNeeded(ProxyInfo proxyInfo,
      String nodeId) {
    if (proxyInfo.proxy == null) {
      InetSocketAddress address = omProxyInfos.get(nodeId).getAddress();
      try {
        proxyInfo.proxy = createOMProxy(address);
      } catch (IOException ioe) {
        LOG.error("{} Failed to create RPC proxy to OM at {}",
            this.getClass().getSimpleName(), address, ioe);
        throw new RuntimeException(ioe);
      }
    }
  }

  public Text getCurrentProxyDelegationToken() {
    return delegationTokenService;
  }

  private Text computeDelegationTokenService() {
    // For HA, this will return "," separated address of all OM's.
    StringBuilder rpcAddress = new StringBuilder();
    int count = 0;
    for (Map.Entry<String, OMProxyInfo> omProxyInfoSet :
        omProxyInfos.entrySet()) {
      count++;
      rpcAddress =
          rpcAddress.append(
              omProxyInfoSet.getValue().getDelegationTokenService());

      if (omProxyInfos.size() != count) {
        rpcAddress.append(",");
      }
    }

    return new Text(rpcAddress.toString());
  }



  /**
   * Called whenever an error warrants failing over. It is determined by the
   * retry policy.
   */
  @Override
  public void performFailover(OzoneManagerProtocolPB currentProxy) {
    int newProxyIndex = incrementProxyIndex();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Failing over OM proxy to index: {}, nodeId: {}",
          newProxyIndex, omNodeIDList.get(newProxyIndex));
    }
  }

  /**
   * Update the proxy index to the next proxy in the list.
   * @return the new proxy index
   */
  private synchronized int incrementProxyIndex() {
    currentProxyIndex = (currentProxyIndex + 1) % omProxies.size();
    currentProxyOMNodeId = omNodeIDList.get(currentProxyIndex);
    return currentProxyIndex;
  }

  @Override
  public Class<OzoneManagerProtocolPB> getInterface() {
    return OzoneManagerProtocolPB.class;
  }

  /**
   * Performs failover if the leaderOMNodeId returned through OMReponse does
   * not match the current leaderOMNodeId cached by the proxy provider.
   */
  public void performFailoverIfRequired(String newLeaderOMNodeId) {
    if (newLeaderOMNodeId == null) {
      LOG.debug("No suggested leader nodeId. Performing failover to next peer" +
          " node");
      performFailover(null);
    } else {
      if (updateLeaderOMNodeId(newLeaderOMNodeId)) {
        LOG.debug("Failing over OM proxy to nodeId: {}", newLeaderOMNodeId);
      }
    }
  }

  /**
   * Failover to the OM proxy specified by the new leader OMNodeId.
   * @param newLeaderOMNodeId OMNodeId to failover to.
   * @return true if failover is successful, false otherwise.
   */
  synchronized boolean updateLeaderOMNodeId(String newLeaderOMNodeId) {
    if (!currentProxyOMNodeId.equals(newLeaderOMNodeId)) {
      if (omProxies.containsKey(newLeaderOMNodeId)) {
        currentProxyOMNodeId = newLeaderOMNodeId;
        currentProxyIndex = omNodeIDList.indexOf(currentProxyOMNodeId);
        return true;
      }
    }
    return false;
  }

  /**
   * Close all the proxy objects which have been opened over the lifetime of
   * the proxy provider.
   */
  @Override
  public synchronized void close() throws IOException {
    for (ProxyInfo<OzoneManagerProtocolPB> proxy : omProxies.values()) {
      OzoneManagerProtocolPB omProxy = proxy.proxy;
      if (omProxy != null) {
        RPC.stopProxy(omProxy);
      }
    }
  }

  @VisibleForTesting
  public List<ProxyInfo> getOMProxies() {
    return new ArrayList<ProxyInfo>(omProxies.values());
  }

  @VisibleForTesting
  public List<OMProxyInfo> getOMProxyInfos() {
    return new ArrayList<OMProxyInfo>(omProxyInfos.values());
  }
}

