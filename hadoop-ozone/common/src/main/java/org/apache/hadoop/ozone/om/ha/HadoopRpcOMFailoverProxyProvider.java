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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.security.UserGroupInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;

/**
 * A failover proxy provider implementation which allows clients to configure
 * multiple OMs to connect to. In case of OM failover, client can try
 * connecting to another OM node from the list of proxies.
 */
public class HadoopRpcOMFailoverProxyProvider<T> extends
      OMFailoverProxyProviderBase<T> {

  public static final Logger LOG =
      LoggerFactory.getLogger(HadoopRpcOMFailoverProxyProvider.class);

  private final long omVersion;
  private final Text delegationTokenService;
  private final UserGroupInformation ugi;
  private Map<String, OMProxyInfo> omProxyInfos;
  private List<String> retryExceptions = new ArrayList<>();

  // HadoopRpcOMFailoverProxyProvider, on encountering certain exception,
  // tries each OM once in a round robin fashion. After that it waits
  // for configured time before attempting to contact all the OMs again.
  // For other exceptions such as LeaderNotReadyException, the same OM
  // is contacted again with a linearly increasing wait time.

  public HadoopRpcOMFailoverProxyProvider(ConfigurationSource configuration,
                                 UserGroupInformation ugi,
                                 String omServiceId,
                                 Class<T> protocol) throws IOException {
    super(configuration, omServiceId, protocol);
    this.ugi = ugi;
    this.omVersion = RPC.getProtocolVersion(protocol);
    this.delegationTokenService = computeDelegationTokenService();
  }

  protected void loadOMClientConfigs(ConfigurationSource config, String omSvcId)
      throws IOException {
    Map<String, ProxyInfo<T>> omProxies = new HashMap<>();
    this.omProxyInfos = new HashMap<>();
    List<String> omNodeIDList = new ArrayList<>();
    Map<String, InetSocketAddress> omNodeAddressMap = new HashMap<>();

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
        omNodeAddressMap.put(nodeId, omProxyInfo.getAddress());
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
    setOmProxies(omProxies);
    setOmNodeIDList(omNodeIDList);
    setOmNodeAddressMap(omNodeAddressMap);
  }

  private T createOMProxy(InetSocketAddress omAddress) throws IOException {
    Configuration hadoopConf =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(getConf());
    RPC.setProtocolEngine(hadoopConf, getInterface(), ProtobufRpcEngine.class);

    // FailoverOnNetworkException ensures that the IPC layer does not attempt
    // retries on the same OM in case of connection exception. This retry
    // policy essentially results in TRY_ONCE_THEN_FAIL.
    RetryPolicy connectionRetryPolicy = RetryPolicies
        .failoverOnNetworkException(0);

    return (T) RPC.getProtocolProxy(getInterface(), omVersion,
        omAddress, ugi, hadoopConf, NetUtils.getDefaultSocketFactory(
            hadoopConf), (int) OmUtils.getOMClientRpcTimeOut(getConf()),
        connectionRetryPolicy).getProxy();

  }

  /**
   * Get the proxy object which should be used until the next failover event
   * occurs. RPC proxy object is intialized lazily.
   * @return the OM proxy object to invoke methods upon
   */
  @Override
  public synchronized ProxyInfo<T> getProxy() {
    ProxyInfo currentProxyInfo = getOMProxyMap().get(getCurrentProxyOMNodeId());
    if (currentProxyInfo == null) {
      currentProxyInfo = createOMProxy(getCurrentProxyOMNodeId());
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
      getOMProxyMap().put(nodeId, proxyInfo);
    } catch (IOException ioe) {
      LOG.error("{} Failed to create RPC proxy to OM at {}",
          this.getClass().getSimpleName(), address, ioe);
      throw new RuntimeException(ioe);
    }
    return proxyInfo;
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

  /**
   * Close all the proxy objects which have been opened over the lifetime of
   * the proxy provider.
   */
  @Override
  public synchronized void close() throws IOException {
    for (ProxyInfo<T> proxyInfo : getOMProxies()) {
      if (proxyInfo != null) {
        RPC.stopProxy(proxyInfo.proxy);
      }
    }
  }

  @VisibleForTesting
  public List<OMProxyInfo> getOMProxyInfos() {
    return new ArrayList<OMProxyInfo>(omProxyInfos.values());
  }

  @VisibleForTesting
  public Map<String, OMProxyInfo> getOMProxyInfoMap() {
    return omProxyInfos;
  }

  @VisibleForTesting
  protected void setProxiesForTesting(
      Map<String, ProxyInfo<T>> setOMProxies,
      Map<String, OMProxyInfo> setOMProxyInfos,
      List<String> setOMNodeIDList,
      Map<String, InetSocketAddress> setOMNodeAddress) {
    setOmProxies(setOMProxies);
    this.omProxyInfos = setOMProxyInfos;
    setOmNodeIDList(setOMNodeIDList);
    setOmNodeAddressMap(setOMNodeAddress);
  }

}

