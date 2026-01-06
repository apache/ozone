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

package org.apache.hadoop.ozone.om.ha;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A failover proxy provider implementation which allows clients to configure
 * multiple OMs to connect to. In case of OM failover, client can try
 * connecting to another OM node from the list of proxies.
 */
public class HadoopRpcOMFailoverProxyProvider<T> extends
      OMFailoverProxyProviderBase<T> {

  private static final Logger LOG =
      LoggerFactory.getLogger(HadoopRpcOMFailoverProxyProvider.class);

  private final Text delegationTokenService;

  // HadoopRpcOMFailoverProxyProvider, on encountering certain exception,
  // tries each OM once in a round robin fashion. After that it waits
  // for configured time before attempting to contact all the OMs again.
  // For other exceptions such as LeaderNotReadyException, the same OM
  // is contacted again with a linearly increasing wait time.

  public HadoopRpcOMFailoverProxyProvider(ConfigurationSource configuration,
                                 UserGroupInformation ugi,
                                 String omServiceId,
                                 Class<T> protocol) throws IOException {
    super(configuration, ugi, omServiceId, protocol);
    this.delegationTokenService = computeDelegationTokenService();
  }

  @Override
  protected void loadOMClientConfigs(ConfigurationSource config, String omSvcId)
      throws IOException {
    Map<String, OMProxyInfo<T>> omProxies = new HashMap<>();

    List<String> omNodeIDList = new ArrayList<>();

    Collection<String> omNodeIds = OmUtils.getActiveNonListenerOMNodeIds(config,
        omSvcId);

    for (String nodeId : OmUtils.emptyAsSingletonNull(omNodeIds)) {

      String rpcAddrKey = ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
          omSvcId, nodeId);
      String rpcAddrStr = OmUtils.getOmRpcAddress(config, rpcAddrKey);
      if (rpcAddrStr == null) {
        continue;
      }

      // ProxyInfo.proxy will be set during first time call to server.
      OMProxyInfo<T> omProxyInfo = new OMProxyInfo<>(omSvcId, nodeId, rpcAddrStr);

      if (omProxyInfo.getAddress() != null) {
        // For a non-HA OM setup, nodeId might be null. If so, we assign it
        // the default value
        if (nodeId == null) {
          nodeId = OzoneConsts.OM_DEFAULT_NODE_ID;
        }
        omProxies.put(nodeId, omProxyInfo);
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
    setOmProxies(omProxies);
    setOmNodeIDList(omNodeIDList);
  }

  /**
   * Get the proxy object which should be used until the next failover event
   * occurs. RPC proxy object is intialized lazily.
   * @return the OM proxy object to invoke methods upon
   */
  @Override
  public synchronized ProxyInfo<T> getProxy() {
    ProxyInfo<T> current = getOMProxyMap().get(getCurrentProxyOMNodeId());
    return createOMProxyIfNeeded(current);
  }

  /**
   * Creates proxy object.
   */
  protected ProxyInfo<T> createOMProxyIfNeeded(ProxyInfo<T> pi) {
    if (pi.proxy == null) {
      OMProxyInfo<T> omProxyInfo = (OMProxyInfo<T>) pi;
      try {
        pi.proxy = createOMProxy(omProxyInfo.getAddress());
      } catch (IOException ioe) {
        LOG.error("{} Failed to create RPC proxy to OM at {}",
            this.getClass().getSimpleName(), omProxyInfo.getAddress(), ioe);
        throw new RuntimeException(ioe);
      }
    }
    return pi;
  }

  public Text getCurrentProxyDelegationToken() {
    return delegationTokenService;
  }

  protected Text computeDelegationTokenService() {
    // For HA, this will return "," separated address of all OM's.
    List<String> addresses = new ArrayList<>();

    for (Map.Entry<String, OMProxyInfo<T>> omProxyInfoSet :
        getOMProxyMap().entrySet()) {
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
      if (proxyInfo.proxy != null) {
        if (proxyInfo.proxy instanceof Closeable) {
          ((Closeable)proxyInfo.proxy).close();
        } else {
          RPC.stopProxy(proxyInfo.proxy);
        }
      }
    }
  }

  @VisibleForTesting
  protected void setProxiesForTesting(
      Map<String, OMProxyInfo<T>> setOMProxies,
      List<String> setOMNodeIDList) {
    setOmProxies(setOMProxies);
    setOmNodeIDList(setOMNodeIDList);
  }

}

