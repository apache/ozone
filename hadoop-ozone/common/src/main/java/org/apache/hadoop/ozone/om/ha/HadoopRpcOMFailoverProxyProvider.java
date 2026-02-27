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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ozone.OmUtils;
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

  protected static final Logger LOG =
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
  protected List<OMProxyInfo<T>> initOmProxiesFromConfigs(ConfigurationSource config, String omSvcId) {
    final List<OMProxyInfo<T>> omProxies = new ArrayList<>();

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
      final OMProxyInfo<T> omProxyInfo = OMProxyInfo.newInstance(null, omSvcId, nodeId, rpcAddrStr);

      if (omProxyInfo.getAddress() != null) {
        omProxies.add(omProxyInfo);
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
    Collections.shuffle(omProxies);
    return omProxies;
  }

  /**
   * Get the proxy object which should be used until the next failover event
   * occurs. RPC proxy object is intialized lazily.
   * @return the OM proxy object to invoke methods upon
   */
  @Override
  public OMProxyInfo<T> getProxy() {
    return createOMProxyIfNeeded(getCurrentProxyOMNodeId());
  }

  protected OMProxyInfo<T> createOMProxyIfNeeded(String nodeId) {
    final OMProxyInfo<T> omProxyInfo = getOMProxyMap().get(nodeId);
    omProxyInfo.createProxyIfNeeded(this::createOMProxy);
    return omProxyInfo;
  }

  public Text getCurrentProxyDelegationToken() {
    return delegationTokenService;
  }

  protected Text computeDelegationTokenService() {
    // For HA, this will return "," separated address of all OM's.
    List<String> addresses = new ArrayList<>();

    for (OMProxyInfo<T> omProxyInfo : getOMProxyMap().getProxies()) {
      final Text dtService = omProxyInfo.getDelegationTokenService();

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
    for (OMProxyInfo<T> proxyInfo : getOMProxies()) {
      final T p = proxyInfo.getProxy();
      if (p instanceof Closeable) {
        ((Closeable) p).close();
      } else if (p != null) {
        RPC.stopProxy(p);
      }
    }
  }
}

