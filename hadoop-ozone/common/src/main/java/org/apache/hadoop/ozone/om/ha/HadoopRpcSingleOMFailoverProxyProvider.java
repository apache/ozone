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

import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A failover proxy provider implementation which does nothing in the
 * event of OM failover, and always returns the same proxy object. In case of OM failover,
 * client will keep retrying to connect to the same OM node.
 */
public class HadoopRpcSingleOMFailoverProxyProvider<T> extends
    SingleOMFailoverProxyProviderBase<T> {

  public static final Logger LOG =
      LoggerFactory.getLogger(HadoopRpcSingleOMFailoverProxyProvider.class);

  private final Text delegationTokenService;
  private OMProxyInfo omProxyInfo;

  // HadoopRpcOMFailoverProxyProvider, on encountering certain exception,
  // will immediately fail. It only communicates with a single OM, regardless
  // whether there are other OMs in the OM service.
  public HadoopRpcSingleOMFailoverProxyProvider(ConfigurationSource configuration,
                                                UserGroupInformation ugi,
                                                String omServiceId,
                                                String omNodeId,
                                                Class<T> protocol) throws IOException {
    super(configuration, ugi, omServiceId, omNodeId, protocol);
    this.delegationTokenService = computeDelegationTokenService();
  }

  @Override
  protected void loadOMClientConfig(ConfigurationSource config, String omSvcId, String omNodeId) throws IOException {
    String rpcAddrKey = ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY, omSvcId, omNodeId);
    String rpcAddrStr = OmUtils.getOmRpcAddress(config, rpcAddrKey);
    if (rpcAddrStr == null) {
      throw new IllegalArgumentException("Could not find any configured " +
          "addresses for OM. Please configure the system with "
          + OZONE_OM_ADDRESS_KEY);
    }

    this.omProxyInfo =  new OMProxyInfo(omSvcId, omNodeId, rpcAddrStr);
    if (omProxyInfo.getAddress() == null) {
      LOG.error("Failed to create OM proxy for {} at address {}",
          omNodeId, rpcAddrStr);
      throw new IllegalArgumentException("Could not find any configured " +
          "addresses for OM. Please configure the system with "
          + OZONE_OM_ADDRESS_KEY);
    }
    setOmProxy(createOMProxy());
  }

  /**
   * Creates proxy object.
   */
  protected ProxyInfo<T> createOMProxy() {
    InetSocketAddress address = omProxyInfo.getAddress();
    ProxyInfo<T> proxyInfo;
    try {
      T proxy = createOMProxy(address);
      // Create proxyInfo here, to make it work with all Hadoop versions.
      proxyInfo = new ProxyInfo<>(proxy, omProxyInfo.toString());

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
    String address = null;
    Text dtService = omProxyInfo.getDelegationTokenService();

    // During client object creation when one of the OM configured address
    // in unreachable, dtService can be null.
    if (dtService != null) {
      address = dtService.toString();
    }

    if (address != null) {
      return new Text(address);
    } else {
      // If all OM addresses is unresolvable, set dt service to null. Let
      // this fail in later step when during connection setup.
      return null;
    }
  }

  @Override
  public void close() throws IOException {
    ProxyInfo<T> proxyInfo = getProxy();
    if (proxyInfo != null) {
      RPC.stopProxy(proxyInfo.proxy);
    }
  }
}
