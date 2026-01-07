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

import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.protocolPB.GrpcOmTransport;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Grpc s3gateway om transport failover proxy provider implementation
 * extending the ozone client OM failover proxy provider.  This implementation
 * allows the Grpc OMTransport reuse OM failover retry policies and
 * getRetryAction methods.  In case of OM failover, client can try
 * connecting to another OM node from the list of proxies.
 */
public class GrpcOMFailoverProxyProvider<T> extends
    OMFailoverProxyProviderBase<T> {
  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcOMFailoverProxyProvider.class);

  public GrpcOMFailoverProxyProvider(ConfigurationSource configuration,
                                     UserGroupInformation ugi,
                                     String omServiceId,
                                     Class<T> protocol) throws IOException {
    super(configuration, ugi, omServiceId, protocol);
  }

  @Override
  protected void initOmProxiesFromConfigs(ConfigurationSource config, String omSvcId)
      throws IOException {

    Collection<String> omNodeIds = OmUtils.getActiveNonListenerOMNodeIds(config, omSvcId);
    Map<String, OMProxyInfo<T>> omProxies = new HashMap<>();
    List<String> omNodeIDList = new ArrayList<>();

    for (String nodeId : OmUtils.emptyAsSingletonNull(omNodeIds)) {
      String rpcAddrKey = ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
          omSvcId, nodeId);
      Optional<String> hostAddr = getHostNameFromConfigKeys(config,
          rpcAddrKey);
      OptionalInt hostport = HddsUtils.getNumberFromConfigKeys(config,
          ConfUtils.addKeySuffixes(OMConfigKeys.OZONE_OM_GRPC_PORT_KEY,
              omSvcId, nodeId),
          OMConfigKeys.OZONE_OM_GRPC_PORT_KEY);
      if (nodeId == null) {
        nodeId = OzoneConsts.OM_DEFAULT_NODE_ID;
      }
      if (hostAddr.isPresent()) {
        int port = hostport
            .orElse(config.getObject(GrpcOmTransport.GrpcOmTransportConfig.class).getPort());
        String rpcAddrStr = hostAddr.get() + ":" + port;
        OMProxyInfo<T> proxyInfo =
            new OMProxyInfo<>(createOMProxy(), omSvcId, nodeId, rpcAddrStr, rpcAddrStr);
        omProxies.put(nodeId, proxyInfo);
      } else {
        LOG.error("expected host address not defined for: {}", rpcAddrKey);
        throw new ConfigurationException(rpcAddrKey + "is not defined");
      }
      omNodeIDList.add(nodeId);
    }

    if (omProxies.isEmpty()) {
      throw new IllegalArgumentException("Could not find any configured " +
          "addresses for OM. Please configure the system with "
          + OZONE_OM_ADDRESS_KEY);
    }
    setOmProxies(omProxies);
    Collections.shuffle(omNodeIDList);
    setOmNodesInOrder(omNodeIDList);
  }

  private T createOMProxy() throws IOException {
    InetSocketAddress addr = new InetSocketAddress(0);
    return createOMProxy(addr);
  }

  /**
   * Get the proxy object which should be used until the next failover event
   * occurs. RPC proxy object is initialized lazily.
   * @return the OM proxy object to invoke methods upon
   */
  @Override
  public synchronized ProxyInfo<T> getProxy() {
    return getOMProxyMap().get(getCurrentProxyOMNodeId());
  }

  @Override
  protected synchronized boolean shouldFailover(Exception ex) {
    if (ex instanceof StatusRuntimeException) {
      StatusRuntimeException srexp = (StatusRuntimeException)ex;
      Status status = srexp.getStatus();
      if (status.getCode() == Status.Code.RESOURCE_EXHAUSTED) {
        LOG.debug("Grpc response has invalid length, {}", srexp.getMessage());
        return false;
      } else if (status.getCode() == Status.Code.DATA_LOSS) {
        LOG.debug("Grpc unrecoverable data loss or corruption, {}",
                srexp.getMessage());
        return false;
      }
    }
    return super.shouldFailover(ex);
  }

  @Override
  public synchronized void close() throws IOException { }

  // need to throw if nodeID not in omAddresses
  public String getGrpcProxyAddress(String nodeId) throws IOException {
    Map<String, OMProxyInfo<T>> omProxies = getOMProxyMap();
    if (omProxies.containsKey(nodeId)) {
      return omProxies.get(nodeId).proxyInfo;
    } else {
      LOG.error("expected nodeId not found in omProxies for proxyhost {}",
          nodeId);
      throw new IOException(
          "expected nodeId not found in omProxies for proxyhost");
    }
  }

  public List<String> getGrpcOmNodeIDList() {
    return getOmNodesInOrder();
  }
}
