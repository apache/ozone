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

import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.protocolPB.GrpcOmTransport;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;

/**
 * The Grpc s3gateway om transport failover proxy provider implementation
 * extending the ozone client OM failover proxy provider.  This implmentation
 * allows the Grpc OMTransport reuse OM failover retry policies and
 * getRetryAction methods.  In case of OM failover, client can try
 * connecting to another OM node from the list of proxies.
 */
public class GrpcOMFailoverProxyProvider<T> extends
    OMFailoverProxyProvider<T> {

  private Map<String, String> omAddresses;

  public GrpcOMFailoverProxyProvider(ConfigurationSource configuration,
                                     UserGroupInformation ugi,
                                     String omServiceId,
                                     Class<T> protocol) throws IOException {
    super(configuration, ugi, omServiceId, protocol);
  }

  @Override
  protected void loadOMClientConfigs(ConfigurationSource config, String omSvcId)
      throws IOException {
    // to be used for base class omProxies,
    // ProxyInfo not applicable for gRPC, just need key set
    Map<String, ProxyInfo<T>> omProxiesNodeIdKeyset = new HashMap<>();
    // to be used for base class omProxyInfos
    // OMProxyInfo not applicable for gRPC, just need key set
    Map<String, OMProxyInfo> omProxyInfosNodeIdKeyset = new HashMap<>();
    List<String> omNodeIDList = new ArrayList<>();
    omAddresses = new HashMap<>();

    Collection<String> omServiceIds = Collections.singletonList(omSvcId);

    for (String serviceId : OmUtils.emptyAsSingletonNull(omServiceIds)) {
      Collection<String> omNodeIds = OmUtils.getOMNodeIds(config, serviceId);

      for (String nodeId : OmUtils.emptyAsSingletonNull(omNodeIds)) {

        String rpcAddrKey = ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
            serviceId, nodeId);

        Optional<String> hostaddr = getHostNameFromConfigKeys(config,
            rpcAddrKey);

        OptionalInt hostport = HddsUtils.getNumberFromConfigKeys(config,
            ConfUtils.addKeySuffixes(OMConfigKeys.OZONE_OM_GRPC_PORT_KEY,
                serviceId, nodeId),
            OMConfigKeys.OZONE_OM_GRPC_PORT_KEY);
        if (nodeId == null) {
          nodeId = OzoneConsts.OM_DEFAULT_NODE_ID;
        }
        omProxiesNodeIdKeyset.put(nodeId, null);
        omProxyInfosNodeIdKeyset.put(nodeId, null);
        if (hostaddr.isPresent()) {
          omAddresses.put(nodeId,
              hostaddr.get() + ":"
                  + hostport.orElse(config
                  .getObject(GrpcOmTransport
                      .GrpcOmTransportConfig.class)
                  .getPort()));
        } else {
          LOG.error("expected host address not defined: {}", rpcAddrKey);
          throw new ConfigurationException(rpcAddrKey + "is not defined");
        }
        omNodeIDList.add(nodeId);
      }
    }

    if (omProxiesNodeIdKeyset.isEmpty()) {
      throw new IllegalArgumentException("Could not find any configured " +
          "addresses for OM. Please configure the system with "
          + OZONE_OM_ADDRESS_KEY);
    }

    // set base class omProxies, omProxyInfos, omNodeIDList

    // omProxies needed in base class
    // omProxies.size == number of om nodes
    // omProxies key needs to be valid nodeid
    // omProxyInfos keyset needed in base class
    setProxies(omProxiesNodeIdKeyset, omProxyInfosNodeIdKeyset, omNodeIDList);
  }

  @Override
  protected Text computeDelegationTokenService() {
    return new Text();
  }

  // need to throw if nodeID not in omAddresses
  public String getGrpcProxyAddress(String nodeId) throws IOException {
    if (omAddresses.containsKey(nodeId)) {
      return omAddresses.get(nodeId);
    } else {
      LOG.error("expected nodeId not found in omAddresses for proxyhost {}",
          nodeId);
      throw new IOException(
          "expected nodeId not found in omAddresses for proxyhost");
    }

  }

  public List<String> getGrpcOmNodeIDList() {
    return getOmNodeIDList();
  }
}
