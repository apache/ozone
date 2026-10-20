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

package org.apache.hadoop.ozone.jobworker.utils;

import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DECOMMISSIONED_NODES_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.conf.OMJobworkerConfiguration;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.jobworker.JobworkerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for JobWorker.
 */
public final class JobworkerUtils {

  public static final String DEFAULT_OM_SERVICE_ID = "DEFAULT";
  private static final Logger LOG = LoggerFactory.getLogger(JobworkerUtils.class);

  private JobworkerUtils() {
    // Do not instantiate
  }

  /**
   * Return list of OM addresses by service ids - when HA is enabled.
   *
   * @param conf {@link ConfigurationSource}
   * @return {service.id -> [{@link InetSocketAddress}]}
   */
  public static Map<String, List<InetSocketAddress>> getOmJobworkerHAAddressesById(
      ConfigurationSource conf) {
    Map<String, List<InetSocketAddress>> result = new HashMap<>();
    JobworkerConfiguration jobworkerConfiguration = conf.getObject(JobworkerConfiguration.class);
    for (String serviceId : org.apache.hadoop.util.StringUtils.getTrimmedStringCollection(
        jobworkerConfiguration.getOmServiceIds())) {
      result.computeIfAbsent(serviceId, x -> new ArrayList<>());
      for (String nodeId : getActiveOMNodeIds(conf, serviceId)) {
        String rpcAddr = getOmJobWorkerRpcAddress(conf, serviceId, nodeId);
        if (rpcAddr != null) {
          result.get(serviceId).add(NetUtils.createSocketAddr(rpcAddr));
        } else {
          throw new IllegalArgumentException(String.format(
              "Address undefined for nodeId: %s for service %s", nodeId, serviceId));
        }
      }
    }
    return result;
  }

  /**
   * Retrieve the socket address that is used by OM as specified by the confKey.
   * Return null if the specified conf key is not set.
   *
   * @param conf    configuration
   * @param serviceId Service ID this OM belongs to
   * @param nodeId Node ID of this OM.
   * @return Target InetSocketAddress for the OM RPC server.
   */
  public static String getOmJobWorkerRpcAddress(
      ConfigurationSource conf, String serviceId, String nodeId) throws IllegalStateException {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY, serviceId, nodeId));
    if (!host.isPresent()) {
      return null;
    }
    int port = getOmJobworkerRpcPortOrDefault(conf,
        ConfUtils.addKeySuffixes(OMJobworkerConfiguration.getGrpcPortKey(), serviceId, nodeId));
    return host.get() + ":" + port;
  }

  private static int getOmJobworkerRpcPortOrDefault(
      ConfigurationSource conf, String confKey) throws IllegalStateException {
    int port = conf.getInt(confKey, -1);
    if (port > -1) {
      return port;
    } else {
      // Use the default port
      return conf.getObject(OMJobworkerConfiguration.class).getGrpcPort();
    }
  }

  /**
   * Retrieve the socket address used by OM.
   *
   * @param conf
   * @return Target InetSocketAddress for the OM service endpoint,
   * If not configured, returns an empty string
   */
  public static String getOmJobWorkerRpcAddress(ConfigurationSource conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        OZONE_OM_ADDRESS_KEY);
    int port = getOmJobworkerRpcPortOrDefault(conf, OMJobworkerConfiguration.getGrpcPortKey());

    return host.map(s -> s + ":" + port).orElse("");
  }

  /**
   * Get a collection of all active omNodeIds (excluding decommissioned nodes)
   * for the given omServiceId.
   */
  public static Collection<String> getActiveOMNodeIds(ConfigurationSource conf,
                                                      String omServiceId) {
    String nodeIdsKey = ConfUtils.addSuffix(OZONE_OM_NODES_KEY, omServiceId);
    Collection<String> nodeIds = conf.getTrimmedStringCollection(nodeIdsKey);
    String decommNodesKey = ConfUtils.addKeySuffixes(
        OZONE_OM_DECOMMISSIONED_NODES_KEY, omServiceId);
    Collection<String> decommNodeIds = conf.getTrimmedStringCollection(
        decommNodesKey);
    nodeIds.removeAll(decommNodeIds);

    return nodeIds;
  }

  /**
   * Utility method to retrieve OM addresses for JobWorker from configuration.
   * This method supports multiple OM service groups.
   *
   * @param conf The configuration source
   * @return Collection of OM addresses as InetSocketAddress
   * @throws IllegalArgumentException If the configuration is invalid
   */
  public static Map<String, List<InetSocketAddress>> getOMsAddressForJobworker(
      ConfigurationSource conf) throws IllegalStateException {
    Map<String, List<InetSocketAddress>> haAddresses = getOmJobworkerHAAddressesById(conf);
    if (!haAddresses.isEmpty()) {
      return haAddresses;
    }

    // Fallback to OZONE_OM_ADDRESS_KEY
    String omAddress = getOmJobWorkerRpcAddress(conf);
    if (StringUtils.isEmpty(omAddress)) {
      throw new IllegalStateException(String.format("OM address is not configured properly. "
              + "Please set either %s with per-node addresses (for HA), "
              + "or configure a valid value for %s for non-HA mode.",
          JobworkerConfiguration.OZONE_JOBWORKER_OM_SERVICE_IDS_KEY, OZONE_OM_ADDRESS_KEY));
    }
    return Collections.singletonMap(DEFAULT_OM_SERVICE_ID,
        Collections.singletonList(NetUtils.createSocketAddr(omAddress)));
  }

}
