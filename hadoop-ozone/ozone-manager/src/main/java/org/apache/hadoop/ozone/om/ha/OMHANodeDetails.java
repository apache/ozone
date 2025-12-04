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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FLEXIBLE_FQDN_RESOLUTION_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FLEXIBLE_FQDN_RESOLUTION_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_INTERNAL_SERVICE_ID;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODE_ID_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_PORT_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneIllegalArgumentException;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.util.OzoneNetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class which maintains peer information and it's own OM node information.
 */
public class OMHANodeDetails {

  private static String[] genericConfigKeys = new String[] {
      OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY,
      OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY,
      OMConfigKeys.OZONE_OM_HTTP_BIND_HOST_KEY,
      OMConfigKeys.OZONE_OM_HTTPS_BIND_HOST_KEY,
      OMConfigKeys.OZONE_OM_DB_DIRS,
      OMConfigKeys.OZONE_OM_ADDRESS_KEY,
  };

  private static final Logger LOG =
      LoggerFactory.getLogger(OMHANodeDetails.class);
  private final OMNodeDetails localNodeDetails;
  private final List<OMNodeDetails> peerNodeDetails;

  public OMHANodeDetails(OMNodeDetails localNodeDetails,
      List<OMNodeDetails> peerNodeDetails) {
    this.localNodeDetails = localNodeDetails;
    this.peerNodeDetails = peerNodeDetails;
  }

  public OMNodeDetails getLocalNodeDetails() {
    return localNodeDetails;
  }

  public Map<String, OMNodeDetails> getPeerNodesMap() {
    Map<String, OMNodeDetails> peerNodesMap = new HashMap<>();
    for (OMNodeDetails peeNode : peerNodeDetails) {
      peerNodesMap.put(peeNode.getNodeId(), peeNode);
    }
    return peerNodesMap;
  }

  /**
   * Inspects and loads OM node configurations.
   *
   * If {@link OMConfigKeys#OZONE_OM_SERVICE_IDS_KEY} is configured with
   * multiple ids and/ or if {@link OMConfigKeys#OZONE_OM_NODE_ID_KEY} is not
   * specifically configured , this method determines the omServiceId
   * and omNodeId by matching the node's address with the configured
   * addresses. When a match is found, it sets the omServicId and omNodeId from
   * the corresponding configuration key. This method also finds the OM peers
   * nodes belonging to the same OM service.
   *
   * @param conf
   */
  public static OMHANodeDetails loadOMHAConfig(OzoneConfiguration conf) {
    InetSocketAddress localRpcAddress = null;
    String localOMServiceId = null;
    String localOMNodeId = null;
    int localRatisPort = 0;
    boolean localIsListener = false;

    Collection<String> omServiceIds;

    localOMServiceId = conf.getTrimmed(OZONE_OM_INTERNAL_SERVICE_ID);

    if (localOMServiceId == null) {
      // There is no internal om service id is being set, fall back to ozone
      // .om.service.ids.
      LOG.info("{} is not defined, falling back to {} to find serviceID for "
          + "OzoneManager if it is HA enabled cluster",
              OZONE_OM_INTERNAL_SERVICE_ID, OZONE_OM_SERVICE_IDS_KEY);
      omServiceIds = conf.getTrimmedStringCollection(
          OZONE_OM_SERVICE_IDS_KEY);
    } else {
      LOG.info("ServiceID for OzoneManager is {}", localOMServiceId);
      omServiceIds = Collections.singletonList(localOMServiceId);
    }

    String knownOMNodeId = conf.get(OZONE_OM_NODE_ID_KEY);
    int found = 0;
    boolean isOMAddressSet = false;

    for (String serviceId : omServiceIds) {
      Collection<String> omNodeIds = OmUtils.getActiveOMNodeIds(conf,
          serviceId);
      Collection<String> listenerOmNodeIds = OmUtils.getListenerOMNodeIds(conf, serviceId);

      if (omNodeIds.isEmpty()) {
        throwConfException("Configuration does not have any value set for %s " +
            "for the service %s. List of OM Node ID's should be specified " +
            "for an OM service", OZONE_OM_NODES_KEY, serviceId);
        return null;
      }

      List<OMNodeDetails> peerNodesList = new ArrayList<>();
      boolean isPeer;
      for (String nodeId : omNodeIds) {
        if (knownOMNodeId != null && !knownOMNodeId.equals(nodeId)) {
          isPeer = true;
        } else {
          isPeer = false;
        }
        String rpcAddrKey = ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
            serviceId, nodeId);
        String rpcAddrStr = OmUtils.getOmRpcAddress(conf, rpcAddrKey);
        if (rpcAddrStr == null || rpcAddrStr.isEmpty()) {
          throwConfException("Configuration does not have any value set for " +
              "%s. OM RPC Address should be set for all nodes in an OM " +
              "service.", rpcAddrKey);
          return null;
        }

        // If OM address is set for any node id, we will not fallback to the
        // default
        isOMAddressSet = true;

        String ratisPortKey = ConfUtils.addKeySuffixes(OZONE_OM_RATIS_PORT_KEY,
            serviceId, nodeId);
        int ratisPort = conf.getInt(ratisPortKey, OZONE_OM_RATIS_PORT_DEFAULT);

        InetSocketAddress addr = null;
        try {
          addr = NetUtils.createSocketAddr(rpcAddrStr);
        } catch (Exception e) {
          LOG.error("Couldn't create socket address for OM {} : {}", nodeId,
              rpcAddrStr, e);
          throw e;
        }

        boolean flexibleFqdnResolutionEnabled = conf.getBoolean(
                OZONE_FLEXIBLE_FQDN_RESOLUTION_ENABLED,
                OZONE_FLEXIBLE_FQDN_RESOLUTION_ENABLED_DEFAULT);
        if (OzoneNetUtils.isUnresolved(flexibleFqdnResolutionEnabled, addr)) {
          LOG.error("Address for OM {} : {} couldn't be resolved. Proceeding " +
                  "with unresolved host to create Ratis ring.", nodeId,
              rpcAddrStr);
        }

        boolean isListener = listenerOmNodeIds.contains(nodeId);
        if (!isPeer
                && OzoneNetUtils
                .isAddressLocal(flexibleFqdnResolutionEnabled, addr)) {
          localRpcAddress = addr;
          localOMServiceId = serviceId;
          localOMNodeId = nodeId;
          localRatisPort = ratisPort;
          localIsListener = isListener;
          found++;
        } else {
          // This OMNode belongs to same OM service as the current OMNode.
          // Add it to peerNodes list.
          peerNodesList.add(getHAOMNodeDetails(conf, serviceId,
              nodeId, addr, ratisPort, isListener));
        }
      }
      if (found == 1) {

        LOG.info("Found matching OM address with OMServiceId: {}, " +
                "OMNodeId: {}, RPC Address: {} ,Ratis port: {} and isListener: {}",
            localOMServiceId, localOMNodeId,
            NetUtils.getHostPortString(localRpcAddress), localRatisPort, localIsListener);

        ConfUtils.setNodeSpecificConfigs(genericConfigKeys, conf,
            localOMServiceId, localOMNodeId, LOG);
        return new OMHANodeDetails(getHAOMNodeDetails(conf, localOMServiceId,
            localOMNodeId, localRpcAddress, localRatisPort, localIsListener), peerNodesList);

      } else if (found > 1) {
        throwConfException("Configuration has multiple %s addresses that " +
            "match local node's address. Please configure the system with %s " +
            "and %s", OZONE_OM_ADDRESS_KEY, OZONE_OM_SERVICE_IDS_KEY,
            OZONE_OM_ADDRESS_KEY);
        return null;
      }
    }

    if (!isOMAddressSet) {
      // No OM address is set. Fallback to default
      InetSocketAddress omAddress = OmUtils.getOmAddress(conf);
      int ratisPort = conf.getInt(OZONE_OM_RATIS_PORT_KEY,
          OZONE_OM_RATIS_PORT_DEFAULT);

      LOG.info("Configuration does not have {} set. Falling back to the " +
          "default OM address {}", OZONE_OM_ADDRESS_KEY, omAddress);

      return new OMHANodeDetails(getOMNodeDetailsForNonHA(conf, null,
          null, omAddress, ratisPort), new ArrayList<>());

    } else {
      throwConfException("Configuration has no %s address that matches local " +
          "node's address.", OZONE_OM_ADDRESS_KEY);
      return null;
    }
  }

  /**
   * Create Local OM Node Details.
   * @param serviceId - Service ID this OM belongs to,
   * @param nodeId - Node ID of this OM.
   * @param rpcAddress - Rpc Address of the OM.
   * @param ratisPort - Ratis port of the OM.
   * @return OMNodeDetails
   */
  public static OMNodeDetails getOMNodeDetailsForNonHA(OzoneConfiguration conf,
      String serviceId, String nodeId, InetSocketAddress rpcAddress,
      int ratisPort) {
    return getOMNodeDetailsForNonHA(conf, serviceId, nodeId, rpcAddress, ratisPort, false);
  }

  public static OMNodeDetails getOMNodeDetailsForNonHA(OzoneConfiguration conf,
      String serviceId, String nodeId, InetSocketAddress rpcAddress,
      int ratisPort, boolean isListener) {

    if (serviceId == null) {
      // If no serviceId is set, take the default serviceID om-service
      serviceId = OzoneConsts.OM_SERVICE_ID_DEFAULT;
      LOG.info("OM Service ID is not set. Setting it to the default ID: {}",
          serviceId);
    }

    if (nodeId == null) {
      // If no nodeId is provided, set the default nodeID - om1
      nodeId = OzoneConsts.OM_DEFAULT_NODE_ID;
      LOG.info("OM Node ID is not set. Setting it to the default ID: {}",
          nodeId);
    }

    // We need to pass null for serviceID and nodeID as this is set for
    // non-HA cluster. This means one node OM cluster.
    String httpAddr = OmUtils.getHttpAddressForOMPeerNode(conf,
        null, null, rpcAddress.getHostName());
    String httpsAddr = OmUtils.getHttpsAddressForOMPeerNode(conf,
        null, null, rpcAddress.getHostName());

    return new OMNodeDetails.Builder()
        .setOMServiceId(serviceId)
        .setOMNodeId(nodeId)
        .setRpcAddress(rpcAddress)
        .setRatisPort(ratisPort)
        .setHttpAddress(httpAddr)
        .setHttpsAddress(httpsAddr)
        .setIsListener(isListener)
        .build();
  }

  /**
   * Create Local OM Node Details.
   * @param serviceId - Service ID this OM belongs to,
   * @param nodeId - Node ID of this OM.
   * @param rpcAddress - Rpc Address of the OM.
   * @param ratisPort - Ratis port of the OM.
   * @return OMNodeDetails
   */
  public static OMNodeDetails getHAOMNodeDetails(OzoneConfiguration conf,
      String serviceId, String nodeId, InetSocketAddress rpcAddress,
      int ratisPort) {
    return getHAOMNodeDetails(conf, serviceId, nodeId, rpcAddress, ratisPort, false);
  }

  public static OMNodeDetails getHAOMNodeDetails(OzoneConfiguration conf,
      String serviceId, String nodeId, InetSocketAddress rpcAddress,
      int ratisPort, boolean isListener) {
    Objects.requireNonNull(serviceId, "serviceId == null");
    Objects.requireNonNull(nodeId, "nodeId == null");

    String httpAddr = OmUtils.getHttpAddressForOMPeerNode(conf,
        serviceId, nodeId, rpcAddress.getHostName());
    String httpsAddr = OmUtils.getHttpsAddressForOMPeerNode(conf,
        serviceId, nodeId, rpcAddress.getHostName());

    return new OMNodeDetails.Builder()
        .setOMServiceId(serviceId)
        .setOMNodeId(nodeId)
        .setRpcAddress(rpcAddress)
        .setRatisPort(ratisPort)
        .setHttpAddress(httpAddr)
        .setHttpsAddress(httpsAddr)
        .setIsListener(isListener)
        .build();
  }

  private static void throwConfException(String message, String... arguments)
      throws IllegalArgumentException {
    String exceptionMsg = String.format(message, arguments);
    LOG.error(exceptionMsg);
    throw new OzoneIllegalArgumentException(exceptionMsg);
  }
}
