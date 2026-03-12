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

package org.apache.hadoop.hdds.scm.ha;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DB_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEFAULT_SERVICE_ID;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_GRPC_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_GRPC_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTPS_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FLEXIBLE_FQDN_RESOLUTION_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FLEXIBLE_FQDN_RESOLUTION_ENABLED_DEFAULT;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ScmUtils;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.util.OzoneNetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SCM HA node details.
 */
public class SCMHANodeDetails {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMHANodeDetails.class);

  private final SCMNodeDetails localNodeDetails;
  private final List<SCMNodeDetails> peerNodeDetails;

  private static String[] nodeSpecificConfigKeys = new String[] {
      OZONE_SCM_DATANODE_ADDRESS_KEY,
      OZONE_SCM_DATANODE_PORT_KEY,
      OZONE_SCM_DATANODE_BIND_HOST_KEY,
      OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
      OZONE_SCM_BLOCK_CLIENT_PORT_KEY,
      OZONE_SCM_BLOCK_CLIENT_BIND_HOST_KEY,
      OZONE_SCM_CLIENT_ADDRESS_KEY,
      OZONE_SCM_CLIENT_PORT_KEY,
      OZONE_SCM_CLIENT_BIND_HOST_KEY,
      OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY,
      OZONE_SCM_SECURITY_SERVICE_PORT_KEY,
      OZONE_SCM_SECURITY_SERVICE_BIND_HOST_KEY,
      OZONE_SCM_RATIS_PORT_KEY,
      OZONE_SCM_GRPC_PORT_KEY,
      OZONE_SCM_HTTP_BIND_HOST_KEY,
      OZONE_SCM_HTTPS_BIND_HOST_KEY,
      OZONE_SCM_HTTP_ADDRESS_KEY,
      OZONE_SCM_HTTPS_ADDRESS_KEY,
      OZONE_SCM_DB_DIRS,
      OZONE_SCM_ADDRESS_KEY
  };

  /**
   * SCMHANodeDetails object.
   * @param localNodeDetails
   * @param peerNodeDetails
   */
  public SCMHANodeDetails(SCMNodeDetails localNodeDetails,
      List<SCMNodeDetails> peerNodeDetails) {
    this.localNodeDetails = localNodeDetails;
    this.peerNodeDetails = peerNodeDetails;
  }

  public SCMNodeDetails getLocalNodeDetails() {
    return localNodeDetails;
  }

  public List< SCMNodeDetails > getPeerNodeDetails() {
    return peerNodeDetails;
  }

  public static SCMHANodeDetails loadDefaultConfig(
      OzoneConfiguration conf) throws IOException {
    int ratisPort = conf.getInt(
        ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY,
        ScmConfigKeys.OZONE_SCM_RATIS_PORT_DEFAULT);
    int grpcPort = conf.getInt(
        ScmConfigKeys.OZONE_SCM_GRPC_PORT_KEY,
        ScmConfigKeys.OZONE_SCM_GRPC_PORT_DEFAULT);
    InetSocketAddress rpcAddress = new InetSocketAddress(
        InetAddress.getLocalHost(), 0);
    SCMNodeDetails scmNodeDetails = new SCMNodeDetails.Builder()
        .setRatisPort(ratisPort)
        .setGrpcPort(grpcPort)
        .setRpcAddress(rpcAddress)
        .setDatanodeProtocolServerAddress(
            HddsServerUtil.getScmDataNodeBindAddress(conf))
        .setDatanodeAddressKey(OZONE_SCM_DATANODE_ADDRESS_KEY)
        .setBlockProtocolServerAddress(
            HddsServerUtil.getScmBlockClientBindAddress(conf))
        .setBlockProtocolServerAddressKey(
            ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY)
        .setClientProtocolServerAddress(
            HddsServerUtil.getScmClientBindAddress(conf))
        .setClientProtocolServerAddressKey(
            ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY)
        .build();
    return new SCMHANodeDetails(scmNodeDetails, Collections.emptyList());
  }

  public static SCMHANodeDetails loadSCMHAConfig(OzoneConfiguration conf,
                                                 SCMStorageConfig storageConfig)
      throws IOException {
    InetSocketAddress localRpcAddress = null;
    String localScmServiceId = null;
    String localScmNodeId = null;
    int localRatisPort = 0;
    int localGrpcPort = 0;

    Collection<String> scmServiceIds;

    localScmServiceId = conf.getTrimmed(
        ScmConfigKeys.OZONE_SCM_DEFAULT_SERVICE_ID);

    LOG.info("ServiceID for StorageContainerManager is {}", localScmServiceId);
    if (localScmServiceId == null) {
      // There is no internal scm service id is being set, fall back to ozone
      // .scm.service.ids.
      LOG.info("{} is not defined, falling back to {} to find serviceID for "
              + "StorageContainerManager if it is HA enabled cluster",
          OZONE_SCM_DEFAULT_SERVICE_ID, OZONE_SCM_SERVICE_IDS_KEY);
      scmServiceIds = conf.getTrimmedStringCollection(
          OZONE_SCM_SERVICE_IDS_KEY);
    } else {
      LOG.info("ServiceID for StorageContainerManager is {}",
          localScmServiceId);
      scmServiceIds = Collections.singleton(localScmServiceId);
    }

    localScmNodeId = conf.get(ScmConfigKeys.OZONE_SCM_NODE_ID_KEY);
    int found = 0;
    boolean isSCMddressSet = false;

    for (String serviceId : scmServiceIds) {
      Collection<String> scmNodeIds = HddsUtils.getSCMNodeIds(conf, serviceId);

      // TODO: need to fall back to ozone.scm.names in case scm node ids are
      // not defined.
      if (scmNodeIds.isEmpty()) {
        throw new IllegalArgumentException(
            String.format("Configuration does not have any value set for %s " +
                "for the service %s. List of SCM Node ID's should be " +
                "specified for an SCM service",
                ScmConfigKeys.OZONE_SCM_NODES_KEY, serviceId));
      }
      // TODO: load Ratis peers configuration
      boolean isPeer;
      List<SCMNodeDetails> peerNodesList = new ArrayList<>();
      for (String nodeId : scmNodeIds) {
        if (localScmNodeId != null && !localScmNodeId.equals(nodeId)) {
          isPeer = true;
        } else {
          isPeer = false;
        }

        String rpcAddrKey = ConfUtils.addKeySuffixes(
            OZONE_SCM_ADDRESS_KEY, serviceId, nodeId);
        String rpcAddrStr = conf.get(rpcAddrKey);
        if (rpcAddrStr == null || rpcAddrStr.isEmpty()) {
          throwConfException("Configuration does not have any value set for " +
              "%s. SCM RPC Address should be set for all nodes in a SCM " +
              "service.", rpcAddrKey);
        }
        isSCMddressSet = true;

        String ratisPortKey = ConfUtils.addKeySuffixes(OZONE_SCM_RATIS_PORT_KEY,
            serviceId, nodeId);
        int ratisPort = conf.getInt(ratisPortKey,
            conf.getInt(OZONE_SCM_RATIS_PORT_KEY,
                OZONE_SCM_RATIS_PORT_DEFAULT));

        String grpcPortKey = ConfUtils
            .addKeySuffixes(ScmConfigKeys.OZONE_SCM_GRPC_PORT_KEY, serviceId,
                nodeId);
        int grpcPort = conf.getInt(grpcPortKey,
            conf.getInt(OZONE_SCM_GRPC_PORT_KEY, OZONE_SCM_GRPC_PORT_DEFAULT));

        InetSocketAddress addr = null;
        try {
          addr = NetUtils.createSocketAddr(rpcAddrStr, ratisPort);
        } catch (Exception e) {
          LOG.error("Couldn't create socket address for SCM {} : {}", nodeId,
              rpcAddrStr, e);
          throw e;
        }

        boolean flexibleFqdnResolutionEnabled = conf.getBoolean(
                OZONE_FLEXIBLE_FQDN_RESOLUTION_ENABLED,
                OZONE_FLEXIBLE_FQDN_RESOLUTION_ENABLED_DEFAULT);
        if (OzoneNetUtils.isUnresolved(flexibleFqdnResolutionEnabled, addr)) {
          LOG.error("Address for SCM {} : {} couldn't be resolved. Proceeding "
                  + "with unresolved host to create Ratis ring.", nodeId,
              rpcAddrStr);
        }

        if (!isPeer
                && OzoneNetUtils
                .isAddressLocal(flexibleFqdnResolutionEnabled, addr)) {
          localRpcAddress = addr;
          localScmServiceId = serviceId;
          localScmNodeId = nodeId;
          localRatisPort = ratisPort;
          localGrpcPort = grpcPort;
          found++;
        } else {
          peerNodesList.add(getHASCMNodeDetails(conf, serviceId,
              nodeId, addr, ratisPort, grpcPort));
        }
      }

      if (found == 1) {
        LOG.info("Found matching SCM address with SCMServiceId: {}, " +
                "SCMNodeId: {}, RPC Address: {} and Ratis port: {}",
            localScmServiceId, localScmNodeId,
            NetUtils.getHostPortString(localRpcAddress), localRatisPort);

        // Set SCM node specific config keys.
        ConfUtils.setNodeSpecificConfigs(nodeSpecificConfigKeys, conf,
            localScmServiceId, localScmNodeId, LOG);

        return new SCMHANodeDetails(
            getHASCMNodeDetails(conf, localScmServiceId, localScmNodeId,
                localRpcAddress, localRatisPort, localGrpcPort), peerNodesList);

      } else if (found > 1) {
        throwConfException("Configuration has multiple %s addresses that " +
                "match local node's address. Please configure the system " +
                "with %s and %s", OZONE_SCM_ADDRESS_KEY,
            OZONE_SCM_SERVICE_IDS_KEY, OZONE_SCM_ADDRESS_KEY);
      }
    }

    if (!isSCMddressSet) {
      // If HA config is not set, fall back to default configuration
      return loadDefaultConfig(conf);
    } else {
      return null;
    }
  }

  public static SCMNodeDetails getHASCMNodeDetails(OzoneConfiguration conf,
      String localScmServiceId, String localScmNodeId,
      InetSocketAddress rpcAddress, int ratisPort, int grpcPort) {
    Objects.requireNonNull(localScmServiceId, "localScmServiceId == null");
    Objects.requireNonNull(localScmNodeId, "localScmNodeId == null");

    SCMNodeDetails.Builder builder = new SCMNodeDetails.Builder();
    builder
        .setRpcAddress(rpcAddress)
        .setRatisPort(ratisPort)
        .setGrpcPort(grpcPort)
        .setSCMServiceId(localScmServiceId)
        .setSCMNodeId(localScmNodeId)
        .setBlockProtocolServerAddress(
            ScmUtils.getScmBlockProtocolServerAddress(
            conf, localScmServiceId, localScmNodeId))
        .setBlockProtocolServerAddressKey(
            ScmUtils.getScmBlockProtocolServerAddressKey(
                localScmServiceId, localScmNodeId))
        .setClientProtocolServerAddress(
            ScmUtils.getClientProtocolServerAddress(conf,
            localScmServiceId, localScmNodeId))
        .setClientProtocolServerAddressKey(
            ScmUtils.getClientProtocolServerAddressKey(localScmServiceId,
                localScmNodeId))
        .setDatanodeProtocolServerAddress(
            ScmUtils.getScmDataNodeBindAddress(conf, localScmServiceId,
                localScmNodeId))
        .setDatanodeAddressKey(
            ScmUtils.getScmDataNodeBindAddressKey(localScmServiceId,
                localScmNodeId));

    return builder.build();
  }

  private static void throwConfException(String message, String... arguments)
      throws IllegalArgumentException {
    String exceptionMsg = String.format(message, arguments);
    LOG.error(exceptionMsg);
    throw new IllegalArgumentException(exceptionMsg);
  }
}
