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

import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.hdds.HddsUtils.getPortNumberFromConfigKeys;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NODES_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_PORT_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_DUMMY_NODEID;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_DUMMY_SERVICE_ID;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class which builds SCM Node Information.
 *
 * This class is used by SCM clients like OzoneManager, Client, Admin
 * commands to figure out SCM Node Information to make contact to SCM.
 */
@Immutable
public class SCMNodeInfo {

  private static final Logger LOG = LoggerFactory.getLogger(SCMNodeInfo.class);
  private final String serviceId;
  private final String nodeId;
  private final String blockClientAddress;
  private final String scmClientAddress;
  private final String scmSecurityAddress;
  private final String scmDatanodeAddress;

  /**
   * Build SCM Node information from configuration.
   * @param conf
   */
  public static List<SCMNodeInfo> buildNodeInfo(ConfigurationSource conf) {

    // First figure out scm client address from HA style config.
    // If service Id is not defined, fall back to non-HA config.

    List<SCMNodeInfo> scmNodeInfoList = new ArrayList<>();
    String scmServiceId = HddsUtils.getScmServiceId(conf);
    if (scmServiceId != null) {
      LOG.info("{} for StorageContainerManager is {}", OZONE_SCM_SERVICE_IDS_KEY, scmServiceId);
      ArrayList< String > scmNodeIds = new ArrayList<>(
          HddsUtils.getSCMNodeIds(conf, scmServiceId));
      if (scmNodeIds.isEmpty()) {
        throw new ConfigurationException(
            String.format("Configuration does not have any value set for %s " +
                    "for the SCM serviceId %s. List of SCM Node ID's should " +
                    "be specified for an SCM HA service", OZONE_SCM_NODES_KEY,
                scmServiceId));
      }

      for (String scmNodeId : scmNodeIds) {
        String addressKey = ConfUtils.addKeySuffixes(
            OZONE_SCM_ADDRESS_KEY, scmServiceId, scmNodeId);
        String scmAddress = conf.get(addressKey);
        if (scmAddress == null) {
          throw new ConfigurationException(addressKey + "is not defined");
        }

        // Get port from Address Key if defined, else fall back to port key.
        int scmClientPort = getPort(conf, scmServiceId, scmNodeId,
            OZONE_SCM_CLIENT_ADDRESS_KEY, OZONE_SCM_CLIENT_PORT_KEY,
            OZONE_SCM_CLIENT_PORT_DEFAULT);

        int scmBlockClientPort = getPort(conf, scmServiceId, scmNodeId,
            OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
            OZONE_SCM_BLOCK_CLIENT_PORT_KEY,
            OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT);

        int scmSecurityPort = getPort(conf, scmServiceId, scmNodeId,
            OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY,
            OZONE_SCM_SECURITY_SERVICE_PORT_KEY,
            OZONE_SCM_SECURITY_SERVICE_PORT_DEFAULT);

        int scmDatanodePort = getPort(conf, scmServiceId, scmNodeId,
            OZONE_SCM_DATANODE_ADDRESS_KEY, OZONE_SCM_DATANODE_PORT_KEY,
            OZONE_SCM_DATANODE_PORT_DEFAULT);

        scmNodeInfoList.add(new SCMNodeInfo(scmServiceId, scmNodeId,
            buildAddress(scmAddress, scmBlockClientPort),
            buildAddress(scmAddress, scmClientPort),
            buildAddress(scmAddress, scmSecurityPort),
            buildAddress(scmAddress, scmDatanodePort)));
      }
      return scmNodeInfoList;
    } else {
      scmServiceId = SCM_DUMMY_SERVICE_ID;
      LOG.info("ServiceID for StorageContainerManager is dummy service ID {}", scmServiceId);

      // Following current approach of fall back to
      // OZONE_SCM_CLIENT_ADDRESS_KEY to figure out hostname.

      String scmBlockClientAddress = getHostNameFromConfigKeys(conf,
          OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
          OZONE_SCM_CLIENT_ADDRESS_KEY).orElse(null);

      String scmClientAddress = getHostNameFromConfigKeys(conf,
          OZONE_SCM_CLIENT_ADDRESS_KEY).orElse(null);

      String scmSecurityClientAddress =
          getHostNameFromConfigKeys(conf,
              OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY,
              OZONE_SCM_CLIENT_ADDRESS_KEY).orElse(null);

      String scmDatanodeAddress =
          getHostNameFromConfigKeys(conf,
              OZONE_SCM_DATANODE_ADDRESS_KEY,
              OZONE_SCM_CLIENT_ADDRESS_KEY, OZONE_SCM_NAMES).orElse(null);

      int scmBlockClientPort = getPortNumberFromConfigKeys(conf,
          OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY)
          .orElse(conf.getInt(OZONE_SCM_BLOCK_CLIENT_PORT_KEY,
              OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT));

      int scmClientPort = getPortNumberFromConfigKeys(conf,
          OZONE_SCM_CLIENT_ADDRESS_KEY)
          .orElse(conf.getInt(OZONE_SCM_CLIENT_PORT_KEY,
              OZONE_SCM_CLIENT_PORT_DEFAULT));

      int scmSecurityPort = getPortNumberFromConfigKeys(conf,
          OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY)
          .orElse(conf.getInt(OZONE_SCM_SECURITY_SERVICE_PORT_KEY,
              OZONE_SCM_SECURITY_SERVICE_PORT_DEFAULT));

      int scmDatanodePort = getPortNumberFromConfigKeys(conf,
          OZONE_SCM_DATANODE_ADDRESS_KEY)
          .orElse(conf.getInt(OZONE_SCM_DATANODE_PORT_KEY,
              OZONE_SCM_DATANODE_PORT_DEFAULT));

      scmNodeInfoList.add(new SCMNodeInfo(scmServiceId,
          SCM_DUMMY_NODEID,
          scmBlockClientAddress == null ? null :
              buildAddress(scmBlockClientAddress, scmBlockClientPort),
          scmClientAddress == null ? null :
              buildAddress(scmClientAddress, scmClientPort),
          scmSecurityClientAddress == null ? null :
              buildAddress(scmSecurityClientAddress, scmSecurityPort),
          scmDatanodeAddress == null ? null :
              buildAddress(scmDatanodeAddress, scmDatanodePort)));

      return scmNodeInfoList;

    }

  }

  private static String buildAddress(String address, int port) {
    return new StringBuilder().append(address).append(':')
        .append(port).toString();
  }

  private static int getPort(ConfigurationSource conf,
      String scmServiceId, String scmNodeId, String configKey,
      String portKey, int defaultPort) {
    String suffixKey = ConfUtils.addKeySuffixes(configKey, scmServiceId,
        scmNodeId);
    OptionalInt port = getPortNumberFromConfigKeys(conf, suffixKey);

    if (port.isPresent()) {
      LOG.info("ConfigKey {} is deprecated, For configuring different " +
          "ports for each SCM use PortConfigKey {} appended with serviceId " +
          "and nodeId", configKey, portKey);
      return port.getAsInt();
    } else {
      return conf.getInt(ConfUtils.addKeySuffixes(portKey, scmServiceId,
          scmNodeId), conf.getInt(portKey, defaultPort));
    }
  }

  /**
   * SCM Node Info which contains information about scm service address.
   * @param serviceId
   * @param nodeId
   * @param blockClientAddress
   * @param scmClientAddress
   * @param scmSecurityAddress
   * @param scmDatanodeAddress
   */
  public SCMNodeInfo(String serviceId, String nodeId,
      String blockClientAddress, String scmClientAddress,
      String scmSecurityAddress, String scmDatanodeAddress) {
    this.serviceId = serviceId;
    this.nodeId = nodeId;
    this.blockClientAddress = blockClientAddress;
    this.scmClientAddress = scmClientAddress;
    this.scmSecurityAddress = scmSecurityAddress;
    this.scmDatanodeAddress = scmDatanodeAddress;
  }

  public String getServiceId() {
    return serviceId;
  }

  public String getNodeId() {
    return nodeId;
  }

  public String getBlockClientAddress() {
    return blockClientAddress;
  }

  public String getScmClientAddress() {
    return scmClientAddress;
  }

  public String getScmSecurityAddress() {
    return scmSecurityAddress;
  }

  public String getScmDatanodeAddress() {
    return scmDatanodeAddress;
  }
}
