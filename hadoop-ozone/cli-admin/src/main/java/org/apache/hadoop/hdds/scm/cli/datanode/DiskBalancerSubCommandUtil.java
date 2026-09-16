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

package org.apache.hadoop.hdds.scm.cli.datanode;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_CLIENT_PORT_DEFAULT;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port;
import org.apache.hadoop.hdds.protocol.DiskBalancerProtocol;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocolPB.DiskBalancerProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * DiskBalancer subcommand utilities.
 */
final class DiskBalancerSubCommandUtil {

  private static final Pattern DATANODE_UUID_PATTERN = Pattern.compile(
      "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

  static final class DatanodeTarget {
    private final String clientRpcAddress;
    private final String displayName;

    DatanodeTarget(String clientRpcAddress, String displayName) {
      this.clientRpcAddress = clientRpcAddress;
      this.displayName = displayName;
    }

    String getClientRpcAddress() {
      return clientRpcAddress;
    }

    String getDisplayName() {
      return displayName;
    }
  }

  private DiskBalancerSubCommandUtil() {
  }

  /**
   * Returns true if the argument is a canonical datanode UUID rather than a host or address.
   */
  static boolean isDatanodeUuid(String nodeArg) {
    if (!DATANODE_UUID_PATTERN.matcher(nodeArg).matches()) {
      return false;
    }
    try {
      UUID.fromString(nodeArg);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Normalizes {@code --node-id} values, including comma-separated lists and trailing commas when
   * the shell splits {@code uuid1, uuid2} into separate arguments.
   */
  static List<String> normalizeNodeIds(List<String> rawNodeIds) {
    List<String> normalized = new ArrayList<>();
    if (rawNodeIds == null) {
      return normalized;
    }
    for (String rawNodeId : rawNodeIds) {
      if (rawNodeId == null || rawNodeId.isEmpty()) {
        continue;
      }
      for (String nodeId : rawNodeId.split(",\\s*")) {
        String trimmed = nodeId.trim();
        if (!trimmed.isEmpty()) {
          normalized.add(trimmed);
        }
      }
    }
    return normalized;
  }

  /**
   * Resolves a datanode hostname or host:port to a CLIENT_RPC address without contacting SCM.
   */
  static DatanodeTarget resolveDatanodeAddress(String nodeArg) {
    return new DatanodeTarget(nodeArg, nodeArg);
  }

  /**
   * Resolves a datanode UUID to a CLIENT_RPC address via SCM.
   */
  static DatanodeTarget resolveDatanodeTargetByUuid(ScmClient scmClient, String nodeUuid)
      throws IOException {
    if (!isDatanodeUuid(nodeUuid)) {
      throw new IOException("Invalid datanode UUID: " + nodeUuid);
    }

    HddsProtos.Node node = scmClient.queryNode(UUID.fromString(nodeUuid));
    HddsProtos.DatanodeDetailsProto nodeId = node.getNodeID();
    if (!node.hasNodeID() || (!nodeId.hasUuid() && !nodeId.hasUuid128() && !nodeId.hasId())) {
      throw new IOException("Datanode not found: " + nodeUuid);
    }

    DatanodeDetails details = DatanodeDetails.getFromProtoBuf(nodeId);
    if (details.getIpAddress() == null || details.getIpAddress().isEmpty()) {
      throw new IOException("Datanode not found: " + nodeUuid);
    }

    String address = getClientRpcAddress(details);
    return new DatanodeTarget(address, nodeUuid);
  }

  /**
   * Resolves a datanode identifier to a CLIENT_RPC address.
   * Accepts datanode UUID, hostname, or host:port.
   */
  static DatanodeTarget resolveDatanodeTarget(ScmClient scmClient, String nodeArg)
      throws IOException {
    if (!isDatanodeUuid(nodeArg)) {
      return resolveDatanodeAddress(nodeArg);
    }
    return resolveDatanodeTargetByUuid(scmClient, nodeArg);
  }

  /**
   * Creates a DiskBalancerProtocol proxy for a single datanode.
   * 
   * <p>The address can be provided in two formats:
   * <ul>
   *   <li>"hostname:port" - Uses the specified port</li>
   *   <li>"hostname" - Uses the default CLIENT_RPC port (HDDS_DATANODE_CLIENT_PORT_DEFAULT)</li>
   * </ul>
   * 
   * @param address the datanode address in "host:port" or "host" format
   * @return DiskBalancerProtocol proxy
   * @throws IOException if proxy creation fails
   */
  public static DiskBalancerProtocol getSingleNodeDiskBalancerProxy(
      String address) throws IOException {
    OzoneConfiguration ozoneConf = new OzoneConfiguration();
    UserGroupInformation user = UserGroupInformation.getCurrentUser();
    
    // Parse address and add default port if not specified
    InetSocketAddress nodeAddr;
    if (address.contains(":")) {
      // Port is specified, use NetUtils to parse
      nodeAddr = NetUtils.createSocketAddr(address);
    } else {
      // Port not specified, use default
      nodeAddr = NetUtils.createSocketAddr(address, HDDS_DATANODE_CLIENT_PORT_DEFAULT);
    }
    return new DiskBalancerProtocolClientSideTranslatorPB(
        nodeAddr, user, ozoneConf);
  }

  /**
   * Retrieves all IN_SERVICE and HEALTHY datanode addresses with their hostnames from SCM.
   * Used for batch operations with --in-service-datanodes flag.
   *
   * @param scmClient the SCM client
   * @return map of address (ip:port) to display string (hostname (ip:port) or ip:port)
   * @throws IOException if SCM query fails
   */
  public static Map<String, String> getAllOperableNodesClientRpcAddress(
      ScmClient scmClient) throws IOException {
    List<HddsProtos.Node> nodes = scmClient.queryNode(
        NodeOperationalState.IN_SERVICE, HddsProtos.NodeState.HEALTHY,
        HddsProtos.QueryScope.CLUSTER, "");

    Map<String, String> addressToDisplay = new LinkedHashMap<>();
    for (HddsProtos.Node node : nodes) {
      DatanodeDetails details =
          DatanodeDetails.getFromProtoBuf(node.getNodeID());
      if (node.getNodeStates(0).equals(HddsProtos.NodeState.DEAD)) {
        continue;
      }
      try {
        String address = getClientRpcAddress(details);
        addressToDisplay.put(address, getDatanodeHostAndIp(node.getNodeID()));
      } catch (IOException e) {
        System.err.println(e.getMessage());
      }
    }

    return addressToDisplay;
  }

  static String getClientRpcAddress(DatanodeDetails details) throws IOException {
    Port port = details.getPort(Port.Name.CLIENT_RPC);
    if (port == null) {
      throw new IOException(String.format("host: %s(%s) %s port not found",
          details.getHostName(), details.getIpAddress(), Port.Name.CLIENT_RPC.name()));
    }
    return details.getIpAddress() + ":" + port.getValue();
  }

  /**
   * Returns a formatted string combining hostname and IP address from DatanodeDetailsProto.
   * Format: {@code hostname (ip:port)} or {@code ip:port}.
   *
   * @param nodeProto the DatanodeDetailsProto from the diskbalancer info
   * @return formatted datanode identifier for status/report output
   */
  public static String getDatanodeHostAndIp(HddsProtos.DatanodeDetailsProto nodeProto) {
    String hostname = nodeProto.getHostName();
    String ipAddress = nodeProto.getIpAddress();
    int port = nodeProto.getPortsList().stream()
        .filter(p -> p.getName().equals(
            DatanodeDetails.Port.Name.CLIENT_RPC.name()))
        .mapToInt(HddsProtos.Port::getValue)
        .findFirst()
        .orElse(HDDS_DATANODE_CLIENT_PORT_DEFAULT); // Default port if not found

    String addressPort = ipAddress + ":" + port;
    if (hostname != null && !hostname.isEmpty() && !hostname.equals(ipAddress)) {
      return hostname + " (" + addressPort + ")";
    }
    return addressPort;
  }
}
