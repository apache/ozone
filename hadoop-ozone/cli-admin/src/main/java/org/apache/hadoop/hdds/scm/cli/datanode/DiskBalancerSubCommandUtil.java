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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

  private DiskBalancerSubCommandUtil() {
  }

  /**
   * Creates a DiskBalancerProtocol proxy for a single datanode.
   * 
   * <p>The address can be provided in two formats:
   * <ul>
   *   <li>"hostname:port" - Uses the specified port</li>
   *   <li>"hostname" - Uses the default CLIENT_RPC port (19864)</li>
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
    InetSocketAddress nodeAddr = parseAddress(address, HDDS_DATANODE_CLIENT_PORT_DEFAULT);
    
    return new DiskBalancerProtocolClientSideTranslatorPB(
        nodeAddr, user, ozoneConf);
  }

  /**
   * Parses a datanode address string and returns an InetSocketAddress.
   * If the address doesn't contain a port, uses the provided default port.
   * 
   * @param address the address string (e.g., "host:port" or "host")
   * @param defaultPort the default port to use if not specified
   * @return InetSocketAddress with the parsed or default port
   */
  private static InetSocketAddress parseAddress(String address, int defaultPort) {
    if (address.contains(":")) {
      // Port is specified, use NetUtils to parse
      return NetUtils.createSocketAddr(address);
    } else {
      // Port not specified, use default
      return NetUtils.createSocketAddr(address, defaultPort);
    }
  }

  /**
   * Retrieves all IN_SERVICE datanode addresses with their hostnames from SCM.
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
      Port port = details.getPort(Port.Name.CLIENT_RPC);
      if (port != null) {
        String address = details.getIpAddress() + ":" + port.getValue();
        String display = getDatanodeHostAndIp(
            details.getHostName(), details.getIpAddress(), port.getValue());
        addressToDisplay.put(address, display);
      } else {
        System.out.printf("host: %s(%s) %s port not found%n",
            details.getHostName(), details.getIpAddress(),
            Port.Name.CLIENT_RPC.name());
      }
    }

    return addressToDisplay;
  }

  /**
   * Extracts hostname, IP address, and port from a DatanodeDetailsProto of status and report.
   *
   * @param nodeProto the DatanodeDetailsProto from the diskbalancer info
   * @return array with [hostname, ipAddress, port] where port is the CLIENT_RPC port
   */
  public static String[] extractHostIpAndPort(HddsProtos.DatanodeDetailsProto nodeProto) {
    String hostname = nodeProto.getHostName();
    String ipAddress = nodeProto.getIpAddress();
    int port = nodeProto.getPortsList().stream()
        .filter(p -> p.getName().equals(
            DatanodeDetails.Port.Name.CLIENT_RPC.name()))
        .mapToInt(HddsProtos.Port::getValue)
        .findFirst()
        .orElse(19864); // Default port if not found
    return new String[]{hostname, ipAddress, String.valueOf(port)};
  }

  /**
   * Returns a formatted string combining hostname and IP address.
   * If hostname is null or empty, returns just "ip:port".
   * 
   * @param hostname the hostname of the datanode
   * @param ipAddress the IP address of the datanode
   * @param port the port of the datanode
   * @return formatted string "hostname (ip:port)" or "ip:port" if hostname is not available
   */
  public static String getDatanodeHostAndIp(String hostname,
      String ipAddress, int port) {
    String addressPort = ipAddress + ":" + port;
    if (hostname != null && !hostname.isEmpty() && !hostname.equals(ipAddress)) {
      return hostname + " (" + addressPort + ")";
    }
    return addressPort;
  }

}

