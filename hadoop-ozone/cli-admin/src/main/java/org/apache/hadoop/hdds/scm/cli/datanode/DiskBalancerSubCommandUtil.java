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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
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
   * Retrieves all IN_SERVICE datanode addresses from SCM.
   * Used for batch operations with --in-service-datanodes flag.
   * 
   * @param scmClient the SCM client
   * @return list of datanode addresses in "ip:port" format
   * @throws IOException if SCM query fails
   */
  public static List<String> getAllOperableNodesClientRpcAddress(
      ScmClient scmClient) throws IOException {
    List<HddsProtos.Node> nodes = scmClient.queryNode(
        NodeOperationalState.IN_SERVICE, HddsProtos.NodeState.HEALTHY,
        HddsProtos.QueryScope.CLUSTER, "");

    List<String> addresses = new ArrayList<>();
    for (HddsProtos.Node node : nodes) {
      DatanodeDetails details =
          DatanodeDetails.getFromProtoBuf(node.getNodeID());
      if (node.getNodeStates(0).equals(HddsProtos.NodeState.DEAD)) {
        continue;
      }
      Port port = details.getPort(Port.Name.CLIENT_RPC);
      if (port != null) {
        // Use IP address for reliable connection (hostnames with underscores may not be valid)
        addresses.add(details.getIpAddress() + ":" + port.getValue());
      } else {
        System.out.printf("host: %s(%s) %s port not found%n",
            details.getHostName(), details.getIpAddress(),
            Port.Name.CLIENT_RPC.name());
      }
    }

    return addresses;
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
   * Gets the hostname and IP address for a datanode given its address (hostname or IP) and port.
   * Queries SCM to find the matching datanode and returns both hostname and IP address.
   * Internal helper method used by getDatanodeHostAndIp(ScmClient, String).
   * 
   * @param scmClient the SCM client
   * @param address the hostname or IP address of the datanode
   * @param port the port of the datanode
   * @return array with [hostname, ipAddress] if found, null otherwise
   */
  private static String[] getHostnameAndIpFromAddress(ScmClient scmClient,
      String address, int port) {
    try {
      // Resolve hostname to IP if it's a hostname (not an IP address)
      String ipToMatch = address;
      try {
        // Try to resolve - if it's already an IP, getByName will return it
        InetAddress inetAddr = InetAddress.getByName(address);
        ipToMatch = inetAddr.getHostAddress();
      } catch (UnknownHostException e) {
        // If resolution fails, use original address
      }
      
      List<HddsProtos.Node> nodes = scmClient.queryNode(
          NodeOperationalState.IN_SERVICE, null,
          HddsProtos.QueryScope.CLUSTER, "");
      
      for (HddsProtos.Node node : nodes) {
        DatanodeDetails details =
            DatanodeDetails.getFromProtoBuf(node.getNodeID());
        Port datanodePort = details.getPort(Port.Name.CLIENT_RPC);
        if (datanodePort != null && datanodePort.getValue() == port) {
          String hostname = details.getHostName();
          String ipAddress = details.getIpAddress();
          // Match by IP address (more reliable than hostname matching)
          if (ipToMatch.equals(ipAddress)) {
            return new String[]{hostname, ipAddress};
          }
        }
      }
    } catch (IOException e) {
      // Return null if query fails
    }
    return null;
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

  /**
   * Parses "hostname:port", "ip:port", or "hostname" address, queries SCM to get both hostname and IP.
   * Returns a formatted string combining hostname and IP address.
   * 
   * @param scmClient the SCM client
   * @param address the datanode address in "hostname:port", "ip:port", or "hostname" format
   * @return formatted string "hostname (ip:port)" or "ip:port" if hostname is not available
   */
  public static String getDatanodeHostAndIp(ScmClient scmClient,
      String address) {
    if (address == null || address.isEmpty()) {
      return address;
    }
    
    String addressPart;
    int port;
    
    // Parse address - handle both "hostname:port" and "hostname" formats
    String[] parts = address.split(":");
    if (parts.length == 2) {
      // Format: "hostname:port" or "ip:port"
      addressPart = parts[0];
      try {
        port = Integer.parseInt(parts[1]);
      } catch (NumberFormatException e) {
        return address;
      }
    } else if (parts.length == 1) {
      // Format: "hostname" or "ip" - use default port
      addressPart = parts[0];
      port = HDDS_DATANODE_CLIENT_PORT_DEFAULT;
    } else {
      // Invalid format
      return address;
    }
    
    // Query SCM to get both hostname and IP address
    String[] hostnameAndIp = getHostnameAndIpFromAddress(scmClient, addressPart, port);
    if (hostnameAndIp != null && hostnameAndIp.length == 2) {
      String hostname = hostnameAndIp[0];
      String ipAddress = hostnameAndIp[1];
      return getDatanodeHostAndIp(hostname, ipAddress, port);
    }
    
    // If not found in SCM, return the original address
    return address;
  }
}

