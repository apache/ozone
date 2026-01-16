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

package org.apache.hadoop.hdds.scm.cli;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.net.NetUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * This is the handler that process safe mode check command.
 */
@Command(
    name = "status",
    description = "Check if SCM is in safe mode",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class SafeModeCheckSubcommand extends ScmSubcommand {
  @CommandLine.Option(names = {"--all", "-a"},
      description = "Show safe mode status for all SCM nodes in the service. " +
          "When multiple SCM service IDs are configured, --service-id must be specified.")
  private boolean allNodes;

  private String serviceId;
  private List<SCMNodeInfo> nodes;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    OzoneConfiguration conf = getOzoneConf();
    serviceId = HddsUtils.getScmServiceId(conf);
    String scmAddress = getScmOption().getScm();
    if (serviceId != null) {
      nodes = SCMNodeInfo.buildNodeInfo(conf);
    }
    
    if (allNodes) {
      executeForAllNodes(scmClient);
    } else if (StringUtils.isNotEmpty(scmAddress)) {
      executeForSpecificNode(scmClient, scmAddress);
    } else {
      executeForSingleNode(scmClient);
    }
  }

  private void executeForSingleNode(ScmClient scmClient) throws IOException {
    boolean inSafeMode;
    Map<String, Pair<Boolean, String>> rules = null;
    String leaderNodeId;

    // If SCM HA mode, query the leader node.
    if (serviceId != null) {
      leaderNodeId = findLeaderNodeId(scmClient);
      inSafeMode = scmClient.inSafeModeForNode(leaderNodeId);
      if (isVerbose()) {
        rules = scmClient.getSafeModeRuleStatusesForNode(leaderNodeId);
      }
    } else {
      // Non-HA mode
      inSafeMode = scmClient.inSafeMode();
      if (isVerbose()) {
        rules = scmClient.getSafeModeRuleStatuses();
      }
    }
    
    if (inSafeMode) {
      System.out.println("SCM is in safe mode.");
    } else {
      System.out.println("SCM is out of safe mode.");
    }
    if (isVerbose() && rules != null) {
      printSafeModeRules(rules);
    }
  }

  /**
   * Find the leader node ID from SCM roles.
   * @param scmClient the SCM client
   * @return the leader node ID, or null if not found
   */
  private String findLeaderNodeId(ScmClient scmClient) throws IOException {
    try {
      List<String> roles = scmClient.getScmRoles();
      for (String role : roles) {
        String[] parts = role.split(":");
        if (parts.length >= 3 && "LEADER".equalsIgnoreCase(parts[2])) {
          String leaderHostname = parts[0];
          for (SCMNodeInfo node : nodes) {
            String nodeHostname = node.getScmClientAddress().split(":")[0];
            if (nodeHostname.equalsIgnoreCase(leaderHostname)) {
              return node.getNodeId();
            }
          }
        }
      }
      return null;
    } catch (IOException e) {
      throw new IOException("Could not determine leader node for service: " + serviceId, e);
    }
  }

  private void executeForSpecificNode(ScmClient scmClient, String scmAddress) throws IOException {
    if (serviceId == null) {
      executeForSingleNode(scmClient);
      return;
    }

    System.out.println("Service ID: " + serviceId);
    // Find the node matching the --scm address
    List<SCMNodeInfo> matchedNodes = nodes.stream()
        .filter(node -> matchesAddress(node, scmAddress))
        .collect(Collectors.toList());

    if (matchedNodes.isEmpty()) {
      throw new IOException("Specified --scm address " + scmAddress +
          " does not match any node in service " + serviceId +
          ". Available nodes: " + nodes.stream()
              .map(n -> n.getScmClientAddress() + " [" + n.getNodeId() + "]")
              .collect(Collectors.joining(", ")));
    }
    
    queryNode(scmClient, matchedNodes.get(0));
  }

  private void executeForAllNodes(ScmClient scmClient) throws IOException {
    if (serviceId == null) {
      executeForSingleNode(scmClient);
      return;
    }

    System.out.println("Service ID: " + serviceId);

    for (SCMNodeInfo node : nodes) {
      queryNode(scmClient, node);
    }
  }

  private void queryNode(ScmClient scmClient, SCMNodeInfo node) {
    String nodeId = node.getNodeId();
    
    try {
      boolean inSafeMode = scmClient.inSafeModeForNode(nodeId);

      System.out.printf("%s [%s]: %s%n",
          node.getScmClientAddress(),
          nodeId,
          inSafeMode ? "IN SAFE MODE" : "OUT OF SAFE MODE");

      if (isVerbose()) {
        Map<String, Pair<Boolean, String>> rules = scmClient.getSafeModeRuleStatusesForNode(nodeId);
        if (rules != null && !rules.isEmpty()) {
          printSafeModeRules(rules);
        }
      }
    } catch (Exception e) {
      System.out.printf("%s [%s]: ERROR: Failed to get safe mode status - %s%n",
          node.getScmClientAddress(), nodeId, e.getMessage());
    }
  }

  /**
   * Check if the given SCMNodeInfo matches the target address.
   * Tries to match by direct string comparison and by resolved address.
   */
  private boolean matchesAddress(SCMNodeInfo node, String targetAddress) {
    String nodeAddress = node.getScmClientAddress();

    // Direct match
    if (nodeAddress.equals(targetAddress)) {
      return true;
    }

    // Try normalizing both addresses and comparing
    try {
      InetSocketAddress target = NetUtils.createSocketAddr(targetAddress);
      InetSocketAddress nodeAddr = NetUtils.createSocketAddr(nodeAddress);

      // Match by resolved IP and port
      return target.getPort() == nodeAddr.getPort() &&
          target.getAddress().equals(nodeAddr.getAddress());
    } catch (Exception e) {
      // If address resolution fails, no match
      return false;
    }
  }
  
  private void printSafeModeRules(Map<String, Pair<Boolean, String>> rules) {
    for (Map.Entry<String, Pair<Boolean, String>> entry : rules.entrySet()) {
      Pair<Boolean, String> value = entry.getValue();
      System.out.printf("validated:%s, %s, %s%n",
          value.getLeft(), entry.getKey(), value.getRight());
    }
  }
}
