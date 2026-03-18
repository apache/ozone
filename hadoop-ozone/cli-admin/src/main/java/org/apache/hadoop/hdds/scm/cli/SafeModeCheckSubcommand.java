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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB.ScmNodeTarget;
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
public class SafeModeCheckSubcommand extends AbstractSubcommand implements Callable<Void> {
  @CommandLine.Mixin
  private ScmOption scmOption;

  @CommandLine.Option(names = {"--all", "-a"},
      description = "Show safe mode status for all SCM nodes in the service. " +
          "When multiple SCM service IDs are configured, --service-id must be specified.")
  private boolean allNodes;

  private String serviceId;
  private List<SCMNodeInfo> nodes;

  @Override
  public Void call() throws Exception {
    OzoneConfiguration conf = getOzoneConf();
    serviceId = HddsUtils.getScmServiceId(conf);
    String scmAddress = scmOption.getScm();

    ScmNodeTarget targetScmNode = new ScmNodeTarget();
    try (ScmClient scmClient = scmOption.createScmClient(conf, targetScmNode)) {
      nodes = SCMNodeInfo.buildNodeInfo(conf);

      if (serviceId != null) {
        System.out.println("Service ID: " + serviceId);
      }
      
      if (allNodes) {
        executeForAllNodes(scmClient, targetScmNode);
      } else if (StringUtils.isNotEmpty(scmAddress)) {
        executeForSpecificNode(scmClient, targetScmNode, scmAddress);
      } else {
        executeForSingleNode(scmClient, targetScmNode);
      }
    }
    return null;
  }

  private void executeForSingleNode(ScmClient scmClient, ScmNodeTarget targetScmNode) throws IOException {
    SCMNodeInfo targetNode;
    if (serviceId != null) {
      // HA mode: find leader
      targetNode = findLeaderNode(scmClient);
      if (targetNode == null) {
        throw new IOException("Could not determine leader node");
      }
    } else {
      // Non-HA mode: use single node
      targetNode = nodes.get(0);
    }
    
    queryNode(scmClient, targetScmNode, targetNode);
  }

  /**
   * Find the leader node from SCM roles.
   * @param scmClient the SCM client
   * @return the leader SCMNodeInfo
   */
  private SCMNodeInfo findLeaderNode(ScmClient scmClient) throws IOException {
    try {
      List<String> roles = scmClient.getScmRoles();
      for (String role : roles) {
        String[] parts = role.split(":");
        if (parts.length < 3 || !"LEADER".equalsIgnoreCase(parts[2])) {
          continue;
        }
        String leaderHost = parts[0];
        String leaderIp = parts.length >= 5 ? parts[4] : null;
        for (SCMNodeInfo node : nodes) {
          String nodeHost = node.getScmClientAddress().split(":")[0];

          if (matchesAddress(leaderHost, nodeHost) || (leaderIp != null && !leaderIp.isEmpty() &&
                  matchesAddress(leaderIp, nodeHost))) {
            return node;
          }
        }
      }

      return null;
    } catch (IOException e) {
      throw new IOException("Could not determine leader node", e);
    }
  }

  private void executeForSpecificNode(ScmClient scmClient, ScmNodeTarget targetScmNode, 
      String scmAddress) throws IOException {
    SCMNodeInfo matchedNode = nodes.stream()
        .filter(node -> matchesAddress(node.getScmClientAddress(), scmAddress))
        .findFirst()
        .orElseThrow(() -> new IOException("Specified --scm address " + scmAddress +
            " does not match any node in service " + serviceId +
            ". Nodes: " + nodes.stream()
            .map(n -> n.getScmClientAddress() + " [" + n.getNodeId() + "]")
            .collect(Collectors.joining(", "))));
    
    queryNode(scmClient, targetScmNode, matchedNode);
  }

  private void executeForAllNodes(ScmClient scmClient, ScmNodeTarget targetScmNode) throws IOException {
    for (SCMNodeInfo node : nodes) {
      queryNode(scmClient, targetScmNode, node);
    }
  }

  private void queryNode(ScmClient scmClient, ScmNodeTarget targetScmNode, SCMNodeInfo node) {
    String nodeId = node.getNodeId();
    
    try {
      // Set the targetScmNode to target this specific node
      targetScmNode.setNodeId(nodeId);
      
      boolean inSafeMode = scmClient.inSafeMode();

      if (serviceId != null) {
        System.out.printf("%s [%s]: %s%n",
            node.getScmClientAddress(),
            nodeId,
            inSafeMode ? "in safe mode" : "out of safe mode");
      } else {
        System.out.printf("SCM is %s safe mode.%n", inSafeMode ? "in" : "out of");
      }

      if (isVerbose()) {
        Map<String, Pair<Boolean, String>> rules = scmClient.getSafeModeRuleStatuses();
        if (rules != null && !rules.isEmpty()) {
          printSafeModeRules(rules);
        }
      }
    } catch (Exception e) {
      System.out.printf("%s [%s]: ERROR: Failed to get safe mode status for SCM node: %s%n",
          node.getScmClientAddress(), nodeId, e.getMessage());
    }
  }

  /**
   * Check if the given SCMNodeInfo matches the target address.
   * Tries to match by direct string comparison and by resolved address.
   */
  private boolean matchesAddress(String address1, String address2) {
    if (address1.equalsIgnoreCase(address2)) {
      return true;
    }

    try {
      // Parse both addresses into host:port components
      String[] parts1 = address1.split(":", 2);
      String[] parts2 = address2.split(":", 2);

      String host1 = parts1[0];
      String host2 = parts2[0];
      
      // Hostnames must match
      if (!host1.equalsIgnoreCase(host2)) {
        return false;
      }

      // If both have ports specified, they must match
      if (parts1.length > 1 && parts2.length > 1) {
        return parts1[1].equals(parts2[1]);
      }

      return true;
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
