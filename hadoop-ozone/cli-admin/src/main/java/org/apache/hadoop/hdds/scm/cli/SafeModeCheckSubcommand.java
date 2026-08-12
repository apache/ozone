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
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
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
        String[] parts;
        try {
          parts = HddsUtils.parseRatisRoleString(role);
        } catch (IllegalArgumentException e) {
          continue;
        }
        if (!"LEADER".equalsIgnoreCase(parts[2])) {
          continue;
        }
        String leaderHost = parts[0];
        String leaderIp = parts[4];
        for (SCMNodeInfo node : nodes) {
          String nodeHost = HddsUtils.getHostName(node.getScmClientAddress()).orElse("");

          if (matchesAddress(leaderHost, nodeHost) || (!leaderIp.isEmpty() &&
                  matchesAddress(leaderIp, nodeHost))) {
            return node;
          }
        }
      }

      return null;
    } catch (IOException e) {
      throw new IOException("Could not determine leader node. " + e.getMessage(), e);
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
      rootCommand().printError(e);
    }
  }

  /**
   * Check if the given addresses match by comparing host portions and ports.
   * Inputs may be bare hosts or host:port strings. Handles IPv6 equivalence
   * (e.g. 2001:db8::1 vs 2001:db8:0:0:0:0:0:1) by resolving to InetAddress.
   */
  private boolean matchesAddress(String address1, String address2) {
    if (address1.equalsIgnoreCase(address2)) {
      return true;
    }

    try {
      String host1 = HddsUtils.getHostName(address1).orElse(address1);
      String host2 = HddsUtils.getHostName(address2).orElse(address2);

      boolean hostsMatch = host1.equalsIgnoreCase(host2);
      if (!hostsMatch) {
        InetAddress inet1 = InetAddress.getByName(host1);
        InetAddress inet2 = InetAddress.getByName(host2);
        hostsMatch = inet1.equals(inet2);
      }
      if (!hostsMatch) {
        return false;
      }

      OptionalInt port1 = HddsUtils.getHostPort(address1);
      OptionalInt port2 = HddsUtils.getHostPort(address2);
      if (port1.isPresent() && port2.isPresent()) {
        return port1.getAsInt() == port2.getAsInt();
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
