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

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.shell.ListLimitOptions;
import picocli.CommandLine;

/**
 * Handler of list datanodes info command.
 */
@CommandLine.Command(
    name = "list",
    description = "List info of datanodes",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ListInfoSubcommand extends ScmSubcommand {

  @CommandLine.Option(names = {"--operational-state"},
      description = "Show info by datanode NodeOperationalState(" +
          "IN_SERVICE, " +
          "DECOMMISSIONING, DECOMMISSIONED, " +
          "ENTERING_MAINTENANCE, IN_MAINTENANCE).",
      defaultValue = "")
  private String nodeOperationalState;

  @CommandLine.Option(names = {"--node-state"},
      description = "Show info by datanode NodeState(" +
      " HEALTHY, STALE, DEAD)",
      defaultValue = "")
  private String nodeState;

  @CommandLine.Option(names = { "--json" },
       description = "Show info in json format",
       defaultValue = "false")
  private boolean json;

  @CommandLine.ArgGroup(exclusive = true, multiplicity = "0..1")
  private ExclusiveNodeOptions exclusiveNodeOptions;

  @CommandLine.Mixin
  private ListLimitOptions listLimitOptions;

  private List<Pipeline> pipelines;

  static class ExclusiveNodeOptions extends NodeSelectionMixin {
    @CommandLine.Option(names = {"--most-used"},
        description = "Show datanodes sorted by Utilization (most to least).")
    private boolean mostUsed;

    @CommandLine.Option(names = {"--least-used"},
        description = "Show datanodes sorted by Utilization (least to most).")
    private boolean leastUsed;
  }

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    pipelines = scmClient.listPipelines();
    if (exclusiveNodeOptions != null && !Strings.isNullOrEmpty(exclusiveNodeOptions.getNodeId())) {
      HddsProtos.Node node = scmClient.queryNode(UUID.fromString(exclusiveNodeOptions.getNodeId()));
      Integer totalVolumeCount = node.hasTotalVolumeCount() ? node.getTotalVolumeCount() : null;
      Integer healthyVolumeCount = node.hasHealthyVolumeCount() ? node.getHealthyVolumeCount() : null;
      BasicDatanodeInfo singleNodeInfo = new BasicDatanodeInfo.Builder(
          DatanodeDetails.getFromProtoBuf(node.getNodeID()), node.getNodeOperationalStates(0),
          node.getNodeStates(0)).withVolumeCounts(totalVolumeCount, healthyVolumeCount).build();
      if (json) {
        List<BasicDatanodeInfo> dtoList = Collections.singletonList(singleNodeInfo);
        System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(dtoList));
      } else {
        printDatanodeInfo(singleNodeInfo);
      }
      return;
    }
    Stream<BasicDatanodeInfo> allNodes = getAllNodes(scmClient).stream();
    if (exclusiveNodeOptions != null && !Strings.isNullOrEmpty(exclusiveNodeOptions.getIp())) {
      allNodes = allNodes.filter(p -> p.getIpAddress()
          .compareToIgnoreCase(exclusiveNodeOptions.getIp()) == 0);
    }
    if (exclusiveNodeOptions != null && !Strings.isNullOrEmpty(exclusiveNodeOptions.getHostname())) {
      allNodes = allNodes.filter(p -> p.getHostName()
          .compareToIgnoreCase(exclusiveNodeOptions.getHostname()) == 0);
    }
    if (!Strings.isNullOrEmpty(nodeOperationalState)) {
      allNodes = allNodes.filter(p -> p.getOpState().toString()
          .compareToIgnoreCase(nodeOperationalState) == 0);
    }
    if (!Strings.isNullOrEmpty(nodeState)) {
      allNodes = allNodes.filter(p -> p.getHealthState().toString()
          .compareToIgnoreCase(nodeState) == 0);
    }

    if (!listLimitOptions.isAll()) {
      allNodes = allNodes.limit(listLimitOptions.getLimit());
    }
    
    if (json) {
      List<BasicDatanodeInfo> datanodeList = allNodes.collect(Collectors.toList());
      System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(datanodeList));
    } else {
      allNodes.forEach(this::printDatanodeInfo);
    }
  }

  private List<BasicDatanodeInfo> getAllNodes(ScmClient scmClient)
      throws IOException {

    // If sorting is requested
    if (exclusiveNodeOptions != null && (exclusiveNodeOptions.mostUsed || exclusiveNodeOptions.leastUsed)) {
      boolean sortByMostUsed = exclusiveNodeOptions.mostUsed;
      List<HddsProtos.DatanodeUsageInfoProto> usageInfos;
      try {
        usageInfos = scmClient.getDatanodeUsageInfo(sortByMostUsed, Integer.MAX_VALUE);
      } catch (Exception e) {
        System.err.println("Failed to get datanode usage info: " + e.getMessage());
        return Collections.emptyList();
      }

      return usageInfos.stream()
          .map(p -> {
            try {
              String uuidStr = p.getNode().getUuid();
              UUID parsedUuid = UUID.fromString(uuidStr);
              HddsProtos.Node node = scmClient.queryNode(parsedUuid);
              long capacity = p.getCapacity();
              long used = capacity - p.getRemaining();
              double percentUsed = (capacity > 0) ? (used * 100.0) / capacity : 0.0;
              Integer totalVolumeCount = node.hasTotalVolumeCount() ? node.getTotalVolumeCount() : null;
              Integer healthyVolumeCount = node.hasHealthyVolumeCount() ? node.getHealthyVolumeCount() : null;
              return new BasicDatanodeInfo.Builder(
                  DatanodeDetails.getFromProtoBuf(node.getNodeID()),
                  node.getNodeOperationalStates(0), node.getNodeStates(0))
                  .withUsageInfo(used, capacity, percentUsed)
                  .withVolumeCounts(totalVolumeCount, healthyVolumeCount).build();
            } catch (Exception e) {
              String reason = "Could not process info for an unknown datanode";
              if (p != null && p.getNode() != null && !Strings.isNullOrEmpty(p.getNode().getUuid())) {
                reason = "Could not process node info for " + p.getNode().getUuid();
              }
              System.err.printf("Error: %s: %s%n", reason, e.getMessage());
              return null;
            }
          })
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
    }
    
    List<HddsProtos.Node> nodes = scmClient.queryNode(null,
        null, HddsProtos.QueryScope.CLUSTER, "");

    return nodes.stream().map(p -> {
      Integer totalVolumeCount = p.hasTotalVolumeCount() ? p.getTotalVolumeCount() : null;
      Integer healthyVolumeCount = p.hasHealthyVolumeCount() ? p.getHealthyVolumeCount() : null;
      return new BasicDatanodeInfo.Builder(
          DatanodeDetails.getFromProtoBuf(p.getNodeID()), p.getNodeOperationalStates(0), p.getNodeStates(0))
          .withVolumeCounts(totalVolumeCount, healthyVolumeCount).build(); })
        .sorted(Comparator.comparing(BasicDatanodeInfo::getHealthState))
        .collect(Collectors.toList());
  }

  private void printDatanodeInfo(BasicDatanodeInfo dn) {
    StringBuilder pipelineListInfo = new StringBuilder();
    DatanodeDetails datanode = dn.getDatanodeDetails();
    int relatedPipelineNum = 0;
    if (!pipelines.isEmpty()) {
      List<Pipeline> relatedPipelines = pipelines.stream().filter(
          p -> p.getNodes().contains(datanode)).collect(Collectors.toList());
      if (relatedPipelines.isEmpty()) {
        pipelineListInfo.append("No related pipelines" +
            " or the node is not in Healthy state.\n");
      } else {
        relatedPipelineNum = relatedPipelines.size();
        relatedPipelines.forEach(
            p -> pipelineListInfo.append(p.getId().getId().toString())
                .append('/').append(p.getReplicationConfig().toString())
                .append('/').append(p.getType().toString())
                .append('/').append(p.getPipelineState().toString()).append('/')
                .append(datanode.getID().equals(p.getLeaderId()) ?
                    "Leader" : "Follower")
                .append(System.getProperty("line.separator")));
      }
    } else {
      pipelineListInfo.append("No pipelines in cluster.");
    }
    System.out.println("Datanode: " + datanode.getUuid().toString() +
        " (" + datanode.getNetworkLocation() + "/" + datanode.getIpAddress()
        + "/" + datanode.getHostName() + "/" + relatedPipelineNum +
        " pipelines)");
    System.out.println("Operational State: " + dn.getOpState());
    System.out.println("Health State: " + dn.getHealthState());
    if (dn.getTotalVolumeCount() != null && dn.getHealthyVolumeCount() != null) {
      System.out.println("Total volume count: " + dn.getTotalVolumeCount() + "\n" +
          "Healthy volume count: " + dn.getHealthyVolumeCount());
    }
    System.out.println("Related pipelines:\n" + pipelineListInfo);

    if (dn.getUsed() != null && dn.getCapacity() != null && dn.getUsed() >= 0 && dn.getCapacity() > 0) {
      System.out.println("Capacity: " + dn.getCapacity());
      System.out.println("Used: " + dn.getUsed());
      System.out.printf("Percentage Used : %.2f%%%n%n", dn.getPercentUsed());
    }
  }
}
