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

import static org.apache.hadoop.util.StringUtils.byteDesc;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeUsageInfoProto;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerClusterAnalyzer;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerClusterSnapshot;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerConfiguration;
import org.apache.hadoop.hdds.scm.container.balancer.StorageTypeSnapshot;
import org.apache.hadoop.hdds.server.JsonUtils;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Reports cluster imbalance without starting the container balancer. */
@Command(
    name = "assessment",
    description = "Report cluster imbalance before starting ContainerBalancer",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ContainerBalancerAssessmentSubcommand extends ScmSubcommand {

  @Option(names = {"-t", "--threshold"},
      description = "Percentage deviation from average utilization of " +
          "the cluster after which a datanode is considered over- or under-utilized. " +
          "The value should be in the range [0.0, 100.0), with a default of 10 " +
          "(specify '10' for 10%%).")
  private Optional<Double> threshold;

  @Option(names = {"-n", "--limit"},
      description = "Maximum number of over- and under-utilized datanodes to list in the report. Default: 5.")
  private int nodeLimit = 5;

  @Option(names = {"--include-datanodes"},
      description = "A list of Datanode hostnames or ip addresses separated by commas. " +
          "Only the Datanodes specified in this list are included in the assessment.")
  private Optional<String> includeNodes;

  @Option(names = {"--exclude-datanodes"},
      description = "A list of Datanode hostnames or ip addresses separated by commas. " +
          "The Datanodes specified in this list are excluded from the assessment.")
  private Optional<String> excludeNodes;

  @Option(names = {"--json"},
      defaultValue = "false",
      description = "Format output as JSON")
  private boolean json;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    ContainerBalancerConfiguration config =
        getOzoneConf().getObject(ContainerBalancerConfiguration.class);

    if (nodeLimit < 1) {
      throw new IllegalArgumentException("limit must be at least 1.");
    }

    double thresholdRatio = threshold
        .map(t -> {
          if (t < 0d || t >= 100d) {
            throw new IllegalArgumentException(
                "Threshold must be a percentage in the range [0.0, 100.0).");
          }
          return t / 100.0;
        })
        .orElseGet(config::getThresholdAsRatio);

    Set<String> include = includeNodes
        .map(ContainerBalancerAssessmentSubcommand::parseNodeList)
        .orElseGet(config::getIncludeNodes);
    Set<String> exclude = excludeNodes
        .map(ContainerBalancerAssessmentSubcommand::parseNodeList)
        .orElseGet(config::getExcludeNodes);

    List<DatanodeUsageInfoProto> nodes =
        scmClient.getDatanodeUsageInfo(true, Integer.MAX_VALUE);

    ContainerBalancerClusterSnapshot snapshot =
        ContainerBalancerClusterAnalyzer.analyze(nodes, thresholdRatio, include, exclude, nodeLimit);

    if (json) {
      printJsonReport(snapshot, thresholdRatio);
    } else {
      printReport(snapshot, thresholdRatio, nodeLimit);
    }
  }

  private static void printReport(ContainerBalancerClusterSnapshot snapshot, double thresholdRatio, int nodeLimit) {
    System.out.println("CLUSTER BALANCE ASSESSMENT");

    if (snapshot.getTotalEligibleDatanodes() == 0) {
      System.out.println(getSummaryPrettyString(snapshot, thresholdRatio));
      System.out.println();
      System.out.println("No eligible datanodes found for assessment.");
      return;
    }

    System.out.println(getSummaryPrettyString(snapshot, thresholdRatio));
    System.out.println();
    if (!snapshot.getStorageTypeSnapshots().isEmpty()) {
      if (snapshot.hasIncompleteStorageTypeUsage()) {
        System.out.println("WARNING: Missing or incomplete storage-type usage; storage-type assessment is partial.");
      }
      StorageTypeSnapshot mostImbalanced = snapshot.getStorageTypeSnapshots().get(
          snapshot.getMostImbalancedStorageType());
      System.out.printf("Maximum storage-type drift: %s (%s, max utilization %s - min utilization %s)%n",
          formatPercent(snapshot.getMaxImbalance()), mostImbalanced.getStorageType(),
          formatPercent(mostImbalanced.getMaxUtilization()), formatPercent(mostImbalanced.getMinUtilization()));
      for (Map.Entry<StorageType, StorageTypeSnapshot> entry
          : snapshot.getStorageTypeSnapshots().entrySet()) {
        StorageTypeSnapshot typeSnapshot = entry.getValue();
        System.out.println("Storage type: " + entry.getKey());
        System.out.println(getStorageTypeSummaryPrettyString(typeSnapshot, thresholdRatio));
        System.out.println(getSourceNodesPrettyString(typeSnapshot.getSourceCount(), typeSnapshot.getTopSourceNodes(),
            typeSnapshot.getAverageUtilization(), nodeLimit));
        System.out.println(getTargetNodesPrettyString(typeSnapshot.getTargetCount(),
            typeSnapshot.getBottomTargetNodes(), typeSnapshot.getAverageUtilization(), nodeLimit));
        System.out.println(getMovementSummaryPrettyString(
            typeSnapshot.getBytesToMove(), typeSnapshot.getCapacityBytes(),
            typeSnapshot.getStorageType() + " capacity"));
      }
      System.out.println("All storage types (shared iteration limits):");
      System.out.println(getMovementSummaryPrettyString(
          snapshot.getTotalBytesToMove(), snapshot.getClusterCapacityBytes(), "cluster capacity"));
      return;
    }
    System.out.println(getSourceNodesPrettyString(snapshot.getSourceCount(),
        snapshot.getTopSourceNodesAcrossStorageTypes(), snapshot.getClusterAvgUtilization(), nodeLimit));
    System.out.println();
    System.out.println(getTargetNodesPrettyString(snapshot.getTargetCount(),
        snapshot.getBottomTargetNodesAcrossStorageTypes(), snapshot.getClusterAvgUtilization(), nodeLimit));
    System.out.println();
    System.out.println(getMovementSummaryPrettyString(
        snapshot.getTotalBytesToMove(), snapshot.getClusterCapacityBytes(), "cluster capacity"));
  }

  private static String getSummaryPrettyString(ContainerBalancerClusterSnapshot snapshot,
      double thresholdRatio) {
    if (snapshot.getTotalEligibleDatanodes() == 0) {
      return String.format("%-50s %s%n" +
              "%-50s %s%n" +
              "%-50s %s%n" +
              "%-50s %s%n" +
              "%-50s %s%n", "Key", "Value",
          "Threshold", formatPercent(thresholdRatio),
          "Upper utilization limit", formatPercent(snapshot.getUpperLimit()),
          "Lower utilization limit", formatPercent(snapshot.getLowerLimit()),
          "Eligible datanodes", "0 datanodes");
    }
    return String.format("%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %s%n", "Key", "Value",
        "Threshold", formatPercent(thresholdRatio),
        snapshot.getStorageTypeSnapshots().isEmpty() ? "Upper utilization limit" : "Overall upper utilization limit",
        formatPercent(snapshot.getUpperLimit()),
        snapshot.getStorageTypeSnapshots().isEmpty() ? "Lower utilization limit" : "Overall lower utilization limit",
        formatPercent(snapshot.getLowerLimit()),
        snapshot.getStorageTypeSnapshots().isEmpty() ? "Drift" : "Overall datanode drift",
        String.format(Locale.US, "%s (max utilization %s - min utilization %s)",
            formatPercent(snapshot.getImbalance()),
            formatPercent(snapshot.getMaxUtilization()),
            formatPercent(snapshot.getMinUtilization())),
        snapshot.getStorageTypeSnapshots().isEmpty() ? "Mean Utilization" : "Overall mean utilization",
        formatPercent(snapshot.getClusterAvgUtilization()),
        "Eligible datanodes", snapshot.getTotalEligibleDatanodes() + " datanode"
            + (snapshot.getTotalEligibleDatanodes() == 1 ? "" : "s"),
        snapshot.getStorageTypeSnapshots().isEmpty() ? "Category" : "Category (maximum storage-type imbalance)",
        formatCategory(snapshot));
  }

  private static String getStorageTypeSummaryPrettyString(StorageTypeSnapshot snapshot, double thresholdRatio) {
    return String.format("%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %s%n", "Key", "Value",
        "Threshold", formatPercent(thresholdRatio),
        "Upper utilization limit", formatPercent(snapshot.getUpperLimit()),
        "Lower utilization limit", formatPercent(snapshot.getLowerLimit()),
        "Drift", String.format(Locale.US, "%s (max utilization %s - min utilization %s)",
            formatPercent(snapshot.getImbalance()), formatPercent(snapshot.getMaxUtilization()),
            formatPercent(snapshot.getMinUtilization())),
        "Mean Utilization", formatPercent(snapshot.getAverageUtilization()),
        "Eligible datanodes", snapshot.getEligibleDatanodes() + " datanode"
            + (snapshot.getEligibleDatanodes() == 1 ? "" : "s"),
        "Category", formatClusterSize(snapshot.getEligibleDatanodes()) + ", "
            + formatImbalance(snapshot.getImbalance()) + ", "
            + formatMovementRatio(snapshot.getBytesToMove(), snapshot.getCapacityBytes()));
  }

  private static String getSourceNodesPrettyString(int sourceCount,
      List<ContainerBalancerClusterSnapshot.NodeUtilization> nodes, double averageUtilization, int nodeLimit) {
    StringBuilder builder = new StringBuilder();
    builder.append("Source Nodes (over-utilized):").append(System.lineSeparator())
        .append(String.format("%-50s %s%n", "Datanodes above threshold", sourceCount));
    appendNodeList(builder, nodes, averageUtilization, true, nodeLimit);
    return builder.toString();
  }

  private static String getTargetNodesPrettyString(int targetCount,
      List<ContainerBalancerClusterSnapshot.NodeUtilization> nodes, double averageUtilization, int nodeLimit) {
    StringBuilder builder = new StringBuilder();
    builder.append("Target Nodes (under-utilized):").append(System.lineSeparator())
        .append(String.format("%-50s %s%n", "Datanodes below threshold", targetCount));
    appendNodeList(builder, nodes, averageUtilization, false, nodeLimit);
    return builder.toString();
  }

  private static String getMovementSummaryPrettyString(
      long bytesToMove, long capacityBytes, String capacityDescription) {
    double movementRatio = capacityBytes == 0 ? 0 : (double) bytesToMove / capacityBytes;
    return String.format("Movement Summary:%n" +
            "%-50s %s%n" +
            "%-50s %s%n", "Total bytes to move", byteDesc(bytesToMove),
        "Movement ratio",
        formatPercent(movementRatio) + " of " + capacityDescription);
  }

  private static void appendNodeList(StringBuilder builder,
                                     List<ContainerBalancerClusterSnapshot.NodeUtilization> nodes,
                                     double clusterAvgUtilization, boolean aboveMean, int nodeLimit) {
    if (nodes.isEmpty()) {
      builder.append(System.lineSeparator())
          .append("(none)")
          .append(System.lineSeparator());
      return;
    }
    int headerCount = Math.min(nodeLimit, nodes.size());
    builder.append(System.lineSeparator())
        .append(aboveMean ? "Top " + headerCount + ":" : "Bottom " + headerCount + ":")
        .append(System.lineSeparator());
    for (ContainerBalancerClusterSnapshot.NodeUtilization node : nodes) {
      double deltaFromMean = (node.getUtilization() - clusterAvgUtilization) * 100;
      builder.append(String.format(Locale.US, "%-50s %s%n", node.getHostname(),
          String.format(Locale.US, "%s (%+.1f%% %s mean)",
              formatPercent(node.getUtilization()), deltaFromMean, aboveMean ? "above" : "below")));
    }
  }

  private static String formatPercent(double ratio) {
    return String.format(Locale.US, "%.1f%%", ratio * 100);
  }

  private static double toJsonPercent(double ratio) {
    return Double.parseDouble(String.format(Locale.US, "%.2f", ratio * 100));
  }

  private static String formatCategory(ContainerBalancerClusterSnapshot snapshot) {
    return formatClusterSize(snapshot.getTotalEligibleDatanodes()) + ", "
        + formatImbalance(snapshot.getMaxImbalance()) + ", "
        + formatMovementRatio(snapshot.getTotalBytesToMove(), snapshot.getClusterCapacityBytes());
  }

  private static String formatClusterSize(int eligibleDatanodes) {
    if (eligibleDatanodes >= 100) {
      return "Large cluster";
    }
    if (eligibleDatanodes >= 20) {
      return "Medium cluster";
    }
    return "Small cluster";
  }

  private static String formatImbalance(double imbalance) {
    if (imbalance >= 0.20) {
      return "High imbalance";
    }
    if (imbalance >= 0.10) {
      return "Medium imbalance";
    }
    return "Low imbalance";
  }

  private static String formatMovementRatio(long bytesToMove, long capacityBytes) {
    if (capacityBytes == 0) {
      return "Low movement ratio";
    }
    double ratio = (double) bytesToMove / capacityBytes;
    if (ratio >= 0.05) {
      return "High movement ratio";
    }
    if (ratio >= 0.01) {
      return "Medium movement ratio";
    }
    return "Low movement ratio";
  }

  private static void printJsonReport(ContainerBalancerClusterSnapshot snapshot, double thresholdRatio)
      throws IOException {
    System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(jsonReport(snapshot, thresholdRatio)));
  }

  private static Map<String, Object> jsonReport(ContainerBalancerClusterSnapshot snapshot, double thresholdRatio) {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("totalEligibleDatanodes", snapshot.getTotalEligibleDatanodes());
    result.put("thresholdPercentage", toJsonPercent(thresholdRatio));
    result.put("clusterAvgUtilizationPercentage", toJsonPercent(snapshot.getClusterAvgUtilization()));
    result.put("driftPercentage", toJsonPercent(snapshot.getImbalance()));
    result.put("maxUtilizationPercentage", toJsonPercent(snapshot.getMaxUtilization()));
    result.put("minUtilizationPercentage", toJsonPercent(snapshot.getMinUtilization()));
    result.put("upperLimitPercentage", toJsonPercent(snapshot.getUpperLimit()));
    result.put("lowerLimitPercentage", toJsonPercent(snapshot.getLowerLimit()));

    boolean typed = !snapshot.getStorageTypeSnapshots().isEmpty();
    result.put(typed ? "sourceNodesAcrossStorageTypes" : "sourceNodes",
        jsonNodesSummary(snapshot.getSourceCount(), snapshot.getTopSourceNodesAcrossStorageTypes()));
    result.put(typed ? "targetNodesAcrossStorageTypes" : "targetNodes",
        jsonNodesSummary(snapshot.getTargetCount(), snapshot.getBottomTargetNodesAcrossStorageTypes()));

    double movementRatio = snapshot.getClusterCapacityBytes() == 0 ? 0
        : (double) snapshot.getTotalBytesToMove() / snapshot.getClusterCapacityBytes();
    result.put(typed ? "totalBytesToMove" : "bytesToMove", snapshot.getTotalBytesToMove());
    result.put("movementRatioPercentage", toJsonPercent(movementRatio));

    if (!snapshot.getStorageTypeSnapshots().isEmpty()) {
      result.put("maxStorageTypeImbalancePercentage", toJsonPercent(snapshot.getMaxImbalance()));
      result.put("mostImbalancedStorageType", snapshot.getMostImbalancedStorageType().name());
      Map<String, Object> storageTypes = new LinkedHashMap<>();
      snapshot.getStorageTypeSnapshots().forEach((type, usage) ->
          storageTypes.put(type.name(), jsonReport(usage, thresholdRatio)));
      result.put("storageTypes", storageTypes);
      result.put("incompleteStorageTypeUsage", snapshot.hasIncompleteStorageTypeUsage());
    }
    return result;
  }

  private static Map<String, Object> jsonReport(StorageTypeSnapshot snapshot, double thresholdRatio) {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("totalEligibleDatanodes", snapshot.getEligibleDatanodes());
    result.put("thresholdPercentage", toJsonPercent(thresholdRatio));
    result.put("averageUtilizationPercentage", toJsonPercent(snapshot.getAverageUtilization()));
    result.put("driftPercentage", toJsonPercent(snapshot.getImbalance()));
    result.put("maxUtilizationPercentage", toJsonPercent(snapshot.getMaxUtilization()));
    result.put("minUtilizationPercentage", toJsonPercent(snapshot.getMinUtilization()));
    result.put("upperLimitPercentage", toJsonPercent(snapshot.getUpperLimit()));
    result.put("lowerLimitPercentage", toJsonPercent(snapshot.getLowerLimit()));
    result.put("sourceNodes", jsonNodesSummary(snapshot.getSourceCount(), snapshot.getTopSourceNodes()));
    result.put("targetNodes", jsonNodesSummary(snapshot.getTargetCount(), snapshot.getBottomTargetNodes()));
    result.put("bytesToMove", snapshot.getBytesToMove());
    double movementRatio = snapshot.getCapacityBytes() == 0 ? 0
        : (double) snapshot.getBytesToMove() / snapshot.getCapacityBytes();
    result.put("movementRatioPercentage", toJsonPercent(movementRatio));
    return result;
  }

  private static Map<String, Object> jsonNodesSummary(int count,
      List<ContainerBalancerClusterSnapshot.NodeUtilization> nodes) {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("count", count);
    result.put("nodes", nodes.stream().map(node -> {
      Map<String, Object> nodeMap = new LinkedHashMap<>();
      nodeMap.put("hostname", node.getHostname());
      nodeMap.put("utilizationPercentage", toJsonPercent(node.getUtilization()));
      if (node.getStorageType() != null) {
        nodeMap.put("storageType", node.getStorageType().name());
      }
      return nodeMap;
    }).collect(Collectors.toList()));
    return result;
  }

  private static Set<String> parseNodeList(String commaSeparated) {
    if (commaSeparated == null || commaSeparated.isEmpty()) {
      return Collections.emptySet();
    }
    return Arrays.stream(commaSeparated.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toSet());
  }
}
