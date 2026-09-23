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
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeUsageInfoProto;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerClusterAnalyzer;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerClusterSnapshot;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerConfiguration;
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
    System.out.println(getSourceNodesPrettyString(snapshot, nodeLimit));
    System.out.println();
    System.out.println(getTargetNodesPrettyString(snapshot, nodeLimit));
    System.out.println();
    System.out.println(getMovementSummaryPrettyString(snapshot));
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
          "Upper limit", formatPercent(snapshot.getUpperLimit()),
          "Lower limit", formatPercent(snapshot.getLowerLimit()),
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
        "Upper limit", formatPercent(snapshot.getUpperLimit()),
        "Lower limit", formatPercent(snapshot.getLowerLimit()),
        "Drift", String.format(Locale.US, "%s (max utilization %s - min utilization %s)",
            formatPercent(snapshot.getImbalance()),
            formatPercent(snapshot.getMaxUtilization()),
            formatPercent(snapshot.getMinUtilization())),
        "Mean Utilization", formatPercent(snapshot.getClusterAvgUtilization()),
        "Eligible datanodes", snapshot.getTotalEligibleDatanodes() + " datanode"
            + (snapshot.getTotalEligibleDatanodes() == 1 ? "" : "s"),
        "Category", formatCategory(snapshot));
  }

  private static String getSourceNodesPrettyString(ContainerBalancerClusterSnapshot snapshot, int nodeLimit) {
    StringBuilder builder = new StringBuilder();
    builder.append("Source Nodes (over-utilized):").append(System.lineSeparator())
        .append(String.format("%-50s %s%n", "Datanodes above threshold", snapshot.getSourceCount()));
    appendNodeList(builder, snapshot.getTopSourceNodes(), snapshot.getClusterAvgUtilization(), true, nodeLimit);
    return builder.toString();
  }

  private static String getTargetNodesPrettyString(ContainerBalancerClusterSnapshot snapshot, int nodeLimit) {
    StringBuilder builder = new StringBuilder();
    builder.append("Target Nodes (under-utilized):").append(System.lineSeparator())
        .append(String.format("%-50s %s%n", "Datanodes below threshold", snapshot.getTargetCount()));
    appendNodeList(builder, snapshot.getBottomTargetNodes(), snapshot.getClusterAvgUtilization(), false, nodeLimit);
    return builder.toString();
  }

  private static String getMovementSummaryPrettyString(ContainerBalancerClusterSnapshot snapshot) {
    double movementRatio = snapshot.getClusterCapacityBytes() == 0 ? 0
        : (double) snapshot.getBytesToMove() / snapshot.getClusterCapacityBytes();
    return String.format("Movement Summary:%n" +
            "%-50s %s%n" +
            "%-50s %s%n", "Total bytes to move", byteDesc(snapshot.getBytesToMove()),
        "Movement ratio",
        formatPercent(movementRatio) + " of cluster capacity");
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

  private static String formatCategory(ContainerBalancerClusterSnapshot snapshot) {
    return formatClusterSize(snapshot.getTotalEligibleDatanodes()) + ", "
        + formatImbalance(snapshot.getImbalance()) + ", "
        + formatMovementRatio(snapshot);
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

  private static String formatMovementRatio(ContainerBalancerClusterSnapshot snapshot) {
    if (snapshot.getClusterCapacityBytes() == 0) {
      return "Low movement ratio";
    }
    double ratio = (double) snapshot.getBytesToMove() / snapshot.getClusterCapacityBytes();
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
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("totalEligibleDatanodes", snapshot.getTotalEligibleDatanodes());
    result.put("thresholdPercentage", formatPercent(thresholdRatio));
    result.put("clusterAvgUtilizationPercentage", formatPercent(snapshot.getClusterAvgUtilization()));
    result.put("driftPercentage", formatPercent(snapshot.getImbalance()));
    result.put("maxUtilizationPercentage", formatPercent(snapshot.getMaxUtilization()));
    result.put("minUtilizationPercentage", formatPercent(snapshot.getMinUtilization()));
    result.put("upperLimitPercentage", formatPercent(snapshot.getUpperLimit()));
    result.put("lowerLimitPercentage", formatPercent(snapshot.getLowerLimit()));

    Map<String, Object> sourceNodes = new LinkedHashMap<>();
    sourceNodes.put("count", snapshot.getSourceCount());
    sourceNodes.put("nodes", snapshot.getTopSourceNodes().stream()
        .map(node -> {
          Map<String, Object> nodeMap = new LinkedHashMap<>();
          nodeMap.put("hostname", node.getHostname());
          nodeMap.put("utilizationPercentage", formatPercent(node.getUtilization()));
          return nodeMap;
        })
        .collect(Collectors.toList()));
    result.put("sourceNodes", sourceNodes);

    Map<String, Object> targetNodes = new LinkedHashMap<>();
    targetNodes.put("count", snapshot.getTargetCount());
    targetNodes.put("nodes", snapshot.getBottomTargetNodes().stream()
        .map(node -> {
          Map<String, Object> nodeMap = new LinkedHashMap<>();
          nodeMap.put("hostname", node.getHostname());
          nodeMap.put("utilizationPercentage", formatPercent(node.getUtilization()));
          return nodeMap;
        })
        .collect(Collectors.toList()));
    result.put("targetNodes", targetNodes);

    double movementRatio = snapshot.getClusterCapacityBytes() == 0 ? 0
        : (double) snapshot.getBytesToMove() / snapshot.getClusterCapacityBytes();
    result.put("bytesToMove", byteDesc(snapshot.getBytesToMove()));
    result.put("movementRatioPercentage", formatPercent(movementRatio));

    System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(result));
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
