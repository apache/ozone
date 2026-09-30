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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeUsageInfoProto;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerAdvisor;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerRecommendation;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Recommends container balancer configuration for SLOW, MEDIUM, and FAST profiles
 * without starting the balancer.
 */
@Command(
    name = "recommend",
    description = "Recommend container balancer configuration based on current cluster imbalance. "
        + "When --profile is omitted, SLOW, MEDIUM, and FAST are recommended. Does not start the balancer.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ContainerBalancerRecommendSubcommand extends ScmSubcommand {

  private static final int PARAM_COLUMN_WIDTH = 42;
  private static final int VALUE_COLUMN_WIDTH = 14;

  @Option(names = {"-t", "--threshold"},
      description = "Percentage deviation from average utilization of "
          + "the cluster after which a datanode will be rebalanced. The value "
          + "should be in the range [0.0, 100.0), with a default of 10 "
          + "(specify '10' for 10%%).")
  private Optional<Double> threshold;

  @Option(names = {"--include-datanodes"},
      description = "A list of Datanode hostnames or ip addresses separated by commas. Only the "
          + "Datanodes specified in this list are balanced.")
  private Optional<String> includeNodes;

  @Option(names = {"--exclude-datanodes"},
      description = "A list of Datanode hostnames or ip addresses separated by commas. The "
          + "Datanodes specified in this list are excluded from balancing.")
  private Optional<String> excludeNodes;

  @Option(names = {"--profile"},
      description = "Throttling profile: SLOW, MEDIUM, or FAST. When set, only this profile is "
          + "recommended. When omitted, SLOW, MEDIUM, and FAST are recommended.")
  private Optional<String> profileName;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    List<DatanodeUsageInfoProto> nodes = scmClient.getDatanodeUsageInfo(true, Integer.MAX_VALUE);
    if (nodes == null || nodes.isEmpty()) {
      throw new IOException("No datanode usage information available from SCM.");
    }

    OzoneConfiguration conf = getOzoneConf();
    ContainerBalancerAdvisor.AdvisorRequest request = buildRequest(nodes);
    List<ContainerBalancerRecommendation> recommendations;
    try {
      recommendations = ContainerBalancerAdvisor.recommend(conf, request);
    } catch (IllegalArgumentException e) {
      throw new IOException(e.getMessage(), e);
    }

    if (!recommendations.isEmpty()
        && recommendations.stream().allMatch(ContainerBalancerRecommendation::isClusterBalanced)) {
      out().println("Cluster is already balanced within the configured threshold. "
          + "No balancing recommended.");
      return;
    }

    boolean anySucceeded = false;
    for (ContainerBalancerRecommendation recommendation : recommendations) {
      printRecommendation(recommendation);
      if (recommendation.succeeded()) {
        anySucceeded = true;
      }
    }
    if (!anySucceeded) {
      throw new IOException(recommendations.get(0).getFailureMessage());
    }
  }

  private ContainerBalancerAdvisor.AdvisorRequest buildRequest(List<DatanodeUsageInfoProto> nodes)
      throws IOException {
    ContainerBalancerAdvisor.AdvisorRequest request =
        new ContainerBalancerAdvisor.AdvisorRequest().setNodes(nodes);
    threshold.ifPresent(request::setThresholdPercent);
    includeNodes.ifPresent(value -> request.setIncludeNodes(ContainerBalancerCliHelper.parseNodeSet(value)));
    excludeNodes.ifPresent(value -> request.setExcludeNodes(ContainerBalancerCliHelper.parseNodeSet(value)));
    if (profileName.isPresent()) {
      request.setProfile(ContainerBalancerCliHelper.parseProfile(profileName.get()));
    }
    return request;
  }

  private void printRecommendation(ContainerBalancerRecommendation recommendation) {
    out().printf("RECOMMENDED CONFIGURATION (profile: %s)%n", recommendation.getProfile().name());
    out().println();

    if (!recommendation.succeeded()) {
      out().printf("Recommendation failed: %s%n%n", recommendation.getFailureMessage());
      return;
    }

    printRecommendedParameters(recommendation);
    out().println();
    out().println(" Estimation:");
    ContainerBalancerCliHelper.printEstimation(out(), recommendation.getEstimation());

    out().println(" Suggested commands:");
    out().println("   " + ContainerBalancerCliHelper.formatStartCommand(
        recommendation, includeNodes, excludeNodes));
    out().println();
  }

  private void printRecommendedParameters(ContainerBalancerRecommendation recommendation) {
    Map<String, String> rationale = recommendation.getRationale();
    long moveTimeoutMinutes = Math.round(recommendation.getMoveTimeoutMillis() / 60000d);
    long moveReplicationTimeoutMinutes =
        Math.round(recommendation.getMoveReplicationTimeoutMillis() / 60000d);
    long balancingIntervalMinutes = Math.round(recommendation.getBalancingIntervalMillis() / 60000d);

    out().println(" Recommended parameters:");
    printParameterRow("Parameter", "Value", "Rationale");
    printParameterRow("--threshold",
        String.format("%.1f%%", recommendation.getThresholdPercent()),
        rationale.get("threshold"));
    printParameterRow("--max-datanodes-percentage-to-involve",
        String.format("%d%%", recommendation.getMaxDatanodesPercentage()),
        rationale.get("maxDatanodesPercentage"));
    printParameterRow("--max-size-to-move-per-iteration-in-gb",
        byteDesc(recommendation.getMaxSizeToMovePerIteration()),
        rationale.get("maxSizeToMovePerIteration"));
    printParameterRow("--max-size-entering-target-in-gb",
        byteDesc(recommendation.getMaxSizeEnteringTarget()) + " / node",
        rationale.get("maxSizeEnteringTarget"));
    printParameterRow("--max-size-leaving-source-in-gb",
        byteDesc(recommendation.getMaxSizeLeavingSource()) + " / node",
        rationale.get("maxSizeLeavingSource"));
    printParameterRow("--move-timeout-minutes",
        String.format("%d min", moveTimeoutMinutes),
        rationale.get("moveTimeout"));
    printParameterRow("--move-replication-timeout-minutes",
        String.format("%d min", moveReplicationTimeoutMinutes),
        rationale.get("moveReplicationTimeout"));
    printParameterRow("--balancing-iteration-interval-minutes",
        String.format("%d min", balancingIntervalMinutes),
        rationale.get("balancingInterval"));
    printParameterRow("--iterations",
        String.valueOf(recommendation.getRecommendedIterations()),
        rationale.get("iterations"));
  }

  private void printParameterRow(String parameter, String value, String rationaleText) {
    String rationale = rationaleText == null ? "" : rationaleText;
    out().printf(" %-" + PARAM_COLUMN_WIDTH + "s %-" + VALUE_COLUMN_WIDTH + "s %s%n",
        parameter, value, rationale);
  }

}
