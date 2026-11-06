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
import java.util.Optional;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeUsageInfoProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartContainerBalancerResponseProto;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerRecommendation;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Handler to start container balancer.
 */
@Command(
    name = "start",
    description = "Start ContainerBalancer. With --auto-config, recommends configuration from "
        + "current cluster imbalance (profile MEDIUM by default), applying any explicit balancer "
        + "flags during recommendation, and starts with the recommended values.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ContainerBalancerStartSubcommand extends ScmSubcommand {

  @CommandLine.Mixin
  private ContainerBalancerConfigOptions configOptions;

  @Option(names = {"--auto-config"},
      description = "Recommend configuration from current cluster imbalance and start with those "
          + "values. Uses profile MEDIUM by default; override with --profile. Default false.")
  private boolean autoConfig;

  @Option(names = {"--profile"},
      description = "Throttling profile for --auto-config: SLOW, MEDIUM, or FAST. Default MEDIUM.")
  private Optional<String> profileName;

  @Option(names = {"-i", "--iterations"},
      description = "Maximum consecutive iterations that " +
          "balancer will run for. The value should be positive " +
          "or -1, with a default of 10 (specify '10' for 10 iterations).")
  private Optional<Integer> iterations;

  @Option(names = {"--move-network-topology-enable"},
      description = "Whether to take network topology into account when " +
          "selecting a target for a source. " +
          "This configuration is false by default.")
  private Optional<Boolean> networkTopologyEnable;

  @Option(names = {"--exclude-containers"},
      description = "A list of container IDs separated by commas. " +
          "The containers specified in this list are excluded from balancing. " +
          "This configuration is empty by default " +
          "(specify \"1,2,3\" for container IDs).")
  private Optional<String> excludeContainers;

  @Option(names = {"--include-containers"},
      description = "A list of container IDs separated by commas. " +
          "Only the containers specified in this list will be included in balancing." +
          " If --exclude-containers is also specified, those containers will " +
          "be excluded. This configuration is empty by default " +
          "(specify \"1,2,3\" for container IDs).")
  private Optional<String> includeContainers;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    if (!autoConfig && profileName.isPresent()) {
      throw new IOException("--profile requires --auto-config.");
    }

    Optional<Double> threshold = configOptions.getThreshold();
    Optional<Integer> effectiveIterations = iterations;
    Optional<Integer> maxDatanodesPercentage = configOptions.getMaxDatanodesPercentageToInvolvePerIteration();
    Optional<Long> maxSizeToMovePerIterationInGB = configOptions.getMaxSizeToMovePerIterationInGB();
    Optional<Long> maxSizeEnteringTargetInGB = configOptions.getMaxSizeEnteringTargetInGB();
    Optional<Long> maxSizeLeavingSourceInGB = configOptions.getMaxSizeLeavingSourceInGB();
    Optional<Integer> balancingIntervalMinutes = configOptions.getBalancingIntervalMinutes();
    Optional<Integer> moveTimeoutMinutes = configOptions.getMoveTimeoutMinutes();
    Optional<Integer> moveReplicationTimeoutMinutes = configOptions.getMoveReplicationTimeoutMinutes();

    if (autoConfig) {
      List<DatanodeUsageInfoProto> nodes = scmClient.getDatanodeUsageInfo(true, Integer.MAX_VALUE);
      if (nodes == null || nodes.isEmpty()) {
        throw new IOException("No datanode usage information available from SCM.");
      }
      
      ContainerBalancerRecommendation recommendation = ContainerBalancerCliHelper.recommendForAutoConfig(
          getOzoneConf(), nodes, configOptions, profileName);
      if (recommendation.isClusterBalanced()) {
        out().println("Cluster is already balanced within the configured threshold. "
            + "No balancing recommended.");
        return;
      }
      
      ContainerBalancerCliHelper.EffectiveStartParameters effective =
          ContainerBalancerCliHelper.startParametersFromRecommendation(recommendation, iterations, configOptions);
      threshold = effective.getThreshold();
      effectiveIterations = effective.getIterations();
      maxDatanodesPercentage = effective.getMaxDatanodesPercentageToInvolvePerIteration();
      maxSizeToMovePerIterationInGB = effective.getMaxSizeToMovePerIterationInGB();
      maxSizeEnteringTargetInGB = effective.getMaxSizeEnteringTargetInGB();
      maxSizeLeavingSourceInGB = effective.getMaxSizeLeavingSourceInGB();
      balancingIntervalMinutes = effective.getBalancingIntervalMinutes();
      moveTimeoutMinutes = effective.getMoveTimeoutMinutes();
      moveReplicationTimeoutMinutes = effective.getMoveReplicationTimeoutMinutes();
      out().printf("Starting with auto-configured parameters (profile: %s).%n",
          recommendation.getProfile().name());
    }

    StartContainerBalancerResponseProto response = scmClient.startContainerBalancer(
        threshold,
        effectiveIterations,
        maxDatanodesPercentage,
        maxSizeToMovePerIterationInGB,
        maxSizeEnteringTargetInGB,
        maxSizeLeavingSourceInGB,
        balancingIntervalMinutes,
        moveTimeoutMinutes,
        moveReplicationTimeoutMinutes,
        networkTopologyEnable,
        configOptions.getIncludeNodes(),
        configOptions.getExcludeNodes(),
        excludeContainers,
        includeContainers);
    if (response.getStart()) {
      System.out.println("Container Balancer started successfully.");
    } else {
      String reason = "";
      if (response.hasMessage()) {
        reason = response.getMessage();
      }
      throw new IOException("Failed to start Container Balancer. " + reason);
    }
  }
}
