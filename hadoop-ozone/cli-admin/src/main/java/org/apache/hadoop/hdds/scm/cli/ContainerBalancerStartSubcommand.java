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
import java.util.Optional;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartContainerBalancerResponseProto;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Handler to start container balancer.
 */
@Command(
    name = "start",
    description = "Start ContainerBalancer",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ContainerBalancerStartSubcommand extends ScmSubcommand {

  @Option(names = {"-t", "--threshold"},
      description = "Percentage deviation from average utilization of " +
          "the cluster after which a datanode will be rebalanced. The value " +
          "should be in the range [0.0, 100.0), with a default of 10 " +
          "(specify '10' for 10%%).")
  private Optional<Double> threshold;

  @Option(names = {"-i", "--iterations"},
      description = "Maximum consecutive iterations that " +
          "balancer will run for. The value should be positive " +
          "or -1, with a default of 10 (specify '10' for 10 iterations).")
  private Optional<Integer> iterations;

  @Option(names = {"-d", "--max-datanodes-percentage-to-involve-per-iteration",
      "--maxDatanodesPercentageToInvolvePerIteration"},
      description = "Max percentage of healthy, in service datanodes " +
          "that can be involved in balancing in one iteration. The value " +
          "should be in the range [0,100], with a default of 20 (specify " +
          "'20' for 20%%).")
  private Optional<Integer> maxDatanodesPercentageToInvolvePerIteration;

  @Option(names = {"-s", "--max-size-to-move-per-iteration-in-gb",
      "--maxSizeToMovePerIterationInGB"},
      description = "Maximum size that can be moved per iteration of " +
          "balancing. The value should be positive, with a default of 500 " +
          "(specify '500' for 500GB).")
  private Optional<Long> maxSizeToMovePerIterationInGB;

  @Option(names = {"-e", "--max-size-entering-target-in-gb",
      "--maxSizeEnteringTargetInGB"},
      description = "Maximum size that can enter a target datanode while " +
          "balancing. This is the sum of data from multiple sources. The value " +
          "should be positive, with a default of 26 (specify '26' for 26GB).")
  private Optional<Long> maxSizeEnteringTargetInGB;

  @Option(names = {"-l", "--max-size-leaving-source-in-gb",
      "--maxSizeLeavingSourceInGB"},
      description = "Maximum size that can leave a source datanode while " +
          "balancing. This is the sum of data moving to multiple targets. " +
          "The value should be positive, with a default of 26 " +
          "(specify '26' for 26GB).")
  private Optional<Long> maxSizeLeavingSourceInGB;

  @Option(names = {"--balancing-iteration-interval-minutes"},
      description = "The interval period in minutes between each iteration of Container Balancer. " +
          "The value should be positive, with a default of 70 (specify '70' for 70 minutes).")
  private Optional<Integer> balancingInterval;

  @Option(names = {"--move-timeout-minutes"},
      description = "The amount of time in minutes to allow a single container to move " +
          "from source to target. The value should be positive, with a default of 65 " +
          "(specify '65' for 65 minutes).")
  private Optional<Integer> moveTimeout;

  @Option(names = {"--move-replication-timeout-minutes"},
      description = "The " +
          "amount of time in minutes to allow a single container's replication from source " +
          "to target as part of container move. The value should be positive, with " +
          "a default of 50. For example, if \"hdds.container" +
          ".balancer.move.timeout\" is 65 minutes, then out of those 65 minutes " +
          "50 minutes will be the deadline for replication to complete (specify " +
          "'50' for 50 minutes).")
  private Optional<Integer> moveReplicationTimeout;

  @Option(names = {"--move-network-topology-enable"},
      description = "Whether to take network topology into account when " +
          "selecting a target for a source. " +
          "This configuration is false by default.")
  private Optional<Boolean> networkTopologyEnable;

  @Option(names = {"--include-datanodes"},
      description = "A list of Datanode " +
          "hostnames or ip addresses separated by commas. Only the Datanodes " +
          "specified in this list are balanced. This configuration is empty by " +
          "default and is applicable only if it is non-empty (specify \"hostname1,hostname2,hostname3\").")
  private Optional<String> includeNodes;

  @Option(names = {"--exclude-datanodes"},
      description =  "A list of Datanode " +
          "hostnames or ip addresses separated by commas. The Datanodes specified " +
          "in this list are excluded from balancing. This configuration is empty " +
          "by default (specify \"hostname1,hostname2,hostname3\").")
  private Optional<String> excludeNodes;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    StartContainerBalancerResponseProto response = scmClient.
        startContainerBalancer(threshold, iterations,
        maxDatanodesPercentageToInvolvePerIteration,
        maxSizeToMovePerIterationInGB, maxSizeEnteringTargetInGB,
        maxSizeLeavingSourceInGB, balancingInterval, moveTimeout,
        moveReplicationTimeout, networkTopologyEnable, includeNodes,
        excludeNodes);
    if (response.getStart()) {
      System.out.println("Container Balancer started successfully.");
    } else {
      String reason = "";
      System.err.println("Failed to start Container Balancer.");
      if (response.hasMessage()) {
        reason = response.getMessage();
        System.err.printf("Failure reason: %s%n", reason);
      }
      throw new IOException("Failed to start Container Balancer. " + reason);
    }
  }
}
