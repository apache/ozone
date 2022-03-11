/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.cli;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.util.Optional;

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
          "the cluster after which a datanode will be rebalanced (for " +
          "example, '10' for 10%%).")
  private Optional<Double> threshold;

  @Option(names = {"-i", "--iterations"},
      description = "Maximum consecutive iterations that" +
          " balancer will run for.")
  private Optional<Integer> iterations;

  @Option(names = {"-d", "--maxDatanodesPercentageToInvolvePerIteration"},
      description = "Max percentage of healthy, in service datanodes " +
          "that can be involved in balancing in one iteration (for example, " +
          "'20' for 20%%).")
  private Optional<Integer> maxDatanodesPercentageToInvolvePerIteration;

  @Option(names = {"-s", "--maxSizeToMovePerIterationInGB"},
      description = "Maximum size that can be moved per iteration of " +
          "balancing (for example, '500' for 500GB).")
  private Optional<Long> maxSizeToMovePerIterationInGB;

  @Option(names = {"-e", "--maxSizeEnteringTargetInGB"},
      description = "Maximum size that can enter a target datanode while " +
          "balancing. This is the sum of data from multiple sources (for " +
          "example, '26' for 26GB).")
  private Optional<Long> maxSizeEnteringTargetInGB;

  @Option(names = {"-l", "--maxSizeLeavingSourceInGB"},
      description = "Maximum size that can leave a source datanode while " +
          "balancing. This is the sum of data moving to multiple targets " +
          "(for example, '26' for 26GB).")
  private Optional<Long> maxSizeLeavingSourceInGB;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    boolean result = scmClient.startContainerBalancer(threshold, iterations,
        maxDatanodesPercentageToInvolvePerIteration,
        maxSizeToMovePerIterationInGB, maxSizeEnteringTargetInGB,
        maxSizeLeavingSourceInGB);
    if (result) {
      System.out.println("Container Balancer started successfully.");
      return;
    }
    System.out.println("Container Balancer is either already running or " +
        "failed to start.\nPlease check the logs for more info.");
  }
}