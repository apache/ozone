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
      description = "Threshold target whether the cluster is balanced")
  private Optional<Double> threshold;

  @Option(names = {"-i", "--iterations"},
      description = "Maximum consecutive iterations that" +
          " balancer will run for")
  private Optional<Integer> iterations;

  @Option(names = {"-d", "--maxDatanodesPercentageToInvolvePerIteration"},
      description = "The max percentage of healthy, in service datanodes " +
          "that can be involved in balancing in one iteration.")
  private Optional<Integer> maxDatanodesPercentageToInvolvePerIteration;


  @Option(names = {"-s", "--maxSizeToMovePerIterationInGB"},
      description = "Maximum size to move per iteration of balancing in GB, " +
          "for 10GB it should be set as 10")
  private Optional<Long> maxSizeToMovePerIterationInGB;

  @Option(names = {"-e", "--maxSizeEnteringTarget"},
      description = "the maximum size that can enter a target datanode while " +
          "balancing in GB. This is the sum of data from multiple sources.")
  private Optional<Long> maxSizeEnteringTargetInGB;

  @Option(names = {"-l", "--maxSizeLeavingSource"},
      description = "maximum size that can leave a source datanode while " +
          "balancing in GB, it is the sum of data moving to multiple targets.")
  private Optional<Long> maxSizeLeavingSourceInGB;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    boolean result = scmClient.startContainerBalancer(threshold, iterations,
        maxDatanodesPercentageToInvolvePerIteration,
        maxSizeToMovePerIterationInGB, maxSizeEnteringTargetInGB,
        maxSizeLeavingSourceInGB);
    if (result) {
      System.out.println("Starting ContainerBalancer Successfully.");
      return;
    }
    System.out.println("ContainerBalancer is already running, " +
        "Please stop it first.");
  }
}