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

  @Option(names = {"-i", "--idleiterations"},
      description = "Maximum consecutive idle iterations")
  private Optional<Integer> idleiterations;

  @Option(names = {"-d", "--maxDatanodesRatioToInvolvePerIteration"},
      description = "The ratio of maximum number of datanodes that should be " +
          "involved in balancing in one iteration to the total number of " +
          "healthy, in service nodes known to container balancer.")
  private Optional<Double> maxDatanodesRatioToInvolvePerIteration;

  @Option(names = {"-s", "--maxSizeToMovePerIterationInGB"},
      description = "Maximum size to move per iteration of balancing in GB, " +
          "for 10GB it should be set as 10")
  private Optional<Long> maxSizeToMovePerIterationInGB;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    boolean result = scmClient.startContainerBalancer(threshold, idleiterations,
        maxDatanodesRatioToInvolvePerIteration, maxSizeToMovePerIterationInGB);
    if (result) {
      System.out.println("Starting ContainerBalancer Successfully.");
      return;
    }
    System.out.println("ContainerBalancer is already running, " +
        "Please stop it first.");
  }
}