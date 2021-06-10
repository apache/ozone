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
  private double threshold;

  @Option(names = {"-i", "--idleiterations"},
      description = "Maximum consecutive idle iterations",
      //idleiteration could be as follows:
      // 0 ： indicate running infinitely
      // positive number ： indicate the specific iterations
      // -1 : user does not set this value
      defaultValue = "-1")
  private int idleiterations;

  @Option(names = {"-d", "--maxDatanodesToBalance"},
      description = "Maximum datanodes to move")
  private int maxDatanodesToBalance;

  @Option(names = {"-s", "--maxSizeToMove"},
      description = "Maximum size to move")
  private long maxSizeToMove;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    boolean result = scmClient.startContainerBalancer(threshold, idleiterations,
        maxDatanodesToBalance, maxSizeToMove);
    if (result) {
      System.out.println("Starting ContainerBalancer Successfully.");
      return;
    }
    System.out.println("ContainerBalancer has been started" +
        " by another cli command.");
  }
}
