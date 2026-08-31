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
import java.util.Locale;
import java.util.Optional;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeUsageInfoProto;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerAdvisor;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerEstimation;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerProfile;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Estimates container balancer bytes to move, per iteration bytes, estimated iterations, estimated duration
 * without starting the balancer.
 */
@Command(
    name = "dry-run",
    description = "Estimate container balancer bytes to move, iterations, per iteration bytes " +
        "and upper-bound duration without starting it. Limits and default profile presets are read from " +
        "local ozone-site.xml, datanode usage is fetched from SCM.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ContainerBalancerDryRunSubcommand extends ScmSubcommand {

  private static final double PLANNING_ITERATION_BUFFER = 1.3d;

  @CommandLine.Mixin
  private ContainerBalancerConfigOptions configOptions;

  @Option(names = {"--profile"},
      description = "Throttling profile: slow, medium, or fast. When set, only this profile is estimated. "
          + "When omitted, dry-run estimates all three profiles. Start does not support --profile yet.")
  private Optional<String> profileName = Optional.empty();

  @Override
  public void execute(ScmClient scmClient) throws IOException {

    List<DatanodeUsageInfoProto> nodes = scmClient.getDatanodeUsageInfo(true, Integer.MAX_VALUE);
    if (nodes == null || nodes.isEmpty()) {
      throw new IOException("No datanode usage information available from SCM.");
    }

    OzoneConfiguration conf = getOzoneConf();
    ContainerBalancerAdvisor.AdvisorRequest request = buildRequest(nodes);
    List<ContainerBalancerEstimation> estimations;
    try {
      estimations = ContainerBalancerAdvisor.estimateDryRun(conf, request);
    } catch (IllegalArgumentException e) {
      throw new IOException(e.getMessage(), e);
    }

    boolean anySucceeded = false;
    for (ContainerBalancerEstimation result : estimations) {
      out().printf("Profile: %s%n", result.getProfile().name());
      printBasedOn(result);
      if (result.succeeded()) {
        anySucceeded = true;
        printEstimation(result);
      } else {
        out().printf(" Estimation failed: %s%n%n", result.getFailureMessage());
      }
    }
    if (!anySucceeded) {
      throw new IOException(estimations.get(0).getFailureMessage());
    }
  }

  private ContainerBalancerAdvisor.AdvisorRequest buildRequest(List<DatanodeUsageInfoProto> nodes) throws IOException {
    ContainerBalancerAdvisor.AdvisorRequest request = new ContainerBalancerAdvisor.AdvisorRequest().setNodes(nodes);
    configOptions.applyToDryRunRequest(request);

    if (profileName.isPresent()) {
      request.setProfile(parseProfile(profileName.get()));
    }
    return request;
  }

  private static ContainerBalancerProfile parseProfile(String name) throws IOException {
    try {
      return ContainerBalancerProfile.valueOf(name.trim().toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid profile: " + name + ". Expected slow, medium, or fast.");
    }
  }

  private void printBasedOn(ContainerBalancerEstimation estimation) {
    long moveTimeoutMinutes = Math.round(estimation.getMoveTimeoutMillis() / 60000d);
    long balancingIntervalMinutes = Math.round(estimation.getBalancingIntervalMillis() / 60000d);
    out().println(" Based on:");
    out().printf(Locale.ENGLISH, "   Datanode involvement:     %d%%%n",
        estimation.getMaxDatanodesPercentage());
    out().printf("   Max entering target:      %s / node%n", byteDesc(estimation.getMaxSizeEnteringTarget()));
    out().printf("   Max leaving source:       %s / node%n", byteDesc(estimation.getMaxSizeLeavingSource()));
    out().printf("   Max per iteration:        %s%n", byteDesc(estimation.getMaxSizeToMovePerIteration()));
    out().printf("   Move timeout:             %d min%n", moveTimeoutMinutes);
    out().printf("   Balancing interval:       %d min%n", balancingIntervalMinutes);
  }

  private void printEstimation(ContainerBalancerEstimation estimation) {
    long estimatedIterations = estimation.getEstimatedIterations();
    long planningIterations = (long) Math.ceil(estimatedIterations * PLANNING_ITERATION_BUFFER);
    long cycleTimeMillis = estimation.getMoveTimeoutMillis() + estimation.getBalancingIntervalMillis();
    long baseDurationMillis = estimation.getEstimatedDurationMillis();
    long planningDurationMillis = planningIterations * cycleTimeMillis;
    out().printf(" Bytes to move:            %s%n", byteDesc(estimation.getBytesToMove()));
    out().printf(" Per iteration (estimate): ~%s%n", byteDesc(estimation.getPerIterationBytes()));
    out().printf(" Estimated iterations:     %d (planning estimate: %d, includes +30%% buffer)%n",
        estimatedIterations, planningIterations);
    out().printf(" Estimated duration:       upper bound %s (planning estimate: %s, includes +30%% buffer)%n",
        formatEstimatedDuration(baseDurationMillis),
        formatEstimatedDuration(planningDurationMillis));
    out().println("                           (assumes full move timeout + interval each cycle)");
    out().println();
  }

  private static String formatEstimatedDuration(long durationMillis) {
    double days = durationMillis / 86400000d;
    if (days >= 1) {
      return String.format(Locale.ENGLISH, "~%.1f days", days);
    }
    double hours = durationMillis / 3600000d;
    if (hours >= 1) {
      return String.format(Locale.ENGLISH, "~%.1f hours", hours);
    }
    long minutes = durationMillis / 60000;
    return String.format(Locale.ENGLISH, "~%d min", minutes);
  }
}
