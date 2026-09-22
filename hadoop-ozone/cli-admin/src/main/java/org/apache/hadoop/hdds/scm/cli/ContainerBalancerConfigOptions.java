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

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerAdvisor;
import org.apache.hadoop.ozone.OzoneConsts;
import picocli.CommandLine.Option;

/**
 * Shared Picocli options for container balancer commands.
 */
public class ContainerBalancerConfigOptions {

  @Option(names = {"-t", "--threshold"},
      description = "Percentage deviation from average utilization of " +
          "the cluster after which a datanode will be rebalanced. The value " +
          "should be in the range [0.0, 100.0), with a default of 10 " +
          "(specify '10' for 10%%).")
  private Optional<Double> threshold;

  @Option(names = {"-d", "--max-datanodes-percentage-to-involve-per-iteration"},
      description = "Max percentage of healthy, in service datanodes " +
          "that can be involved in balancing in one iteration. The value " +
          "should be in the range (0,100]. When omitted on dry-run, each profile uses its default preset. " +
          "When omitted on start, the global config with a default of 20 (specify '20' for 20%%).")
  private Optional<Integer> maxDatanodesPercentageToInvolvePerIteration;

  @Option(names = {"-s", "--max-size-to-move-per-iteration-in-gb"},
      description = "Maximum size that can be moved per iteration of " +
          "balancing. The value should be positive. When omitted, the " +
          "global config default 500 (specify '500' for 500GB) is used on both dry-run and start.")
  private Optional<Long> maxSizeToMovePerIterationInGB;

  @Option(names = {"-e", "--max-size-entering-target-in-gb"},
      description = "Maximum size that can enter a target datanode while " +
          "balancing. This is the sum of data from multiple sources. The value " +
          "should be positive. When omitted on dry-run, each profile uses its default preset. " +
          "When omitted on start, the global config default 26 (specify '26' for 26GB) is used.")
  private Optional<Long> maxSizeEnteringTargetInGB;

  @Option(names = {"-l", "--max-size-leaving-source-in-gb"},
      description = "Maximum size that can leave a source datanode while " +
          "balancing. This is the sum of data moving to multiple targets. " +
          "The value should be positive. When omitted on dry-run, each profile uses its default preset. " +
          "When omitted on start, the global config default 26 (specify '26' for 26GB) is used.")
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
  
  public Optional<Double> getThreshold() {
    return threshold;
  }

  public Optional<Integer> getMaxDatanodesPercentageToInvolvePerIteration() {
    return maxDatanodesPercentageToInvolvePerIteration;
  }

  public Optional<Long> getMaxSizeToMovePerIterationInGB() {
    return maxSizeToMovePerIterationInGB;
  }

  public Optional<Long> getMaxSizeEnteringTargetInGB() {
    return maxSizeEnteringTargetInGB;
  }

  public Optional<Long> getMaxSizeLeavingSourceInGB() {
    return maxSizeLeavingSourceInGB;
  }

  public Optional<Integer> getBalancingIntervalMinutes() {
    return balancingInterval;
  }

  public Optional<Integer> getMoveTimeoutMinutes() {
    return moveTimeout;
  }

  public Optional<Integer> getMoveReplicationTimeoutMinutes() {
    return moveReplicationTimeout;
  }

  public Optional<String> getIncludeNodes() {
    return includeNodes;
  }

  public Optional<String> getExcludeNodes() {
    return excludeNodes;
  }

  /** Applies CLI overrides to a dry-run request. */
  public void applyToDryRunRequest(ContainerBalancerAdvisor.AdvisorRequest request) {
    threshold.ifPresent(request::setThresholdPercent);
    maxDatanodesPercentageToInvolvePerIteration.ifPresent(
        request::setMaxDatanodesPercentageToInvolvePerIteration);
    maxSizeToMovePerIterationInGB.ifPresent(gb ->
        request.setMaxSizeToMovePerIteration(gb * OzoneConsts.GB));
    maxSizeEnteringTargetInGB.ifPresent(gb ->
        request.setMaxSizeEnteringTarget(gb * OzoneConsts.GB));
    maxSizeLeavingSourceInGB.ifPresent(gb ->
        request.setMaxSizeLeavingSource(gb * OzoneConsts.GB));
    balancingInterval.ifPresent(minutes ->
        request.setBalancingIntervalMillis(Duration.ofMinutes(minutes).toMillis()));
    moveTimeout.ifPresent(minutes ->
        request.setMoveTimeoutMillis(Duration.ofMinutes(minutes).toMillis()));
    moveReplicationTimeout.ifPresent(minutes ->
        request.setMoveReplicationTimeoutMillis(Duration.ofMinutes(minutes).toMillis()));
    includeNodes.ifPresent(value -> request.setIncludeNodes(parseNodeSet(value)));
    excludeNodes.ifPresent(value -> request.setExcludeNodes(parseNodeSet(value)));
  }

  private static Set<String> parseNodeSet(String nodes) {
    if (StringUtils.isBlank(nodes)) {
      return Collections.emptySet();
    }
    return Arrays.stream(nodes.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toSet());
  }
}
