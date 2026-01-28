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

package org.apache.hadoop.hdds.scm.container.balancer;

import jakarta.annotation.Nonnull;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerBalancerConfigurationProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains configuration values for the ContainerBalancer.
 */
@ConfigGroup(prefix = "hdds.container.balancer")
public final class ContainerBalancerConfiguration {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerBalancerConfiguration.class);

  @Config(key = "hdds.container.balancer.utilization.threshold", type = ConfigType.AUTO, defaultValue =
      "10", tags = {ConfigTag.BALANCER},
      description = "Threshold is a percentage in the range of 0 to 100. A " +
          "cluster is considered balanced if for each datanode, the " +
          "utilization of the datanode (used space to capacity ratio) differs" +
          " from the utilization of the cluster (used space to capacity ratio" +
          " of the entire cluster) no more than the threshold.")
  private String threshold = "10";

  @Config(key = "hdds.container.balancer.datanodes.involved.max.percentage.per.iteration", type =
      ConfigType.INT, defaultValue = "20", tags = {ConfigTag.BALANCER},
      description = "Maximum percentage of healthy, in service datanodes " +
          "that can be involved in balancing in one iteration.")
  private int maxDatanodesPercentageToInvolvePerIteration = 20;

  @Config(key = "hdds.container.balancer.size.moved.max.per.iteration", type = ConfigType.SIZE,
      defaultValue = "500GB", tags = {ConfigTag.BALANCER},
      description = "The maximum size of data in bytes that will be moved " +
          "by Container Balancer in one iteration.")
  private long maxSizeToMovePerIteration = 500 * OzoneConsts.GB;

  @Config(key = "hdds.container.balancer.size.entering.target.max", type = ConfigType.SIZE,
      defaultValue = "26GB", tags = {ConfigTag.BALANCER}, description = "The " +
      "maximum size that can enter a target datanode in each " +
      "iteration while balancing. This is the sum of data from multiple " +
      "sources. The value must be greater than the configured" +
      " (or default) ozone.scm.container.size.")
  private long maxSizeEnteringTarget;

  @Config(key = "hdds.container.balancer.size.leaving.source.max", type = ConfigType.SIZE,
      defaultValue = "26GB", tags = {ConfigTag.BALANCER}, description = "The " +
      "maximum size that can leave a source datanode in each " +
      "iteration while balancing. This is the sum of data moving to multiple " +
      "targets. The value must be greater than the configured" +
      " (or default) ozone.scm.container.size.")
  private long maxSizeLeavingSource;

  @Config(key = "hdds.container.balancer.iterations", type = ConfigType.INT,
      defaultValue = "10", tags = {ConfigTag.BALANCER},
      description = "The number of iterations that Container Balancer will " +
          "run for.")
  private int iterations = 10;

  @Config(key = "hdds.container.balancer.exclude.containers", type = ConfigType.STRING, defaultValue =
      "", tags = {ConfigTag.BALANCER}, description = "List of container IDs " +
      "to exclude from balancing. For example \"1, 4, 5\" or \"1,4,5\".")
  private String excludeContainers = "";

  @Config(key = "hdds.container.balancer.move.timeout", type = ConfigType.TIME, defaultValue = "65m",
      tags = {ConfigTag.BALANCER}, description =
      "The amount of time to allow a single container to move " +
          "from source to target.")
  private long moveTimeout = Duration.ofMinutes(65).toMillis();

  @Config(key = "hdds.container.balancer.move.replication.timeout", type = ConfigType.TIME,
      defaultValue = "50m", tags = {ConfigTag.BALANCER}, description = "The " +
      "amount of time to allow a single container's replication from source " +
      "to target as part of container move. For example, if \"hdds.container" +
      ".balancer.move.timeout\" is 65 minutes, then out of those 65 minutes " +
      "50 minutes will be the deadline for replication to complete.")
  private long moveReplicationTimeout = Duration.ofMinutes(50).toMillis();

  @Config(key = "hdds.container.balancer.balancing.iteration.interval", type = ConfigType.TIME,
      defaultValue = "70m", tags = {ConfigTag.BALANCER}, description =
      "The interval period between each iteration of Container Balancer.")
  private long balancingInterval = Duration.ofMinutes(70).toMillis();

  @Config(key = "hdds.container.balancer.include.datanodes", type = ConfigType.STRING, defaultValue =
      "", tags = {ConfigTag.BALANCER}, description = "A list of Datanode " +
      "hostnames or ip addresses separated by commas. Only the Datanodes " +
      "specified in this list are balanced. This configuration is empty by " +
      "default and is applicable only if it is non-empty.")
  private String includeNodes = "";

  @Config(key = "hdds.container.balancer.exclude.datanodes", type = ConfigType.STRING, defaultValue =
      "", tags = {ConfigTag.BALANCER}, description = "A list of Datanode " +
      "hostnames or ip addresses separated by commas. The Datanodes specified" +
      " in this list are excluded from balancing. This configuration is empty" +
      " by default.")
  private String excludeNodes = "";

  @Config(key = "hdds.container.balancer.move.networkTopology.enable", type = ConfigType.BOOLEAN,
      defaultValue = "false", tags = {ConfigTag.BALANCER},
      description = "whether to take network topology into account when " +
          "selecting a target for a source. " +
          "This configuration is false by default.")
  private boolean networkTopologyEnable = false;

  @Config(key = "hdds.container.balancer.trigger.du.before.move.enable", type = ConfigType.BOOLEAN,
      defaultValue = "false", tags = {ConfigTag.BALANCER},
      description = "whether to send command to all the healthy and " +
          "in-service data nodes to run du immediately before starting" +
          "a balance iteration. note that running du is very time " +
          "consuming , especially when the disk usage rate of a " +
          "data node is very high")
  private boolean triggerDuEnable = false;

  /**
   * Gets the threshold value for Container Balancer.
   *
   * @return percentage value in the range 0 to 100
   */
  public double getThreshold() {
    return Double.parseDouble(threshold);
  }

  public double getThresholdAsRatio() {
    return Double.parseDouble(threshold) / 100;
  }

  /**
   * Sets the threshold value for Container Balancer.
   *
   * @param threshold a percentage value in the range 0 to 100
   */
  public void setThreshold(double threshold) {
    if (threshold < 0d || threshold >= 100d) {
      throw new IllegalArgumentException(
          "Threshold must be a percentage(double) in the range 0 to 100.");
    }
    this.threshold = String.valueOf(threshold);
  }

  /**
   * Gets the iteration count for Container Balancer. A value of -1 means
   * infinite number of iterations.
   *
   * @return a value greater than 0, or -1
   */
  public int getIterations() {
    return iterations;
  }

  /**
   * Sets the number of iterations for Container Balancer.
   *
   * @param count a value greater than 0, or -1 for running balancer infinitely
   */
  public void setIterations(int count) {
    if (count < -1 || 0 == count) {
      throw new IllegalArgumentException(
          "Iteration count must be greater than 0, or " +
              "-1(for running balancer infinitely).");
    }
    this.iterations = count;
  }

  /**
   * Gets the maximum percentage of healthy, in-service datanodes that will be
   * involved in balancing in one iteration.
   *
   * @return percentage as an integer from 0 up to and including 100
   */
  public int getMaxDatanodesPercentageToInvolvePerIteration() {
    return maxDatanodesPercentageToInvolvePerIteration;
  }

  /**
   * Get the NetworkTopologyEnable value for Container Balancer.
   *
   * @return the boolean value of networkTopologyEnable
   */
  public Boolean getNetworkTopologyEnable() {
    return networkTopologyEnable;
  }

  /**
   * Get the triggerDuEnable value for Container Balancer.
   *
   * @return the boolean value of triggerDuEnable
   */
  public Boolean getTriggerDuEnable() {
    return triggerDuEnable;
  }

  public void setTriggerDuEnable(boolean enable) {
    triggerDuEnable = enable;
  }

  /**
   * Set the NetworkTopologyEnable value for Container Balancer.
   *
   * @param enable the boolean value to be set to networkTopologyEnable
   */
  public void setNetworkTopologyEnable(Boolean enable) {
    networkTopologyEnable = enable;
  }

  /**
   * Gets the ratio of maximum datanodes involved in balancing to the total
   * number of healthy, in-service datanodes known to SCM.
   *
   * @return ratio as a double from 0 up to and including 1
   */
  public double getMaxDatanodesRatioToInvolvePerIteration() {
    return maxDatanodesPercentageToInvolvePerIteration / 100d;
  }

  /**
   * Sets the maximum percentage of healthy, in-service datanodes that will be
   * involved in balancing in one iteration.
   *
   * @param maxDatanodesPercentageToInvolvePerIteration number of datanodes
   *                                                    to involve divided by
   *                                                    total number of
   *                                                    healthy, in-service
   *                                                    datanodes multiplied
   *                                                    by 100
   */
  public void setMaxDatanodesPercentageToInvolvePerIteration(
      int maxDatanodesPercentageToInvolvePerIteration) {
    if (maxDatanodesPercentageToInvolvePerIteration < 0 ||
        maxDatanodesPercentageToInvolvePerIteration > 100) {
      throw new IllegalArgumentException(String.format("Argument %d is " +
              "illegal. Percentage must be from 0 up to and including 100.",
          maxDatanodesPercentageToInvolvePerIteration));
    }
    this.maxDatanodesPercentageToInvolvePerIteration =
        maxDatanodesPercentageToInvolvePerIteration;
  }

  /**
   * Gets the maximum size that will be moved by Container Balancer in one
   * iteration.
   *
   * @return maximum size in Bytes
   */
  public long getMaxSizeToMovePerIteration() {
    return maxSizeToMovePerIteration;
  }

  /**
   * Sets the value of maximum size that will be moved by Container Balancer
   * in one iteration.
   *
   * @param maxSizeToMovePerIteration maximum number of Bytes
   */
  public void setMaxSizeToMovePerIteration(long maxSizeToMovePerIteration) {
    this.maxSizeToMovePerIteration = maxSizeToMovePerIteration;
  }

  public long getMaxSizeEnteringTarget() {
    return maxSizeEnteringTarget;
  }

  public void setMaxSizeEnteringTarget(long maxSizeEnteringTarget) {
    this.maxSizeEnteringTarget = maxSizeEnteringTarget;
  }

  public long getMaxSizeLeavingSource() {
    return maxSizeLeavingSource;
  }

  public void setMaxSizeLeavingSource(long maxSizeLeavingSource) {
    this.maxSizeLeavingSource = maxSizeLeavingSource;
  }

  public Set<ContainerID> getExcludeContainers() {
    if (excludeContainers.isEmpty()) {
      return new HashSet<>();
    }
    return Arrays.stream(excludeContainers.split(","))
        .map(s -> {
          s = s.trim();
          return ContainerID.valueOf(Long.parseLong(s));
        }).collect(Collectors.toSet());
  }

  /**
   * Sets containers to exclude from balancing.
   * @param excludeContainers String of {@link ContainerID} to exclude. For
   *                          example, "1, 4, 5" or "1,4,5".
   */
  public void setExcludeContainers(String excludeContainers) {
    this.excludeContainers = excludeContainers;
  }

  public Duration getMoveTimeout() {
    return Duration.ofMillis(moveTimeout);
  }

  public void setMoveTimeout(Duration duration) {
    this.moveTimeout = duration.toMillis();
  }

  public void setMoveTimeout(long millis) {
    this.moveTimeout = millis;
  }

  public Duration getMoveReplicationTimeout() {
    return Duration.ofMillis(moveReplicationTimeout);
  }

  public void setMoveReplicationTimeout(Duration duration) {
    this.moveReplicationTimeout = duration.toMillis();
  }

  public void setMoveReplicationTimeout(long millis) {
    this.moveReplicationTimeout = millis;
  }

  public Duration getBalancingInterval() {
    return Duration.ofMillis(balancingInterval);
  }

  public void setBalancingInterval(Duration balancingInterval) {
    this.balancingInterval = balancingInterval.toMillis();
  }

  public void setBalancingInterval(long millis) {
    this.balancingInterval = millis;
  }

  /**
   * Gets a set of datanode hostnames or ip addresses that will be the exclusive
   * participants in balancing.
   * @return Set of hostname or ip address strings, or an empty set if the
   * configuration is empty
   */
  public Set<String> getIncludeNodes() {
    if (includeNodes.isEmpty()) {
      return Collections.emptySet();
    }
    return Arrays.stream(includeNodes.split(","))
        .map(String::trim)
        .collect(Collectors.toSet());
  }

  /**
   * Sets the datanodes that will be the exclusive participants in balancing.
   * Applicable only if the specified string is non-empty.
   * @param includeNodes a String of datanode hostnames or ip addresses
   *                     separated by commas
   */
  public void setIncludeNodes(String includeNodes) {
    this.includeNodes = includeNodes;
  }

  /**
   * Gets a set of datanode hostnames or ip addresses that will be excluded
   * from balancing.
   * @return Set of hostname or ip address strings, or an empty set if the
   * configuration is empty
   */
  public Set<String> getExcludeNodes() {
    if (excludeNodes.isEmpty()) {
      return Collections.emptySet();
    }
    return Arrays.stream(excludeNodes.split(","))
        .map(String::trim)
        .collect(Collectors.toSet());
  }

  /**
   * Sets the datanodes that will be excluded from balancing.
   * @param excludeNodes a String of datanode hostnames or ip addresses
   *                     separated by commas
   */
  public void setExcludeNodes(String excludeNodes) {
    this.excludeNodes = excludeNodes;
  }

  @Override
  public String toString() {
    return String.format("Container Balancer Configuration values:%n" +
            "%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %d%n" +
            "%-50s %dGB%n" +
            "%-50s %dGB%n" +
            "%-50s %dGB%n" +
            "%-50s %d%n" +
            "%-50s %dmin%n" +
            "%-50s %dmin%n" +
            "%-50s %dmin%n" +
            "%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %s%n", "Key", "Value", "Threshold",
        threshold, "Max Datanodes to Involve per Iteration(percent)",
        maxDatanodesPercentageToInvolvePerIteration,
        "Max Size to Move per Iteration",
        maxSizeToMovePerIteration / OzoneConsts.GB,
        "Max Size Entering Target per Iteration",
        maxSizeEnteringTarget / OzoneConsts.GB,
        "Max Size Leaving Source per Iteration",
        maxSizeLeavingSource / OzoneConsts.GB,
        "Number of Iterations",
        iterations,
        "Time Limit for Single Container's Movement",
        Duration.ofMillis(moveTimeout).toMinutes(),
        "Time Limit for Single Container's Replication",
        Duration.ofMillis(moveReplicationTimeout).toMinutes(),
        "Interval between each Iteration",
        Duration.ofMillis(balancingInterval).toMinutes(),
        "Whether to Enable Network Topology",
        networkTopologyEnable,
        "Whether to Trigger Refresh Datanode Usage Info",
        triggerDuEnable,
        "Container IDs to Exclude from Balancing",
        excludeContainers.equals("") ? "None" : excludeContainers,
        "Datanodes Specified to be Balanced",
        includeNodes.equals("") ? "None" : includeNodes,
        "Datanodes Excluded from Balancing",
        excludeNodes.equals("") ? "None" : excludeNodes);
  }

  public ContainerBalancerConfigurationProto.Builder toProtobufBuilder() {
    ContainerBalancerConfigurationProto.Builder builder =
        ContainerBalancerConfigurationProto.newBuilder();

    builder.setUtilizationThreshold(threshold)
        .setDatanodesInvolvedMaxPercentagePerIteration(
            maxDatanodesPercentageToInvolvePerIteration)
        .setSizeMovedMaxPerIteration(maxSizeToMovePerIteration)
        .setSizeEnteringTargetMax(maxSizeEnteringTarget)
        .setSizeLeavingSourceMax(maxSizeLeavingSource)
        .setIterations(iterations)
        .setExcludeContainers(excludeContainers)
        .setMoveTimeout(moveTimeout)
        .setBalancingIterationInterval(balancingInterval)
        .setIncludeDatanodes(includeNodes)
        .setExcludeDatanodes(excludeNodes)
        .setMoveNetworkTopologyEnable(networkTopologyEnable)
        .setTriggerDuBeforeMoveEnable(triggerDuEnable)
        .setMoveReplicationTimeout(moveReplicationTimeout);
    return builder;
  }

  static ContainerBalancerConfiguration fromProtobuf(
      @Nonnull ContainerBalancerConfigurationProto proto,
      @Nonnull OzoneConfiguration ozoneConfiguration) {
    ContainerBalancerConfiguration config =
        ozoneConfiguration.getObject(ContainerBalancerConfiguration.class);
    if (proto.hasUtilizationThreshold()) {
      config.setThreshold(Double.parseDouble(proto.getUtilizationThreshold()));
    }
    if (proto.hasDatanodesInvolvedMaxPercentagePerIteration()) {
      config.setMaxDatanodesPercentageToInvolvePerIteration(
          proto.getDatanodesInvolvedMaxPercentagePerIteration());
    }
    if (proto.hasSizeMovedMaxPerIteration()) {
      config.setMaxSizeToMovePerIteration(proto.getSizeMovedMaxPerIteration());
    }
    if (proto.hasSizeEnteringTargetMax()) {
      config.setMaxSizeEnteringTarget(proto.getSizeEnteringTargetMax());
    }
    if (proto.hasSizeLeavingSourceMax()) {
      config.setMaxSizeLeavingSource(proto.getSizeLeavingSourceMax());
    }
    if (proto.hasIterations()) {
      config.setIterations(proto.getIterations());
    }
    if (proto.hasExcludeContainers()) {
      config.setExcludeContainers(proto.getExcludeContainers());
    }
    if (proto.hasMoveTimeout()) {
      config.setMoveTimeout(proto.getMoveTimeout());
    }
    if (proto.hasBalancingIterationInterval()) {
      config.setBalancingInterval(proto.getBalancingIterationInterval());
    }
    if (proto.hasIncludeDatanodes()) {
      config.setIncludeNodes(proto.getIncludeDatanodes());
    }
    if (proto.hasExcludeDatanodes()) {
      config.setExcludeNodes(proto.getExcludeDatanodes());
    }
    if (proto.hasMoveNetworkTopologyEnable()) {
      config.setNetworkTopologyEnable(proto.getMoveNetworkTopologyEnable());
    }
    if (proto.hasTriggerDuBeforeMoveEnable()) {
      config.setTriggerDuEnable(proto.getTriggerDuBeforeMoveEnable());
    }
    if (proto.hasMoveReplicationTimeout()) {
      config.setMoveReplicationTimeout(proto.getMoveReplicationTimeout());
    }
    return config;
  }
}
