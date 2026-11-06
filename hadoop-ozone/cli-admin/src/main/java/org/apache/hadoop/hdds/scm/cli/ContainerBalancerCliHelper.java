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
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeUsageInfoProto;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerAdvisor;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerEstimation;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerProfile;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerRecommendation;
import org.apache.hadoop.ozone.OzoneConsts;

/** Shared parsing and CLI output for container balancer subcommands. */
public final class ContainerBalancerCliHelper {

  /** Multiplier for recommended/planning iteration counts and CLI planning display (+30%). */
  public static final double PLANNING_ITERATION_BUFFER = 1.3d;

  private ContainerBalancerCliHelper() {
  }

  public static ContainerBalancerProfile parseProfile(String name) throws IOException {
    try {
      return ContainerBalancerProfile.valueOf(name.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid profile: " + name + ". Expected SLOW, MEDIUM, or FAST.");
    }
  }

  public static Set<String> parseNodeSet(String nodes) {
    if (StringUtils.isBlank(nodes)) {
      return Collections.emptySet();
    }
    return Arrays.stream(nodes.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toSet());
  }

  public static void printEstimation(PrintWriter out, ContainerBalancerEstimation estimation) {
    long estimatedIterations = estimation.getEstimatedIterations();
    long planningIterations = (long) Math.ceil(estimatedIterations * PLANNING_ITERATION_BUFFER);
    long cycleTimeMillis = estimation.getMoveTimeoutMillis() + estimation.getBalancingIntervalMillis();
    long baseDurationMillis = estimation.getEstimatedDurationMillis();
    long planningDurationMillis = planningIterations * cycleTimeMillis;
    out.printf(" Bytes to move:            %s%n", byteDesc(estimation.getBytesToMove()));
    out.printf(" Per iteration (estimate): ~%s%n", byteDesc(estimation.getPerIterationBytes()));
    out.printf(" Estimated iterations:     %d (planning estimate: %d, includes +30%% buffer)%n",
        estimatedIterations, planningIterations);
    out.printf(" Estimated duration:       upper bound %s (planning estimate: %s, includes +30%% buffer)%n",
        formatEstimatedDuration(baseDurationMillis),
        formatEstimatedDuration(planningDurationMillis));
    out.println("                           (assumes full move timeout + interval each cycle)");
    out.println();
  }

  public static String formatEstimatedDuration(long durationMillis) {
    double days = durationMillis / 86400000d;
    if (days >= 1) {
      return String.format("~%.1f days", days);
    }
    double hours = durationMillis / 3600000d;
    if (hours >= 1) {
      return String.format("~%.1f hours", hours);
    }
    long minutes = durationMillis / 60000;
    return String.format("~%d min", minutes);
  }

  public static String formatStartCommand(
      ContainerBalancerRecommendation recommendation,
      Optional<String> includeNodes,
      Optional<String> excludeNodes) {
    StringBuilder sb = new StringBuilder("ozone admin containerbalancer start");
    sb.append(" -t ").append(formatThreshold(recommendation.getThresholdPercent()))
        .append(" -i ").append(recommendation.getRecommendedIterations())
        .append(" -d ").append(recommendation.getMaxDatanodesPercentage())
        .append(" -s ").append(bytesToGb(recommendation.getMaxSizeToMovePerIteration()))
        .append(" -e ").append(bytesToGb(recommendation.getMaxSizeEnteringTarget()))
        .append(" -l ").append(bytesToGb(recommendation.getMaxSizeLeavingSource()))
        .append(" --balancing-iteration-interval-minutes ")
        .append(recommendation.getBalancingIntervalMillis() / 60000)
        .append(" --move-timeout-minutes ")
        .append(recommendation.getMoveTimeoutMillis() / 60000)
        .append(" --move-replication-timeout-minutes ")
        .append(recommendation.getMoveReplicationTimeoutMillis() / 60000);
    appendNodeFilters(sb, includeNodes, excludeNodes);
    return sb.toString();
  }

  private static void appendNodeFilters(
      StringBuilder sb,
      Optional<String> includeNodes,
      Optional<String> excludeNodes) {
    includeNodes.filter(s -> !StringUtils.isBlank(s))
        .ifPresent(s -> sb.append(" --include-datanodes ").append(s.trim()));
    excludeNodes.filter(s -> !StringUtils.isBlank(s))
        .ifPresent(s -> sb.append(" --exclude-datanodes ").append(s.trim()));
  }

  private static long bytesToGb(long bytes) {
    return bytes / OzoneConsts.GB;
  }

  private static String formatThreshold(double thresholdPercent) {
    if (thresholdPercent == Math.rint(thresholdPercent)) {
      return String.valueOf((long) thresholdPercent);
    }
    return String.valueOf(thresholdPercent);
  }

  /** Start parameters derived from a recommendation for {@code startContainerBalancer}. */
  public static final class EffectiveStartParameters {
    private final Optional<Double> threshold;
    private final Optional<Integer> iterations;
    private final Optional<Integer> maxDatanodesPercentageToInvolvePerIteration;
    private final Optional<Long> maxSizeToMovePerIterationInGB;
    private final Optional<Long> maxSizeEnteringTargetInGB;
    private final Optional<Long> maxSizeLeavingSourceInGB;
    private final Optional<Integer> balancingIntervalMinutes;
    private final Optional<Integer> moveTimeoutMinutes;
    private final Optional<Integer> moveReplicationTimeoutMinutes;

    private EffectiveStartParameters(Builder b) {
      this.threshold = b.threshold;
      this.iterations = b.iterations;
      this.maxDatanodesPercentageToInvolvePerIteration = b.maxDatanodesPercentageToInvolvePerIteration;
      this.maxSizeToMovePerIterationInGB = b.maxSizeToMovePerIterationInGB;
      this.maxSizeEnteringTargetInGB = b.maxSizeEnteringTargetInGB;
      this.maxSizeLeavingSourceInGB = b.maxSizeLeavingSourceInGB;
      this.balancingIntervalMinutes = b.balancingIntervalMinutes;
      this.moveTimeoutMinutes = b.moveTimeoutMinutes;
      this.moveReplicationTimeoutMinutes = b.moveReplicationTimeoutMinutes;
    }
    
    static Builder builder() {
      return new Builder();
    }
    
    static final class Builder {
      private Optional<Double> threshold = Optional.empty();
      private Optional<Integer> iterations = Optional.empty();
      private Optional<Integer> maxDatanodesPercentageToInvolvePerIteration = Optional.empty();
      private Optional<Long> maxSizeToMovePerIterationInGB = Optional.empty();
      private Optional<Long> maxSizeEnteringTargetInGB = Optional.empty();
      private Optional<Long> maxSizeLeavingSourceInGB = Optional.empty();
      private Optional<Integer> balancingIntervalMinutes = Optional.empty();
      private Optional<Integer> moveTimeoutMinutes = Optional.empty();
      private Optional<Integer> moveReplicationTimeoutMinutes = Optional.empty();
      
      Builder threshold(Optional<Double> v) { 
        this.threshold = v; return this; 
      }
      
      Builder iterations(Optional<Integer> v) { 
        this.iterations = v; return this;
      }
      
      Builder maxDatanodesPercentageToInvolvePerIteration(Optional<Integer> v) {
        this.maxDatanodesPercentageToInvolvePerIteration = v; return this;
      }
      
      Builder maxSizeToMovePerIterationInGB(Optional<Long> v) {
        this.maxSizeToMovePerIterationInGB = v; return this;
      }
      
      Builder maxSizeEnteringTargetInGB(Optional<Long> v) {
        this.maxSizeEnteringTargetInGB = v; return this;
      }
      
      Builder maxSizeLeavingSourceInGB(Optional<Long> v) {
        this.maxSizeLeavingSourceInGB = v; return this;
      }
      
      Builder balancingIntervalMinutes(Optional<Integer> v) {
        this.balancingIntervalMinutes = v; return this;
      }
      
      Builder moveTimeoutMinutes(Optional<Integer> v) { 
        this.moveTimeoutMinutes = v; return this;
      }
      
      Builder moveReplicationTimeoutMinutes(Optional<Integer> v) {
        this.moveReplicationTimeoutMinutes = v; return this;
      }
      
      EffectiveStartParameters build() {
        return new EffectiveStartParameters(this);
      }
    }

    public Optional<Double> getThreshold() {
      return threshold;
    }

    public Optional<Integer> getIterations() {
      return iterations;
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
      return balancingIntervalMinutes;
    }

    public Optional<Integer> getMoveTimeoutMinutes() {
      return moveTimeoutMinutes;
    }

    public Optional<Integer> getMoveReplicationTimeoutMinutes() {
      return moveReplicationTimeoutMinutes;
    }
  }

  public static ContainerBalancerRecommendation recommendForAutoConfig(
      OzoneConfiguration conf, List<DatanodeUsageInfoProto> nodes, ContainerBalancerConfigOptions configOptions,
      Optional<String> profileName) throws IOException {
    ContainerBalancerAdvisor.AdvisorRequest request = new ContainerBalancerAdvisor.AdvisorRequest().setNodes(nodes);
    configOptions.applyToEstimateRequest(request);
    ContainerBalancerProfile profile = profileName.isPresent()
        ? parseProfile(profileName.get())
        : ContainerBalancerProfile.MEDIUM;
    request.setProfile(profile);

    List<ContainerBalancerRecommendation> recommendations;
    try {
      recommendations = ContainerBalancerAdvisor.recommend(conf, request);
    } catch (IllegalArgumentException e) {
      throw new IOException(e.getMessage(), e);
    }

    if (recommendations.isEmpty()) {
      throw new IOException("No container balancer recommendation returned.");
    }

    ContainerBalancerRecommendation recommendation = recommendations.get(0);
    if (!recommendation.succeeded()) {
      throw new IOException(recommendation.getFailureMessage());
    }
    return recommendation;
  }

  /** Maps a recommendation to explicit start optionals, {@code -i} overrides recommended iterations. */
  public static EffectiveStartParameters startParametersFromRecommendation(
      ContainerBalancerRecommendation recommendation,
      Optional<Integer> iterationsOverride, ContainerBalancerConfigOptions configOptions) {
    Optional<Integer> effectiveIterations = iterationsOverride.isPresent()
        ? iterationsOverride
        : Optional.of(recommendation.getRecommendedIterations());
    return EffectiveStartParameters.builder()
        .threshold(Optional.of(recommendation.getThresholdPercent()))
        .iterations(effectiveIterations)
        .maxDatanodesPercentageToInvolvePerIteration(Optional.of(recommendation.getMaxDatanodesPercentage()))
        .maxSizeToMovePerIterationInGB(Optional.of(bytesToGb(recommendation.getMaxSizeToMovePerIteration())))
        .maxSizeEnteringTargetInGB(Optional.of(bytesToGb(recommendation.getMaxSizeEnteringTarget())))
        .maxSizeLeavingSourceInGB(Optional.of(bytesToGb(recommendation.getMaxSizeLeavingSource())))
        .balancingIntervalMinutes(configOptions.getBalancingIntervalMinutes())
        .moveTimeoutMinutes(configOptions.getMoveTimeoutMinutes())
        .moveReplicationTimeoutMinutes(configOptions.getMoveReplicationTimeoutMinutes())
        .build();
  }
}
