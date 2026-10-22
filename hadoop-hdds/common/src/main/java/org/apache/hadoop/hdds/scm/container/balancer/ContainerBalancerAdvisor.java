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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeUsageInfoProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;

/**
 * Orchestrates cluster analysis, estimation and recommendation for container balancer.
 */
public final class ContainerBalancerAdvisor {

  private static final long MIN_DELETE_PHASE_MILLIS = Duration.ofMinutes(9).toMillis();
  private static final String DATANODE_OFFSET_KEY =
      "hdds.scm.replication.event.timeout.datanode.offset";

  private ContainerBalancerAdvisor() {
  }

  /**
   * Estimates per-iteration size, iterations and duration for one or more balancer profiles.
   *
   * If {@link AdvisorRequest#allProfiles} is true, returns SLOW, MEDIUM, and FAST.
   * If profile is set, returns a result for that profile only.
   * Otherwise returns MEDIUM only.
   * Per-profile validation failures are returned with {@link ContainerBalancerEstimation#succeeded()} false
   * instead of aborting other profiles.
   */
  public static List<ContainerBalancerEstimation> estimate(OzoneConfiguration conf, AdvisorRequest request) {
    Objects.requireNonNull(conf, "conf");
    Objects.requireNonNull(request, "request");
    List<DatanodeUsageInfoProto> nodes = Objects.requireNonNull(request.nodes, "nodes");

    ContainerBalancerConfiguration balancerConfig = conf.getObject(ContainerBalancerConfiguration.class);

    double thresholdPercent = request.thresholdPercent != null
        ? request.thresholdPercent
        : balancerConfig.getThreshold();
    validateThresholdPercent(thresholdPercent);
    double thresholdRatio = thresholdPercent / 100.0;
    Set<String> includeNodes = request.includeNodes != null
        ? request.includeNodes
        : balancerConfig.getIncludeNodes();
    Set<String> excludeNodes = request.excludeNodes != null
        ? request.excludeNodes
        : balancerConfig.getExcludeNodes();

    ContainerBalancerClusterSnapshot snapshot = ContainerBalancerClusterAnalyzer.analyze(nodes, thresholdRatio, 
        includeNodes, excludeNodes);
    validateSnapshotForEstimation(snapshot);

    List<ContainerBalancerProfile> profiles = selectProfiles(request);
    List<ContainerBalancerEstimation> estimations = new ArrayList<>(profiles.size());
    for (ContainerBalancerProfile profile : profiles) {
      estimations.add(estimateForProfile(conf, request, profile, snapshot, balancerConfig, thresholdPercent));
    }
    return Collections.unmodifiableList(estimations);
  }

  private static ContainerBalancerEstimation estimateForProfile(OzoneConfiguration conf, AdvisorRequest request,
      ContainerBalancerProfile profile, ContainerBalancerClusterSnapshot snapshot,
      ContainerBalancerConfiguration balancerConfig, double thresholdPercent) {
    
    boolean userProvidedMaxDatanodesPercentage =
        request.maxDatanodesPercentageToInvolvePerIteration != null;
    int maxDatanodesPercentage = userProvidedMaxDatanodesPercentage
        ? request.maxDatanodesPercentageToInvolvePerIteration
        : profile.getDatanodesMaxPercentage();
    long maxSizeEnteringTarget = request.maxSizeEnteringTarget != null
        ? request.maxSizeEnteringTarget
        : profile.getMaxSizeEnteringTarget();
    long maxSizeLeavingSource = request.maxSizeLeavingSource != null
        ? request.maxSizeLeavingSource
        : profile.getMaxSizeLeavingSource();
    long maxSizeToMovePerIteration = request.maxSizeToMovePerIteration != null
        ? request.maxSizeToMovePerIteration
        : balancerConfig.getMaxSizeToMovePerIteration();
    long moveTimeoutMillis = request.moveTimeoutMillis != null
        ? request.moveTimeoutMillis
        : balancerConfig.getMoveTimeout().toMillis();
    long moveReplicationTimeoutMillis = request.moveReplicationTimeoutMillis != null
        ? request.moveReplicationTimeoutMillis
        : balancerConfig.getMoveReplicationTimeout().toMillis();
    long balancingIntervalMillis = request.balancingIntervalMillis != null
        ? request.balancingIntervalMillis
        : balancerConfig.getBalancingInterval().toMillis();

    ContainerBalancerEstimation.Builder builder = ContainerBalancerEstimation.newBuilder()
        .setProfile(profile)
        .setThresholdPercent(thresholdPercent)
        .setMaxSizeEnteringTarget(maxSizeEnteringTarget)
        .setMaxSizeLeavingSource(maxSizeLeavingSource)
        .setMaxSizeToMovePerIteration(maxSizeToMovePerIteration)
        .setMoveTimeoutMillis(moveTimeoutMillis)
        .setBalancingIntervalMillis(balancingIntervalMillis);

    try {
      validateMaxDatanodesPercentageToInvolvePerIteration(maxDatanodesPercentage);
      validateMoveTimeouts(conf, moveReplicationTimeoutMillis, moveTimeoutMillis);
      validateBalancingIntervalMillis(balancingIntervalMillis);
      validateResolvedMoveLimits(conf, maxSizeEnteringTarget, maxSizeLeavingSource, maxSizeToMovePerIteration);

      int eligibleDatanodeCount = snapshot.getTotalEligibleDatanodes();
      int maxInvolved = ContainerBalancerConfiguration.computeMaxDatanodesToInvolvePerIteration(
          maxDatanodesPercentage / 100d, eligibleDatanodeCount);

      if (!userProvidedMaxDatanodesPercentage && maxInvolved < 2) {
        int maxProfileDatanodesPercentage = ContainerBalancerProfile.FAST.getDatanodesMaxPercentage();
        maxDatanodesPercentage = minimumPercentForAtLeastTwoNodes(
            eligibleDatanodeCount, maxDatanodesPercentage, maxProfileDatanodesPercentage);
        maxInvolved = ContainerBalancerConfiguration.computeMaxDatanodesToInvolvePerIteration(
            maxDatanodesPercentage / 100d, eligibleDatanodeCount);
      }

      if (maxInvolved < 2) {
        throw new IllegalArgumentException(String.format(
            "max-datanodes-percentage-to-involve-per-iteration=%d allows at most %d datanode(s) "
                + "per iteration with %d eligible datanode(s), but at least 2 are required for a "
                + "source and target datanode pair.",
            maxDatanodesPercentage, maxInvolved, eligibleDatanodeCount));
      }

      int[] involved = computeInvolvedDatanodeCounts(snapshot.getSourceCount(), snapshot.getTargetCount(), maxInvolved);

      long bytesToMove = snapshot.getBytesToMove();
      long perIterationBytes = computePerIterationBytes(
          bytesToMove,
          maxSizeToMovePerIteration,
          maxSizeLeavingSource,
          maxSizeEnteringTarget,
          involved);
      long estimatedIterations = computeEstimatedIterations(perIterationBytes, bytesToMove);
      long cycleTimeMillis = computeCycleTimeMillis(moveTimeoutMillis, balancingIntervalMillis);
      long estimatedDurationMillis = estimatedIterations * cycleTimeMillis;

      return builder
          .setMaxDatanodesPercentage(maxDatanodesPercentage)
          .setBytesToMove(bytesToMove)
          .setPerIterationBytes(perIterationBytes)
          .setEstimatedIterations(estimatedIterations)
          .setEstimatedDurationMillis(estimatedDurationMillis)
          .build();
    } catch (IllegalArgumentException e) {
      return builder
          .setMaxDatanodesPercentage(maxDatanodesPercentage)
          .setFailureMessage(e.getMessage())
          .build();
    }
  }

  /** Raises datanode involvement percent until at least two datanodes can be involved. */
  static int minimumPercentForAtLeastTwoNodes(
      int eligibleDatanodeCount, int startPercent, int maxPercent) {
    for (int percent = startPercent; percent <= maxPercent; percent++) {
      if (ContainerBalancerConfiguration.computeMaxDatanodesToInvolvePerIteration(
          percent / 100d, eligibleDatanodeCount) >= 2) {
        return percent;
      }
    }
    return maxPercent;
  }

  /**
   * Estimated bytes moved in one iteration: minimum of global cap, source cap,
   * target cap, and total bytes to move.
   */
  static long computePerIterationBytes(
      long bytesToMove,
      long maxSizeToMovePerIteration,
      long maxSizeLeavingSource,
      long maxSizeEnteringTarget,
      int[] involved) {
    
    long fromLeaving  = involved[0] * maxSizeLeavingSource;
    long fromEntering = involved[1] * maxSizeEnteringTarget;
    
    return minPositive(
        maxSizeToMovePerIteration,
        fromLeaving,
        fromEntering,
        bytesToMove);
  }

  static long computeCycleTimeMillis(long moveTimeoutMillis, long balancingIntervalMillis) {
    return moveTimeoutMillis + balancingIntervalMillis;
  }

  static long computeEstimatedIterations(long perIterationBytes, long bytesToMove) {
    if (perIterationBytes <= 0) {
      throw new IllegalArgumentException("Per-iteration move size must be positive.");
    }
    return (long) Math.ceil(bytesToMove / (double) perIterationBytes);
  }

  /**
   * Estimated source and target datanode counts for one iteration (50/50 split heuristic).
   *
   * @return {@code [involvedSources, involvedTargets]}
   */
  static int[] computeInvolvedDatanodeCounts(
      int sourceCount,
      int targetCount,
      int maxInvolved) {
    if (sourceCount <= 0 || targetCount <= 0 || maxInvolved < 2) {
      return new int[] {0, 0};
    }
    
    int sEff = Math.min(sourceCount, (maxInvolved + 1) / 2);
    int tEff = Math.min(targetCount, maxInvolved / 2);
    return new int[] {sEff, tEff};
  }

  private static long minPositive(long... values) {
    long result = Long.MAX_VALUE;
    for (long value : values) {
      if (value > 0 && value < result) {
        result = value;
      }
    }
    return result;
  }

  private static void validateSnapshotForEstimation(ContainerBalancerClusterSnapshot snapshot) {
    if (snapshot.getSourceCount() < 1) {
      throw new IllegalArgumentException("No over-utilized datanodes (sources) found.");
    }
    if (snapshot.getTargetCount() < 1) {
      throw new IllegalArgumentException("No under-utilized datanodes (targets) found.");
    }
    if (snapshot.getBytesToMove() <= 0) {
      throw new IllegalArgumentException("No bytes to move.");
    }
    if (snapshot.getTotalEligibleDatanodes() < 2) {
      throw new IllegalArgumentException(String.format(
          "Container Balancer found %d eligible datanode(s) but requires at least 2.",
          snapshot.getTotalEligibleDatanodes()));
    }
  }

  private static void validateResolvedMoveLimits(OzoneConfiguration conf, long maxSizeEnteringTarget,
      long maxSizeLeavingSource, long maxSizeToMovePerIteration) {
    long containerSizeBytes = (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT,
        StorageUnit.BYTES);

    if (maxSizeEnteringTarget <= 0 || maxSizeEnteringTarget <= containerSizeBytes) {
      throw new IllegalArgumentException(
          "max-size-entering-target must be greater than ozone.scm.container.size ("
              + containerSizeBytes + " bytes).");
    }
    if (maxSizeLeavingSource <= 0 || maxSizeLeavingSource <= containerSizeBytes) {
      throw new IllegalArgumentException(
          "max-size-leaving-source must be greater than ozone.scm.container.size ("
              + containerSizeBytes + " bytes).");
    }
    if (maxSizeToMovePerIteration <= 0) {
      throw new IllegalArgumentException("Max Size To Move Per Iteration In GB must be positive.");
    }
    if (maxSizeEnteringTarget > maxSizeToMovePerIteration) {
      throw new IllegalArgumentException(
          "max-size-entering-target must be less than or equal to "
              + "max-size-to-move-per-iteration.");
    }
    if (maxSizeLeavingSource > maxSizeToMovePerIteration) {
      throw new IllegalArgumentException(
          "max-size-leaving-source must be less than or equal to "
              + "max-size-to-move-per-iteration.");
    }
  }

  private static void validateMoveTimeouts(OzoneConfiguration conf, long moveReplicationTimeoutMillis,
      long moveTimeoutMillis) {
    if (moveTimeoutMillis <= 0 || moveReplicationTimeoutMillis <= 0) {
      throw new IllegalArgumentException(
          "Move timeout and move replication timeout must each be greater than zero.");
    }
    if (moveReplicationTimeoutMillis >= moveTimeoutMillis) {
      throw new IllegalArgumentException("hdds.container.balancer.move.replication.timeout should " +
          "be less than hdds.container.balancer.move.timeout.");
    }
    long datanodeOffsetMillis = conf.getTimeDuration(
        DATANODE_OFFSET_KEY, Duration.ofMinutes(6).toMillis(), TimeUnit.MILLISECONDS);
    if ((moveTimeoutMillis - moveReplicationTimeoutMillis - datanodeOffsetMillis)
        < MIN_DELETE_PHASE_MILLIS) {
      String msg = String.format("(hdds.container.balancer.move.timeout (%sms) - " +
              "hdds.container.balancer.move.replication.timeout (%sms) - " +
              "hdds.scm.replication.event.timeout.datanode.offset (%sms)) " +
              "should be greater than or equal to 540000ms or 9 minutes.",
          moveTimeoutMillis,
          moveReplicationTimeoutMillis,
          datanodeOffsetMillis);
      throw new IllegalArgumentException(msg);
    }
  }

  private static void validateThresholdPercent(double thresholdPercent) {
    if (thresholdPercent < 0d || thresholdPercent >= 100d) {
      throw new IllegalArgumentException("Threshold should be specified in the range [0.0, 100.0).");
    }
  }

  private static void validateMaxDatanodesPercentageToInvolvePerIteration(int percentage) {
    if (percentage <= 0 || percentage > 100) {
      throw new IllegalArgumentException("Max Datanodes Percentage To Involve Per Iteration "
          + "should be specified in the range (0, 100]");
    }
  }

  private static void validateBalancingIntervalMillis(long balancingIntervalMillis) {
    if (balancingIntervalMillis <= 0) {
      throw new IllegalArgumentException("Balancing Interval must be greater than zero.");
    }
  }

  private static List<ContainerBalancerProfile> selectProfiles(AdvisorRequest request) {
    if (request.allProfiles) {
      return Arrays.asList(
          ContainerBalancerProfile.SLOW,
          ContainerBalancerProfile.MEDIUM,
          ContainerBalancerProfile.FAST);
    }
    if (request.profile != null) {
      return Collections.singletonList(request.profile);
    }
    return Collections.singletonList(ContainerBalancerProfile.MEDIUM);
  }

  /**
   * Input for {@link ContainerBalancerAdvisor}: cluster usage data and optional overrides.
   * Unset fields fall back to {@link ContainerBalancerConfiguration} or profile presets.
   */
  public static final class AdvisorRequest {
    private List<DatanodeUsageInfoProto> nodes;
    private Set<String> includeNodes;
    private Set<String> excludeNodes;
    private Double thresholdPercent;
    private ContainerBalancerProfile profile;
    private boolean allProfiles;
    private Integer maxDatanodesPercentageToInvolvePerIteration;
    private Long maxSizeToMovePerIteration;
    private Long maxSizeEnteringTarget;
    private Long maxSizeLeavingSource;
    private Long moveTimeoutMillis;
    private Long moveReplicationTimeoutMillis;
    private Long balancingIntervalMillis;

    public AdvisorRequest setNodes(List<DatanodeUsageInfoProto> nodesList) {
      this.nodes = nodesList;
      return this;
    }

    public AdvisorRequest setIncludeNodes(Set<String> includeNodesSet) {
      this.includeNodes = includeNodesSet;
      return this;
    }

    public AdvisorRequest setExcludeNodes(Set<String> excludeNodesSet) {
      this.excludeNodes = excludeNodesSet;
      return this;
    }

    public AdvisorRequest setThresholdPercent(Double threshold) {
      this.thresholdPercent = threshold;
      return this;
    }

    public AdvisorRequest setProfile(ContainerBalancerProfile profileValue) {
      this.profile = profileValue;
      return this;
    }

    public AdvisorRequest setAllProfiles(boolean allProfilesValue) {
      this.allProfiles = allProfilesValue;
      return this;
    }

    public AdvisorRequest setMaxDatanodesPercentageToInvolvePerIteration(Integer percentage) {
      this.maxDatanodesPercentageToInvolvePerIteration = percentage;
      return this;
    }

    public AdvisorRequest setMaxSizeToMovePerIteration(Long maxSize) {
      this.maxSizeToMovePerIteration = maxSize;
      return this;
    }

    public AdvisorRequest setMaxSizeEnteringTarget(Long maxSize) {
      this.maxSizeEnteringTarget = maxSize;
      return this;
    }

    public AdvisorRequest setMaxSizeLeavingSource(Long maxSize) {
      this.maxSizeLeavingSource = maxSize;
      return this;
    }

    public AdvisorRequest setMoveTimeoutMillis(Long timeoutMillis) {
      this.moveTimeoutMillis = timeoutMillis;
      return this;
    }

    public AdvisorRequest setMoveReplicationTimeoutMillis(Long timeoutMillis) {
      this.moveReplicationTimeoutMillis = timeoutMillis;
      return this;
    }

    public AdvisorRequest setBalancingIntervalMillis(Long intervalMillis) {
      this.balancingIntervalMillis = intervalMillis;
      return this;
    }
  }
}
