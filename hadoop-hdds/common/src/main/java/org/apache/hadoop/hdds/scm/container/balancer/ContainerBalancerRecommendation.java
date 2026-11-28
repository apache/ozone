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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Recommended balancer configuration for a single profile.
 */
public final class ContainerBalancerRecommendation {

  private final ContainerBalancerProfile profile;
  private final String failureMessage;
  private final double thresholdPercent;
  private final int maxDatanodesPercentage;
  private final long maxSizeToMovePerIterationInGB;
  private final long maxSizeEnteringTargetInGB;
  private final long maxSizeLeavingSourceInGB;
  private final long moveTimeoutMinutes;
  private final long moveReplicationTimeoutMinutes;
  private final long balancingIntervalMinutes;
  private final int recommendedIterations;
  private final Map<String, String> rationale;
  private final ContainerBalancerEstimation estimation;
  private final boolean clusterBalanced;

  private ContainerBalancerRecommendation(Builder b) {
    this.profile = Objects.requireNonNull(b.profile, "profile == null");
    this.failureMessage = b.failureMessage;
    this.thresholdPercent = b.thresholdPercent;
    this.maxDatanodesPercentage = b.maxDatanodesPercentage;
    this.maxSizeToMovePerIterationInGB = b.maxSizeToMovePerIterationInGB;
    this.maxSizeEnteringTargetInGB = b.maxSizeEnteringTargetInGB;
    this.maxSizeLeavingSourceInGB = b.maxSizeLeavingSourceInGB;
    this.moveTimeoutMinutes = b.moveTimeoutMinutes;
    this.moveReplicationTimeoutMinutes = b.moveReplicationTimeoutMinutes;
    this.balancingIntervalMinutes = b.balancingIntervalMinutes;
    this.recommendedIterations = b.recommendedIterations;
    this.rationale = b.rationale == null
        ? Collections.emptyMap()
        : Collections.unmodifiableMap(b.rationale);
    this.estimation = b.estimation;
    this.clusterBalanced = b.clusterBalanced;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public ContainerBalancerProfile getProfile() {
    return profile;
  }

  public boolean succeeded() {
    return failureMessage == null;
  }

  public boolean isClusterBalanced() {
    return clusterBalanced;
  }

  public String getFailureMessage() {
    return failureMessage;
  }

  public double getThresholdPercent() {
    return thresholdPercent;
  }

  public int getMaxDatanodesPercentage() {
    return maxDatanodesPercentage;
  }

  public long getMaxSizeToMovePerIterationInGB() {
    return maxSizeToMovePerIterationInGB;
  }

  public long getMaxSizeEnteringTargetInGB() {
    return maxSizeEnteringTargetInGB;
  }

  public long getMaxSizeLeavingSourceInGB() {
    return maxSizeLeavingSourceInGB;
  }

  public long getMoveTimeoutMinutes() {
    return moveTimeoutMinutes;
  }

  public long getMoveReplicationTimeoutMinutes() {
    return moveReplicationTimeoutMinutes;
  }

  public long getBalancingIntervalMinutes() {
    return balancingIntervalMinutes;
  }

  public int getRecommendedIterations() {
    return recommendedIterations;
  }

  public Map<String, String> getRationale() {
    return rationale;
  }

  public ContainerBalancerEstimation getEstimation() {
    return estimation;
  }

  /** Builder for {@link ContainerBalancerRecommendation}. */
  public static final class Builder {
    private ContainerBalancerProfile profile;
    private String failureMessage;
    private double thresholdPercent;
    private int maxDatanodesPercentage;
    private long maxSizeToMovePerIterationInGB;
    private long maxSizeEnteringTargetInGB;
    private long maxSizeLeavingSourceInGB;
    private long moveTimeoutMinutes;
    private long moveReplicationTimeoutMinutes;
    private long balancingIntervalMinutes;
    private int recommendedIterations;
    private Map<String, String> rationale;
    private ContainerBalancerEstimation estimation;
    private boolean clusterBalanced;

    private Builder() {
    }

    public Builder setProfile(ContainerBalancerProfile profileValue) {
      this.profile = profileValue;
      return this;
    }

    public Builder setFailureMessage(String message) {
      this.failureMessage = message;
      return this;
    }

    public Builder setThresholdPercent(double threshold) {
      this.thresholdPercent = threshold;
      return this;
    }

    public Builder setMaxDatanodesPercentage(int percentage) {
      this.maxDatanodesPercentage = percentage;
      return this;
    }

    public Builder setMaxSizeToMovePerIterationInGB(long gb) {
      this.maxSizeToMovePerIterationInGB = gb;
      return this;
    }

    public Builder setMaxSizeEnteringTargetInGB(long gb) {
      this.maxSizeEnteringTargetInGB = gb;
      return this;
    }

    public Builder setMaxSizeLeavingSourceInGB(long gb) {
      this.maxSizeLeavingSourceInGB = gb;
      return this;
    }

    public Builder setMoveTimeoutMinutes(long minutes) {
      this.moveTimeoutMinutes = minutes;
      return this;
    }

    public Builder setMoveReplicationTimeoutMinutes(long minutes) {
      this.moveReplicationTimeoutMinutes = minutes;
      return this;
    }

    public Builder setBalancingIntervalMinutes(long minutes) {
      this.balancingIntervalMinutes = minutes;
      return this;
    }

    public Builder setRecommendedIterations(int iterations) {
      this.recommendedIterations = iterations;
      return this;
    }

    public Builder setRationale(Map<String, String> rationaleMap) {
      this.rationale = rationaleMap;
      return this;
    }

    public Builder setEstimation(ContainerBalancerEstimation estimationValue) {
      this.estimation = estimationValue;
      return this;
    }

    public Builder setClusterBalanced(boolean balanced) {
      this.clusterBalanced = balanced;
      return this;
    }

    public ContainerBalancerRecommendation build() {
      return new ContainerBalancerRecommendation(this);
    }
  }
}
