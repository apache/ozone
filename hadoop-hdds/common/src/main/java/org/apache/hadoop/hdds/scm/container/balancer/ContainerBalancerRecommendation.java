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
  private final long maxSizeToMovePerIteration;
  private final long maxSizeEnteringTarget;
  private final long maxSizeLeavingSource;
  private final long moveTimeoutMillis;
  private final long moveReplicationTimeoutMillis;
  private final long balancingIntervalMillis;
  private final int recommendedIterations;
  private final Map<String, String> rationale;
  private final ContainerBalancerEstimation estimation;

  private ContainerBalancerRecommendation(Builder b) {
    this.profile = Objects.requireNonNull(b.profile, "profile == null");
    this.failureMessage = b.failureMessage;
    this.thresholdPercent = b.thresholdPercent;
    this.maxDatanodesPercentage = b.maxDatanodesPercentage;
    this.maxSizeToMovePerIteration = b.maxSizeToMovePerIteration;
    this.maxSizeEnteringTarget = b.maxSizeEnteringTarget;
    this.maxSizeLeavingSource = b.maxSizeLeavingSource;
    this.moveTimeoutMillis = b.moveTimeoutMillis;
    this.moveReplicationTimeoutMillis = b.moveReplicationTimeoutMillis;
    this.balancingIntervalMillis = b.balancingIntervalMillis;
    this.recommendedIterations = b.recommendedIterations;
    this.rationale = b.rationale == null
        ? Collections.emptyMap()
        : Collections.unmodifiableMap(b.rationale);
    this.estimation = b.estimation;
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

  public String getFailureMessage() {
    return failureMessage;
  }

  public double getThresholdPercent() {
    return thresholdPercent;
  }

  public int getMaxDatanodesPercentage() {
    return maxDatanodesPercentage;
  }

  public long getMaxSizeToMovePerIteration() {
    return maxSizeToMovePerIteration;
  }

  public long getMaxSizeEnteringTarget() {
    return maxSizeEnteringTarget;
  }

  public long getMaxSizeLeavingSource() {
    return maxSizeLeavingSource;
  }

  public long getMoveTimeoutMillis() {
    return moveTimeoutMillis;
  }

  public long getMoveReplicationTimeoutMillis() {
    return moveReplicationTimeoutMillis;
  }

  public long getBalancingIntervalMillis() {
    return balancingIntervalMillis;
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
    private long maxSizeToMovePerIteration;
    private long maxSizeEnteringTarget;
    private long maxSizeLeavingSource;
    private long moveTimeoutMillis;
    private long moveReplicationTimeoutMillis;
    private long balancingIntervalMillis;
    private int recommendedIterations;
    private Map<String, String> rationale;
    private ContainerBalancerEstimation estimation;

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

    public Builder setMaxSizeToMovePerIteration(long bytes) {
      this.maxSizeToMovePerIteration = bytes;
      return this;
    }

    public Builder setMaxSizeEnteringTarget(long bytes) {
      this.maxSizeEnteringTarget = bytes;
      return this;
    }

    public Builder setMaxSizeLeavingSource(long bytes) {
      this.maxSizeLeavingSource = bytes;
      return this;
    }

    public Builder setMoveTimeoutMillis(long millis) {
      this.moveTimeoutMillis = millis;
      return this;
    }

    public Builder setMoveReplicationTimeoutMillis(long millis) {
      this.moveReplicationTimeoutMillis = millis;
      return this;
    }

    public Builder setBalancingIntervalMillis(long millis) {
      this.balancingIntervalMillis = millis;
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

    public ContainerBalancerRecommendation build() {
      return new ContainerBalancerRecommendation(this);
    }
  }
}
