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

import java.util.Objects;

/**
 * Estimate for a single container balancer profile.
 */
public final class ContainerBalancerEstimation {

  private final ContainerBalancerProfile profile;
  private final String failureMessage;
  private final long bytesToMove;
  private final long perIterationBytes;
  private final long estimatedIterations;
  private final long estimatedDurationMillis;
  private final double thresholdPercent;
  private final int maxDatanodesPercentage;
  private final long maxSizeEnteringTarget;
  private final long maxSizeLeavingSource;
  private final long maxSizeToMovePerIteration;
  private final long moveTimeoutMillis;
  private final long balancingIntervalMillis;

  private ContainerBalancerEstimation(Builder b) {
    this.profile = Objects.requireNonNull(b.profile, "profile == null");
    this.failureMessage = b.failureMessage;
    this.bytesToMove = b.bytesToMove;
    this.perIterationBytes = b.perIterationBytes;
    this.estimatedIterations = b.estimatedIterations;
    this.estimatedDurationMillis = b.estimatedDurationMillis;
    this.thresholdPercent = b.thresholdPercent;
    this.maxDatanodesPercentage = b.maxDatanodesPercentage;
    this.maxSizeEnteringTarget = b.maxSizeEnteringTarget;
    this.maxSizeLeavingSource = b.maxSizeLeavingSource;
    this.maxSizeToMovePerIteration = b.maxSizeToMovePerIteration;
    this.moveTimeoutMillis = b.moveTimeoutMillis;
    this.balancingIntervalMillis = b.balancingIntervalMillis;
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

  public long getBytesToMove() {
    return bytesToMove;
  }

  public long getPerIterationBytes() {
    return perIterationBytes;
  }

  public long getEstimatedIterations() {
    return estimatedIterations;
  }

  public long getEstimatedDurationMillis() {
    return estimatedDurationMillis;
  }

  public double getThresholdPercent() {
    return thresholdPercent;
  }

  public int getMaxDatanodesPercentage() {
    return maxDatanodesPercentage;
  }

  public long getMaxSizeEnteringTarget() {
    return maxSizeEnteringTarget;
  }

  public long getMaxSizeLeavingSource() {
    return maxSizeLeavingSource;
  }

  public long getMaxSizeToMovePerIteration() {
    return maxSizeToMovePerIteration;
  }

  public long getMoveTimeoutMillis() {
    return moveTimeoutMillis;
  }

  public long getBalancingIntervalMillis() {
    return balancingIntervalMillis;
  }

  /** Builder for {@link ContainerBalancerEstimation}. */
  public static final class Builder {
    private ContainerBalancerProfile profile;
    private String failureMessage;
    private long bytesToMove;
    private long perIterationBytes;
    private long estimatedIterations;
    private long estimatedDurationMillis;
    private double thresholdPercent;
    private int maxDatanodesPercentage;
    private long maxSizeEnteringTarget;
    private long maxSizeLeavingSource;
    private long maxSizeToMovePerIteration;
    private long moveTimeoutMillis;
    private long balancingIntervalMillis;

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

    public Builder setBytesToMove(long bytes) {
      this.bytesToMove = bytes;
      return this;
    }

    public Builder setPerIterationBytes(long bytes) {
      this.perIterationBytes = bytes;
      return this;
    }

    public Builder setEstimatedIterations(long iterations) {
      this.estimatedIterations = iterations;
      return this;
    }

    public Builder setEstimatedDurationMillis(long durationMillis) {
      this.estimatedDurationMillis = durationMillis;
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

    public Builder setMaxSizeEnteringTarget(long bytes) {
      this.maxSizeEnteringTarget = bytes;
      return this;
    }

    public Builder setMaxSizeLeavingSource(long bytes) {
      this.maxSizeLeavingSource = bytes;
      return this;
    }

    public Builder setMaxSizeToMovePerIteration(long bytes) {
      this.maxSizeToMovePerIteration = bytes;
      return this;
    }

    public Builder setMoveTimeoutMillis(long millis) {
      this.moveTimeoutMillis = millis;
      return this;
    }

    public Builder setBalancingIntervalMillis(long millis) {
      this.balancingIntervalMillis = millis;
      return this;
    }

    public ContainerBalancerEstimation build() {
      return new ContainerBalancerEstimation(this);
    }
  }
}
