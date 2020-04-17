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
package org.apache.hadoop.ozone.recon.api.types;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import java.util.List;
import java.util.UUID;

/**
 * Metadata object that represents a Pipeline.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public final class PipelineMetadata {

  @XmlElement(name = "pipelineId")
  private UUID pipelineId;

  @XmlElement(name = "status")
  private PipelineState status;

  @XmlElement(name = "leaderNode")
  private String leaderNode;

  @XmlElement(name = "datanodes")
  private List<String> datanodes;

  @XmlElement(name = "lastLeaderElection")
  private long lastLeaderElection;

  @XmlElement(name = "duration")
  private long duration;

  @XmlElement(name = "leaderElections")
  private long leaderElections;

  @XmlElement(name = "replicationType")
  private String replicationType;

  @XmlElement(name = "replicationFactor")
  private int replicationFactor;

  @XmlElement(name = "containers")
  private int containers;

  public UUID getPipelineId() {
    return pipelineId;
  }

  public PipelineState getStatus() {
    return status;
  }

  public String getLeaderNode() {
    return leaderNode;
  }

  public List<String> getDatanodes() {
    return datanodes;
  }

  public long getLastLeaderElection() {
    return lastLeaderElection;
  }

  public long getDuration() {
    return duration;
  }

  public long getLeaderElections() {
    return leaderElections;
  }

  public String getReplicationType() {
    return replicationType;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public int getContainers() {
    return containers;
  }


  /**
   * Returns new builder class that builds a PipelineMetadata.
   *
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  private PipelineMetadata(Builder b) {
    this.pipelineId = b.pipelineId;
    this.status = b.status;
    this.leaderNode = b.leaderNode;
    this.datanodes = b.datanodes;
    this.lastLeaderElection = b.lastLeaderElection;
    this.duration = b.duration;
    this.leaderElections = b.leaderElections;
    this.replicationType = b.replicationType;
    this.replicationFactor = b.replicationFactor;
    this.containers = b.containers;
  }

  /**
   * Builder for PipelineMetadata.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static class Builder {
    private UUID pipelineId;
    private PipelineState status;
    private String leaderNode;
    private List<String> datanodes;
    private long lastLeaderElection;
    private long duration;
    private long leaderElections;
    private String replicationType;
    private int replicationFactor;
    private int containers;

    public Builder() {
      // Default values
      this.lastLeaderElection = 0L;
      this.leaderElections = 0L;
      this.duration = 0L;
      this.containers = 0;
      this.leaderNode = StringUtils.EMPTY;
    }

    /**
     * Constructs PipelineMetadata.
     *
     * @return instance of PipelineMetadata.
     */
    public PipelineMetadata build() {
      Preconditions.checkNotNull(pipelineId);
      Preconditions.checkNotNull(status);
      Preconditions.checkNotNull(datanodes);
      Preconditions.checkNotNull(replicationType);

      return new PipelineMetadata(this);
    }

    public Builder setPipelineId(UUID pipelineId) {
      this.pipelineId = pipelineId;
      return this;
    }

    public Builder setStatus(PipelineState status) {
      this.status = status;
      return this;
    }

    public Builder setLeaderNode(String leaderNode) {
      this.leaderNode = leaderNode;
      return this;
    }

    public Builder setDatanodes(List<String> datanodes) {
      this.datanodes = datanodes;
      return this;
    }

    public Builder setLastLeaderElection(long lastLeaderElection) {
      this.lastLeaderElection = lastLeaderElection;
      return this;
    }

    public Builder setDuration(long duration) {
      this.duration = duration;
      return this;
    }

    public Builder setLeaderElections(long leaderElections) {
      this.leaderElections = leaderElections;
      return this;
    }

    public Builder setReplicationType(String replicationType) {
      this.replicationType = replicationType;
      return this;
    }

    public Builder setReplicationFactor(int replicationFactor) {
      this.replicationFactor = replicationFactor;
      return this;
    }

    public Builder setContainers(int containers) {
      this.containers = containers;
      return this;
    }
  }
}
