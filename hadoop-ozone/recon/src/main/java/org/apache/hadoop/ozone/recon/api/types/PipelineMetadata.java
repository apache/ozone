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

package org.apache.hadoop.ozone.recon.api.types;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;

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
  private List<DatanodeDetails> datanodes;

  @XmlElement(name = "lastLeaderElection")
  private long lastLeaderElection;

  @XmlElement(name = "duration")
  private long duration;

  @XmlElement(name = "leaderElections")
  private long leaderElections;

  @XmlElement(name = "replicationType")
  private String replicationType;

  // TODO: name can be changed to just "replication". Currently EC replication
  //  also showed with below parameter but in String format.
  @XmlElement(name = "replicationFactor")
  private String replicationFactor;

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

  public List<DatanodeDetails> getDatanodes() {
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

  public String getReplicationFactor() {
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
  public static class Builder {
    private UUID pipelineId;
    private PipelineState status;
    private String leaderNode;
    private List<DatanodeDetails> datanodes;
    private long lastLeaderElection;
    private long duration;
    private long leaderElections;
    private String replicationType;
    private String replicationFactor;
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
      Objects.requireNonNull(pipelineId, "pipelineId == null");
      Objects.requireNonNull(status, "status == null");
      Objects.requireNonNull(datanodes, "datanodes == null");
      Objects.requireNonNull(replicationType, "replicationType == null");

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

    public Builder setDatanodes(List<DatanodeDetails> datanodes) {
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

    public Builder setReplicationConfig(ReplicationConfig replicationConfig) {
      this.replicationType = replicationConfig.getReplicationType().toString();
      this.replicationFactor = replicationConfig.getReplication();
      return this;
    }

    public Builder setContainers(int containers) {
      this.containers = containers;
      return this;
    }
  }
}
