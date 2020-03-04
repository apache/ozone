/**
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
public class PipelineMetadata {

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

  @SuppressWarnings("parameternumber")
  public PipelineMetadata(UUID pipelineId, PipelineState status,
                          String leaderNode, List<String> datanodes,
                          long lastLeaderElection, long duration,
                          long leaderElections, String replicationType,
                          int replicationFactor, int containers) {
    this.pipelineId = pipelineId;
    this.status = status;
    this.leaderNode = leaderNode;
    this.datanodes = datanodes;
    this.lastLeaderElection = lastLeaderElection;
    this.duration = duration;
    this.leaderElections = leaderElections;
    this.replicationType = replicationType;
    this.replicationFactor = replicationFactor;
    this.containers = containers;
  }

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
}
