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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;

/**
 * This class wraps deleted container info for API response.
 */
public class DeletedContainerInfo {

  @JsonProperty("containerId")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private long containerID;

  @JsonProperty("pipelineID")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private PipelineID pipelineID;

  @JsonProperty("numberOfKeys")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private long numberOfKeys;

  @JsonProperty("containerState")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private String containerState;

  @JsonProperty("stateEnterTime")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private long stateEnterTime;

  @JsonProperty("lastUsed")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private long lastUsed;

  @JsonProperty("usedBytes")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private long usedBytes;

  @JsonProperty("replicationConfig")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private ReplicationConfig replicationConfig;

  @JsonProperty("replicationFactor")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private String replicationFactor;

  public DeletedContainerInfo() {

  }

  public long getContainerID() {
    return containerID;
  }

  public void setContainerID(long containerID) {
    this.containerID = containerID;
  }

  public PipelineID getPipelineID() {
    return pipelineID;
  }

  public void setPipelineID(PipelineID pipelineID) {
    this.pipelineID = pipelineID;
  }

  public long getNumberOfKeys() {
    return numberOfKeys;
  }

  public void setNumberOfKeys(long numberOfKeys) {
    this.numberOfKeys = numberOfKeys;
  }

  public String getContainerState() {
    return containerState;
  }

  public void setContainerState(String containerState) {
    this.containerState = containerState;
  }

  public long getStateEnterTime() {
    return stateEnterTime;
  }

  public void setStateEnterTime(long stateEnterTime) {
    this.stateEnterTime = stateEnterTime;
  }

  public long getLastUsed() {
    return lastUsed;
  }

  public void setLastUsed(long lastUsed) {
    this.lastUsed = lastUsed;
  }

  public long getUsedBytes() {
    return usedBytes;
  }

  public void setUsedBytes(long usedBytes) {
    this.usedBytes = usedBytes;
  }

  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  public void setReplicationConfig(
      ReplicationConfig replicationConfig) {
    this.replicationConfig = replicationConfig;
  }

  public String getReplicationFactor() {
    return replicationFactor;
  }

  public void setReplicationFactor(String replicationFactor) {
    this.replicationFactor = replicationFactor;
  }
}
