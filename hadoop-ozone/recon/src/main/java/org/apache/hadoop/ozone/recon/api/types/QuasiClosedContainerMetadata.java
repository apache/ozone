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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.apache.hadoop.ozone.recon.persistence.ContainerHistory;

/**
 * JSON response DTO for a single QUASI_CLOSED container.
 * Uses semantically correct field names (stateEnterTime, actualReplicaCount)
 * instead of reusing the unhealthy-container vocabulary.
 */
public class QuasiClosedContainerMetadata {

  @JsonProperty("containerID")
  private long containerID;

  @JsonProperty("pipelineID")
  private String pipelineID;

  @JsonProperty("keys")
  private long keys;

  /** Epoch millis when the container entered QUASI_CLOSED state per SCM. */
  @JsonProperty("stateEnterTime")
  private long stateEnterTime;

  @JsonProperty("expectedReplicaCount")
  private long expectedReplicaCount;

  @JsonProperty("actualReplicaCount")
  private long actualReplicaCount;

  @JsonProperty("replicas")
  private List<ContainerHistory> replicas;

  public QuasiClosedContainerMetadata() {
  }

  public QuasiClosedContainerMetadata(
      long containerID, String pipelineID, long keys,
      long stateEnterTime, long expectedReplicaCount,
      long actualReplicaCount, List<ContainerHistory> replicas) {
    this.containerID = containerID;
    this.pipelineID = pipelineID;
    this.keys = keys;
    this.stateEnterTime = stateEnterTime;
    this.expectedReplicaCount = expectedReplicaCount;
    this.actualReplicaCount = actualReplicaCount;
    this.replicas = replicas;
  }

  public long getContainerID() {
    return containerID;
  }

  public void setContainerID(long containerID) {
    this.containerID = containerID;
  }

  public String getPipelineID() {
    return pipelineID;
  }

  public void setPipelineID(String pipelineID) {
    this.pipelineID = pipelineID;
  }

  public long getKeys() {
    return keys;
  }

  public void setKeys(long keys) {
    this.keys = keys;
  }

  public long getStateEnterTime() {
    return stateEnterTime;
  }

  public void setStateEnterTime(long stateEnterTime) {
    this.stateEnterTime = stateEnterTime;
  }

  public long getExpectedReplicaCount() {
    return expectedReplicaCount;
  }

  public void setExpectedReplicaCount(long expectedReplicaCount) {
    this.expectedReplicaCount = expectedReplicaCount;
  }

  public long getActualReplicaCount() {
    return actualReplicaCount;
  }

  public void setActualReplicaCount(long actualReplicaCount) {
    this.actualReplicaCount = actualReplicaCount;
  }

  public List<ContainerHistory> getReplicas() {
    return replicas;
  }

  public void setReplicas(List<ContainerHistory> replicas) {
    this.replicas = replicas;
  }
}
