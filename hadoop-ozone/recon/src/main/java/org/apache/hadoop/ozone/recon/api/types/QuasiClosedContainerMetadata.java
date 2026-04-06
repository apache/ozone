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
import java.util.List;
import org.apache.hadoop.ozone.recon.persistence.ContainerHistory;

/**
 * JSON response DTO for a single QUASI_CLOSED container.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class QuasiClosedContainerMetadata {

  private long containerID;
  private String pipelineID;
  private int datanodeCount;
  private long keyCount;
  private long dataSize;
  private String replicationType;
  private int replicationFactor;
  /** When the container entered QUASI_CLOSED state according to SCM. */
  private long stateEnterTime;
  /** When Recon first observed this container as QUASI_CLOSED. */
  private long firstSeenTime;
  /** When Recon last confirmed this container is still QUASI_CLOSED. */
  private long lastScanTime;
  /** Current datanode replicas — reuses the existing ContainerHistory type. */
  private List<ContainerHistory> replicas;

  public QuasiClosedContainerMetadata() {
  }

  public QuasiClosedContainerMetadata(
      long containerID, String pipelineID, int datanodeCount,
      long keyCount, long dataSize, String replicationType,
      int replicationFactor, long stateEnterTime,
      long firstSeenTime, long lastScanTime,
      List<ContainerHistory> replicas) {
    this.containerID = containerID;
    this.pipelineID = pipelineID;
    this.datanodeCount = datanodeCount;
    this.keyCount = keyCount;
    this.dataSize = dataSize;
    this.replicationType = replicationType;
    this.replicationFactor = replicationFactor;
    this.stateEnterTime = stateEnterTime;
    this.firstSeenTime = firstSeenTime;
    this.lastScanTime = lastScanTime;
    this.replicas = replicas;
  }

  public long getContainerID() { return containerID; }
  public void setContainerID(long containerID) { this.containerID = containerID; }

  public String getPipelineID() { return pipelineID; }
  public void setPipelineID(String pipelineID) { this.pipelineID = pipelineID; }

  public int getDatanodeCount() { return datanodeCount; }
  public void setDatanodeCount(int datanodeCount) { this.datanodeCount = datanodeCount; }

  public long getKeyCount() { return keyCount; }
  public void setKeyCount(long keyCount) { this.keyCount = keyCount; }

  public long getDataSize() { return dataSize; }
  public void setDataSize(long dataSize) { this.dataSize = dataSize; }

  public String getReplicationType() { return replicationType; }
  public void setReplicationType(String replicationType) { this.replicationType = replicationType; }

  public int getReplicationFactor() { return replicationFactor; }
  public void setReplicationFactor(int replicationFactor) { this.replicationFactor = replicationFactor; }

  public long getStateEnterTime() { return stateEnterTime; }
  public void setStateEnterTime(long stateEnterTime) { this.stateEnterTime = stateEnterTime; }

  public long getFirstSeenTime() { return firstSeenTime; }
  public void setFirstSeenTime(long firstSeenTime) { this.firstSeenTime = firstSeenTime; }

  public long getLastScanTime() { return lastScanTime; }
  public void setLastScanTime(long lastScanTime) { this.lastScanTime = lastScanTime; }

  public List<ContainerHistory> getReplicas() { return replicas; }
  public void setReplicas(List<ContainerHistory> replicas) { this.replicas = replicas; }
}
