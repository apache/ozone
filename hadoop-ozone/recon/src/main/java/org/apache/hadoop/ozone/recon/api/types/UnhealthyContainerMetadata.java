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

import org.apache.hadoop.ozone.recon.persistence.ContainerHistory;
import org.hadoop.ozone.recon.schema.tables.pojos.UnhealthyContainers;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import java.util.List;
import java.util.UUID;

/**
 * Metadata object that represents an unhealthy Container.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class UnhealthyContainerMetadata {

  @XmlElement(name = "containerID")
  private long containerID;

  @XmlElement(name = "containerState")
  private String containerState;

  @XmlElement(name = "unhealthySince")
  private long unhealthySince;

  @XmlElement(name = "expectedReplicaCount")
  private long expectedReplicaCount = 0;

  @XmlElement(name = "actualReplicaCount")
  private long actualReplicaCount = 0;

  @XmlElement(name = "replicaDeltaCount")
  private long replicaDeltaCount = 0;

  @XmlElement(name = "reason")
  private String reason;

  @XmlElement(name = "keys")
  private long keys;

  @XmlElement(name = "pipelineID")
  private UUID pipelineID;

  @XmlElement(name = "replicas")
  private List<ContainerHistory> replicas;

  public UnhealthyContainerMetadata(UnhealthyContainers rec,
      List<ContainerHistory> replicas, UUID pipelineID, long keyCount) {
    this.containerID = rec.getContainerId();
    this.containerState = rec.getContainerState();
    this.unhealthySince = rec.getInStateSince();
    this.actualReplicaCount = rec.getActualReplicaCount();
    this.expectedReplicaCount = rec.getExpectedReplicaCount();
    this.replicaDeltaCount = rec.getReplicaDelta();
    this.reason = rec.getReason();
    this.replicas = replicas;
    this.pipelineID = pipelineID;
    this.keys = keyCount;
  }

  public long getContainerID() {
    return containerID;
  }

  public long getKeys() {
    return keys;
  }

  public List<ContainerHistory> getReplicas() {
    return replicas;
  }

  public String getContainerState() {
    return containerState;
  }

  public long getExpectedReplicaCount() {
    return expectedReplicaCount;
  }

  public long getActualReplicaCount() {
    return actualReplicaCount;
  }

  public long getReplicaDeltaCount() {
    return replicaDeltaCount;
  }

  public String getReason() {
    return reason;
  }

  public long getUnhealthySince() {
    return unhealthySince;
  }

  public UUID getPipelineID() {
    return pipelineID;
  }

}
