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
import java.util.UUID;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import org.apache.hadoop.ozone.recon.persistence.ContainerHistory;

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

  @SuppressWarnings("checkstyle:ParameterNumber")
  public UnhealthyContainerMetadata(long containerID, String containerState,
      long unhealthySince, long expectedReplicaCount, long actualReplicaCount,
      long replicaDeltaCount, String reason, List<ContainerHistory> replicas,
      UUID pipelineID, long keyCount) {
    this.containerID = containerID;
    this.containerState = containerState;
    this.unhealthySince = unhealthySince;
    this.actualReplicaCount = actualReplicaCount;
    this.expectedReplicaCount = expectedReplicaCount;
    this.replicaDeltaCount = replicaDeltaCount;
    this.reason = reason;
    this.replicas = replicas;
    this.pipelineID = pipelineID;
    this.keys = keyCount;
  }

  // Default constructor, used by jackson lib for object deserialization.
  public UnhealthyContainerMetadata() {
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
