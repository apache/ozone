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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import java.util.List;
import java.util.UUID;

/**
 * Metadata object that represents a Missing Container.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class MissingContainerMetadata {

  @XmlElement(name = "containerID")
  private long containerID;

  @XmlElement(name = "missingSince")
  private long missingSince;

  @XmlElement(name = "keys")
  private long keys;

  @XmlElement(name = "pipelineID")
  private UUID pipelineID;

  @XmlElement(name = "replicas")
  private List<ContainerHistory> replicas;

  public MissingContainerMetadata(long containerID, long missingSince,
                                  long keys, UUID pipelineID,
                                  List<ContainerHistory> replicas) {
    this.containerID = containerID;
    this.missingSince = missingSince;
    this.keys = keys;
    this.pipelineID = pipelineID;
    this.replicas = replicas;
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

  public long getMissingSince() {
    return missingSince;
  }

  public UUID getPipelineID() {
    return pipelineID;
  }
}
