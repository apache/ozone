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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import java.util.List;
import java.util.UUID;

/**
 * Metadata object that represents a Datanode.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class DatanodeMetadata {

  @XmlElement(name = "hostname")
  private String hostname;

  @XmlElement(name = "state")
  private NodeState state;

  @XmlElement(name = "lastHeartbeat")
  private long lastHeartbeat;

  @XmlElement(name = "storageReport")
  private DatanodeStorageReport datanodeStorageReport;

  @XmlElement(name = "pipelineIDs")
  private List<UUID> pipelineIDs;

  @XmlElement(name = "containers")
  private int containers;

  public DatanodeMetadata(String hostname,
                          NodeState state,
                          long lastHeartbeat,
                          DatanodeStorageReport storageReport,
                          List<UUID> pipelineIDs,
                          int containers) {
    this.hostname = hostname;
    this.state = state;
    this.lastHeartbeat = lastHeartbeat;
    this.datanodeStorageReport = storageReport;
    this.pipelineIDs = pipelineIDs;
    this.containers = containers;
  }

  public String getHostname() {
    return hostname;
  }

  public NodeState getState() {
    return state;
  }

  public long getLastHeartbeat() {
    return lastHeartbeat;
  }

  public DatanodeStorageReport getDatanodeStorageReport() {
    return datanodeStorageReport;
  }

  public List<UUID> getPipelineIDs() {
    return pipelineIDs;
  }

  public int getContainers() {
    return containers;
  }
}
