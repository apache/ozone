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

package org.apache.hadoop.hdds.scm.cli.datanode;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;

/**
 * Represents filtered Datanode information for json use.
 */

public class DatanodeInfoJson {
  private final String id;
  private final String hostName;
  private final String ipAddress;
  private final List<DatanodeDetails.Port> ports;
  private final long setupTime;
  private final int currentVersion;
  private final String opState;
  private final long persistedOpStateExpiryEpochSec;
  private final String healthState;
  private final boolean decommissioned;
  private final boolean maintenance;
  private final int level;
  private final int cost;
  private final int numOfLeaves;
  private final String networkFullPath;
  private final String networkLocation;
  private final String networkName;
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Long used = null;
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Long capacity = null;
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Double percentUsed = null;
  
  DatanodeInfoJson(ListInfoSubcommand.DatanodeWithAttributes dna) {
    DatanodeDetails dn = dna.getDatanodeDetails();
    this.id = dn.getUuid().toString();
    this.ports = dn.getPorts();
    this.opState = String.valueOf(dna.getOpState());
    this.healthState = String.valueOf(dna.getHealthState());
    this.hostName = dn.getHostName();
    this.ipAddress = dn.getIpAddress();
    this.persistedOpStateExpiryEpochSec = dn.getPersistedOpStateExpiryEpochSec();
    this.decommissioned = dn.isDecommissioned();
    this.maintenance = dn.isMaintenance();
    this.level = dn.getLevel();
    this.cost = dn.getCost();
    this.numOfLeaves = dn.getNumOfLeaves();
    this.networkFullPath = dn.getNetworkFullPath();
    this.networkLocation = dn.getNetworkLocation();
    this.networkName = dn.getNetworkName();
    this.setupTime = dn.getSetupTime();
    this.currentVersion = dn.getCurrentVersion();
    this.used = dna.getUsed();
    this.capacity = dna.getCapacity();
    this.percentUsed = dna.getPercentUsed();
  }
  
  public String getId() {
    return id;
  }
  
  public List<DatanodeDetails.Port> getPorts() {
    return ports;
  }
  
  public String getOpState() {
    return opState;
  }
  
  public String getHealthState() {
    return healthState;
  }
  
  public String getHostName() {
    return hostName;
  }
  
  public String getIpAddress() {
    return ipAddress;
  }
  
  public long getPersistedOpStateExpiryEpochSec() {
    return persistedOpStateExpiryEpochSec;
  }
  
  public boolean isDecommissioned() {
    return decommissioned;
  }
  
  public boolean isMaintenance() {
    return maintenance;
  }
  
  public int getLevel() {
    return level;
  }
  
  public int getCost() {
    return cost;
  }
  
  public int getNumOfLeaves() {
    return numOfLeaves;
  }
  
  public String getNetworkFullPath() {
    return networkFullPath;
  }
  
  public String getNetworkLocation() {
    return networkLocation;
  }
  
  public String getNetworkName() {
    return networkName;
  }
  
  public long getSetupTime() {
    return setupTime;
  }
  
  public int getCurrentVersion() {
    return currentVersion;
  }
  
  public Long getUsed() {
    return used;
  }

  public Long getCapacity() {
    return capacity;
  }

  public Double getPercentUsed() {
    return percentUsed;
  }
}
