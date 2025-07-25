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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * Represents filtered Datanode information for json use.
 */

public class BasicDatanodeInfoJson {
  private final String id;
  private final String hostName;
  private final String ipAddress;
  private final List<DatanodeDetails.Port> ports;
  private final long setupTime;
  private final int currentVersion;
  private final HddsProtos.NodeOperationalState persistedOpState;
  private final HddsProtos.NodeOperationalState opState;
  private final long persistedOpStateExpiryEpochSec;
  private final HddsProtos.NodeState healthState;
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
  @JsonIgnore
  private DatanodeDetails dn;
  
  public BasicDatanodeInfoJson(DatanodeDetails dnDetails, HddsProtos.NodeOperationalState opState,
      HddsProtos.NodeState healthState) {
    this.dn = dnDetails;
    this.id = dnDetails.getUuid().toString();
    this.ports = dnDetails.getPorts();
    this.persistedOpState = dnDetails.getPersistedOpState();
    this.opState = opState;
    this.healthState = healthState;
    this.hostName = dnDetails.getHostName();
    this.ipAddress = dnDetails.getIpAddress();
    this.persistedOpStateExpiryEpochSec = dnDetails.getPersistedOpStateExpiryEpochSec();
    this.decommissioned = dnDetails.isDecommissioned();
    this.maintenance = dnDetails.isMaintenance();
    this.level = dnDetails.getLevel();
    this.cost = dnDetails.getCost();
    this.numOfLeaves = dnDetails.getNumOfLeaves();
    this.networkFullPath = dnDetails.getNetworkFullPath();
    this.networkLocation = dnDetails.getNetworkLocation();
    this.networkName = dnDetails.getNetworkName();
    this.setupTime = dnDetails.getSetupTime();
    this.currentVersion = dnDetails.getCurrentVersion();
  }

  public BasicDatanodeInfoJson(DatanodeDetails dnDetails, HddsProtos.NodeOperationalState opState,
      HddsProtos.NodeState healthState,
      long used, long capacity, double percentUsed) {
    this(dnDetails, opState, healthState);
    this.used = used;
    this.capacity = capacity;
    this.percentUsed = percentUsed;
  }
  
  public String getId() {
    return id;
  }
  
  public List<DatanodeDetails.Port> getPorts() {
    return ports;
  }
  
  public HddsProtos.NodeOperationalState getPersistedOpState() {
    return persistedOpState;
  }

  public HddsProtos.NodeOperationalState getOpState() {
    return opState;
  }
  
  public HddsProtos.NodeState getHealthState() {
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

  @JsonIgnore
  public DatanodeDetails getDatanodeDetails() {
    return dn;
  }
}
