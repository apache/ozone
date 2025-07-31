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
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.List;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * Represents filtered Datanode information for json use.
 */
@JsonPropertyOrder({
    "id",
    "hostName",
    "ipAddress",
    "ports",
    "setupTime",
    "currentVersion",
    "persistedOpState",
    "opState",
    "persistedOpStateExpiryEpochSec",
    "healthState",
    "decommissioned",
    "maintenance",
    "level",
    "cost",
    "numOfLeaves",
    "networkFullPath",
    "networkLocation",
    "networkName"
})
public class BasicDatanodeInfo {
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Long used = null;
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Long capacity = null;
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Double percentUsed = null;
  private final DatanodeDetails dn;
  private final HddsProtos.NodeOperationalState opState;
  private final HddsProtos.NodeState healthState;
  
  public BasicDatanodeInfo(DatanodeDetails dnDetails, HddsProtos.NodeOperationalState opState,
      HddsProtos.NodeState healthState) {
    this.dn = dnDetails;
    this.opState = opState;
    this.healthState = healthState;
  }

  public BasicDatanodeInfo(DatanodeDetails dnDetails, HddsProtos.NodeOperationalState opState,
      HddsProtos.NodeState healthState, long used, long capacity, double percentUsed) {
    this(dnDetails, opState, healthState);
    this.used = used;
    this.capacity = capacity;
    this.percentUsed = percentUsed;
  }
  
  public String getId() {
    return dn.getUuidString();
  }
  
  public List<DatanodeDetails.Port> getPorts() {
    return dn.getPorts();
  }
  
  public HddsProtos.NodeOperationalState getPersistedOpState() {
    return dn.getPersistedOpState();
  }

  public HddsProtos.NodeOperationalState getOpState() {
    return opState;
  }
  
  public HddsProtos.NodeState getHealthState() {
    return healthState;
  }
  
  public String getHostName() {
    return dn.getHostName();
  }
  
  public String getIpAddress() {
    return dn.getIpAddress();
  }
  
  public long getPersistedOpStateExpiryEpochSec() {
    return dn.getPersistedOpStateExpiryEpochSec();
  }
  
  public boolean isDecommissioned() {
    return dn.isDecommissioned();
  }
  
  public boolean isMaintenance() {
    return dn.isMaintenance();
  }
  
  public int getLevel() {
    return dn.getLevel();
  }
  
  public int getCost() {
    return dn.getCost();
  }
  
  public int getNumOfLeaves() {
    return dn.getNumOfLeaves();
  }
  
  public String getNetworkFullPath() {
    return dn.getNetworkFullPath();
  }
  
  public String getNetworkLocation() {
    return dn.getNetworkLocation();
  }
  
  public String getNetworkName() {
    return dn.getNetworkName();
  }
  
  public long getSetupTime() {
    return dn.getSetupTime();
  }
  
  public int getCurrentVersion() {
    return dn.getCurrentVersion();
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
