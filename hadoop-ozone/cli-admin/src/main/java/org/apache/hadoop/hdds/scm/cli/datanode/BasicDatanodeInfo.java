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
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * Represents filtered Datanode information for json use.
 */
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
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Integer totalVolumeCount = null;
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Integer healthyVolumeCount = null;
  
  public BasicDatanodeInfo(DatanodeDetails dnDetails, HddsProtos.NodeOperationalState opState,
      HddsProtos.NodeState healthState, List<Integer> volumeCounts) {
    this.dn = dnDetails;
    this.opState = opState;
    this.healthState = healthState;
    this.totalVolumeCount = volumeCounts.get(0);
    this.healthyVolumeCount = volumeCounts.get(1);
  }

  public BasicDatanodeInfo(DatanodeDetails dnDetails, HddsProtos.NodeOperationalState opState,
      HddsProtos.NodeState healthState, long used, long capacity, double percentUsed, List<Integer> volumeCounts) {
    this(dnDetails, opState, healthState, volumeCounts);
    this.used = used;
    this.capacity = capacity;
    this.percentUsed = percentUsed;
  }
  
  @JsonProperty(index = 5)
  public String getId() {
    return dn.getUuidString();
  }
  
  @JsonProperty(index = 10)
  public String getHostName() {
    return dn.getHostName();
  }
  
  @JsonProperty(index = 15)
  public String getIpAddress() {
    return dn.getIpAddress();
  }
  
  @JsonProperty(index = 20)
  public List<DatanodeDetails.Port> getPorts() {
    return dn.getPorts();
  }

  @JsonProperty(index = 25)
  public long getSetupTime() {
    return dn.getSetupTime();
  }

  @JsonProperty(index = 30)
  public int getCurrentVersion() {
    return dn.getCurrentVersion();
  }

  @JsonProperty(index = 35)
  public HddsProtos.NodeOperationalState getOpState() {
    return opState;
  }

  @JsonProperty(index = 40)
  public HddsProtos.NodeOperationalState getPersistedOpState() {
    return dn.getPersistedOpState();
  }

  @JsonProperty(index = 45)
  public long getPersistedOpStateExpiryEpochSec() {
    return dn.getPersistedOpStateExpiryEpochSec();
  }
  
  @JsonProperty(index = 50)
  public HddsProtos.NodeState getHealthState() {
    return healthState;
  }

  @JsonProperty(index = 55)
  public boolean isDecommissioned() {
    return dn.isDecommissioned();
  }
  
  @JsonProperty(index = 60)
  public boolean isMaintenance() {
    return dn.isMaintenance();
  }
  
  @JsonProperty(index = 65)
  public int getLevel() {
    return dn.getLevel();
  }
  
  @JsonProperty(index = 70)
  public int getCost() {
    return dn.getCost();
  }
  
  @JsonProperty(index = 75)
  public int getNumOfLeaves() {
    return dn.getNumOfLeaves();
  }
  
  @JsonProperty(index = 80)
  public String getNetworkFullPath() {
    return dn.getNetworkFullPath();
  }
  
  @JsonProperty(index = 85)
  public String getNetworkLocation() {
    return dn.getNetworkLocation();
  }
  
  @JsonProperty(index = 90)
  public String getNetworkName() {
    return dn.getNetworkName();
  }
  
  @JsonProperty(index = 95)
  public Long getUsed() {
    return used;
  }

  @JsonProperty(index = 100)
  public Long getCapacity() {
    return capacity;
  }

  @JsonProperty(index = 105)
  public Double getPercentUsed() {
    return percentUsed;
  }

  @JsonProperty(index = 110)
  public Integer getTotalVolumeCount() {
    return totalVolumeCount;
  }

  @JsonProperty(index = 115)
  public Integer getHealthyVolumeCount() {
    return healthyVolumeCount;
  }

  @JsonIgnore
  public DatanodeDetails getDatanodeDetails() {
    return dn;
  }
}
