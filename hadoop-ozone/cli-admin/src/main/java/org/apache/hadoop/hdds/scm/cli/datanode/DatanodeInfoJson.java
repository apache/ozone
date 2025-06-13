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
  
  DatanodeInfoJson(ListInfoSubcommand.DatanodeWithAttributes dna) {
    this.id = dna.getDatanodeDetails().getUuid().toString();
    this.ports = dna.getDatanodeDetails().getPorts();
    this.opState = String.valueOf(dna.getOpState());
    this.healthState = String.valueOf(dna.getHealthState());
    this.hostName = dna.getDatanodeDetails().getHostName();
    this.ipAddress = dna.getDatanodeDetails().getIpAddress();
    this.persistedOpStateExpiryEpochSec = dna.getDatanodeDetails().getPersistedOpStateExpiryEpochSec();
    this.decommissioned = dna.getDatanodeDetails().isDecommissioned();
    this.maintenance = dna.getDatanodeDetails().isMaintenance();
    this.level = dna.getDatanodeDetails().getLevel();
    this.cost = dna.getDatanodeDetails().getCost();
    this.numOfLeaves = dna.getDatanodeDetails().getNumOfLeaves();
    this.networkFullPath = dna.getDatanodeDetails().getNetworkFullPath();
    this.networkLocation = dna.getDatanodeDetails().getNetworkLocation();
    this.networkName = dna.getDatanodeDetails().getNetworkName();
    this.setupTime = dna.getDatanodeDetails().getSetupTime();
    this.currentVersion = dna.getDatanodeDetails().getCurrentVersion();
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
}
