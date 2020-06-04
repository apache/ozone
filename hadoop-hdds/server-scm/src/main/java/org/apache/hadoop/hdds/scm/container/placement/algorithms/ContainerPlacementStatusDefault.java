/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container.placement.algorithms;

import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;

/**
 *  Simple Status object to check if a container is replicated across enough
 *  racks.
 */
public class ContainerPlacementStatusDefault
    implements ContainerPlacementStatus {

  private final int requiredRacks;
  private final int currentRacks;
  private final int totalRacks;

  public ContainerPlacementStatusDefault(int currentRacks, int requiredRacks,
      int totalRacks) {
    this.requiredRacks = requiredRacks;
    this.currentRacks = currentRacks;
    this.totalRacks = totalRacks;
  }

  @Override
  public boolean isPolicySatisfied() {
    return currentRacks >= totalRacks || currentRacks >= requiredRacks;
  }

  @Override
  public String misReplicatedReason() {
    if (isPolicySatisfied()) {
      return null;
    }
    return "The container is mis-replicated as it is on " + currentRacks +
        " racks but should be on " + requiredRacks + " racks.";
  }

  @Override
  public int misReplicationCount() {
    if (isPolicySatisfied()) {
      return 0;
    }
    return requiredRacks - currentRacks;
  }

  @Override
  public int expectedPlacementCount() {
    return Math.min(requiredRacks, totalRacks);
  }

  @Override
  public int actualPlacementCount() {
    return currentRacks;
  }
}
