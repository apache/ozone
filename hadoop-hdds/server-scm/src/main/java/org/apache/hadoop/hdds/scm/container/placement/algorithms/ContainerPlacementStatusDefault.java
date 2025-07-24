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

package org.apache.hadoop.hdds.scm.container.placement.algorithms;

import java.util.Collections;
import java.util.List;
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

  private final int maxReplicasPerRack;
  private final List<Integer> rackReplicaCnts;

  public ContainerPlacementStatusDefault(int currentRacks, int requiredRacks,
      int totalRacks, int maxReplicasPerRack, List<Integer> rackReplicaCnts) {
    this.requiredRacks = requiredRacks;
    this.currentRacks = currentRacks;
    this.totalRacks = totalRacks;
    this.maxReplicasPerRack = maxReplicasPerRack;
    this.rackReplicaCnts = rackReplicaCnts;
  }

  public ContainerPlacementStatusDefault(int currentRacks, int requiredRacks,
      int totalRacks) {
    this(currentRacks, requiredRacks, totalRacks, 1,
         currentRacks == 0 ? Collections.emptyList()
                 : Collections.nCopies(currentRacks, 1));
  }

  @Override
  public boolean isPolicySatisfied() {
    if (currentRacks < Math.min(totalRacks, requiredRacks)) {
      return false;
    }
    return rackReplicaCnts.stream().allMatch(cnt -> cnt <= maxReplicasPerRack);
  }

  @Override
  public String misReplicatedReason() {
    if (isPolicySatisfied()) {
      return null;
    }
    if (currentRacks < expectedPlacementCount()) {
      return "The container is mis-replicated as it is on " + currentRacks +
              " racks but should be on " + expectedPlacementCount() + " racks.";
    }
    return "The container is mis-replicated as max number of replicas per rack "
            + "is " + maxReplicasPerRack + " but number of replicas per rack" +
            " are " + rackReplicaCnts.toString();
  }

  @Override
  public int misReplicationCount() {
    if (isPolicySatisfied()) {
      return 0;
    }
    return Math.max(expectedPlacementCount() - currentRacks,
            rackReplicaCnts.stream().mapToInt(
                    cnt -> Math.max(cnt - maxReplicasPerRack, 0)).sum());
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
