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
package org.apache.hadoop.hdds.scm.container.balancer;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;

import java.util.List;

/**
 * Proxy class to validate the placement policy for different type
 * of containers.
 */
public class PlacementPolicyValidateProxy {
  private PlacementPolicy defaultPlacementPolicy;
  private PlacementPolicy ecPlacementPolicy;

  public PlacementPolicyValidateProxy(StorageContainerManager scm) {
    this.defaultPlacementPolicy = scm.getContainerPlacementPolicy();
    this.ecPlacementPolicy = scm.getEcContainerPlacementPolicy();
  }

  public ContainerPlacementStatus validateContainerPlacement(
      List<DatanodeDetails> replicaList, ContainerInfo cInfo) {
    switch (cInfo.getReplicationType()) {
    case EC:
      return ecPlacementPolicy.validateContainerPlacement(
          replicaList, cInfo.getReplicationConfig().getRequiredNodes());
    default:
      return defaultPlacementPolicy.validateContainerPlacement(replicaList,
          cInfo.getReplicationConfig().getRequiredNodes());
    }
  }
}
