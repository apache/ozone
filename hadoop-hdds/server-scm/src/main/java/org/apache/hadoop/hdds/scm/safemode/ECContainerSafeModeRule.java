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

package org.apache.hadoop.hdds.scm.safemode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.server.events.EventQueue;

/**
 * Safe mode rule for EC containers.
 * This rule validates that a configurable percentage of EC containers have a minimum
 * number of replicas reported by the DataNodes. This rule is not satisfied until this
 * condition is met.
 */
public class ECContainerSafeModeRule extends AbstractContainerSafeModeRule {

  private final Map<ContainerID, Map<DatanodeID, DatanodeID>> ecContainerDNsMap = new ConcurrentHashMap<>();

  public ECContainerSafeModeRule(EventQueue eventQueue,
      ConfigurationSource conf, ContainerManager containerManager,
      SCMSafeModeManager manager) {
    super(conf, manager, containerManager, eventQueue);
  }

  @Override
  protected ReplicationType getContainerType() {
    return ReplicationType.EC;
  }

  @Override
  protected void handleReportedContainer(ContainerID containerID, DatanodeID datanodeID) {
    if (getContainers().containsKey(containerID)) {
      final Map<DatanodeID, DatanodeID> replicas =
          ecContainerDNsMap.computeIfAbsent(containerID, key -> new ConcurrentHashMap<>());
      replicas.put(datanodeID, datanodeID);

      if (replicas.size() >= getMinReplica(containerID)) {
        getContainers().remove(containerID);
        incrementContainersWithMinReplicas();
        getSafeModeMetrics().incCurrentContainersWithECDataReplicaReportedCount();
      }
    }
  }

  @Override
  protected void cleanup() {
    super.cleanup();
    ecContainerDNsMap.clear();
  }
}
