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

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.ratis.util.Preconditions;

/**
 * Class defining Safe mode exit criteria for Ratis Containers.
 * This rule validates that a configurable percentage of Ratis containers have a minimum
 * number of replicas reported by the DataNodes. This rule is not satisfied until this
 * condition is met.
 */
public class RatisContainerSafeModeRule extends AbstractContainerSafeModeRule {

  public RatisContainerSafeModeRule(EventQueue eventQueue,
      ConfigurationSource conf, ContainerManager containerManager,
      SCMSafeModeManager manager) {
    super(conf, manager, containerManager, eventQueue);
  }

  @Override
  protected ReplicationType getContainerType() {
    return ReplicationType.RATIS;
  }

  @Override
  protected void handleReportedContainer(ContainerID containerID, DatanodeID datanodeID) {
    final int minReplica = getMinReplica(containerID);
    if (getContainers().remove(containerID) != null) {
      // Assume minReplica == 1 for Ratis Containers.
      Preconditions.assertSame(1, minReplica, "minReplica");
      incrementContainersWithMinReplicas();
      getSafeModeMetrics().incCurrentContainersWithOneReplicaReportedCount();
    }
  }

}
