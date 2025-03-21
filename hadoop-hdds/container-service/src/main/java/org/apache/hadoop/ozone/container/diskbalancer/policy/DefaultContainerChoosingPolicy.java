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

package org.apache.hadoop.ozone.container.diskbalancer.policy;

import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.replication.AbstractReplicationTask;
import org.apache.hadoop.ozone.container.replication.ReplicationSupervisor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Choose a container from specified volume, make sure it's not being balancing.
 */
public class DefaultContainerChoosingPolicy implements ContainerChoosingPolicy {
  public static final Logger LOG = LoggerFactory.getLogger(
      DefaultContainerChoosingPolicy.class);

  @Override
  public ContainerData chooseContainer(OzoneContainer ozoneContainer,
      HddsVolume hddsVolume, Set<Long> inProgressContainerIDs,
      ReplicationSupervisor replicationSupervisor) {
    Iterator<Container<?>> itr = ozoneContainer.getController()
        .getContainers(hddsVolume);
    while (itr.hasNext()) {
      ContainerData containerData = itr.next().getContainerData();
      long containerID = containerData.getContainerID();

      if (!inPushModeContainers(containerID, replicationSupervisor) &&
          !inProgressContainerIDs.contains(containerID) &&
          containerData.isClosed() && !containerData.isEmpty()) {
        return containerData;
      }
    }
    return null;
  }

  private boolean inPushModeContainers(long containerID, ReplicationSupervisor replicationSupervisor) {
    for (AbstractReplicationTask task : replicationSupervisor.getInFlightTasks()) {
      if (task.getContainerId() == containerID) {
        return true;
      }
    }
    return false;
  }
}
