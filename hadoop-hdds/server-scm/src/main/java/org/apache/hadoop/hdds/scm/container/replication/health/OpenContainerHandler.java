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

package org.apache.hadoop.hdds.scm.container.replication.health;

import static org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.compareState;

import java.util.Set;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used in Replication Manager to check open container health for both
 * EC and Ratis containers. Healthy open containers are skipped, while
 * containers where some replicas are not in the same state as the container
 * will be closed.
 */
public class OpenContainerHandler extends AbstractCheck {

  private static final Logger LOG =
      LoggerFactory.getLogger(OpenContainerHandler.class);

  private final ReplicationManager replicationManager;

  public OpenContainerHandler(ReplicationManager replicationManager) {
    this.replicationManager = replicationManager;
  }

  @Override
  public boolean handle(ContainerCheckRequest request) {
    ContainerInfo containerInfo = request.getContainerInfo();
    if (containerInfo.getState() == HddsProtos.LifeCycleState.OPEN) {
      LOG.debug("Checking open container {} in OpenContainerHandler",
          containerInfo);
      final boolean noPipeline = !replicationManager.hasHealthyPipeline(containerInfo);
      // Minor optimization. If noPipeline is true, isOpenContainerHealthy will not
      // be called.
      final boolean unhealthy = noPipeline || !isOpenContainerHealthy(containerInfo,
          request.getContainerReplicas());
      if (unhealthy) {
        // For an OPEN container, we close the container
        // if the container has no Pipeline or if the container is unhealthy.
        LOG.info("Container {} is open but {}. Triggering close.",
            containerInfo, noPipeline ? "has no Pipeline" : "unhealthy");

        request.getReport().incrementAndSample(noPipeline ?
                ContainerHealthState.OPEN_WITHOUT_PIPELINE :
                ContainerHealthState.OPEN_UNHEALTHY,
            containerInfo);

        if (!request.isReadOnly()) {
          replicationManager
              .sendCloseContainerEvent(containerInfo.containerID());
        }
      }
      // For open containers we do not want to do any further processing in RM
      // so return true to stop the command chain.
      return true;
    }
    // The container is not open, so we return false to let the next handler in
    // the chain process it.
    return false;
  }

  /**
   * An open container is healthy if all its replicas are in the same state as
   * the container.
   * @param container The container to check
   * @param replicas The replicas belonging to the container
   * @return True if the container is healthy, false otherwise
   */
  private boolean isOpenContainerHealthy(
      ContainerInfo container, Set< ContainerReplica > replicas) {
    HddsProtos.LifeCycleState state = container.getState();
    return replicas.stream()
        .allMatch(r -> compareState(state, r.getState()));
  }
}
