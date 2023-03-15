/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container.replication;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class used to pick messages from the respective ReplicationManager
 * unhealthy replicated queue,
 * calculate the delete commands and assign to the datanodes via the eventQueue.
 *
 */
public abstract class UnhealthyReplicationProcessor<HealthResult extends
        ContainerHealthResult> implements Runnable {
  private static final Logger LOG = LoggerFactory
          .getLogger(UnhealthyReplicationProcessor.class);
  private final ReplicationManager replicationManager;
  private volatile boolean runImmediately = false;
  private final long intervalInMillis;

  public UnhealthyReplicationProcessor(ReplicationManager replicationManager,
                                       long intervalInMillis) {
    this.replicationManager = replicationManager;
    this.intervalInMillis = intervalInMillis;
  }

  /**
   * Read messages from the respective queue from ReplicationManager
   * for processing the health result.
   * @return next HealthResult from the replication manager
   */
  protected abstract HealthResult dequeueHealthResultFromQueue(
          ReplicationManager rm);

  /**
   * Requeue HealthResult to ReplicationManager
   * for reprocessing the health result.
   * @return next HealthResult from the replication manager
   */
  protected abstract void requeueHealthResultFromQueue(
          ReplicationManager rm, HealthResult healthResult);

  /**
   * Read messages from the ReplicationManager under replicated queue and,
   * form commands to correct replication. The commands are added
   * to the event queue and the PendingReplicaOps are adjusted.
   *
   * Note: this is a temporary implementation of this feature. A future
   * version will need to limit the amount of messages assigned to each
   * datanode, so they are not assigned too much work.
   */
  public void processAll() {
    int processed = 0;
    int failed = 0;
    Map<ContainerHealthResult.HealthState, Integer> healthStateCntMap =
            Maps.newHashMap();
    List<HealthResult> failedOnes = new LinkedList<>();
    while (true) {
      if (!replicationManager.shouldRun()) {
        break;
      }
      HealthResult healthResult =
              dequeueHealthResultFromQueue(replicationManager);
      if (healthResult == null) {
        break;
      }
      try {
        processContainer(healthResult);
        processed++;
        healthStateCntMap.compute(healthResult.getHealthState(),
                (healthState, cnt) -> cnt == null ? 1 : (cnt + 1));
      } catch (Exception e) {
        LOG.error("Error processing Health result of class: {} for " +
                   "container {}", healthResult.getClass(),
                healthResult.getContainerInfo(), e);
        failed++;
        failedOnes.add(healthResult);
      }
    }

    failedOnes.forEach(result ->
        requeueHealthResultFromQueue(replicationManager, result));

    if (processed > 0 || failed > 0) {
      LOG.info("Processed {} containers with health state counts {}, " +
          "failed processing {}", processed, healthStateCntMap, failed);
    }
  }

  /**
   * Gets the commands to be run datanode to process the
   * container health result.
   * @return Commands to be run on Datanodes
   */
  protected abstract Set<Pair<DatanodeDetails, SCMCommand<?>>>
      getDatanodeCommands(ReplicationManager rm, HealthResult healthResult)
          throws IOException;

  private void processContainer(HealthResult healthResult) throws IOException {
    ContainerInfo containerInfo = healthResult.getContainerInfo();
    synchronized (containerInfo) {
      Set<Pair<DatanodeDetails, SCMCommand<?>>> cmds = getDatanodeCommands(
          replicationManager, healthResult);
      for (Map.Entry<DatanodeDetails, SCMCommand<?>> cmd : cmds) {
        replicationManager.sendDatanodeCommand(cmd.getValue(),
            healthResult.getContainerInfo(), cmd.getKey());
      }
    }
  }

  @Override
  public void run() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        if (replicationManager.shouldRun()) {
          processAll();
        }
        synchronized (this) {
          if (!runImmediately) {
            wait(intervalInMillis);
          }
          runImmediately = false;
        }
      }
    } catch (InterruptedException e) {
      LOG.warn("{} interrupted. Exiting...", Thread.currentThread().getName());
      Thread.currentThread().interrupt();
    }
  }

  @VisibleForTesting
  synchronized void runImmediately() {
    runImmediately = true;
    notify();
  }
}
