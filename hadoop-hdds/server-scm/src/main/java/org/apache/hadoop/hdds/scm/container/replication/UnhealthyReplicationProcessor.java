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

package org.apache.hadoop.hdds.scm.container.replication;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final Supplier<Duration> interval;

  public UnhealthyReplicationProcessor(ReplicationManager replicationManager,
                                       Supplier<Duration> interval) {
    this.replicationManager = replicationManager;
    this.interval = interval;
  }

  /**
   * Read messages from the respective replication queue
   * for processing the health result.
   * @return next HealthResult from the replication queue
   */
  protected abstract HealthResult dequeueHealthResultFromQueue(
          ReplicationQueue queue);

  /**
   * Requeue HealthResult to ReplicationManager
   * for reprocessing the health result.
   */
  protected abstract void requeueHealthResult(
          ReplicationQueue queue, HealthResult healthResult);

  /**
   * Check if the pending operation limit is reached. For under replicated
   * containers, this is the limit of inflight replicas which are scheduled
   * to be created. For over replicated containers, which sends delete commands
   * there is no limit.
   * @param rm The ReplicationManager instance
   * @param inflightLimit The limit of operations allowed
   * @return True if the limit is reached, false otherwise.
   */
  protected abstract boolean inflightOperationLimitReached(
      ReplicationManager rm, long inflightLimit);

  /**
   * Read messages from the ReplicationManager under replicated queue and,
   * form commands to correct replication. The commands are added
   * to the event queue and the PendingReplicaOps are adjusted.
   */
  public void processAll(ReplicationQueue queue) {
    int processed = 0;
    int failed = 0;
    int overloaded = 0;
    Map<ContainerHealthResult.HealthState, Integer> healthStateCntMap =
            Maps.newHashMap();
    List<HealthResult> failedOnes = new LinkedList<>();
    // Getting the limit requires iterating over all nodes registered in
    // NodeManager and counting the healthy ones. This is somewhat expensive
    // so we get get the count once per iteration as it should not change too
    // often.
    long inflightLimit = replicationManager.getReplicationInFlightLimit();
    while (true) {
      if (!replicationManager.shouldRun()) {
        break;
      }
      if (inflightLimit > 0 &&
          inflightOperationLimitReached(replicationManager, inflightLimit)) {
        LOG.info("The maximum number of pending replicas ({}) are scheduled. " +
            "Ending the iteration.", inflightLimit);
        replicationManager
            .getMetrics().incrPendingReplicationLimitReachedTotal();
        break;
      }
      HealthResult healthResult =
              dequeueHealthResultFromQueue(queue);
      if (healthResult == null) {
        break;
      }
      try {
        processContainer(healthResult);
        processed++;
        healthStateCntMap.compute(healthResult.getHealthState(),
            (healthState, cnt) -> cnt == null ? 1 : (cnt + 1));
      } catch (CommandTargetOverloadedException e) {
        LOG.debug("All targets overloaded when processing Health result of " +
            "class: {} for container {}", healthResult.getClass(),
            healthResult.getContainerInfo());
        overloaded++;
        failedOnes.add(healthResult);
      } catch (Exception e) {
        LOG.error("Error processing Health result of class: {} for " +
                "container {}", healthResult.getClass(),
            healthResult.getContainerInfo(), e);
        failed++;
        failedOnes.add(healthResult);
      }
    }

    failedOnes.forEach(result -> requeueHealthResult(queue, result));

    if (processed > 0 || failed > 0 || overloaded > 0) {
      LOG.info("Processed {} containers with health state counts {}, " +
          "failed processing {}, deferred due to load {}",
          processed, healthStateCntMap, failed, overloaded);
    }
  }

  /**
   * Gets the commands to be run datanode to process the
   * container health result.
   * @return Commands to be run on Datanodes
   */
  protected abstract int
      sendDatanodeCommands(ReplicationManager rm, HealthResult healthResult)
          throws IOException;

  private void processContainer(HealthResult healthResult) throws IOException {
    ContainerInfo containerInfo = healthResult.getContainerInfo();
    synchronized (containerInfo) {
      sendDatanodeCommands(replicationManager, healthResult);
    }
  }

  @Override
  public void run() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        if (replicationManager.shouldRun()) {
          processAll(replicationManager.getQueue());
        }

        final Duration duration = interval.get();
        if (!runImmediately && LOG.isDebugEnabled()) {
          LOG.debug("May wait {} before next run", duration);
        }
        synchronized (this) {
          if (!runImmediately) {
            wait(duration.toMillis());
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
