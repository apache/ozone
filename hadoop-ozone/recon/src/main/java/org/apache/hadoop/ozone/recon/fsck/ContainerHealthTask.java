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

package org.apache.hadoop.ozone.recon.fsck;

import javax.inject.Inject;
import org.apache.hadoop.ozone.recon.metrics.ContainerHealthTaskMetrics;
import org.apache.hadoop.ozone.recon.scm.ReconScmTask;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * New implementation of Container Health Task using Local ReplicationManager.
 *
 * <p><b>Solution:</b></p>
 * <ul>
 *   <li>Uses Recon's local ReplicationManager (not RPC to SCM)</li>
 *   <li>Calls processAll() once to check all containers in batch</li>
 *   <li>ReplicationManager uses stub PendingOps (NoOpsContainerReplicaPendingOps)</li>
 *   <li>No false positives despite stub - health determination ignores pending ops</li>
 *   <li>All database operations handled inside ReconReplicationManager</li>
 * </ul>
 *
 * <p><b>Benefits over RPC call to SCM 3:</b></p>
 * <ul>
 *   <li>Zero RPC overhead (no per-container calls to SCM)</li>
 *   <li>Zero SCM load</li>
 *   <li>Simpler code - single method call</li>
 *   <li>Perfect accuracy (proven via code analysis)</li>
 *   <li>Captures ALL container health states (no 100-sample limit)</li>
 * </ul>
 *
 * @see ReconReplicationManager
 * @see NoOpsContainerReplicaPendingOps
 */
public class ContainerHealthTask extends ReconScmTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerHealthTask.class);
  private static final long MIN_NEXT_RUN_INTERVAL_MS = 60_000L;

  private final ReconStorageContainerManagerFacade reconScm;
  private final long interval;
  private final ContainerHealthTaskMetrics taskMetrics;

  @Inject
  public ContainerHealthTask(
      ReconTaskConfig reconTaskConfig,
      ReconTaskStatusUpdaterManager taskStatusUpdaterManager,
      ReconStorageContainerManagerFacade reconScm) {
    super(taskStatusUpdaterManager);
    this.reconScm = reconScm;
    this.interval = reconTaskConfig.getMissingContainerTaskInterval().toMillis();
    this.taskMetrics = ContainerHealthTaskMetrics.create();
    LOG.info("Initialized ContainerHealthTask with Local ReplicationManager, interval={}ms",
        interval);
  }

  @Override
  protected void run() {
    while (canRun()) {
      long cycleStart = Time.monotonicNow();
      try {
        initializeAndRunTask();
        long elapsed = Time.monotonicNow() - cycleStart;
        long sleepMs = Math.max(MIN_NEXT_RUN_INTERVAL_MS, interval - elapsed);
        if (sleepMs > 0) {
          Thread.sleep(sleepMs);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.info("ContainerHealthTask interrupted");
        break;
      } catch (Exception e) {
        LOG.error("Error in ContainerHealthTask", e);
      }
    }
  }

  /**
   * Main task execution - uses Recon's local ReplicationManager.
   *
   * <p>Simply calls processAll() on ReconReplicationManager, which:
   * <ul>
   *   <li>Processes all containers in batch using inherited health check chain</li>
   *   <li>Captures ALL unhealthy containers (no 100-sample limit)</li>
   *   <li>Stores results in UNHEALTHY_CONTAINERS table</li>
   * </ul>
   */
  @Override
  protected void runTask() throws Exception {
    long start = Time.monotonicNow();
    LOG.info("ContainerHealthTask starting - using local ReplicationManager");

    // Get Recon's ReplicationManager (actually a ReconReplicationManager instance)
    ReconReplicationManager reconRM =
        (ReconReplicationManager) reconScm.getReplicationManager();

    // Call processAll() ONCE - processes all containers in batch!
    // This:
    // 1. Runs health checks on all containers using inherited SCM logic
    // 2. Captures ALL unhealthy containers (no sampling)
    // 3. Stores all health states in database
    boolean succeeded = false;
    try {
      reconRM.processAll();
      taskMetrics.incrSuccess();
      succeeded = true;
    } catch (Exception e) {
      taskMetrics.incrFailure();
      throw e;
    } finally {
      long durationMs = Time.monotonicNow() - start;
      taskMetrics.addRunTime(durationMs);
      LOG.info("ContainerHealthTask completed with status={} in {} ms",
          succeeded ? "success" : "failure", durationMs);
    }
  }

  @Override
  public synchronized void stop() {
    super.stop();
    taskMetrics.unRegister();
  }
}
