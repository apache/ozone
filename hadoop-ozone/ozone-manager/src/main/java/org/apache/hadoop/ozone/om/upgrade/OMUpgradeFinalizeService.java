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

package org.apache.hadoop.ozone.om.upgrade;

import static org.apache.hadoop.ozone.OzoneConsts.FINALIZATION_IN_PROGRESS_KEY;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ScmClient;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A background service that periodically checks whether SCM has completed finalization of an upgrade and, if so,
 * finalizes the OM upgrade.
 */
public class OMUpgradeFinalizeService extends BackgroundService {

  private static final Logger LOG = LoggerFactory.getLogger(OMUpgradeFinalizeService.class);

  private static final int THREAD_POOL_SIZE = 1;
  private static final TimeUnit INTERVAL_UNIT = TimeUnit.MILLISECONDS;
  private static final long TIMEOUT = 60000;
  private static final AtomicLong RUN_COUNT = new AtomicLong(0);

  private final OzoneManager ozoneManager;
  private final OMVersionManager versionManager;
  private final ScmClient scmClient;
  private final AtomicBoolean stopInitiated = new AtomicBoolean(false);
  private final ClientId clientId = ClientId.randomId();

  /**
   * Creates an {@code OMUpgradeFinalizeService} with a custom check interval.
   *
   * @param ozoneManager   the OzoneManager instance
   * @param versionManager the {@link OMVersionManager} to query the finalization status
   * @param scmClient      the scmClient instance used to query SCM
   * @param intervalMs     the duration to wait between checks
   */
  public OMUpgradeFinalizeService(OzoneManager ozoneManager, OMVersionManager versionManager, ScmClient scmClient,
      long intervalMs) {
    super("OMUpgradeFinalizeService", intervalMs, INTERVAL_UNIT, THREAD_POOL_SIZE, TIMEOUT,
        ozoneManager.getThreadNamePrefix());
    this.ozoneManager = ozoneManager;
    this.versionManager = versionManager;
    this.scmClient = scmClient;
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    if (!versionManager.needsFinalization()) {
      // Finalization is done (or was never needed), so this service can now shutdown. To avoid deadlocking on the
      // executor.awaitTermination by calling shutdown directly, spawn a thread to perform the shutdown which will
      // block until this task / thread completes in the executor.
      if (stopInitiated.compareAndSet(false, true)) {
        LOG.info("OMUpgradeFinalizeService: finalization is no longer needed, shutting down.");
        Thread stopper = new Thread(this::shutdown, "OMUpgradeFinalizeService-stopper");
        stopper.setDaemon(true);
        stopper.start();
      }
      return queue; // empty — PeriodicalTask.run() will return without scheduling work
    }
    if (ozoneManager.isLeaderReady()) {
      queue.add(new UpgradeStatusCheckTask());
    }
    return queue;
  }

  /**
   * Periodic task that checks upgrade finalization status and logs the result.
   */
  private class UpgradeStatusCheckTask implements BackgroundTask {

    @Override
    public BackgroundTaskResult call() {
      final long run =  RUN_COUNT.incrementAndGet();
      if (!ozoneManager.isLeaderReady()) {
        LOG.debug("OMUpgradeFinalizeService: skipping check — not the leader. Run count {}", run);
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }
      if (versionManager.needsFinalization()) {
        try {
          // To finalize OM, first finalization needs to have been started. Then SCM needs to indicate that it has
          // completed its finalization work. Only once both of those things have happened can OM finalize.
          String finalizationInProgress =
              ozoneManager.getMetadataManager().getMetaTable().get(FINALIZATION_IN_PROGRESS_KEY);
          if (finalizationInProgress == null) {
            LOG.debug("OMUpgradeFinalizeService: skipping check — finalization is not in progress. Run count {}", run);
            return BackgroundTaskResult.EmptyTaskResult.newResult();
          }

          HddsProtos.UpgradeStatus upgradeStatus = scmClient.getContainerClient().queryUpgradeStatus();
          if (upgradeStatus.getShouldFinalize()) {
            LOG.info("The SCM Upgrade has been finalized. OM will now finalize. Run count {}", run);

            OzoneManagerProtocolProtos.OMRequest omRequest = OzoneManagerProtocolProtos.OMRequest.newBuilder()
                .setCmdType(OzoneManagerProtocolProtos.Type.FinalizeUpgrade)
                .setClientId(clientId.toString())
                .build();
            OzoneManagerProtocolProtos.OMResponse response = OzoneManagerRatisUtils.submitRequest(
                ozoneManager, omRequest, clientId, run);
            if (!response.getSuccess()) {
              LOG.error("Failed to send FinalizeUpgradeRequest to over Ratis. {}. Run count {}",
                  response.getMessage(), run);
            }
          } else {
            LOG.debug("The SCM Upgrade has not been finalized. Run count {}", run);
          }
        } catch (Exception e) {
          LOG.error("An exception occurred while trying to check the SCM Upgrade status or finalize OM. Run count {}",
              run, e);
        }
      } else {
        LOG.debug("Finalization is not in progress. Run count {}", run);
      }
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }
  }
}
