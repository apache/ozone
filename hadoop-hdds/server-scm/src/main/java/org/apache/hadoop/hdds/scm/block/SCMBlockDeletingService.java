/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.block;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult.EmptyTaskResult;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT;

/**
 * A background service running in SCM to delete blocks. This service scans
 * block deletion log in certain interval and caches block deletion commands
 * in {@link org.apache.hadoop.hdds.scm.node.CommandQueue}, asynchronously
 * SCM HB thread polls cached commands and sends them to datanode for physical
 * processing.
 */
public class SCMBlockDeletingService extends BackgroundService
    implements SCMService {

  public static final Logger LOG =
      LoggerFactory.getLogger(SCMBlockDeletingService.class);

  private static final int BLOCK_DELETING_SERVICE_CORE_POOL_SIZE = 1;
  private final DeletedBlockLog deletedBlockLog;
  private final NodeManager nodeManager;
  private final EventPublisher eventPublisher;
  private final SCMContext scmContext;
  private final ScmConfig scmConf;

  private int blockDeleteLimitSize;
  private ScmBlockDeletingServiceMetrics metrics;

  /**
   * SCMService related variables.
   */
  private final Lock serviceLock = new ReentrantLock();
  private ServiceStatus serviceStatus = ServiceStatus.PAUSING;

  private long safemodeExitMillis = 0;
  private final long safemodeExitRunDelayMillis;
  private final Clock clock;

  @SuppressWarnings("parameternumber")
  public SCMBlockDeletingService(DeletedBlockLog deletedBlockLog,
             NodeManager nodeManager, EventPublisher eventPublisher,
             SCMContext scmContext, SCMServiceManager serviceManager,
             ConfigurationSource conf,
             ScmBlockDeletingServiceMetrics metrics,
             Clock clock, ReconfigurationHandler reconfigurationHandler) {
    super("SCMBlockDeletingService",
        conf.getObject(ScmConfig.class)
            .getBlockDeletionInterval().toMillis(),
        TimeUnit.MILLISECONDS, BLOCK_DELETING_SERVICE_CORE_POOL_SIZE,
        conf.getTimeDuration(OZONE_BLOCK_DELETING_SERVICE_TIMEOUT,
            OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT,
            TimeUnit.MILLISECONDS), scmContext.threadNamePrefix());

    this.safemodeExitRunDelayMillis = conf.getTimeDuration(
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.clock = clock;
    this.deletedBlockLog = deletedBlockLog;
    this.nodeManager = nodeManager;
    this.eventPublisher = eventPublisher;
    this.scmContext = scmContext;
    this.metrics = metrics;
    scmConf = conf.getObject(ScmConfig.class);
    reconfigurationHandler.register(scmConf);
    blockDeleteLimitSize = scmConf.getBlockDeletionLimit();
    Preconditions.checkArgument(blockDeleteLimitSize > 0,
        "Block deletion limit should be positive.");

    // register SCMBlockDeletingService to SCMServiceManager
    serviceManager.register(this);
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new DeletedBlockTransactionScanner());
    return queue;
  }

  private class DeletedBlockTransactionScanner implements BackgroundTask {

    @Override
    public int getPriority() {
      return 1;
    }

    @Override
    public EmptyTaskResult call() throws Exception {
      if (!shouldRun()) {
        return EmptyTaskResult.newResult();
      }

      long startTime = Time.monotonicNow();
      // Scan SCM DB in HB interval and collect a throttled list of
      // to delete blocks.

      if (LOG.isDebugEnabled()) {
        LOG.debug("Running DeletedBlockTransactionScanner");
      }
      List<DatanodeDetails> datanodes =
          nodeManager.getNodes(NodeStatus.inServiceHealthy());
      if (datanodes != null) {
        // When DN node is healthy and in-service, and previous commands 
        // are handled for deleteBlocks Type, then it will be considered
        // in this iteration
        final Set<DatanodeDetails> included = datanodes.stream().filter(
            dn -> nodeManager.getCommandQueueCount(dn.getUuid(),
                Type.deleteBlocksCommand) == 0).collect(Collectors.toSet());
        try {
          DatanodeDeletedBlockTransactions transactions =
              deletedBlockLog.getTransactions(getBlockDeleteTXNum(), included);

          if (transactions.isEmpty()) {
            return EmptyTaskResult.newResult();
          }

          Set<Long> processedTxIDs = new HashSet<>();
          for (Map.Entry<UUID, List<DeletedBlocksTransaction>> entry :
              transactions.getDatanodeTransactionMap().entrySet()) {
            UUID dnId = entry.getKey();
            List<DeletedBlocksTransaction> dnTXs = entry.getValue();
            if (!dnTXs.isEmpty()) {
              Set<Long> dnTxSet = dnTXs.stream()
                  .map(DeletedBlocksTransaction::getTxID)
                  .collect(Collectors.toSet());
              processedTxIDs.addAll(dnTxSet);
              DeleteBlocksCommand command = new DeleteBlocksCommand(dnTXs);
              command.setTerm(scmContext.getTermOfLeader());
              eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND,
                  new CommandForDatanode<>(dnId, command));
              deletedBlockLog.getSCMDeletedBlockTransactionStatusManager()
                  .recordTransactionCreated(dnId, command.getId(), dnTxSet);
              metrics.incrBlockDeletionCommandSent();
              metrics.incrBlockDeletionTransactionSent(dnTXs.size());
              if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Added delete block command for datanode {} in the queue,"
                        + " number of delete block transactions: {}{}", dnId,
                    dnTXs.size(), LOG.isTraceEnabled() ?
                        ", TxID list: " + String.join(",",
                            transactions.getTransactionIDList(dnId)) : "");
              }
            }
          }
          LOG.info("Totally added {} blocks to be deleted for"
                  + " {} datanodes, task elapsed time: {}ms",
              transactions.getBlocksDeleted(),
              transactions.getDatanodeTransactionMap().size(),
              Time.monotonicNow() - startTime);
          deletedBlockLog.incrementCount(new ArrayList<>(processedTxIDs));
        } catch (NotLeaderException nle) {
          LOG.warn("Skip current run, since not leader any more.", nle);
          return EmptyTaskResult.newResult();
        } catch (IOException e) {
          // We may tolerate a number of failures for sometime
          // but if it continues to fail, at some point we need to raise
          // an exception and probably fail the SCM ? At present, it simply
          // continues to retry the scanning.
          LOG.error("Failed to get block deletion transactions from delTX log",
              e);
          return EmptyTaskResult.newResult();
        }
      }


      return EmptyTaskResult.newResult();
    }
  }

  @VisibleForTesting
  public void setBlockDeleteTXNum(int numTXs) {
    Preconditions.checkArgument(numTXs > 0,
        "Block deletion limit should be positive.");
    scmConf.setBlockDeletionLimit(numTXs);
  }

  public int getBlockDeleteTXNum() {
    return scmConf.getBlockDeletionLimit();
  }

  @Override
  public void notifyStatusChanged() {
    serviceLock.lock();
    try {
      if (scmContext.isLeaderReady() && !scmContext.isInSafeMode() &&
          serviceStatus != ServiceStatus.RUNNING) {
        safemodeExitMillis = clock.millis();
        serviceStatus = ServiceStatus.RUNNING;
      } else {
        serviceStatus = ServiceStatus.PAUSING;
      }
    } finally {
      serviceLock.unlock();
    }
  }

  @Override
  public boolean shouldRun() {
    serviceLock.lock();
    try {
      long alreadyWaitTimeInMillis = clock.millis() - safemodeExitMillis;
      boolean run = serviceStatus == ServiceStatus.RUNNING &&
          (alreadyWaitTimeInMillis >= safemodeExitRunDelayMillis);
      LOG.debug(
          "Check scm block delete run: {} serviceStatus: {} " +
              "safemodeExitRunDelayMillis: {} alreadyWaitTimeInMillis: {}",
          run, serviceStatus, safemodeExitRunDelayMillis,
          alreadyWaitTimeInMillis);
      return run;
    } finally {
      serviceLock.unlock();
    }
  }

  @Override
  public String getServiceName() {
    return SCMBlockDeletingService.class.getSimpleName();
  }

  @Override
  public void stop() {
    shutdown();
  }

  @VisibleForTesting
  public ScmBlockDeletingServiceMetrics getMetrics() {
    return this.metrics;
  }
}
