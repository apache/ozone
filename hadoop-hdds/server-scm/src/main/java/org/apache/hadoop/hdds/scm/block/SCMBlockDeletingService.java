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
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A background service running in SCM to delete blocks. This service scans
 * block deletion log in certain interval and caches block deletion commands
 * in {@link org.apache.hadoop.hdds.scm.node.CommandQueue}, asynchronously
 * SCM HB thread polls cached commands and sends them to datanode for physical
 * processing.
 */
public class SCMBlockDeletingService extends BackgroundService {

  public static final Logger LOG =
      LoggerFactory.getLogger(SCMBlockDeletingService.class);

  private final static int BLOCK_DELETING_SERVICE_CORE_POOL_SIZE = 1;
  private final DeletedBlockLog deletedBlockLog;
  private final ContainerManager containerManager;
  private final NodeManager nodeManager;
  private final EventPublisher eventPublisher;

  private int blockDeleteLimitSize;

  public SCMBlockDeletingService(DeletedBlockLog deletedBlockLog,
      ContainerManager containerManager, NodeManager nodeManager,
      EventPublisher eventPublisher, Duration interval, long serviceTimeout,
      ConfigurationSource conf) {
    super("SCMBlockDeletingService", interval.toMillis(), TimeUnit.MILLISECONDS,
        BLOCK_DELETING_SERVICE_CORE_POOL_SIZE, serviceTimeout);
    this.deletedBlockLog = deletedBlockLog;
    this.containerManager = containerManager;
    this.nodeManager = nodeManager;
    this.eventPublisher = eventPublisher;

    blockDeleteLimitSize =
        conf.getObject(ScmConfig.class).getBlockDeletionLimit();
    Preconditions.checkArgument(blockDeleteLimitSize > 0,
        "Block deletion limit should be " + "positive.");
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new DeletedBlockTransactionScanner());
    return queue;
  }

  void handlePendingDeletes(PendingDeleteStatusList deletionStatusList) {
    DatanodeDetails dnDetails = deletionStatusList.getDatanodeDetails();
    for (PendingDeleteStatusList.PendingDeleteStatus deletionStatus :
        deletionStatusList.getPendingDeleteStatuses()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Block deletion txnID lagging in datanode {} for containerID {}."
                + " Datanode delete txnID: {}, SCM txnID: {}",
            dnDetails.getUuid(), deletionStatus.getContainerId(),
            deletionStatus.getDnDeleteTransactionId(),
            deletionStatus.getScmDeleteTransactionId());
      }
    }
  }

  private class DeletedBlockTransactionScanner implements BackgroundTask {

    @Override
    public int getPriority() {
      return 1;
    }

    @Override
    public EmptyTaskResult call() throws Exception {
      long startTime = Time.monotonicNow();
      // Scan SCM DB in HB interval and collect a throttled list of
      // to delete blocks.

      if (LOG.isDebugEnabled()) {
        LOG.debug("Running DeletedBlockTransactionScanner");
      }
      // TODO - DECOMM - should we be deleting blocks from decom nodes
      //        and what about entering maintenance.
      List<DatanodeDetails> datanodes =
          nodeManager.getNodes(NodeStatus.inServiceHealthy());
      if (datanodes != null) {
        try {
          DatanodeDeletedBlockTransactions transactions =
              deletedBlockLog.getTransactions(blockDeleteLimitSize);
          Map<Long, Long> containerIdToMaxTxnId =
              transactions.getContainerIdToTxnIdMap();

          if (transactions.isEmpty()) {
            return EmptyTaskResult.newResult();
          }

          for (Map.Entry<UUID, List<DeletedBlocksTransaction>> entry :
              transactions.getDatanodeTransactionMap().entrySet()) {
            UUID dnId = entry.getKey();
            List<DeletedBlocksTransaction> dnTXs = entry.getValue();
            if (!dnTXs.isEmpty()) {
              // TODO commandQueue needs a cap.
              // We should stop caching new commands if num of un-processed
              // command is bigger than a limit, e.g 50. In case datanode goes
              // offline for sometime, the cached commands be flooded.
              eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND,
                  new CommandForDatanode<>(dnId,
                      new DeleteBlocksCommand(dnTXs)));
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

          containerManager.updateDeleteTransactionId(containerIdToMaxTxnId);
          LOG.info("Totally added {} blocks to be deleted for"
                  + " {} datanodes, task elapsed time: {}ms",
              transactions.getBlocksDeleted(),
              transactions.getDatanodeTransactionMap().size(),
              Time.monotonicNow() - startTime);
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
    blockDeleteLimitSize = numTXs;
  }
}
