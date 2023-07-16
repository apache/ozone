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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.scm.block;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto.DeleteBlockTransactionResult;
import org.apache.hadoop.hdds.scm.command.CommandStatusReportHandler.DeleteBlockStatus;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import static java.lang.Math.min;
import static org.apache.hadoop.hdds.scm.block.SCMDeletedBlockTransactionStatusManager.SCMDeleteBlocksCommandStatusManager.CmdStatus;
import static org.apache.hadoop.hdds.scm.block.SCMDeletedBlockTransactionStatusManager.SCMDeleteBlocksCommandStatusManager.CmdStatus.EXECUTED;
import static org.apache.hadoop.hdds.scm.block.SCMDeletedBlockTransactionStatusManager.SCMDeleteBlocksCommandStatusManager.CmdStatus.NEED_RESEND;
import static org.apache.hadoop.hdds.scm.block.SCMDeletedBlockTransactionStatusManager.SCMDeleteBlocksCommandStatusManager.CmdStatus.PENDING_EXECUTED;
import static org.apache.hadoop.hdds.scm.block.SCMDeletedBlockTransactionStatusManager.SCMDeleteBlocksCommandStatusManager.CmdStatus.SENT;
import static org.apache.hadoop.hdds.scm.block.SCMDeletedBlockTransactionStatusManager.SCMDeleteBlocksCommandStatusManager.CmdStatus.TO_BE_SENT;

/**
 * This is a class to manage the status of DeletedBlockTransaction,
 * the purpose of this class is to reduce the number of duplicate
 * DeletedBlockTransaction sent to the DN.
 */
public class SCMDeletedBlockTransactionStatusManager
    implements EventHandler<DeleteBlockStatus> {
  public static final Logger LOG =
      LoggerFactory.getLogger(SCMDeletedBlockTransactionStatusManager.class);
  // Maps txId to set of DNs which are successful in committing the transaction
  private final Map<Long, Set<UUID>> transactionToDNsCommitMap;
  // Maps txId to its retry counts;
  private final Map<Long, Integer> transactionToRetryCountMap;
  // The access to DeletedBlocksTXTable is protected by
  // DeletedBlockLogStateManager.
  private final DeletedBlockLogStateManager deletedBlockLogStateManager;
  private final ContainerManager containerManager;
  private final ScmBlockDeletingServiceMetrics metrics;
  private final SCMContext scmContext;
  private final Lock lock;
  private final long scmCommandTimeoutMs;

  /**
   * Before the DeletedBlockTransaction is executed on DN and reported to
   * SCM, it is managed by this {@link SCMDeleteBlocksCommandStatusManager}.
   *
   * After the DeletedBlocksTransaction in the DeleteBlocksCommand is
   * committed on the SCM, it is managed by
   * {@link SCMDeletedBlockTransactionStatusManager#transactionToDNsCommitMap}
   */
  private final SCMDeleteBlocksCommandStatusManager
      scmDeleteBlocksCommandStatusManager;

  public SCMDeletedBlockTransactionStatusManager(
      DeletedBlockLogStateManager deletedBlockLogStateManager,
      ContainerManager containerManager, SCMContext scmContext,
      Map<Long, Integer> transactionToRetryCountMap,
      ScmBlockDeletingServiceMetrics metrics,
      Lock lock, long scmCommandTimeoutMs) {
    // maps transaction to dns which have committed it.
    this.deletedBlockLogStateManager = deletedBlockLogStateManager;
    this.metrics = metrics;
    this.containerManager = containerManager;
    this.scmContext = scmContext;
    this.lock = lock;
    this.scmCommandTimeoutMs = scmCommandTimeoutMs;
    this.transactionToDNsCommitMap = new ConcurrentHashMap<>();
    this.transactionToRetryCountMap = transactionToRetryCountMap;
    this.scmDeleteBlocksCommandStatusManager =
        new SCMDeleteBlocksCommandStatusManager();
  }

  /**
   * A class that manages the status of a DeletedBlockTransaction based
   * on DeleteBlocksCommand.
   */
  protected static class SCMDeleteBlocksCommandStatusManager {
    public static final Logger LOG =
        LoggerFactory.getLogger(SCMDeleteBlocksCommandStatusManager.class);
    private final Map<UUID, Map<Long, CmdStatusData>> scmCmdStatusRecord;

    private static final CmdStatus DEFAULT_STATUS = TO_BE_SENT;
    private static final Set<CmdStatus> STATUSES_REQUIRING_TIMEOUT =
        new HashSet<>(Arrays.asList(SENT, PENDING_EXECUTED));
    private static final Set<CmdStatus> FINIAL_STATUSES = new HashSet<>(
        Arrays.asList(EXECUTED, NEED_RESEND));

    public SCMDeleteBlocksCommandStatusManager() {
      this.scmCmdStatusRecord = new ConcurrentHashMap<>();
    }

    /**
     * Status of SCMDeleteBlocksCommand.
     */
    public enum CmdStatus {
      // The DeleteBlocksCommand has not yet been sent.
      // This is the initial status of the command after it's created.
      TO_BE_SENT,
      // This status indicates that the DeleteBlocksCommand has been sent
      // to the DataNode, but the Datanode has not reported any new status
      // for the DeleteBlocksCommand.
      SENT,
      // The DeleteBlocksCommand has been received by Datanode and
      // is waiting for executed.
      PENDING_EXECUTED,
      // The DeleteBlocksCommand was executed, and the execution was successful
      EXECUTED,
      // The DeleteBlocksCommand was executed but failed to execute,
      // or was lost before it was executed.
      NEED_RESEND
    }

    protected static final class CmdStatusData {
      private final UUID dnId;
      private final long scmCmdId;
      private final Set<Long> deletedBlocksTxIds;
      private Instant updateTime;
      private CmdStatus status;

      private CmdStatusData(
          UUID dnId, long scmTxID, Set<Long> deletedBlocksTxIds) {
        this.dnId = dnId;
        this.scmCmdId = scmTxID;
        this.deletedBlocksTxIds = deletedBlocksTxIds;
        setStatus(DEFAULT_STATUS);
      }

      public Set<Long> getDeletedBlocksTxIds() {
        return Collections.unmodifiableSet(deletedBlocksTxIds);
      }

      public UUID getDnId() {
        return dnId;
      }

      public long getScmCmdId() {
        return scmCmdId;
      }

      public CmdStatus getStatus() {
        return status;
      }

      public void setStatus(CmdStatus status) {
        this.updateTime = Instant.now();
        this.status = status;
      }

      public Instant getUpdateTime() {
        return updateTime;
      }

      @Override
      public String toString() {
        return "ScmTxStateMachine" +
            "{dnId=" + dnId +
            ", scmTxID=" + scmCmdId +
            ", deletedBlocksTxIds=" + deletedBlocksTxIds +
            ", updateTime=" + updateTime +
            ", status=" + status +
            '}';
      }
    }

    protected static CmdStatusData createScmCmdStatusData(
        UUID dnId, long scmCmdId, Set<Long> deletedBlocksTxIds) {
      return new CmdStatusData(dnId, scmCmdId, deletedBlocksTxIds);
    }

    protected void recordScmCommand(CmdStatusData statusData) {
      LOG.debug("Record ScmCommand: {}", statusData);
      scmCmdStatusRecord.computeIfAbsent(statusData.getDnId(), k ->
          new ConcurrentHashMap<>()).put(statusData.getScmCmdId(), statusData);
    }

    protected void onSent(UUID dnId, long scmCmdId) {
      updateStatus(dnId, scmCmdId, SENT);
    }

    protected void onDatanodeDead(UUID dnId) {
      LOG.info("Clean SCMCommand record for DN: {}", dnId);
      scmCmdStatusRecord.remove(dnId);
    }

    protected void updateStatusByDNCommandStatus(UUID dnId, long scmCmdId,
        CommandStatus.Status newState) {
      CmdStatus status = fromProtoCommandStatus(newState);
      if (status != null) {
        updateStatus(dnId, scmCmdId, status);
      }
    }

    protected void cleanAllTimeoutSCMCommand(long timeoutMs) {
      for (UUID dnId : scmCmdStatusRecord.keySet()) {
        for (CmdStatus status : STATUSES_REQUIRING_TIMEOUT) {
          removeTimeoutScmCommand(
              dnId, getScmCommandIds(dnId, status), timeoutMs);
        }
      }
    }

    protected void cleanSCMCommandForDn(UUID dnId, long timeoutMs) {
      cleanTimeoutSCMCommand(dnId, timeoutMs);
      cleanFinalStatusSCMCommand(dnId);
    }

    private void cleanTimeoutSCMCommand(UUID dnId, long timeoutMs) {
      for (CmdStatus status : STATUSES_REQUIRING_TIMEOUT) {
        removeTimeoutScmCommand(
            dnId, getScmCommandIds(dnId, status), timeoutMs);
      }
    }

    private void cleanFinalStatusSCMCommand(UUID dnId) {
      for (CmdStatus status : FINIAL_STATUSES) {
        for (Long scmCmdId : getScmCommandIds(dnId, status)) {
          CmdStatusData stateData = removeScmCommand(dnId, scmCmdId);
          LOG.debug("Clean SCMCommand status: {} for DN: {}, stateData: {}",
              status, dnId, stateData);
        }
      }
    }

    private Set<Long> getScmCommandIds(UUID dnId, CmdStatus status) {
      Set<Long> scmCmdIds = new HashSet<>();
      Map<Long, CmdStatusData> record = scmCmdStatusRecord.get(dnId);
      if (record == null) {
        return scmCmdIds;
      }
      for (CmdStatusData statusData : record.values()) {
        if (statusData.getStatus().equals(status)) {
          scmCmdIds.add(statusData.getScmCmdId());
        }
      }
      return scmCmdIds;
    }

    private Instant getUpdateTime(UUID dnId, long scmCmdId) {
      Map<Long, CmdStatusData> record = scmCmdStatusRecord.get(dnId);
      if (record == null || record.get(scmCmdId) == null) {
        return null;
      }
      return record.get(scmCmdId).getUpdateTime();
    }

    private void updateStatus(UUID dnId, long scmCmdId, CmdStatus newStatus) {
      Map<Long, CmdStatusData> recordForDn = scmCmdStatusRecord.get(dnId);
      if (recordForDn == null) {
        LOG.warn("Unknown Datanode: {} scmCmdId {} newStatus {}",
            dnId, scmCmdId, newStatus);
        return;
      }
      if (recordForDn.get(scmCmdId) == null) {
        LOG.warn("Unknown SCM Command: {} Datanode {} newStatus {}",
            scmCmdId, dnId, newStatus);
        return;
      }

      boolean changed = false;
      CmdStatusData statusData = recordForDn.get(scmCmdId);
      CmdStatus oldStatus = statusData.getStatus();

      switch (newStatus) {
      case SENT:
        if (oldStatus == TO_BE_SENT) {
          // TO_BE_SENT -> SENT: The DeleteBlocksCommand is sent by SCM,
          // The follow-up status has not been updated by Datanode.
          statusData.setStatus(SENT);
          changed = true;
        }
        break;
      case PENDING_EXECUTED:
        if (oldStatus == SENT || oldStatus == PENDING_EXECUTED) {
          // SENT -> PENDING_EXECUTED: The DeleteBlocksCommand is sent and
          // received by the Datanode, but the command is not executed by the
          // Datanode, the command is waiting to be executed.

          // PENDING_EXECUTED -> PENDING_EXECUTED: The DeleteBlocksCommand
          // continues to wait to be executed by Datanode.
          statusData.setStatus(PENDING_EXECUTED);
          changed = true;
        }
        break;
      case NEED_RESEND:
        if (oldStatus == SENT || oldStatus == PENDING_EXECUTED) {
          // SENT -> NEED_RESEND: The DeleteBlocksCommand is sent and lost
          // before it is received by the DN.

          // PENDING_EXECUTED -> NEED_RESEND: The DeleteBlocksCommand waited for
          // a while and was executed, but the execution failed;.
          // Or the DeleteBlocksCommand was lost while waiting(such as the
          // Datanode restart).
          statusData.setStatus(NEED_RESEND);
          changed = true;
        }
        break;
      case EXECUTED:
        if (oldStatus == SENT || oldStatus == PENDING_EXECUTED) {
          // PENDING_EXECUTED -> EXECUTED: The Command waits for a period of
          // time on the DN and is executed successfully.

          // SENT -> EXECUTED: The DeleteBlocksCommand has been sent to
          // Datanode, executed by DN, and executed successfully.
          statusData.setStatus(EXECUTED);
          changed = true;
        }
        break;
      default:
        LOG.error("Can not update to Unknown new Status: {}", newStatus);
        break;
      }
      if (!changed) {
        LOG.warn("Cannot update illegal status for DN: {} ScmCommandId {} " +
            "Status From {} to {}", dnId, scmCmdId, oldStatus, newStatus);
      } else {
        LOG.debug("Successful update DN: {} ScmCommandId {} Status From {} to" +
            " {}", dnId, scmCmdId, oldStatus, newStatus);
      }
    }

    private void removeTimeoutScmCommand(UUID dnId,
        Set<Long> scmCmdIds, long timeoutMs) {
      Instant now = Instant.now();
      for (Long scmCmdId : scmCmdIds) {
        Instant updateTime = getUpdateTime(dnId, scmCmdId);
        if (updateTime != null &&
            Duration.between(updateTime, now).toMillis() > timeoutMs) {
          updateStatus(dnId, scmCmdId, NEED_RESEND);
          CmdStatusData state = removeScmCommand(dnId, scmCmdId);
          LOG.warn("Remove Timeout SCM BlockDeletionCommand {} for DN {} " +
              "after without update {}ms}", state, dnId, timeoutMs);
        } else {
          LOG.warn("Timeout SCM scmCmdIds {} for DN {} " +
              "after without update {}ms}", scmCmdIds, dnId, timeoutMs);
        }
      }
    }

    private CmdStatusData removeScmCommand(UUID dnId, long scmCmdId) {
      Map<Long, CmdStatusData> record = scmCmdStatusRecord.get(dnId);
      if (record == null || record.get(scmCmdId) == null) {
        return null;
      }

      CmdStatus status = record.get(scmCmdId).getStatus();
      if (!FINIAL_STATUSES.contains(status)) {
        LOG.error("Cannot Remove ScmCommand {} Non-final Status {} for DN: {}" +
            ". final Status {}", scmCmdId, status, dnId, FINIAL_STATUSES);
        return null;
      }

      CmdStatusData statusData = record.remove(scmCmdId);
      LOG.debug("Remove ScmCommand {} for DN: {} ", statusData, dnId);
      return statusData;
    }

    private static CmdStatus fromProtoCommandStatus(
        CommandStatus.Status protoCmdStatus) {
      switch (protoCmdStatus) {
      case PENDING:
        return CmdStatus.PENDING_EXECUTED;
      case EXECUTED:
        return CmdStatus.EXECUTED;
      case FAILED:
        return CmdStatus.NEED_RESEND;
      default:
        LOG.error("Unknown protoCmdStatus: {} cannot convert " +
            "to ScmDeleteBlockCommandStatus", protoCmdStatus);
        return null;
      }
    }

    public Map<UUID, Map<Long, CmdStatus>> getCommandStatusByTxId(
        Set<UUID> dnIds) {
      Map<UUID, Map<Long, CmdStatus>> result =
          new HashMap<>(scmCmdStatusRecord.size());

      for (UUID dnId : dnIds) {
        Map<Long, CmdStatusData> record = scmCmdStatusRecord.get(dnId);
        if (record == null) {
          continue;
        }
        Map<Long, CmdStatus> dnStatusMap = new HashMap<>();
        for (CmdStatusData statusData : record.values()) {
          CmdStatus status = statusData.getStatus();
          for (Long deletedBlocksTxId : statusData.getDeletedBlocksTxIds()) {
            dnStatusMap.put(deletedBlocksTxId, status);
          }
        }
        result.put(dnId, dnStatusMap);
      }

      return result;
    }

    private void clear() {
      scmCmdStatusRecord.clear();
    }

    @VisibleForTesting
    Map<UUID, Map<Long, CmdStatusData>> getScmCmdStatusRecord() {
      return scmCmdStatusRecord;
    }
  }

  public void onSent(UUID dnId, long scmCmdId) {
    scmDeleteBlocksCommandStatusManager.updateStatus(dnId, scmCmdId, SENT);
  }

  public Map<UUID, Map<Long, CmdStatus>> getCommandStatusByTxId(
      Set<UUID> dnIds) {
    return scmDeleteBlocksCommandStatusManager.getCommandStatusByTxId(dnIds);
  }

  public void recordTransactionCreated(
      UUID dnId, long scmCmdId, Set<Long> dnTxSet) {
    scmDeleteBlocksCommandStatusManager.recordScmCommand(
        SCMDeleteBlocksCommandStatusManager
            .createScmCmdStatusData(dnId, scmCmdId, dnTxSet));
  }

  public void recordTransactionCommitted(long txId) {
    transactionToDNsCommitMap
        .putIfAbsent(txId, new LinkedHashSet<>());
  }

  public void clear() {
    scmDeleteBlocksCommandStatusManager.clear();
    transactionToDNsCommitMap.clear();
  }

  public void cleanAllTimeoutSCMCommand(long timeoutMs) {
    scmDeleteBlocksCommandStatusManager.cleanAllTimeoutSCMCommand(timeoutMs);
  }

  public void onDatanodeDead(UUID dnId) {
    scmDeleteBlocksCommandStatusManager.onDatanodeDead(dnId);
  }

  public boolean isDuplication(DatanodeDetails dnDetail, long tx,
      Map<UUID, Map<Long, CmdStatus>> commandStatus) {
    if (alreadyExecuted(dnDetail.getUuid(), tx)) {
      return true;
    }
    return inProcessing(dnDetail.getUuid(), tx, commandStatus);
  }

  public boolean alreadyExecuted(UUID dnId, long txId) {
    Set<UUID> dnsWithTransactionCommitted =
        transactionToDNsCommitMap.get(txId);
    return dnsWithTransactionCommitted != null && dnsWithTransactionCommitted
        .contains(dnId);
  }

  /**
   * Commits a transaction means to delete all footprints of a transaction
   * from the log. This method doesn't guarantee all transactions can be
   * successfully deleted, it tolerate failures and tries best efforts to.
   *  @param transactionResults - delete block transaction results.
   * @param dnId - ID of datanode which acknowledges the delete block command.
   */
  @VisibleForTesting
  public void commitTransactions(
      List<DeleteBlockTransactionResult> transactionResults, UUID dnId) {

    ArrayList<Long> txIDsToBeDeleted = new ArrayList<>();
    Set<UUID> dnsWithCommittedTxn;
    for (DeleteBlockTransactionResult transactionResult :
        transactionResults) {
      if (isTransactionFailed(transactionResult)) {
        metrics.incrBlockDeletionTransactionFailure();
        continue;
      }
      try {
        metrics.incrBlockDeletionTransactionSuccess();
        long txID = transactionResult.getTxID();
        // set of dns which have successfully committed transaction txId.
        dnsWithCommittedTxn = transactionToDNsCommitMap.get(txID);
        final ContainerID containerId = ContainerID.valueOf(
            transactionResult.getContainerID());
        if (dnsWithCommittedTxn == null) {
          // Mostly likely it's a retried delete command response.
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Transaction txId={} commit by dnId={} for containerID={}"
                    + " failed. Corresponding entry not found.", txID, dnId,
                containerId);
          }
          continue;
        }

        dnsWithCommittedTxn.add(dnId);
        final ContainerInfo container =
            containerManager.getContainer(containerId);
        final Set<ContainerReplica> replicas =
            containerManager.getContainerReplicas(containerId);
        // The delete entry can be safely removed from the log if all the
        // corresponding nodes commit the txn. It is required to check that
        // the nodes returned in the pipeline match the replication factor.
        if (min(replicas.size(), dnsWithCommittedTxn.size())
            >= container.getReplicationConfig().getRequiredNodes()) {
          List<UUID> containerDns = replicas.stream()
              .map(ContainerReplica::getDatanodeDetails)
              .map(DatanodeDetails::getUuid)
              .collect(Collectors.toList());
          if (dnsWithCommittedTxn.containsAll(containerDns)) {
            transactionToDNsCommitMap.remove(txID);
            transactionToRetryCountMap.remove(txID);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Purging txId={} from block deletion log", txID);
            }
            txIDsToBeDeleted.add(txID);
          }
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Datanode txId={} containerId={} committed by dnId={}",
              txID, containerId, dnId);
        }
      } catch (IOException e) {
        LOG.warn("Could not commit delete block transaction: " +
            transactionResult.getTxID(), e);
      }
    }
    try {
      deletedBlockLogStateManager.removeTransactionsFromDB(txIDsToBeDeleted);
      metrics.incrBlockDeletionTransactionCompleted(txIDsToBeDeleted.size());
    } catch (IOException e) {
      LOG.warn("Could not commit delete block transactions: "
          + txIDsToBeDeleted, e);
    }
  }

  @Override
  public void onMessage(
      DeleteBlockStatus deleteBlockStatus, EventPublisher publisher) {
    if (!scmContext.isLeader()) {
      LOG.warn("Skip commit transactions since current SCM is not leader.");
      return;
    }

    DatanodeDetails details = deleteBlockStatus.getDatanodeDetails();
    UUID dnId = details.getUuid();
    for (CommandStatus commandStatus : deleteBlockStatus.getCmdStatus()) {
      CommandStatus.Status status = commandStatus.getStatus();
      lock.lock();
      try {
        if (status == CommandStatus.Status.EXECUTED) {
          ContainerBlocksDeletionACKProto ackProto =
              commandStatus.getBlockDeletionAck();
          commitTransactions(ackProto.getResultsList(), dnId);
          metrics.incrBlockDeletionCommandSuccess();
        } else if (status == CommandStatus.Status.FAILED) {
          metrics.incrBlockDeletionCommandFailure();
        } else {
          LOG.debug("Delete Block Command {} is not executed on the Datanode" +
                  " {}.", commandStatus.getCmdId(), dnId);
        }

        commitSCMCommandStatus(deleteBlockStatus.getCmdStatus(), dnId);
      } finally {
        lock.unlock();
      }
    }
  }

  @VisibleForTesting
  public void commitSCMCommandStatus(List<CommandStatus> deleteBlockStatus,
      UUID dnId) {
    processSCMCommandStatus(deleteBlockStatus, dnId);
    scmDeleteBlocksCommandStatusManager.
        cleanSCMCommandForDn(dnId, scmCommandTimeoutMs);
  }

  private boolean inProcessing(UUID dnId, long deletedBlocksTxId,
      Map<UUID, Map<Long, CmdStatus>> commandStatus) {
    Map<Long, CmdStatus> deletedBlocksTxStatus = commandStatus.get(dnId);
    if (deletedBlocksTxStatus == null ||
        deletedBlocksTxStatus.get(deletedBlocksTxId) == null) {
      return false;
    }
    return deletedBlocksTxStatus.get(deletedBlocksTxId) !=
        CmdStatus.NEED_RESEND;
  }

  private void processSCMCommandStatus(List<CommandStatus> deleteBlockStatus,
      UUID dnID) {
    Map<Long, CommandStatus> lastStatus = new HashMap<>();
    Map<Long, CommandStatus.Status> summary = new HashMap<>();

    // The CommandStatus is ordered in the report. So we can focus only on the
    // last status in the command report.
    deleteBlockStatus.forEach(cmdStatus -> {
      lastStatus.put(cmdStatus.getCmdId(), cmdStatus);
      summary.put(cmdStatus.getCmdId(), cmdStatus.getStatus());
    });
    LOG.debug("CommandStatus {} from Datanode {} ", summary, dnID);
    for (Map.Entry<Long, CommandStatus> entry : lastStatus.entrySet()) {
      CommandStatus.Status status = entry.getValue().getStatus();
      scmDeleteBlocksCommandStatusManager.updateStatusByDNCommandStatus(
          dnID, entry.getKey(), status);
    }
  }

  private boolean isTransactionFailed(DeleteBlockTransactionResult result) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Got block deletion ACK from datanode, TXIDs={}, " + "success={}",
          result.getTxID(), result.getSuccess());
    }
    if (!result.getSuccess()) {
      LOG.warn("Got failed ACK for TXID={}, prepare to resend the "
          + "TX in next interval", result.getTxID());
      return true;
    }
    return false;
  }
}
