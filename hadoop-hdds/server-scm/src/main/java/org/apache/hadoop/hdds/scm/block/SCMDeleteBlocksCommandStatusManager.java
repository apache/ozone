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
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.hdds.scm.block.SCMDeleteBlocksCommandStatusManager.CmdStatus.EXECUTED;
import static org.apache.hadoop.hdds.scm.block.SCMDeleteBlocksCommandStatusManager.CmdStatus.NEED_RESEND;
import static org.apache.hadoop.hdds.scm.block.SCMDeleteBlocksCommandStatusManager.CmdStatus.PENDING_EXECUTED;
import static org.apache.hadoop.hdds.scm.block.SCMDeleteBlocksCommandStatusManager.CmdStatus.SENT;
import static org.apache.hadoop.hdds.scm.block.SCMDeleteBlocksCommandStatusManager.CmdStatus.TO_BE_SENT;

/**
 * SCM DeleteBlocksCommand manager.
 */
public class SCMDeleteBlocksCommandStatusManager {
  public static final Logger LOG =
      LoggerFactory.getLogger(SCMDeleteBlocksCommandStatusManager.class);

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

  private static final CmdStatus DEFAULT_STATUS = TO_BE_SENT;

  private final Set<CmdStatus> statusesRequiringTimeout = new HashSet<>(
      Arrays.asList(SENT, PENDING_EXECUTED));
  private final Set<CmdStatus> finialStatuses = new HashSet<>(
      Arrays.asList(EXECUTED, NEED_RESEND));
  private final Set<CmdStatus> failedStatuses = new HashSet<>(
      Arrays.asList(NEED_RESEND));

  private final Map<UUID, Map<Long, CmdStatusData>> scmCmdStatusRecord;

  public SCMDeleteBlocksCommandStatusManager() {
    this.scmCmdStatusRecord = new ConcurrentHashMap<>();
  }

  public static CmdStatusData createScmCmdStatusData(
      UUID dnId, long scmCmdId, Set<Long> deletedBlocksTxIds) {
    return new CmdStatusData(dnId, scmCmdId, deletedBlocksTxIds);
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

    public boolean isContainDeletedBlocksTx(long deletedBlocksTxId) {
      return deletedBlocksTxIds.contains(deletedBlocksTxId);
    }

    public Set<Long> getDeletedBlocksTxIds() {
      return deletedBlocksTxIds;
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

  public void recordScmCommand(CmdStatusData statusData) {
    LOG.debug("Record ScmCommand: {}", statusData);
    scmCmdStatusRecord.computeIfAbsent(statusData.getDnId(), k ->
        new ConcurrentHashMap<>()).put(statusData.getScmCmdId(), statusData);
  }

  public Map<UUID, Map<Long, CmdStatus>> getCommandStatusByTxId(
      Set<UUID> dnIds) {
    Map<UUID, Map<Long, CmdStatus>> result =
        new HashMap<>(scmCmdStatusRecord.size());

    for (UUID dnId : dnIds) {
      if (scmCmdStatusRecord.get(dnId) == null) {
        continue;
      }
      Map<Long, CmdStatus> dnStatusMap = new HashMap<>();
      Map<Long, CmdStatusData> record = scmCmdStatusRecord.get(dnId);
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

  public void onSent(UUID dnId, long scmCmdId) {
    updateStatus(dnId, scmCmdId, SENT);
  }

  public void updateStatusByDNCommandStatus(UUID dnId, long scmCmdId,
      CommandStatus.Status newState) {
    CmdStatus status = fromProtoCommandStatus(newState);
    if (status != null) {
      updateStatus(dnId, scmCmdId, status);
    }
  }

  public void cleanAllTimeoutSCMCommand(long timeoutMs) {
    for (UUID dnId : scmCmdStatusRecord.keySet()) {
      for (CmdStatus status : statusesRequiringTimeout) {
        removeTimeoutScmCommand(
            dnId, getScmCommandIds(dnId, status), timeoutMs);
      }
    }
  }

  public void cleanSCMCommandForDn(UUID dnId, long timeoutMs) {
    cleanTimeoutSCMCommand(dnId, timeoutMs);
    cleanFinalStatusSCMCommand(dnId);
  }

  private void cleanTimeoutSCMCommand(UUID dnId, long timeoutMs) {
    for (CmdStatus status : statusesRequiringTimeout) {
      removeTimeoutScmCommand(
          dnId, getScmCommandIds(dnId, status), timeoutMs);
    }
  }

  public void clear() {
    scmCmdStatusRecord.clear();
  }

  private void cleanFinalStatusSCMCommand(UUID dnId) {
    for (CmdStatus status : finialStatuses) {
      for (Long scmCmdId : getScmCommandIds(dnId, status)) {
        CmdStatusData stateData = removeScmCommand(dnId, scmCmdId);
        LOG.debug("Clean SCMCommand status: {} for DN: {}, stateData: {}",
            status, dnId, stateData);
      }
    }
  }

  private Set<Long> getScmCommandIds(UUID dnId, CmdStatus status) {
    Set<Long> scmCmdIds = new HashSet<>();
    if (scmCmdStatusRecord.get(dnId) == null) {
      return scmCmdIds;
    }
    for (CmdStatusData statusData : scmCmdStatusRecord.get(dnId).values()) {
      if (statusData.getStatus().equals(status)) {
        scmCmdIds.add(statusData.getScmCmdId());
      }
    }
    return scmCmdIds;
  }

  private Instant getUpdateTime(UUID dnId, long scmCmdId) {
    if (scmCmdStatusRecord.get(dnId) == null ||
        scmCmdStatusRecord.get(dnId).get(scmCmdId) == null) {
      return null;
    }
    return scmCmdStatusRecord.get(dnId).get(scmCmdId).getUpdateTime();
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
        // SENT -> NEED_RESEND: The DeleteBlocksCommand is sent and lost before
        // it is received by the DN.

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

        // SENT -> EXECUTED: The DeleteBlocksCommand has been sent to Datanode,
        // executed by DN, and executed successfully.
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
      LOG.debug("Successful update DN: {} ScmCommandId {} Status From {} to {}",
          dnId, scmCmdId, oldStatus, newStatus);
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
        LOG.warn("FFFFF Timeout SCM scmCmdIds {} for DN {} " +
            "after without update {}ms}", scmCmdIds, dnId, timeoutMs);
      }
    }
  }

  private CmdStatusData removeScmCommand(UUID dnId, long scmCmdId) {
    if (scmCmdStatusRecord.get(dnId) == null ||
        scmCmdStatusRecord.get(dnId).get(scmCmdId) == null) {
      return null;
    }

    CmdStatus status = scmCmdStatusRecord.get(dnId).get(scmCmdId).getStatus();
    if (!finialStatuses.contains(status)) {
      LOG.error("Cannot Remove ScmCommand {} Non-final Status {} for DN: {}." +
          " final Status {}", scmCmdId, status, dnId, finialStatuses);
      return null;
    }

    CmdStatusData statusData = scmCmdStatusRecord.get(dnId).remove(scmCmdId);
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

  @VisibleForTesting
  Map<UUID, Map<Long, CmdStatusData>> getScmCmdStatusRecord() {
    return scmCmdStatusRecord;
  }
}
