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

package org.apache.hadoop.hdds.scm.block;

import static org.apache.hadoop.hdds.scm.block.SCMDeletedBlockTransactionStatusManager.SCMDeleteBlocksCommandStatusManager;
import static org.apache.hadoop.hdds.scm.block.SCMDeletedBlockTransactionStatusManager.SCMDeleteBlocksCommandStatusManager.CmdStatus.SENT;
import static org.apache.hadoop.hdds.scm.block.SCMDeletedBlockTransactionStatusManager.SCMDeleteBlocksCommandStatusManager.CmdStatus.TO_BE_SENT;
import static org.apache.hadoop.hdds.scm.block.SCMDeletedBlockTransactionStatusManager.SCMDeleteBlocksCommandStatusManager.CmdStatusData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A test for SCMDeleteBlocksCommandStatusManager.
 */
public class TestSCMDeleteBlocksCommandStatusManager {

  private SCMDeleteBlocksCommandStatusManager manager;
  private DatanodeID dnId1;
  private DatanodeID dnId2;
  private long scmCmdId1;
  private long scmCmdId2;
  private long scmCmdId3;
  private long scmCmdId4;
  private Set<Long> deletedBlocksTxIds1;
  private Set<Long> deletedBlocksTxIds2;
  private Set<Long> deletedBlocksTxIds3;
  private Set<Long> deletedBlocksTxIds4;

  @BeforeEach
  public void setup() throws Exception {
    ScmBlockDeletingServiceMetrics metrics = mock(ScmBlockDeletingServiceMetrics.class);
    manager = new SCMDeleteBlocksCommandStatusManager(metrics);
    // Create test data
    dnId1 = DatanodeID.randomID();
    dnId2 = DatanodeID.randomID();
    scmCmdId1 = 1L;
    scmCmdId2 = 2L;
    scmCmdId3 = 3L;
    scmCmdId4 = 4L;
    deletedBlocksTxIds1 = new HashSet<>();
    deletedBlocksTxIds1.add(100L);
    deletedBlocksTxIds2 = new HashSet<>();
    deletedBlocksTxIds2.add(200L);
    deletedBlocksTxIds3 = new HashSet<>();
    deletedBlocksTxIds3.add(300L);
    deletedBlocksTxIds4 = new HashSet<>();
    deletedBlocksTxIds4.add(400L);
  }

  @Test
  public void testRecordScmCommand() {
    CmdStatusData statusData =
        SCMDeleteBlocksCommandStatusManager.createScmCmdStatusData(
            dnId1, scmCmdId1, deletedBlocksTxIds1);

    manager.recordScmCommand(statusData);

    assertNotNull(manager.getScmCmdStatusRecord().get(dnId1));
    assertEquals(1, manager.getScmCmdStatusRecord().get(dnId1).size());
    CmdStatusData cmdStatusData =
        manager.getScmCmdStatusRecord().get(dnId1).get(scmCmdId1);
    assertNotNull(cmdStatusData);
    assertEquals(dnId1, statusData.getDnId());
    assertEquals(scmCmdId1, statusData.getScmCmdId());
    assertEquals(deletedBlocksTxIds1, statusData.getDeletedBlocksTxIds());
    // The default status is `CmdStatus.TO_BE_SENT`
    assertEquals(TO_BE_SENT, statusData.getStatus());
  }

  @Test
  public void testOnSent() {
    CmdStatusData statusData =
        SCMDeleteBlocksCommandStatusManager.createScmCmdStatusData(
            dnId1, scmCmdId1, deletedBlocksTxIds1);
    manager.recordScmCommand(statusData);

    Map<Long, CmdStatusData> dnStatusRecord =
        manager.getScmCmdStatusRecord().get(dnId1);
    // After the Command is sent by SCM, the status of the Command
    // will change from TO_BE_SENT to SENT
    assertEquals(TO_BE_SENT, dnStatusRecord.get(scmCmdId1).getStatus());
    manager.onSent(dnId1, scmCmdId1);
    assertEquals(SENT, dnStatusRecord.get(scmCmdId1).getStatus());
  }

  @Test
  public void testUpdateStatusByDNCommandStatus() {
    // Test all Status update by Datanode Heartbeat report.
    //  SENT -> PENDING_EXECUTED: The DeleteBlocksCommand is sent and received
    //  by the Datanode, but the command is not executed by the Datanode,
    //  the command is waiting to be executed.

    //  SENT -> NEED_RESEND: The DeleteBlocksCommand is sent and lost before
    //  it is received by the DN.
    //  SENT -> EXECUTED: The DeleteBlocksCommand has been sent to Datanode,
    //  executed by DN, and executed successfully.
    //
    //  PENDING_EXECUTED -> PENDING_EXECUTED: The DeleteBlocksCommand continues
    //  to wait to be executed by Datanode.
    //  PENDING_EXECUTED -> NEED_RESEND: The DeleteBlocksCommand waited for a
    //  while and was executed, but the execution failed; Or the
    //  DeleteBlocksCommand was lost while waiting(such as the Datanode restart)
    //
    //  PENDING_EXECUTED -> EXECUTED: The Command waits for a period of
    //  time on the DN and is executed successfully.

    recordAndSentCommand(manager, dnId1,
        Arrays.asList(scmCmdId1, scmCmdId2, scmCmdId3, scmCmdId4),
        Arrays.asList(deletedBlocksTxIds1, deletedBlocksTxIds2,
            deletedBlocksTxIds3, deletedBlocksTxIds4));

    Map<Long, CmdStatusData> dnStatusRecord =
        manager.getScmCmdStatusRecord().get(dnId1);
    assertEquals(SENT, dnStatusRecord.get(scmCmdId1).getStatus());
    assertEquals(SENT, dnStatusRecord.get(scmCmdId2).getStatus());
    assertEquals(SENT, dnStatusRecord.get(scmCmdId3).getStatus());
    assertEquals(SENT, dnStatusRecord.get(scmCmdId4).getStatus());

    // SENT -> PENDING_EXECUTED
    manager.updateStatusByDNCommandStatus(dnId1, scmCmdId1,
        StorageContainerDatanodeProtocolProtos.CommandStatus.Status.PENDING);
    // SENT -> EXECUTED
    manager.updateStatusByDNCommandStatus(dnId1, scmCmdId2,
        StorageContainerDatanodeProtocolProtos.CommandStatus.Status.EXECUTED);
    // SENT -> NEED_RESEND
    manager.updateStatusByDNCommandStatus(dnId1, scmCmdId3,
        StorageContainerDatanodeProtocolProtos.CommandStatus.Status.FAILED);
    // SENT -> PENDING_EXECUTED
    manager.updateStatusByDNCommandStatus(dnId1, scmCmdId4,
        StorageContainerDatanodeProtocolProtos.CommandStatus.Status.PENDING);

    assertEquals(SENT, dnStatusRecord.get(scmCmdId1).getStatus());
    assertNull(dnStatusRecord.get(scmCmdId2));
    assertNull(dnStatusRecord.get(scmCmdId3));
    assertEquals(SENT, dnStatusRecord.get(scmCmdId4).getStatus());
  }

  @Test
  public void testCleanSCMCommandForDn() {
    // Transactions in states EXECUTED and NEED_RESEND will be cleaned up
    // directly, while transactions in states PENDING_EXECUTED and SENT
    // will be cleaned up after timeout
    recordAndSentCommand(manager, dnId1,
        Arrays.asList(scmCmdId1, scmCmdId2, scmCmdId3, scmCmdId4),
        Arrays.asList(deletedBlocksTxIds1, deletedBlocksTxIds2,
            deletedBlocksTxIds3, deletedBlocksTxIds4));

    // SENT -> PENDING_EXECUTED
    manager.updateStatusByDNCommandStatus(dnId1, scmCmdId1,
        StorageContainerDatanodeProtocolProtos.CommandStatus.Status.PENDING);
    // SENT -> EXECUTED
    manager.updateStatusByDNCommandStatus(dnId1, scmCmdId2,
        StorageContainerDatanodeProtocolProtos.CommandStatus.Status.EXECUTED);
    // SENT -> NEED_RESEND
    manager.updateStatusByDNCommandStatus(dnId1, scmCmdId3,
        StorageContainerDatanodeProtocolProtos.CommandStatus.Status.FAILED);

    Map<Long, CmdStatusData> dnStatusRecord =
        manager.getScmCmdStatusRecord().get(dnId1);
    assertNotNull(dnStatusRecord.get(scmCmdId1));
    assertNull(dnStatusRecord.get(scmCmdId2));
    assertNull(dnStatusRecord.get(scmCmdId3));
    assertNotNull(dnStatusRecord.get(scmCmdId4));

    manager.cleanTimeoutSCMCommand(dnId1, Long.MAX_VALUE);

    // scmCmdId1 is PENDING_EXECUTED will be cleaned up after timeout
    assertNotNull(dnStatusRecord.get(scmCmdId1));
    assertNull(dnStatusRecord.get(scmCmdId3));
    assertNull(dnStatusRecord.get(scmCmdId2));
    // scmCmdId4 is SENT will be cleaned up after timeout
    assertNotNull(dnStatusRecord.get(scmCmdId4));

    manager.cleanTimeoutSCMCommand(dnId1, -1);
    assertNull(dnStatusRecord.get(scmCmdId1));
    assertNull(dnStatusRecord.get(scmCmdId4));
  }

  @Test
  public void testCleanAllTimeoutSCMCommand() {
    // Test All EXECUTED and NEED_RESEND status in the DN will be cleaned up

    // Transactions in states EXECUTED and NEED_RESEND will be cleaned up
    // directly, while transactions in states PENDING_EXECUTED and SENT
    // will be cleaned up after timeout
    recordAndSentCommand(manager, dnId1, Collections.singletonList(scmCmdId1),
        Collections.singletonList(deletedBlocksTxIds1));
    recordAndSentCommand(manager, dnId2, Collections.singletonList(scmCmdId2),
        Collections.singletonList(deletedBlocksTxIds2));

    Map<Long, CmdStatusData> dn1StatusRecord =
        manager.getScmCmdStatusRecord().get(dnId1);
    Map<Long, CmdStatusData> dn2StatusRecord =
        manager.getScmCmdStatusRecord().get(dnId2);

    // Only let the scmCmdId1 have a Heartbeat report, its status will be
    // updated, the scmCmdId2 still in SENT status.
    // SENT -> PENDING_EXECUTED
    manager.updateStatusByDNCommandStatus(dnId1, scmCmdId1,
        StorageContainerDatanodeProtocolProtos.CommandStatus.Status.PENDING);

    manager.cleanAllTimeoutSCMCommand(Long.MAX_VALUE);
    // scmCmdId1 is PENDING_EXECUTED will be cleaned up after timeout
    assertNotNull(dn1StatusRecord.get(scmCmdId1));
    assertNotNull(dn2StatusRecord.get(scmCmdId2));

    // scmCmdId2 is SENT will be cleaned up after timeout
    manager.cleanAllTimeoutSCMCommand(-1);
    assertNull(dn1StatusRecord.get(scmCmdId1));
    assertNull(dn2StatusRecord.get(scmCmdId2));

  }

  private void recordAndSentCommand(
      SCMDeleteBlocksCommandStatusManager statusManager,
      DatanodeID dnId, List<Long> scmCmdIds, List<Set<Long>> txIds) {
    assertEquals(scmCmdIds.size(), txIds.size());
    for (int i = 0; i < scmCmdIds.size(); i++) {
      long scmCmdId = scmCmdIds.get(i);
      Set<Long> deletedBlocksTxIds = txIds.get(i);
      CmdStatusData statusData =
          SCMDeleteBlocksCommandStatusManager.createScmCmdStatusData(
              dnId, scmCmdId, deletedBlocksTxIds);
      statusManager.recordScmCommand(statusData);
      statusManager.onSent(dnId, scmCmdId);
    }
  }

}
