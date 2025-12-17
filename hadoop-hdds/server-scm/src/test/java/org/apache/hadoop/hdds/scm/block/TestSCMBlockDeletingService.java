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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Test SCMBlockDeletingService.
 */
public class TestSCMBlockDeletingService {
  private SCMBlockDeletingService service;
  private EventPublisher eventPublisher;
  private List<DatanodeDetails> datanodeDetails;
  private OzoneConfiguration conf;
  private NodeManager nodeManager;
  private ScmBlockDeletingServiceMetrics metrics;

  @BeforeEach
  public void setup() throws Exception {
    nodeManager = mock(NodeManager.class);
    eventPublisher = mock(EventPublisher.class);
    conf = new OzoneConfiguration();
    metrics = ScmBlockDeletingServiceMetrics.create(mock(BlockManager.class));
    when(nodeManager.getTotalDatanodeCommandCount(any(),
        any())).thenReturn(0);
    SCMServiceManager scmServiceManager = mock(SCMServiceManager.class);
    SCMContext scmContext = mock(SCMContext.class);

    DatanodeDeletedBlockTransactions ddbt =
        new DatanodeDeletedBlockTransactions();
    DatanodeDetails datanode1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails datanode2 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails datanode3 = MockDatanodeDetails.randomDatanodeDetails();
    datanodeDetails = Arrays.asList(datanode1, datanode2, datanode3);
    when(nodeManager.getNodes(NodeStatus.inServiceHealthy())).thenReturn(
        datanodeDetails);
    DeletedBlocksTransaction tx1 = createTestDeleteTxn(1, Arrays.asList(1L), 1);
    ddbt.addTransactionToDN(datanode1.getID(), tx1);
    ddbt.addTransactionToDN(datanode2.getID(), tx1);
    ddbt.addTransactionToDN(datanode3.getID(), tx1);
    DeletedBlockLog mockDeletedBlockLog = mock(DeletedBlockLog.class);
    when(mockDeletedBlockLog.getTransactions(
        anyInt(), anySet())).thenReturn(ddbt);

    service = spy(new SCMBlockDeletingService(
        mockDeletedBlockLog, nodeManager, eventPublisher, scmContext,
        scmServiceManager, conf, conf.getObject(ScmConfig.class), metrics, Clock.system(
        ZoneOffset.UTC), mock(ReconfigurationHandler.class)));
    when(service.shouldRun()).thenReturn(true);
  }

  @AfterEach
  public void stop() {
    service.stop();
    ScmBlockDeletingServiceMetrics.unRegister();
  }

  @Test
  public void testCall() throws Exception {
    callDeletedBlockTransactionScanner();

    ArgumentCaptor<CommandForDatanode> argumentCaptor =
        ArgumentCaptor.forClass(CommandForDatanode.class);

    // Three Datanode is healthy and in-service, and the task queue is empty,
    // so the transaction will send to all three Datanode
    verify(eventPublisher, times(3)).fireEvent(
        eq(SCMEvents.DATANODE_COMMAND), argumentCaptor.capture());
    List<CommandForDatanode> actualCommands = argumentCaptor.getAllValues();
    final Set<DatanodeID> actualDnIds = actualCommands.stream()
        .map(CommandForDatanode::getDatanodeId)
        .collect(Collectors.toSet());
    final Set<DatanodeID> expectedDnIdsSet = datanodeDetails.stream()
        .map(DatanodeDetails::getID).collect(Collectors.toSet());

    assertEquals(expectedDnIdsSet, actualDnIds);
    assertEquals(datanodeDetails.size(),
        metrics.getNumBlockDeletionCommandSent());
    // Echo Command has one Transaction
    assertEquals(datanodeDetails.size() * 1,
        metrics.getNumBlockDeletionTransactionsOnDatanodes());
  }

  private void callDeletedBlockTransactionScanner() throws Exception {
    service.getTasks().poll().call();
  }

  @Test
  public void testLimitCommandSending() throws Exception {
    DatanodeConfiguration dnConf =
        conf.getObject(DatanodeConfiguration.class);
    int pendingCommandLimit = dnConf.getBlockDeleteQueueLimit();

    // The number of commands pending on all Datanodes has reached the limit.
    when(nodeManager.getTotalDatanodeCommandCount(any(),
        any())).thenReturn(pendingCommandLimit);
    assertEquals(0,
        service.getDatanodesWithinCommandLimit(datanodeDetails).size());

    // The number of commands pending on all Datanodes is 0
    when(nodeManager.getTotalDatanodeCommandCount(any(),
        any())).thenReturn(0);
    assertEquals(datanodeDetails.size(),
        service.getDatanodesWithinCommandLimit(datanodeDetails).size());

    // The number of commands pending on first Datanodes has reached the limit.
    DatanodeDetails fullDatanode = datanodeDetails.get(0);
    when(nodeManager.getTotalDatanodeCommandCount(fullDatanode,
        Type.deleteBlocksCommand)).thenReturn(pendingCommandLimit);
    Set<DatanodeDetails> includeNodes =
        service.getDatanodesWithinCommandLimit(datanodeDetails);
    assertEquals(datanodeDetails.size() - 1,
        includeNodes.size());
    assertFalse(includeNodes.contains(fullDatanode));
  }

  private DeletedBlocksTransaction createTestDeleteTxn(
      long txnID, List<Long> blocks, long containerID) {
    return DeletedBlocksTransaction.newBuilder().setTxID(txnID)
        .setContainerID(containerID).addAllLocalID(blocks).setCount(0).build();
  }
}
