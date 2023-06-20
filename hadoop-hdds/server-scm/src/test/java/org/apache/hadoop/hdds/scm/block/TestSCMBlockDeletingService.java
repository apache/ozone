package org.apache.hadoop.hdds.scm.block;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_PENDING_COMMAND_LIMIT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_PENDING_COMMAND_LIMIT_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    metrics = ScmBlockDeletingServiceMetrics.create();
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
    ddbt.addTransactionToDN(datanode1.getUuid(), tx1);
    ddbt.addTransactionToDN(datanode2.getUuid(), tx1);
    ddbt.addTransactionToDN(datanode3.getUuid(), tx1);
    DeletedBlockLog mockDeletedBlockLog = mock(DeletedBlockLog.class);
    when(mockDeletedBlockLog.getTransactions(
        anyInt(), anySet())).thenReturn(ddbt);

    service = spy(new SCMBlockDeletingService(
        mockDeletedBlockLog, nodeManager, eventPublisher, scmContext,
        scmServiceManager, conf, metrics, Clock.system(
        ZoneOffset.UTC)));
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
    List<UUID> actualDnIds = actualCommands.stream()
        .map(CommandForDatanode::getDatanodeId)
        .collect(Collectors.toList());
    Set<UUID> expectedDnIdsSet = datanodeDetails.stream()
        .map(DatanodeDetails::getUuid).collect(Collectors.toSet());

    assertEquals(expectedDnIdsSet, new HashSet<>(actualDnIds));
    assertEquals(datanodeDetails.size(),
        metrics.getNumBlockDeletionCommandSent());
    // Echo Command has one Transaction
    assertEquals(datanodeDetails.size() * 1,
        metrics.getNumBlockDeletionTransactionSent());
  }

  private void callDeletedBlockTransactionScanner() throws Exception {
    service.getTasks().poll().call();
  }

  @Test
  public void testLimitCommandSending() throws Exception {
    int pendingCommandLimit = conf.getInt(
        OZONE_BLOCK_DELETING_PENDING_COMMAND_LIMIT,
        OZONE_BLOCK_DELETING_PENDING_COMMAND_LIMIT_DEFAULT);


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
