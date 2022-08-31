package org.apache.hadoop.hdds.scm.container.replication.health;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.OPEN;
import static org.mockito.Mockito.times;

public class TestOpenContainerHandler {

  private ReplicationManager replicationManager;
  private OpenContainerHandler openContainerHandler;
  private ECReplicationConfig replicationConfig;

  @BeforeEach
  public void setup() {
    replicationConfig = new ECReplicationConfig(3, 2);
    replicationManager = Mockito.mock(ReplicationManager.class);
    openContainerHandler = new OpenContainerHandler(replicationManager);
  }

  @Test
  public void testClosedContainerReturnsTrue() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        replicationConfig, 1, CLOSED);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil.createReplicas(
        containerInfo.containerID(), ContainerReplicaProto.State.CLOSED,
        1, 2, 3, 4, 5);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .pendingOps(Collections.EMPTY_LIST)
        .report(new ReplicationManagerReport())
        .containerInfo(containerInfo)
        .containerReplicas(containerReplicas)
        .build();
    Assertions.assertFalse(openContainerHandler.handle(request));
    Mockito.verify(replicationManager, times(0))
        .sendCloseContainerEvent(Mockito.any());
  }

  @Test
  public void testOpenContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        replicationConfig, 1, OPEN);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil.createReplicas(
        containerInfo.containerID(), ContainerReplicaProto.State.OPEN,
        1, 2, 3, 4, 5);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .pendingOps(Collections.EMPTY_LIST)
        .report(new ReplicationManagerReport())
        .containerInfo(containerInfo)
        .containerReplicas(containerReplicas)
        .build();
    Assertions.assertTrue(openContainerHandler.handle(request));
    Mockito.verify(replicationManager, times(0))
        .sendCloseContainerEvent(Mockito.any());
  }

  @Test
  public void testOpenUnhealthyContainerIsClosed() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        replicationConfig, 1, OPEN);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil.createReplicas(
        containerInfo.containerID(), ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .pendingOps(Collections.EMPTY_LIST)
        .report(new ReplicationManagerReport())
        .containerInfo(containerInfo)
        .containerReplicas(containerReplicas)
        .build();
    Assertions.assertTrue(openContainerHandler.handle(request));
    Mockito.verify(replicationManager, times(1))
        .sendCloseContainerEvent(containerInfo.containerID());
  }

}
