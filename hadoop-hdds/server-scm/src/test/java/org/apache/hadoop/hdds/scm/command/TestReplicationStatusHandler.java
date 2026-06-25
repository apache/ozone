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

package org.apache.hadoop.hdds.scm.command;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.apache.hadoop.hdds.HddsIdFactory;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.hdds.scm.command.CommandStatusReportHandler.ReplicationStatus;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for ReplicationStatusHandler.
 */
public class TestReplicationStatusHandler {

  @Test
  public void leaderClearsReplicaPendingOp() {
    SCMContext scmContext = mock(SCMContext.class);
    when(scmContext.isLeader()).thenReturn(true);
    ContainerReplicaPendingOps pendingOps = mock(ContainerReplicaPendingOps.class);
    EventPublisher publisher = mock(EventPublisher.class);

    long cmdId = HddsIdFactory.getLongId();
    CommandStatus cmdStatus = CommandStatus.newBuilder()
        .setCmdId(cmdId)
        .setStatus(CommandStatus.Status.FAILED)
        .setType(Type.replicateContainerCommand)
        .build();
    ReplicationStatus status = new ReplicationStatus(
        Collections.singletonList(cmdStatus),
        MockDatanodeDetails.randomDatanodeDetails());

    ReplicationStatusHandler handler = new ReplicationStatusHandler(pendingOps, scmContext);
    handler.onMessage(status, publisher);

    verify(pendingOps).onReplicationCommandFailed(cmdId);
  }

  @Test
  public void followerSkipsClearingReplicaPendingOp() {
    SCMContext scmContext = mock(SCMContext.class);
    when(scmContext.isLeader()).thenReturn(false);
    ContainerReplicaPendingOps pendingOps = mock(ContainerReplicaPendingOps.class);
    EventPublisher publisher = mock(EventPublisher.class);

    long cmdId = HddsIdFactory.getLongId();
    CommandStatus cmdStatus = CommandStatus.newBuilder()
        .setCmdId(cmdId)
        .setStatus(CommandStatus.Status.FAILED)
        .setType(Type.replicateContainerCommand)
        .build();
    ReplicationStatus status = new ReplicationStatus(
        Collections.singletonList(cmdStatus),
        MockDatanodeDetails.randomDatanodeDetails());

    ReplicationStatusHandler handler = new ReplicationStatusHandler(pendingOps, scmContext);
    handler.onMessage(status, publisher);

    verifyNoInteractions(pendingOps);
  }

}
