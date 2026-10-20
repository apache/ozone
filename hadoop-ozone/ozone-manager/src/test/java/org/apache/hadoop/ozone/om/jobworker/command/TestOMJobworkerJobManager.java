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

package org.apache.hadoop.ozone.om.jobworker.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.MockJobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.jobworker.commands.OMJobworkerCommand;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.JobworkerJobRecord;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Coordinator admission, replay, acknowledgement and scan bounds without a mock distributed-state guarantee. */
class TestOMJobworkerJobManager {
  private final Map<String, JobworkerJobRecord> stored = new TreeMap<>();
  private final List<Map.Entry<CacheKey<String>, CacheValue<JobworkerJobRecord>>> cached = new ArrayList<>();
  private OzoneManager om;
  private Table<String, JobworkerJobRecord> table;
  private OMLayoutVersionManager versions;
  private OMJobworkerJobManager jobs;
  private JobworkerDetails worker;

  @BeforeEach
  void setUp() throws Exception {
    om = mock(OzoneManager.class);
    versions = mock(OMLayoutVersionManager.class);
    OMMetadataManager metadata = mock(OMMetadataManager.class);
    table = mock(Table.class);
    when(om.isLeaderReady()).thenReturn(true);
    when(om.getOMServiceId()).thenReturn("service");
    when(om.getVersionManager()).thenReturn(versions);
    when(versions.isAllowed(OMLayoutFeature.JOBWORKER_HA)).thenReturn(true);
    when(om.getMetadataManager()).thenReturn(metadata);
    when(metadata.getJobworkerJobTable()).thenReturn(table);
    when(table.get(any())).thenAnswer(invocation -> stored.get(invocation.getArgument(0)));
    when(table.cacheIterator()).thenAnswer(invocation -> cached.iterator());
    when(table.iterator()).thenAnswer(invocation -> {
      Iterator<Map.Entry<String, JobworkerJobRecord>> entries = new TreeMap<>(stored).entrySet().iterator();
      Table.KeyValueIterator<String, JobworkerJobRecord> iterator = mock(Table.KeyValueIterator.class);
      when(iterator.hasNext()).thenAnswer(call -> entries.hasNext());
      when(iterator.next()).thenAnswer(call -> {
        Map.Entry<String, JobworkerJobRecord> entry = entries.next();
        Table.KeyValue<String, JobworkerJobRecord> value = mock(Table.KeyValue.class);
        when(value.getKey()).thenReturn(entry.getKey());
        when(value.getValue()).thenReturn(entry.getValue());
        return value;
      });
      return iterator;
    });
    worker = MockJobworkerDetails.randomJobworkerDetails();
    jobs = new OMJobworkerJobManager(om);
  }

  private JobworkerJobRecord record(long id, CommandStatus.Status status) {
    OMJobworkerCommandProto command = OMJobworkerCommandProto.newBuilder().setCmdId(id)
        .setCommandType(OMJobworkerCommandProto.Type.mockCommand).setAttemptId(UUID.randomUUID().toString()).build();
    CommandStatus report = CommandStatus.newBuilder().setCmdId(id).setType(command.getCommandType())
        .setAttemptId(command.getAttemptId()).setOmServiceId("service").setStatus(status).build();
    JobworkerJobRecord record = JobworkerJobRecord.newBuilder()
        .setJobKey(JobworkerJobState.key(command.getCommandType(), id)).setWorkerId(worker.getUuidString())
        .setCommand(command.toByteString()).setStatus(report.toByteString()).setRevision(id).build();
    stored.put(record.getJobKey(), record);
    return record;
  }

  private List<CommandStatusReportsProto> report(CommandStatus status) {
    return Collections.singletonList(CommandStatusReportsProto.newBuilder().addCmdStatus(status).build());
  }

  @Test
  void testFollowerAndUnfinalizedLeaderCannotSubmitOrAcknowledge() throws Exception {
    JobworkerJobRecord record = record(42, CommandStatus.Status.SUCCEEDED);
    OMJobworkerCommand<?> command = OMJobworkerCommand.fromEnvelope(
        OMJobworkerCommandProto.parseFrom(record.getCommand()));
    when(om.isLeaderReady()).thenReturn(false);
    assertThrows(IOException.class, () -> jobs.submit(worker.getUuid(), command));
    assertThrows(IOException.class, () -> jobs.reconcile(worker,
        report(CommandStatus.parseFrom(record.getStatus()))));
    when(om.isLeaderReady()).thenReturn(true);
    when(versions.isAllowed(OMLayoutFeature.JOBWORKER_HA)).thenReturn(false);
    assertThrows(IOException.class, () -> jobs.submit(worker.getUuid(), command));
    verify(om, never()).getOmRatisServer();
  }

  @Test
  void testJvmLocalIdsCannotBeUsedForDurableWork() {
    OMJobworkerCommand<OMJobworkerCommandProto> implicit = new OMJobworkerCommand<OMJobworkerCommandProto>() {
      @Override
      public OMJobworkerCommandProto.Type getType() {
        return OMJobworkerCommandProto.Type.mockCommand;
      }

      @Override
      public OMJobworkerCommandProto getProto() {
        return OMJobworkerCommandProto.newBuilder().setCommandType(getType()).build();
      }
    };
    assertThrows(IOException.class, () -> jobs.submit(worker.getUuid(), implicit));
    verify(om, never()).getOmRatisServer();
  }

  @Test
  void testBoundedDeliveryDoesNotStarveLaterAssignments() throws Exception {
    for (long id = 1; id <= 101; id++) {
      record(id, CommandStatus.Status.EXECUTING);
    }
    List<OMJobworkerCommandProto> first = jobs.commandsFor(worker.getUuid());
    List<OMJobworkerCommandProto> second = jobs.commandsFor(worker.getUuid());
    assertEquals(100, first.size());
    assertEquals(1, second.size());
    assertTrue(first.stream().allMatch(OMJobworkerCommandProto::getReconcileOnly));
    assertFalse(first.stream().anyMatch(command -> command.getCmdId() == second.get(0).getCmdId()));
    assertEquals(101, jobs.count(OMJobworkerCommandProto.Type.mockCommand));
  }

  @Test
  void testCommittedCacheOverridesOlderDatabaseSnapshot() throws Exception {
    JobworkerJobRecord old = record(42, CommandStatus.Status.PENDING);
    CommandStatus completed = CommandStatus.parseFrom(old.getStatus()).toBuilder()
        .setStatus(CommandStatus.Status.SUCCEEDED).build();
    JobworkerJobRecord newer = old.toBuilder().setStatus(completed.toByteString()).setRevision(100).build();
    cached.add(new java.util.AbstractMap.SimpleImmutableEntry<>(new CacheKey<>(old.getJobKey()),
        CacheValue.get(100, newer)));
    assertEquals(0, jobs.count(OMJobworkerCommandProto.Type.mockCommand));
    assertTrue(jobs.commandsFor(worker.getUuid()).isEmpty());
  }

  @Test
  void testWrongWorkerAttemptOrServiceCannotResolveAJob() throws Exception {
    JobworkerJobRecord assigned = record(42, CommandStatus.Status.EXECUTING);
    CommandStatus success = CommandStatus.parseFrom(assigned.getStatus()).toBuilder()
        .setStatus(CommandStatus.Status.SUCCEEDED).build();
    assertTrue(jobs.reconcile(MockJobworkerDetails.randomJobworkerDetails(), report(success)).isEmpty());
    assertTrue(jobs.reconcile(worker, report(success.toBuilder().setOmServiceId("other").build())).isEmpty());
    assertTrue(jobs.reconcile(worker, report(success.toBuilder()
        .setAttemptId(UUID.randomUUID().toString()).build())).isEmpty());
    verify(om, never()).getOmRatisServer();
  }

  @Test
  void testListenerFailureRetainsResultAndDuplicateAcknowledgementDoesNotRecommit() throws Exception {
    JobworkerJobRecord terminal = record(42, CommandStatus.Status.SUCCEEDED);
    CommandStatus status = CommandStatus.parseFrom(terminal.getStatus());
    OMJobworkerCommandManager commands = mock(OMJobworkerCommandManager.class);
    when(om.getOMJobworkerCommandManager()).thenReturn(commands);
    doThrow(new IOException("result consumer unavailable")).doNothing()
        .when(commands).notifyCommitted(any(), any(), any());
    assertThrows(IOException.class, () -> jobs.reconcile(worker, report(status)));
    assertEquals(Collections.singletonList(status), jobs.reconcile(worker, report(status)));
    assertEquals(terminal, stored.get(terminal.getJobKey()));
    verify(om, never()).getOmRatisServer();
  }

  @Test
  void testMissingPeerEndpointBlocksTheFirstNewRaftRequest() throws Exception {
    OzoneManagerRatisServer ratis = mock(OzoneManagerRatisServer.class);
    RaftServer.Division division = mock(RaftServer.Division.class);
    RaftConfiguration peers = mock(RaftConfiguration.class);
    when(om.getOMNodeId()).thenReturn("om1");
    when(om.getConfiguration()).thenReturn(new OzoneConfiguration());
    when(om.getOmRatisServer()).thenReturn(ratis);
    when(ratis.getServerDivision()).thenReturn(division);
    when(division.getRaftConf()).thenReturn(peers);
    when(peers.getAllPeers()).thenReturn(Arrays.asList(
        RaftPeer.newBuilder().setId("om1").build(), RaftPeer.newBuilder().setId("om2").build()));
    when(peers.getAllPeers(RaftPeerRole.LISTENER)).thenReturn(Collections.emptyList());
    OMJobworkerCommandProto command = OMJobworkerCommandProto.newBuilder().setCmdId(42)
        .setCommandType(OMJobworkerCommandProto.Type.mockCommand).build();
    assertThrows(IOException.class, () -> jobs.submit(worker.getUuid(), OMJobworkerCommand.fromEnvelope(command)));
    verify(ratis, never()).submitRequest(any(OMRequest.class), any(ClientId.class), anyLong());
  }

  @Test
  void testResolvedUncertaintyReturnsAuthoritativeResultWithoutRecommitting() throws Exception {
    JobworkerJobRecord terminal = record(42, CommandStatus.Status.SUCCEEDED);
    CommandStatus result = CommandStatus.parseFrom(terminal.getStatus());
    OMJobworkerCommandManager commands = mock(OMJobworkerCommandManager.class);
    when(om.getOMJobworkerCommandManager()).thenReturn(commands);
    CommandStatus uncertain = result.toBuilder().setStatus(CommandStatus.Status.UNCERTAIN).build();
    assertEquals(Collections.singletonList(result), jobs.reconcile(worker, report(uncertain)));
    assertEquals(terminal, stored.get(terminal.getJobKey()));
    verify(om, never()).getOmRatisServer();
  }

  @Test
  void testOwnerCannotResolveARunningChangedOrNonterminalOutcome() throws Exception {
    JobworkerJobRecord record = record(42, CommandStatus.Status.EXECUTING);
    CommandStatus success = CommandStatus.parseFrom(record.getStatus()).toBuilder()
        .setStatus(CommandStatus.Status.SUCCEEDED).build();
    assertThrows(IOException.class, () -> jobs.resolveUncertain(record.getJobKey(), record.getRevision(), success));
    JobworkerJobRecord uncertain = record.toBuilder().setStatus(success.toBuilder()
        .setStatus(CommandStatus.Status.UNCERTAIN).build().toByteString()).build();
    stored.put(record.getJobKey(), uncertain);
    assertThrows(IOException.class, () -> jobs.resolveUncertain(record.getJobKey(), 0, success));
    assertThrows(IOException.class, () -> jobs.resolveUncertain(record.getJobKey(), record.getRevision(),
        success.toBuilder().setStatus(CommandStatus.Status.PENDING).build()));
    verify(om, never()).getOmRatisServer();
  }
}
