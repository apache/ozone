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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.jobworker.OMJobworkerJobUpdateRequest;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.JobworkerJobRecord;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateJobworkerJobRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Deterministic replicated transitions, assignment fencing and external-request rejection. */
class TestJobworkerJobUpdateRequest {
  private OzoneManager om;
  private Table<String, JobworkerJobRecord> table;
  private OMLayoutVersionManager versions;
  private JobworkerJobRecord pending;

  @BeforeEach
  void setUp() {
    om = mock(OzoneManager.class);
    when(om.getOMServiceId()).thenReturn("service");
    OMMetadataManager metadata = mock(OMMetadataManager.class);
    table = mock(Table.class);
    versions = mock(OMLayoutVersionManager.class);
    when(om.getMetadataManager()).thenReturn(metadata);
    when(metadata.getJobworkerJobTable()).thenReturn(table);
    when(om.getVersionManager()).thenReturn(versions);
    when(versions.isAllowed(OMLayoutFeature.JOBWORKER_HA)).thenReturn(true);
    OMJobworkerCommandProto command = OMJobworkerCommandProto.newBuilder()
        .setCommandType(OMJobworkerCommandProto.Type.mockCommand).setCmdId(42)
        .setAttemptId(UUID.randomUUID().toString()).build();
    CommandStatus status = CommandStatus.newBuilder().setCmdId(42).setType(command.getCommandType())
        .setAttemptId(command.getAttemptId()).setOmServiceId("service")
        .setStatus(CommandStatus.Status.PENDING).build();
    pending = JobworkerJobRecord.newBuilder().setJobKey(JobworkerJobState.key(command.getCommandType(), 42))
        .setWorkerId(UUID.randomUUID().toString()).setCommand(command.toByteString())
        .setStatus(status.toByteString()).setRevision(0).build();
  }

  private OMJobworkerJobUpdateRequest request(JobworkerJobRecord record, long revision) {
    return new OMJobworkerJobUpdateRequest(OMRequest.newBuilder().setCmdType(Type.UpdateJobworkerJob)
        .setClientId(UUID.randomUUID().toString()).setUpdateJobworkerJobRequest(UpdateJobworkerJobRequest.newBuilder()
            .setRecord(record).setExpectedRevision(revision)).build());
  }

  private JobworkerJobRecord withStatus(JobworkerJobRecord record, CommandStatus.Status status) throws Exception {
    return record.toBuilder().setStatus(CommandStatus.parseFrom(record.getStatus()).toBuilder()
        .setStatus(status).build().toByteString()).build();
  }

  private OMResponse apply(JobworkerJobRecord record, long revision, long index) {
    return request(record, revision).validateAndUpdateCache(om, ExecutionContext.of(index, null)).getOMResponse();
  }

  @Test
  void testAssignmentAndCompletionUseCommittedLogRevision() throws Exception {
    OMResponse created = apply(pending, 0, 10);
    assertTrue(created.getSuccess());
    assertEquals(10, created.getJobworkerJobRecord().getRevision());
    JobworkerJobRecord assigned = created.getJobworkerJobRecord();
    when(table.get(assigned.getJobKey())).thenReturn(assigned);
    OMResponse executing = apply(withStatus(assigned, CommandStatus.Status.EXECUTING), 10, 11);
    assertTrue(executing.getSuccess());
    when(table.get(assigned.getJobKey())).thenReturn(executing.getJobworkerJobRecord());
    OMResponse completed = apply(withStatus(executing.getJobworkerJobRecord(), CommandStatus.Status.SUCCEEDED), 11, 12);
    assertTrue(completed.getSuccess());
    assertEquals(12, completed.getJobworkerJobRecord().getRevision());
  }

  @Test
  void testCompareAndSetRejectsStaleUpdate() throws Exception {
    when(table.get(pending.getJobKey())).thenReturn(pending.toBuilder().setRevision(10).build());
    assertFalse(apply(withStatus(pending, CommandStatus.Status.EXECUTING), 0, 11).getSuccess());
    verify(table, never()).addCacheEntry(any(), any());
  }

  @Test
  void testRejectsWorkerAndAttemptReassignment() throws Exception {
    when(table.get(pending.getJobKey())).thenReturn(pending);
    assertFalse(apply(pending.toBuilder().setWorkerId(UUID.randomUUID().toString()).build(), 0, 11).getSuccess());
    OMJobworkerCommandProto changed = OMJobworkerCommandProto.parseFrom(pending.getCommand()).toBuilder()
        .setAttemptId(UUID.randomUUID().toString()).build();
    CommandStatus status = CommandStatus.parseFrom(pending.getStatus()).toBuilder()
        .setAttemptId(changed.getAttemptId()).build();
    assertFalse(apply(pending.toBuilder().setCommand(changed.toByteString())
        .setStatus(status.toByteString()).build(), 0, 11).getSuccess());
    verify(table, never()).addCacheEntry(any(), any());
  }

  @Test
  void testTerminalIsImmutableAndUncertainSameAttemptCanReconcile() throws Exception {
    JobworkerJobRecord terminal = withStatus(pending, CommandStatus.Status.SUCCEEDED);
    when(table.get(pending.getJobKey())).thenReturn(terminal);
    assertFalse(apply(withStatus(terminal, CommandStatus.Status.FAILED), 0, 11).getSuccess());
    assertFalse(apply(pending, 0, 11).getSuccess());
    verify(table, never()).addCacheEntry(any(), any());
    when(table.get(pending.getJobKey())).thenReturn(withStatus(pending, CommandStatus.Status.UNCERTAIN));
    assertTrue(apply(withStatus(pending, CommandStatus.Status.EXECUTING), 0, 12).getSuccess());
  }

  @Test
  void testCannotStartWithACompletedUnknownJob() throws Exception {
    assertFalse(apply(withStatus(pending, CommandStatus.Status.SUCCEEDED), 0, 11).getSuccess());
    verify(table, never()).addCacheEntry(any(), any());
  }

  @Test
  void testRejectsMismatchedServiceAndStatusIdentity() throws Exception {
    when(table.get(pending.getJobKey())).thenReturn(pending);
    CommandStatus mismatch = CommandStatus.parseFrom(pending.getStatus()).toBuilder().setOmServiceId("other").build();
    assertFalse(apply(pending.toBuilder().setStatus(mismatch.toByteString()).build(), 0, 11).getSuccess());
    mismatch = mismatch.toBuilder().setCmdId(99).build();
    assertFalse(apply(pending.toBuilder().setStatus(mismatch.toByteString()).build(), 0, 11).getSuccess());
  }

  @Test
  void testUpgradeGateAndExternalRpcRejection() throws Exception {
    when(versions.isAllowed(OMLayoutFeature.JOBWORKER_HA)).thenReturn(false);
    OMResponse response = apply(pending, 0, 10);
    assertFalse(response.getSuccess());
    verify(table, never()).addCacheEntry(any(), any());
    assertThrows(org.apache.hadoop.ozone.om.exceptions.OMException.class, () -> request(pending, 0).preExecute(om));
  }
}
