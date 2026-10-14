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

package org.apache.hadoop.ozone.nfs.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.ozone.nfs.NfsChildrenPage;
import org.apache.hadoop.ozone.nfs.NfsFileHandle;
import org.apache.hadoop.ozone.nfs.NfsStatus;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.Caller;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.CapabilityRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.CapabilityResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.CommitRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.CommitResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.CreateRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.CreateResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.GetattrRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.GetattrResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.LookupRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.LookupResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.MkdirRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.MkdirResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.ReadRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.ReadResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.ReaddirRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.ReaddirResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.RemoveRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.RemoveResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.RenameRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.RenameResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.RequestContext;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.Result;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.WriteRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.WriteResponse;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileCommitRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileLeaseResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedStatic;

/** Service dispatch and error contracts; peer policy and OM access have separate coverage. */
class TestNfsGatewayService {
  private final UUID exportId = new UUID(0, 0);
  private final ByteString exportUuid = ByteString.copyFrom(new byte[16]);
  private final RequestContext context = RequestContext.newBuilder().setVersion(1).setExportUuid(exportUuid)
      .setCaller(Caller.newBuilder().setKerberosPrincipal("alice@EXAMPLE.COM").setProtection(Caller.Protection.PRIVACY))
      .build();
  private final ByteString handle = ByteString.copyFrom(new NfsFileHandle(exportId, 1, 2, 3,
      NfsFileHandle.Kind.FILE).toBytes());
  private final ReadRequest request = ReadRequest.newBuilder().setContext(context).setHandle(handle)
      .setOffset(4).setCount(32).build();
  private final NfsObjectReader reader = mock(NfsObjectReader.class);
  private NfsGatewayService service;

  @BeforeEach
  void setUp() throws Exception {
    when(reader.getExportUuid()).thenReturn(exportUuid);
    when(reader.getCallerUserName(any())).thenReturn("alice");
    service = new NfsGatewayService(reader);
    clearInvocations(reader);
  }

  @Test
  void dispatchesRequestsToReaderForTheirExport() throws Exception {
    UUID secondExportId = new UUID(0, 1);
    ByteString secondExportUuid = ByteString.copyFrom(new byte[] {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1});
    NfsObjectReader secondReader = mock(NfsObjectReader.class);
    NfsGatewayService multiExportService = new NfsGatewayService(
        Map.of(exportUuid, reader, secondExportUuid, secondReader));
    RequestContext secondContext = context.toBuilder().setExportUuid(secondExportUuid).build();
    ByteString secondHandle = ByteString.copyFrom(new NfsFileHandle(secondExportId, 1, 2, 3,
        NfsFileHandle.Kind.FILE).toBytes());
    ReadRequest secondRequest = request.toBuilder().setContext(secondContext).setHandle(secondHandle).build();
    ReadResponse response = ReadResponse.newBuilder().setResult(Result.newBuilder().setNfsStatus(0))
        .setData(ByteString.copyFromUtf8("second export")).setEof(true).build();
    when(secondReader.read(secondContext, secondHandle, 4, 32)).thenReturn(response);
    StreamObserver<ReadResponse> observer = observer();
    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      multiExportService.read(secondRequest, observer);
    }
    verify(secondReader).read(secondContext, secondHandle, 4, 32);
    verifyNoInteractions(reader);
    verify(observer).onNext(response);
    verify(observer).onCompleted();
  }

  @Test
  void mkdirReturnsTheCreatedDirectoryHandle() throws Exception {
    OzoneFileStatus directory = mock(OzoneFileStatus.class);
    when(directory.isDirectory()).thenReturn(true);
    when(reader.createDirectory(context, handle, "child")).thenReturn(directory);
    when(reader.handleFor(directory)).thenReturn(handle);
    StreamObserver<MkdirResponse> observer = observer();

    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      service.mkdir(MkdirRequest.newBuilder().setContext(context).setParentHandle(handle).setName("child").build(),
          observer);
    }

    ArgumentCaptor<MkdirResponse> response = ArgumentCaptor.forClass(MkdirResponse.class);
    verify(observer).onNext(response.capture());
    verify(observer).onCompleted();
    assertEquals(NfsStatus.OK.getCode(), response.getValue().getResult().getNfsStatus());
    assertTrue(response.getValue().getAttributes().getDirectory());
    assertEquals(handle, response.getValue().getAttributes().getHandle());
    verify(reader).createDirectory(context, handle, "child");
  }

  @Test
  void removeAndRenameDispatchWithObjectAndParentHandles() throws Exception {
    StreamObserver<RemoveResponse> removeObserver = observer();
    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      service.remove(RemoveRequest.newBuilder().setContext(context).setParentHandle(handle).setObjectHandle(handle)
          .setName("child").build(), removeObserver);
    }
    ArgumentCaptor<RemoveResponse> removeResponse = ArgumentCaptor.forClass(RemoveResponse.class);
    verify(removeObserver).onNext(removeResponse.capture());
    verify(removeObserver).onCompleted();
    assertEquals(NfsStatus.OK.getCode(), removeResponse.getValue().getResult().getNfsStatus());
    verify(reader).remove(context, handle, handle, "child");

    StreamObserver<RenameResponse> renameObserver = observer();
    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      service.rename(RenameRequest.newBuilder().setContext(context).setObjectHandle(handle)
          .setOldParentHandle(handle).setOldName("old").setNewParentHandle(handle).setNewName("new").build(),
          renameObserver);
    }
    ArgumentCaptor<RenameResponse> renameResponse = ArgumentCaptor.forClass(RenameResponse.class);
    verify(renameObserver).onNext(renameResponse.capture());
    verify(renameObserver).onCompleted();
    assertEquals(NfsStatus.OK.getCode(), renameResponse.getValue().getResult().getNfsStatus());
    verify(reader).rename(context, handle, handle, "old", handle, "new");
  }

  @Test
  void returnsDataAndEofFromAuthorizedReader() throws Exception {
    ReadResponse response = ReadResponse.newBuilder().setResult(Result.newBuilder().setNfsStatus(0))
        .setData(ByteString.copyFromUtf8("data")).setEof(true).build();
    when(reader.read(context, handle, 4, 32)).thenReturn(response);
    StreamObserver<ReadResponse> observer = observer();
    try (MockedStatic<NfsTlsPeerInterceptor> tls = mockStatic(NfsTlsPeerInterceptor.class)) {
      service.read(request, observer);
      tls.verify(() -> NfsTlsPeerInterceptor.requireExport(exportUuid));
    }
    verify(reader).read(context, handle, 4, 32);
    verify(observer).onNext(response);
    verify(observer).onCompleted();
    verify(observer, never()).onError(any());
  }

  @Test
  void readsAnOpenFileFromItsDurableStagedRanges() throws Exception {
    NfsExportLeaseManager leaseManager = mock(NfsExportLeaseManager.class);
    NfsExportLeaseManager.StagedRead staged = mock(NfsExportLeaseManager.StagedRead.class);
    when(leaseManager.isActive(exportUuid)).thenReturn(true);
    when(reader.validateWrite(context, handle)).thenReturn(3L);
    when(leaseManager.readStaged(exportUuid, 3, "alice", 4, 32)).thenReturn(staged);
    when(staged.getData()).thenReturn(ByteString.copyFromUtf8("staged"));
    when(staged.isEof()).thenReturn(true);
    NfsGatewayService leasedService = new NfsGatewayService(Map.of(exportUuid, reader), leaseManager);
    StreamObserver<ReadResponse> observer = observer();

    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      leasedService.read(request, observer);
    }

    ArgumentCaptor<ReadResponse> response = ArgumentCaptor.forClass(ReadResponse.class);
    verify(observer).onNext(response.capture());
    verify(observer).onCompleted();
    assertEquals(ByteString.copyFromUtf8("staged"), response.getValue().getData());
    assertTrue(response.getValue().getEof());
    verify(reader, never()).read(context, handle, 4, 32);
  }

  @Test
  void readsCommittedFileWhenNoWriterLeaseCanBeRecovered() throws Exception {
    NfsExportLeaseManager leaseManager = mock(NfsExportLeaseManager.class);
    when(leaseManager.isActive(exportUuid)).thenReturn(true);
    when(reader.validateWrite(context, handle)).thenReturn(3L);
    when(leaseManager.readStaged(exportUuid, 3, "alice", 4, 32)).thenReturn(null);
    when(leaseManager.recoverFileLease(exportUuid, 3, "alice"))
        .thenThrow(new OMException("No recoverable NFS writer lease", OMException.ResultCodes.FILE_NOT_FOUND));
    ReadResponse response = ReadResponse.newBuilder().setResult(Result.newBuilder().setNfsStatus(0))
        .setData(ByteString.copyFromUtf8("committed")).setEof(true).build();
    when(reader.read(context, handle, 4, 32)).thenReturn(response);
    NfsGatewayService leasedService = new NfsGatewayService(Map.of(exportUuid, reader), leaseManager);
    StreamObserver<ReadResponse> observer = observer();

    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      leasedService.read(request, observer);
    }

    verify(leaseManager).recoverFileLease(exportUuid, 3, "alice");
    verify(reader).read(context, handle, 4, 32);
    verify(observer).onNext(response);
    verify(observer).onCompleted();
  }

  @Test
  void readsCommittedFileWhenCallerCannotRecoverAnotherWritersLease() throws Exception {
    NfsExportLeaseManager leaseManager = mock(NfsExportLeaseManager.class);
    when(leaseManager.isActive(exportUuid)).thenReturn(true);
    when(reader.validateWrite(context, handle)).thenReturn(3L);
    when(leaseManager.readStaged(exportUuid, 3, "alice", 4, 32)).thenReturn(null);
    when(leaseManager.recoverFileLease(exportUuid, 3, "alice"))
        .thenThrow(new OMException("Caller cannot recover NFS writer lease",
            OMException.ResultCodes.PERMISSION_DENIED));
    ReadResponse response = ReadResponse.newBuilder().setResult(Result.newBuilder().setNfsStatus(0))
        .setData(ByteString.copyFromUtf8("committed")).setEof(true).build();
    when(reader.read(context, handle, 4, 32)).thenReturn(response);
    NfsGatewayService leasedService = new NfsGatewayService(Map.of(exportUuid, reader), leaseManager);
    StreamObserver<ReadResponse> observer = observer();

    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      leasedService.read(request, observer);
    }

    verify(reader).read(context, handle, 4, 32);
    verify(observer).onNext(response);
    verify(observer).onCompleted();
  }

  @Test
  void returnsAttributesFromAuthorizedObjectLookup() throws Exception {
    GetattrRequest getattr = GetattrRequest.newBuilder().setContext(context).setHandle(handle).build();
    OmKeyInfo keyInfo = mock(OmKeyInfo.class);
    OzoneFileStatus status = mock(OzoneFileStatus.class);
    when(reader.lookup(context, handle, true)).thenReturn(status);
    when(status.isDirectory()).thenReturn(false);
    when(status.getKeyInfo()).thenReturn(keyInfo);
    when(keyInfo.getUpdateID()).thenReturn(7L);
    when(keyInfo.getDataSize()).thenReturn(42L);
    when(keyInfo.getModificationTime()).thenReturn(8L);
    when(keyInfo.getCreationTime()).thenReturn(6L);
    when(keyInfo.getOwnerName()).thenReturn("alice");
    StreamObserver<GetattrResponse> observer = observer();
    try (MockedStatic<NfsTlsPeerInterceptor> tls = mockStatic(NfsTlsPeerInterceptor.class)) {
      service.getattr(getattr, observer);
      tls.verify(() -> NfsTlsPeerInterceptor.requireExport(exportUuid));
    }
    ArgumentCaptor<GetattrResponse> response = ArgumentCaptor.forClass(GetattrResponse.class);
    verify(observer).onNext(response.capture());
    verify(observer).onCompleted();
    verify(observer, never()).onError(any());
    assertEquals(0, response.getValue().getResult().getNfsStatus());
    assertEquals(handle, response.getValue().getAttributes().getHandle());
    assertEquals(3, response.getValue().getAttributes().getObjectId());
    assertEquals(7, response.getValue().getAttributes().getChangeId());
    assertEquals(42, response.getValue().getAttributes().getSize());
    assertEquals("alice", response.getValue().getAttributes().getOwner());
    verify(reader).lookup(context, handle, true);
  }

  @Test
  void mapsMissingHandleDuringGetattrToStale() throws Exception {
    GetattrRequest getattr = GetattrRequest.newBuilder().setContext(context).setHandle(handle).build();
    doThrow(new OMException("stale", OMException.ResultCodes.FILE_NOT_FOUND))
        .when(reader).lookup(context, handle, true);
    StreamObserver<GetattrResponse> observer = observer();
    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      service.getattr(getattr, observer);
    }
    ArgumentCaptor<GetattrResponse> response = ArgumentCaptor.forClass(GetattrResponse.class);
    verify(observer).onNext(response.capture());
    verify(observer).onCompleted();
    assertEquals(NfsStatus.STALE.getCode(), response.getValue().getResult().getNfsStatus());
    assertFalse(response.getValue().hasAttributes());
  }

  @Test
  void resolvesLookupChildAndReturnsPersistentHandleAttributes() throws Exception {
    LookupRequest lookup = LookupRequest.newBuilder().setContext(context)
        .setParentHandle(handle).setName("child").build();
    OzoneFileStatus child = mock(OzoneFileStatus.class);
    OmKeyInfo keyInfo = mock(OmKeyInfo.class);
    ByteString childHandle = ByteString.copyFrom(new NfsFileHandle(exportId, 1, 2, 9,
        NfsFileHandle.Kind.FILE).toBytes());
    when(reader.lookupChildren(context, handle, "child", null, null, null))
        .thenReturn(new NfsChildrenPage(java.util.Collections.singletonList(child), 4, true));
    when(reader.handleFor(child)).thenReturn(childHandle);
    when(child.getKeyInfo()).thenReturn(keyInfo);
    when(child.isDirectory()).thenReturn(false);
    when(keyInfo.getObjectID()).thenReturn(9L);
    when(keyInfo.getUpdateID()).thenReturn(10L);
    when(keyInfo.getDataSize()).thenReturn(11L);
    when(keyInfo.getModificationTime()).thenReturn(12L);
    when(keyInfo.getCreationTime()).thenReturn(13L);
    StreamObserver<LookupResponse> observer = observer();
    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      service.lookup(lookup, observer);
    }
    ArgumentCaptor<LookupResponse> response = ArgumentCaptor.forClass(LookupResponse.class);
    verify(observer).onNext(response.capture());
    verify(observer).onCompleted();
    assertEquals(0, response.getValue().getResult().getNfsStatus());
    assertEquals(childHandle, response.getValue().getAttributes().getHandle());
    assertEquals(9, response.getValue().getAttributes().getObjectId());
  }

  @Test
  void createsFileOnlyThroughTheActiveLeaseManager() throws Exception {
    NfsExportLeaseManager leaseManager = mock(NfsExportLeaseManager.class);
    when(leaseManager.isActive(exportUuid)).thenReturn(true);
    NfsGatewayService leasedService = new NfsGatewayService(Map.of(exportUuid, reader), leaseManager);
    NfsFileCreateResponse created = NfsFileCreateResponse.newBuilder().setObjectID(9)
        .setGatewayGeneration(2).setGeneration(1).setExpiresAtMillis(System.currentTimeMillis() + 10_000)
        .setOwnerToken(ByteString.copyFromUtf8("owner")).build();
    ByteString fileHandle = ByteString.copyFrom(new NfsFileHandle(exportId, 1, 2, 9,
        NfsFileHandle.Kind.FILE).toBytes());
    when(leaseManager.createFile(eq(exportUuid), eq(context), eq(handle), eq("new-file"), anyString()))
        .thenReturn(created);
    when(reader.handleForObjectId(9)).thenReturn(fileHandle);
    CreateRequest create = CreateRequest.newBuilder().setContext(context).setParentHandle(handle)
        .setName("new-file").build();
    StreamObserver<CreateResponse> observer = observer();

    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      leasedService.create(create, observer);
    }

    ArgumentCaptor<CreateResponse> response = ArgumentCaptor.forClass(CreateResponse.class);
    verify(observer).onNext(response.capture());
    verify(observer).onCompleted();
    assertEquals(NfsStatus.OK.getCode(), response.getValue().getResult().getNfsStatus());
    assertEquals(fileHandle, response.getValue().getAttributes().getHandle());
    assertEquals(9, response.getValue().getAttributes().getObjectId());
    assertEquals(36, response.getValue().getTransferId().length());
    verify(leaseManager).createFile(exportUuid, context, handle, "new-file", response.getValue().getTransferId());
  }

  @Test
  void stagesWriteUnderTheCreatedFileLeaseAndReportsUnstableUntilCommit() throws Exception {
    NfsExportLeaseManager leaseManager = mock(NfsExportLeaseManager.class);
    when(leaseManager.isActive(exportUuid)).thenReturn(true);
    when(reader.validateWrite(context, handle)).thenReturn(9L);
    when(leaseManager.recoverFileLease(exportUuid, 9, "alice"))
        .thenReturn(NfsFileLeaseResponse.newBuilder().setGeneration(1).setExpiresAtMillis(1)
            .setTransferID("transfer-a").build());
    when(leaseManager.stageRange(exportUuid, "transfer-a", 9, "alice", 4, new byte[]{1, 2, 3}))
        .thenReturn(org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeResponse
            .newBuilder().setRecorded(true).build());
    NfsGatewayService leasedService = new NfsGatewayService(Map.of(exportUuid, reader), leaseManager);
    WriteRequest writeRequest = WriteRequest.newBuilder().setContext(context).setHandle(handle)
        .setTransferId("")
        .setOffset(4).setData(ByteString.copyFrom(new byte[]{1, 2, 3})).build();
    StreamObserver<WriteResponse> observer = observer();

    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      leasedService.write(writeRequest, observer);
    }

    ArgumentCaptor<WriteResponse> response = ArgumentCaptor.forClass(WriteResponse.class);
    verify(observer).onNext(response.capture());
    verify(observer).onCompleted();
    assertEquals(NfsStatus.OK.getCode(), response.getValue().getResult().getNfsStatus());
    assertEquals(3, response.getValue().getCount());
    assertEquals(WriteResponse.Stability.UNSTABLE, response.getValue().getCommitted());
    verify(leaseManager).recoverFileLease(exportUuid, 9, "alice");
    verify(leaseManager).stageRange(exportUuid, "transfer-a", 9, "alice", 4,
        new byte[]{1, 2, 3});
  }

  @Test
  void commitPublishesStagedFileWithoutReleasingLease() throws Exception {
    NfsExportLeaseManager leaseManager = mock(NfsExportLeaseManager.class);
    when(leaseManager.isActive(exportUuid)).thenReturn(true);
    when(reader.validateWrite(context, handle)).thenReturn(9L);
    NfsFileLeaseResponse lease = NfsFileLeaseResponse.newBuilder().setGeneration(1).setExpiresAtMillis(2)
        .setTransferID("transfer-a").setOwnerToken(ByteString.copyFromUtf8("owner")).build();
    NfsFileCommitRequest commitRequest = NfsFileCommitRequest.newBuilder().setExportUUID(exportId.toString())
        .setGatewayID("gateway").setGatewayGeneration(1).setTransferID("transfer-a").setFileGeneration(1)
        .setOwnerToken(ByteString.copyFromUtf8("owner")).build();
    when(leaseManager.recoverFileLease(exportUuid, 9, "alice")).thenReturn(lease);
    when(leaseManager.fileCommitRequest(exportUuid, lease)).thenReturn(commitRequest);
    when(leaseManager.stagedFileSize(exportUuid, 9, "alice")).thenReturn(7L);
    NfsGatewayService leasedService = new NfsGatewayService(Map.of(exportUuid, reader), leaseManager);
    CommitRequest commit = CommitRequest.newBuilder().setContext(context).setHandle(handle).setFinalize(false).build();
    StreamObserver<CommitResponse> observer = observer();

    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      leasedService.commit(commit, observer);
    }

    ArgumentCaptor<CommitResponse> response = ArgumentCaptor.forClass(CommitResponse.class);
    verify(observer).onNext(response.capture());
    verify(observer).onCompleted();
    assertEquals(NfsStatus.OK.getCode(), response.getValue().getResult().getNfsStatus());
    verify(reader).publishStagedFile(eq(context), eq(handle), eq(commitRequest), eq(7L), any());
    verify(leaseManager, never()).releaseFileLease(exportUuid, "transfer-a");
  }

  @Test
  void finalCommitPublishesThenRemovesRangesAndReleasesLease() throws Exception {
    NfsExportLeaseManager leaseManager = mock(NfsExportLeaseManager.class);
    when(leaseManager.isActive(exportUuid)).thenReturn(true);
    when(reader.validateWrite(context, handle)).thenReturn(9L);
    NfsFileLeaseResponse lease = NfsFileLeaseResponse.newBuilder().setGeneration(1).setExpiresAtMillis(2)
        .setTransferID("transfer-a").setOwnerToken(ByteString.copyFromUtf8("owner")).build();
    NfsFileCommitRequest commitRequest = NfsFileCommitRequest.newBuilder().setExportUUID(exportId.toString())
        .setGatewayID("gateway").setGatewayGeneration(1).setTransferID("transfer-a").setFileGeneration(1)
        .setOwnerToken(ByteString.copyFromUtf8("owner")).build();
    when(leaseManager.recoverFileLease(exportUuid, 9, "alice")).thenReturn(lease);
    when(leaseManager.fileCommitRequest(exportUuid, lease)).thenReturn(commitRequest);
    when(leaseManager.stagedFileSize(exportUuid, 9, "alice")).thenReturn(7L);
    NfsGatewayService leasedService = new NfsGatewayService(Map.of(exportUuid, reader), leaseManager);
    CommitRequest commit = CommitRequest.newBuilder().setContext(context).setHandle(handle).setFinalize(true).build();
    StreamObserver<CommitResponse> observer = observer();

    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      leasedService.commit(commit, observer);
    }

    ArgumentCaptor<CommitResponse> response = ArgumentCaptor.forClass(CommitResponse.class);
    verify(observer).onNext(response.capture());
    verify(observer).onCompleted();
    assertEquals(NfsStatus.OK.getCode(), response.getValue().getResult().getNfsStatus());
    InOrder order = org.mockito.Mockito.inOrder(reader, leaseManager);
    order.verify(reader).publishStagedFile(eq(context), eq(handle), eq(commitRequest), eq(7L), any());
    order.verify(leaseManager).removeStagedRanges(exportUuid, "transfer-a");
    order.verify(leaseManager).releaseFileLease(exportUuid, "transfer-a");
  }

  @Test
  void recoveredPublishedFileSkipsRepublishingRemainingRanges() throws Exception {
    NfsExportLeaseManager leaseManager = mock(NfsExportLeaseManager.class);
    when(leaseManager.isActive(exportUuid)).thenReturn(true);
    when(reader.validateWrite(context, handle)).thenReturn(9L);
    NfsFileLeaseResponse lease = NfsFileLeaseResponse.newBuilder().setGeneration(2).setExpiresAtMillis(2)
        .setTransferID("transfer-a").setOwnerToken(ByteString.copyFromUtf8("owner")).setContentPublished(true)
        .build();
    NfsFileCommitRequest commitRequest = NfsFileCommitRequest.newBuilder().setExportUUID(exportId.toString())
        .setGatewayID("gateway").setGatewayGeneration(1).setTransferID("transfer-a").setFileGeneration(2)
        .setOwnerToken(ByteString.copyFromUtf8("owner")).build();
    when(leaseManager.recoverFileLease(exportUuid, 9, "alice")).thenReturn(lease);
    when(leaseManager.fileCommitRequest(exportUuid, lease)).thenReturn(commitRequest);
    NfsGatewayService leasedService = new NfsGatewayService(Map.of(exportUuid, reader), leaseManager);
    CommitRequest commit = CommitRequest.newBuilder().setContext(context).setHandle(handle).setFinalize(true).build();
    StreamObserver<CommitResponse> observer = observer();

    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      leasedService.commit(commit, observer);
    }

    ArgumentCaptor<CommitResponse> response = ArgumentCaptor.forClass(CommitResponse.class);
    verify(observer).onNext(response.capture());
    verify(observer).onCompleted();
    assertEquals(NfsStatus.OK.getCode(), response.getValue().getResult().getNfsStatus());
    verify(reader, never()).publishStagedFile(any(), any(), any(), anyLong(), any());
    verify(leaseManager).removeStagedRanges(exportUuid, "transfer-a");
    verify(leaseManager).releaseFileLease(exportUuid, "transfer-a");
  }

  @Test
  void rejectsInvalidCreateNameBeforeAcquiringAnOmLease() throws IOException {
    NfsExportLeaseManager leaseManager = mock(NfsExportLeaseManager.class);
    when(leaseManager.isActive(exportUuid)).thenReturn(true);
    NfsGatewayService leasedService = new NfsGatewayService(Map.of(exportUuid, reader), leaseManager);
    CreateRequest create = CreateRequest.newBuilder().setContext(context).setParentHandle(handle)
        .setName("../outside").build();
    StreamObserver<CreateResponse> observer = observer();
    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      leasedService.create(create, observer);
    }
    ArgumentCaptor<CreateResponse> response = ArgumentCaptor.forClass(CreateResponse.class);
    verify(observer).onNext(response.capture());
    verify(observer).onCompleted();
    assertEquals(NfsStatus.BADNAME.getCode(), response.getValue().getResult().getNfsStatus());
    verify(leaseManager, never()).createFile(any(), any(), any(), anyString(), anyString());
  }

  @Test
  void readsDirectoryPageAndEncodesNumericCookieAndVerifier() throws Exception {
    ReaddirRequest readdir = ReaddirRequest.newBuilder().setContext(context).setDirectoryHandle(handle)
        .setMaxEntries(5).build();
    OzoneFileStatus child = mock(OzoneFileStatus.class);
    OmKeyInfo keyInfo = mock(OmKeyInfo.class);
    when(keyInfo.getFileName()).thenReturn("child");
    when(keyInfo.getObjectID()).thenReturn(9L);
    when(keyInfo.getUpdateID()).thenReturn(10L);
    when(keyInfo.getDataSize()).thenReturn(11L);
    when(keyInfo.getModificationTime()).thenReturn(12L);
    when(keyInfo.getCreationTime()).thenReturn(13L);
    when(child.getKeyInfo()).thenReturn(keyInfo);
    when(child.isDirectory()).thenReturn(false);
    ByteString childHandle = ByteString.copyFrom(new NfsFileHandle(exportId, 1, 2, 9,
        NfsFileHandle.Kind.FILE).toBytes());
    when(reader.lookupChildren(context, handle, null, 5, null, null))
        .thenReturn(new NfsChildrenPage(java.util.Collections.singletonList(child), 4, false));
    when(reader.handleFor(child)).thenReturn(childHandle);
    StreamObserver<ReaddirResponse> observer = observer();
    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      service.readdir(readdir, observer);
    }
    ArgumentCaptor<ReaddirResponse> response = ArgumentCaptor.forClass(ReaddirResponse.class);
    verify(observer).onNext(response.capture());
    verify(observer).onCompleted();
    assertEquals(0, response.getValue().getResult().getNfsStatus());
    assertEquals(12, java.nio.ByteBuffer.wrap(response.getValue().getEntries(0).getCookie().toByteArray())
        .getLong());
    assertEquals(4, java.nio.ByteBuffer.wrap(response.getValue().getVerifier().toByteArray()).getLong());
    assertFalse(response.getValue().getEof());
  }

  @Test
  void mapsChangedDirectoryVerifierToBadCookie() throws Exception {
    ByteString verifier = ByteString.copyFrom(new byte[Long.BYTES]);
    ReaddirRequest readdir = ReaddirRequest.newBuilder().setContext(context).setDirectoryHandle(handle)
        .setMaxEntries(5).setCookie(ByteString.copyFrom(new byte[Long.BYTES])).setVerifier(verifier).build();
    doThrow(new OMException("directory changed", OMException.ResultCodes.INVALID_REQUEST))
        .when(reader).lookupChildren(context, handle, null, 5, -3L, 0L);
    StreamObserver<ReaddirResponse> observer = observer();
    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      service.readdir(readdir, observer);
    }
    ArgumentCaptor<ReaddirResponse> response = ArgumentCaptor.forClass(ReaddirResponse.class);
    verify(observer).onNext(response.capture());
    verify(observer).onCompleted();
    assertEquals(NfsStatus.BAD_COOKIE.getCode(), response.getValue().getResult().getNfsStatus());
    assertEquals(0, response.getValue().getEntriesCount());
  }

  @Test
  void passesGaneshaCookieVerifierToOm() throws Exception {
    ByteString cookie = ByteString.copyFrom(java.nio.ByteBuffer.allocate(Long.BYTES).putLong(12).array());
    ByteString verifier = ByteString.copyFrom(java.nio.ByteBuffer.allocate(Long.BYTES).putLong(10).array());
    ReaddirRequest readdir = ReaddirRequest.newBuilder().setContext(context).setDirectoryHandle(handle)
        .setMaxEntries(5).setCookie(cookie).setVerifier(verifier).build();
    when(reader.lookupChildren(context, handle, null, 5, 9L, 10L))
        .thenReturn(new NfsChildrenPage(java.util.Collections.emptyList(), 4, true));
    StreamObserver<ReaddirResponse> observer = observer();
    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      service.readdir(readdir, observer);
    }
    ArgumentCaptor<ReaddirResponse> response = ArgumentCaptor.forClass(ReaddirResponse.class);
    verify(observer).onNext(response.capture());
    verify(observer).onCompleted();
    assertEquals(0, response.getValue().getResult().getNfsStatus());
    assertEquals(4, java.nio.ByteBuffer.wrap(response.getValue().getVerifier().toByteArray()).getLong());
    assertEquals(0, response.getValue().getEntriesCount());
    verify(reader).lookupChildren(context, handle, null, 5, 9L, 10L);
  }

  @Test
  void rejectsMissingTlsContextBeforeReader() {
    StreamObserver<ReadResponse> observer = observer();
    service.read(request, observer);
    assertTransportError(observer, Status.Code.UNAUTHENTICATED);
    verifyNoInteractions(reader);
  }

  @Test
  void preservesOmFailureCategoriesWithoutDataOrEof() throws Exception {
    OMException.ResultCodes[] codes = {OMException.ResultCodes.FILE_NOT_FOUND,
        OMException.ResultCodes.PERMISSION_DENIED, OMException.ResultCodes.OM_NOT_INITIALIZED};
    NfsStatus[] statuses = {NfsStatus.STALE, NfsStatus.ACCESS, NfsStatus.DELAY};
    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      for (int i = 0; i < codes.length; i++) {
        doThrow(new OMException("test", codes[i])).when(reader).read(context, handle, 4, 32);
        StreamObserver<ReadResponse> observer = observer();
        service.read(request, observer);
        assertFailure(observer, statuses[i]);
      }
      doThrow(new IOException("storage failure")).when(reader).read(context, handle, 4, 32);
      StreamObserver<ReadResponse> observer = observer();
      service.read(request, observer);
      assertFailure(observer, NfsStatus.IO);
    }
  }

  @Test
  void distinguishesMalformedHandleFromUnexpectedBackendArgumentFailure() throws Exception {
    ReadRequest malformed = request.toBuilder().setHandle(ByteString.EMPTY).build();
    doThrow(new IllegalArgumentException("malformed")).when(reader).read(context, ByteString.EMPTY, 4, 32);
    doThrow(new IllegalArgumentException("backend configuration")).when(reader).read(context, handle, 4, 32);
    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      StreamObserver<ReadResponse> badHandleObserver = observer();
      service.read(malformed, badHandleObserver);
      assertFailure(badHandleObserver, NfsStatus.BADHANDLE);
      StreamObserver<ReadResponse> unexpectedObserver = observer();
      service.read(request, unexpectedObserver);
      assertTransportError(unexpectedObserver, Status.Code.INTERNAL);
    }
  }

  @Test
  void preservesCallerAuthenticationErrorAndRejectsIncompleteRequest() throws Exception {
    doThrow(Status.PERMISSION_DENIED.asRuntimeException()).when(reader).read(context, handle, 4, 32);
    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      StreamObserver<ReadResponse> denied = observer();
      service.read(request, denied);
      assertTransportError(denied, Status.Code.PERMISSION_DENIED);
      StreamObserver<ReadResponse> incomplete = observer();
      service.read(request.toBuilder().clearCount().buildPartial(), incomplete);
      assertTransportError(incomplete, Status.Code.INVALID_ARGUMENT);
    }
  }

  @Test
  void advertisesImplementedExportOperations() {
    StreamObserver<CapabilityResponse> observer = observer();
    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      service.capabilities(CapabilityRequest.newBuilder().setVersion(1).setExportUuid(exportUuid).build(), observer);
    }
    ArgumentCaptor<CapabilityResponse> response = ArgumentCaptor.forClass(CapabilityResponse.class);
    verify(observer).onNext(response.capture());
    verify(observer).onCompleted();
    assertEquals(NfsStatus.OK.getCode(), response.getValue().getResult().getNfsStatus());
    assertEquals(7, response.getValue().getOperationsCount());
    assertEquals(CapabilityResponse.Operation.GETATTR, response.getValue().getOperations(0));
    assertEquals(CapabilityResponse.Operation.LOOKUP, response.getValue().getOperations(1));
    assertEquals(CapabilityResponse.Operation.READDIR, response.getValue().getOperations(2));
    assertEquals(CapabilityResponse.Operation.READ, response.getValue().getOperations(3));
    assertEquals(CapabilityResponse.Operation.MKDIR, response.getValue().getOperations(4));
    assertEquals(CapabilityResponse.Operation.REMOVE, response.getValue().getOperations(5));
    assertEquals(CapabilityResponse.Operation.RENAME, response.getValue().getOperations(6));
    assertEquals(NfsObjectReader.MAX_READ_BYTES, response.getValue().getMaxReadBytes());
    assertEquals(256, response.getValue().getMaxDirectoryEntries());
    assertFalse(response.getValue().hasRootHandle());
    verifyNoInteractions(reader);
  }

  @Test
  void rejectsUnsupportedCapabilityVersion() {
    StreamObserver<CapabilityResponse> observer = observer();
    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      service.capabilities(CapabilityRequest.newBuilder().setVersion(2).setExportUuid(exportUuid).build(), observer);
    }
    assertTransportError(observer, Status.Code.FAILED_PRECONDITION);
    verifyNoInteractions(reader);
  }

  @Test
  void failsClosedWhenExportLeaseIsNotActive() {
    NfsExportLeaseManager leaseManager = mock(NfsExportLeaseManager.class);
    when(leaseManager.isActive(exportUuid)).thenReturn(false);
    NfsGatewayService leasedService = new NfsGatewayService(Map.of(exportUuid, reader), leaseManager);
    StreamObserver<CapabilityResponse> observer = observer();
    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      leasedService.capabilities(CapabilityRequest.newBuilder().setVersion(1).setExportUuid(exportUuid).build(),
          observer);
    }
    assertTransportError(observer, Status.Code.UNAVAILABLE);
    verify(leaseManager).isActive(exportUuid);
    verifyNoInteractions(reader);
  }

  @Test
  void advertisesCreateOnlyWhenLeaseSupportIsConfigured() {
    NfsExportLeaseManager leaseManager = mock(NfsExportLeaseManager.class);
    when(leaseManager.isActive(exportUuid)).thenReturn(true);
    NfsGatewayService leasedService = new NfsGatewayService(Map.of(exportUuid, reader), leaseManager);
    StreamObserver<CapabilityResponse> observer = observer();

    try (MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      leasedService.capabilities(CapabilityRequest.newBuilder().setVersion(1).setExportUuid(exportUuid).build(),
          observer);
    }

    ArgumentCaptor<CapabilityResponse> response = ArgumentCaptor.forClass(CapabilityResponse.class);
    verify(observer).onNext(response.capture());
    assertEquals(8, response.getValue().getOperationsCount());
    assertTrue(response.getValue().getOperationsList().contains(CapabilityResponse.Operation.CREATE));
  }

  @SuppressWarnings("unchecked")
  private static <T> StreamObserver<T> observer() {
    return mock(StreamObserver.class);
  }

  private static void assertFailure(StreamObserver<ReadResponse> observer, NfsStatus expected) {
    ArgumentCaptor<ReadResponse> response = ArgumentCaptor.forClass(ReadResponse.class);
    verify(observer).onNext(response.capture());
    verify(observer).onCompleted();
    verify(observer, never()).onError(any());
    assertEquals(expected.getCode(), response.getValue().getResult().getNfsStatus());
    assertFalse(response.getValue().hasData());
    assertFalse(response.getValue().hasEof());
  }

  private static <T> void assertTransportError(StreamObserver<T> observer, Status.Code expected) {
    ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
    verify(observer).onError(error.capture());
    verify(observer, never()).onNext(any());
    verify(observer, never()).onCompleted();
    assertEquals(expected, ((StatusRuntimeException) error.getValue()).getStatus().getCode());
  }
}
