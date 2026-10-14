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

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.ozone.nfs.NfsFileHandle;
import org.apache.hadoop.ozone.nfs.NfsStatus;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayGrpc;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.Attributes;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.CapabilityRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.CapabilityResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.CapabilityResponse.Operation;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.CommitRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.CommitResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.CreateRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.CreateResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.DirectoryEntry;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Authenticated NFS backend operations. */
public final class NfsGatewayService extends NfsGatewayGrpc.NfsGatewayImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(NfsGatewayService.class);
  private static final int MAX_DIRECTORY_ENTRIES = 256;
  private final Map<ByteString, NfsObjectReader> readers;
  private final NfsExportLeaseManager leaseManager;

  public NfsGatewayService(NfsObjectReader reader) {
    this(Map.of(Objects.requireNonNull(reader, "reader").getExportUuid(), reader), null);
  }

  public NfsGatewayService(Map<ByteString, NfsObjectReader> readers) {
    this(readers, null);
  }

  NfsGatewayService(Map<ByteString, NfsObjectReader> readers, NfsExportLeaseManager leaseManager) {
    if (readers == null || readers.isEmpty()) {
      throw new IllegalArgumentException("At least one NFS export reader is required");
    }
    this.readers = Map.copyOf(readers);
    this.leaseManager = leaseManager;
  }

  @Override
  public void capabilities(CapabilityRequest request, StreamObserver<CapabilityResponse> observer) {
    try {
      readerFor(request.getExportUuid());
      if (!request.isInitialized() || request.getVersion() != 1) {
        throw Status.FAILED_PRECONDITION.withDescription("Unsupported NFS backend version").asRuntimeException();
      }
      CapabilityResponse.Builder response = CapabilityResponse.newBuilder().setVersion(1)
          .setResult(result(NfsStatus.OK))
          .addOperations(Operation.GETATTR).addOperations(Operation.LOOKUP)
          .addOperations(Operation.READDIR).addOperations(Operation.READ)
          .addOperations(Operation.MKDIR)
          .addOperations(Operation.REMOVE).addOperations(Operation.RENAME)
          .setMaxReadBytes(NfsObjectReader.MAX_READ_BYTES).setMaxDirectoryEntries(MAX_DIRECTORY_ENTRIES);
      if (leaseManager != null) {
        response.addOperations(Operation.CREATE);
      }
      observer.onNext(response.build());
      observer.onCompleted();
    } catch (StatusRuntimeException e) {
      observer.onError(e);
    }
  }

  @Override
  public void read(ReadRequest request, StreamObserver<ReadResponse> observer) {
    ReadResponse response;
    try {
      NfsObjectReader reader = readerFor(request.getContext());
      if (!request.isInitialized()) {
        throw Status.INVALID_ARGUMENT.withDescription("Incomplete NFS read request").asRuntimeException();
      }
      NfsExportLeaseManager.StagedRead staged = null;
      if (leaseManager != null) {
        long objectId = reader.validateWrite(request.getContext(), request.getHandle());
        String principal = reader.getCallerUserName(request.getContext());
        staged = leaseManager.readStaged(request.getContext().getExportUuid(), objectId, principal,
            request.getOffset(), request.getCount());
        if (staged == null) {
          try {
            leaseManager.recoverFileLease(request.getContext().getExportUuid(), objectId, principal);
          } catch (OMException e) {
            if (e.getResult() != OMException.ResultCodes.FILE_NOT_FOUND
                && e.getResult() != OMException.ResultCodes.PERMISSION_DENIED
                && e.getResult() != OMException.ResultCodes.NFS_LEASE_CONFLICT) {
              throw e;
            }
          }
          staged = leaseManager.readStaged(request.getContext().getExportUuid(), objectId, principal,
              request.getOffset(), request.getCount());
        }
      }
      response = staged == null ? reader.read(request.getContext(), request.getHandle(), request.getOffset(),
          request.getCount()) : ReadResponse.newBuilder().setResult(result(NfsStatus.OK))
              .setData(staged.getData()).setEof(staged.isEof()).build();
    } catch (StatusRuntimeException e) {
      observer.onError(e);
      return;
    } catch (OMException e) {
      response = failure(NfsStatus.fromOmException(e, NfsStatus.LookupContext.HANDLE));
    } catch (IOException e) {
      response = failure(NfsStatus.fromIOException(e));
    } catch (IllegalArgumentException e) {
      try {
        NfsFileHandle.fromBytes(request.getHandle().toByteArray());
      } catch (IllegalArgumentException malformed) {
        observer.onNext(failure(NfsStatus.BADHANDLE));
        observer.onCompleted();
        return;
      }
      observer.onError(Status.INTERNAL.withDescription("NFS backend read failed").asRuntimeException());
      return;
    } catch (RuntimeException e) {
      observer.onError(Status.INTERNAL.withDescription("NFS backend read failed").asRuntimeException());
      return;
    }
    observer.onNext(response);
    observer.onCompleted();
  }

  @Override
  public void write(WriteRequest request, StreamObserver<WriteResponse> observer) {
    WriteResponse response;
    try {
      NfsObjectReader reader = readerFor(request.getContext());
      if (!request.isInitialized() || request.getData().isEmpty()
          || request.getData().size() > NfsObjectReader.MAX_READ_BYTES) {
        throw Status.INVALID_ARGUMENT.withDescription("Invalid NFS write request").asRuntimeException();
      }
      if (leaseManager == null) {
        throw Status.FAILED_PRECONDITION.withDescription("NFS writes require an active lease manager")
            .asRuntimeException();
      }
      long objectId = reader.validateWrite(request.getContext(), request.getHandle());
      String principal = reader.getCallerUserName(request.getContext());
      NfsFileLeaseResponse lease = leaseManager.recoverFileLease(request.getContext().getExportUuid(), objectId,
          principal);
      if (!request.getTransferId().isEmpty() && !request.getTransferId().equals(lease.getTransferID())) {
        throw new OMException("NFS write transfer does not match the active file lease",
            OMException.ResultCodes.NFS_LEASE_CONFLICT);
      }
      leaseManager.stageRange(request.getContext().getExportUuid(), lease.getTransferID(), objectId, principal,
          request.getOffset(), request.getData().toByteArray());
      // COMMIT publishes staged ranges into the canonical key, so WRITE must not report FILE_SYNC yet.
      response = WriteResponse.newBuilder().setResult(result(NfsStatus.OK)).setCount(request.getData().size())
          .setCommitted(WriteResponse.Stability.UNSTABLE).build();
    } catch (StatusRuntimeException e) {
      observer.onError(e);
      return;
    } catch (OMException e) {
      response = WriteResponse.newBuilder().setResult(result(NfsStatus.fromOmException(e,
          NfsStatus.LookupContext.HANDLE))).build();
    } catch (IOException e) {
      response = WriteResponse.newBuilder().setResult(result(NfsStatus.fromIOException(e))).build();
    } catch (IllegalArgumentException e) {
      try {
        NfsFileHandle.fromBytes(request.getHandle().toByteArray());
      } catch (IllegalArgumentException malformed) {
        observer.onNext(WriteResponse.newBuilder().setResult(result(NfsStatus.BADHANDLE)).build());
        observer.onCompleted();
        return;
      }
      observer.onError(Status.INTERNAL.withDescription("NFS backend write failed").asRuntimeException());
      return;
    } catch (RuntimeException e) {
      observer.onError(Status.INTERNAL.withDescription("NFS backend write failed").asRuntimeException());
      return;
    }
    observer.onNext(response);
    observer.onCompleted();
  }

  @Override
  public void commit(CommitRequest request, StreamObserver<CommitResponse> observer) {
    CommitResponse response;
    try {
      NfsObjectReader reader = readerFor(request.getContext());
      if (!request.isInitialized() || leaseManager == null) {
        throw Status.FAILED_PRECONDITION.withDescription("NFS file publication requires an active lease manager")
            .asRuntimeException();
      }
      long objectId = reader.validateWrite(request.getContext(), request.getHandle());
      String principal = reader.getCallerUserName(request.getContext());
      NfsFileLeaseResponse lease = leaseManager.recoverFileLease(request.getContext().getExportUuid(), objectId,
          principal);
      NfsFileCommitRequest commitRequest = leaseManager.fileCommitRequest(request.getContext().getExportUuid(),
          objectId, principal);
      if (!lease.getContentPublished()) {
        leaseManager.refreshStagedRanges(request.getContext().getExportUuid(), objectId, principal);
        long fileSize = leaseManager.stagedFileSize(request.getContext().getExportUuid(), objectId, principal);
        reader.publishStagedFile(request.getContext(), request.getHandle(), commitRequest, fileSize,
            (offset, count) -> {
              NfsExportLeaseManager.StagedRead staged = leaseManager.readStaged(request.getContext().getExportUuid(),
                  objectId, principal, offset, count);
              if (staged == null || staged.getData().size() != count) {
                throw new IOException("NFS staged file assembly is missing a range");
              }
              return staged.getData().toByteArray();
            });
        leaseManager.markContentPublished(request.getContext().getExportUuid(), lease.getTransferID());
      }
      if (request.getFinalize()) {
        leaseManager.removeStagedRanges(request.getContext().getExportUuid(), lease.getTransferID());
        leaseManager.releaseFileLease(request.getContext().getExportUuid(), lease.getTransferID());
      }
      response = CommitResponse.newBuilder().setResult(result(NfsStatus.OK)).build();
    } catch (StatusRuntimeException e) {
      observer.onError(e);
      return;
    } catch (OMException e) {
      response = CommitResponse.newBuilder().setResult(result(NfsStatus.fromOmException(e,
          NfsStatus.LookupContext.HANDLE))).build();
    } catch (IllegalArgumentException e) {
      response = CommitResponse.newBuilder().setResult(result(NfsStatus.BADHANDLE)).build();
    } catch (IOException e) {
      LOG.error("NFS file commit failed", e);
      response = CommitResponse.newBuilder().setResult(result(NfsStatus.fromIOException(e))).build();
    } catch (RuntimeException e) {
      observer.onError(Status.INTERNAL.withDescription("NFS backend commit failed").asRuntimeException());
      return;
    }
    observer.onNext(response);
    observer.onCompleted();
  }

  @Override
  public void getattr(GetattrRequest request, StreamObserver<GetattrResponse> observer) {
    GetattrResponse response;
    try {
      NfsObjectReader reader = readerFor(request.getContext());
      if (!request.isInitialized()) {
        throw Status.INVALID_ARGUMENT.withDescription("Incomplete NFS getattr request").asRuntimeException();
      }
      OzoneFileStatus status = reader.lookup(request.getContext(), request.getHandle(), true);
      response = GetattrResponse.newBuilder().setResult(Result.newBuilder().setNfsStatus(0))
          .setAttributes(toAttributes(request.getHandle(), status)).build();
    } catch (StatusRuntimeException e) {
      observer.onError(e);
      return;
    } catch (OMException e) {
      response = GetattrResponse.newBuilder()
          .setResult(result(NfsStatus.fromOmException(e, NfsStatus.LookupContext.HANDLE))).build();
    } catch (IOException e) {
      LOG.warn("NFS GETATTR failed", e);
      response = GetattrResponse.newBuilder().setResult(result(NfsStatus.fromIOException(e))).build();
    } catch (IllegalArgumentException e) {
      try {
        NfsFileHandle.fromBytes(request.getHandle().toByteArray());
      } catch (IllegalArgumentException malformed) {
        observer.onNext(GetattrResponse.newBuilder().setResult(result(NfsStatus.BADHANDLE)).build());
        observer.onCompleted();
        return;
      }
      observer.onError(Status.INTERNAL.withDescription("NFS backend getattr failed").asRuntimeException());
      return;
    } catch (RuntimeException e) {
      observer.onError(Status.INTERNAL.withDescription("NFS backend getattr failed").asRuntimeException());
      return;
    }
    observer.onNext(response);
    observer.onCompleted();
  }

  @Override
  public void lookup(LookupRequest request, StreamObserver<LookupResponse> observer) {
    LookupResponse response;
    try {
      NfsObjectReader reader = readerFor(request.getContext());
      if (!request.isInitialized() || request.getName().isEmpty()) {
        throw Status.INVALID_ARGUMENT.withDescription("Incomplete NFS lookup request").asRuntimeException();
      }
      OzoneFileStatus child = reader.lookupChildren(request.getContext(), request.getParentHandle(),
          request.getName(), null, null, null).getChildren().get(0);
      ByteString handle = reader.handleFor(child);
      response = LookupResponse.newBuilder().setResult(result(NfsStatus.OK))
          .setAttributes(toAttributes(handle, child)).build();
    } catch (StatusRuntimeException e) {
      observer.onError(e);
      return;
    } catch (OMException e) {
      response = LookupResponse.newBuilder().setResult(result(lookupStatus(e))).build();
    } catch (IOException e) {
      response = LookupResponse.newBuilder().setResult(result(NfsStatus.fromIOException(e))).build();
    } catch (IllegalArgumentException e) {
      try {
        NfsFileHandle.fromBytes(request.getParentHandle().toByteArray());
      } catch (IllegalArgumentException malformed) {
        observer.onNext(LookupResponse.newBuilder().setResult(result(NfsStatus.BADHANDLE)).build());
        observer.onCompleted();
        return;
      }
      observer.onError(Status.INTERNAL.withDescription("NFS backend lookup failed").asRuntimeException());
      return;
    } catch (RuntimeException e) {
      observer.onError(Status.INTERNAL.withDescription("NFS backend lookup failed").asRuntimeException());
      return;
    }
    observer.onNext(response);
    observer.onCompleted();
  }

  @Override
  public void create(CreateRequest request, StreamObserver<CreateResponse> observer) {
    CreateResponse response;
    try {
      NfsObjectReader reader = readerFor(request.getContext());
      if (!request.isInitialized()) {
        throw Status.INVALID_ARGUMENT.withDescription("Incomplete NFS create request").asRuntimeException();
      }
      if (leaseManager == null) {
        throw Status.FAILED_PRECONDITION.withDescription("NFS file creation requires an active lease manager")
            .asRuntimeException();
      }
      String name = request.getName();
      if (name.isEmpty() || name.equals(".") || name.equals("..") || name.indexOf('/') >= 0
          || name.indexOf('\0') >= 0 || name.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 255) {
        observer.onNext(CreateResponse.newBuilder().setResult(result(NfsStatus.BADNAME)).build());
        observer.onCompleted();
        return;
      }
      String transferId = java.util.UUID.randomUUID().toString();
      NfsFileCreateResponse lease = leaseManager.createFile(request.getContext().getExportUuid(),
          request.getContext(), request.getParentHandle(), name, transferId);
      ByteString handle = reader.handleForObjectId(lease.getObjectID());
      long now = System.currentTimeMillis();
      Attributes attributes = Attributes.newBuilder().setHandle(handle).setDirectory(false)
          .setObjectId(lease.getObjectID()).setChangeId(0).setSize(0)
          .setModificationTimeMillis(now).setCreationTimeMillis(now).build();
      response = CreateResponse.newBuilder().setResult(result(NfsStatus.OK)).setAttributes(attributes)
          .setTransferId(transferId).build();
    } catch (StatusRuntimeException e) {
      observer.onError(e);
      return;
    } catch (OMException e) {
      LOG.warn("NFS create failed with OM result {}", e.getResult(), e);
      response = CreateResponse.newBuilder().setResult(result(NfsStatus.fromOmException(e,
          NfsStatus.LookupContext.NAME))).build();
    } catch (IOException e) {
      LOG.warn("NFS create failed", e);
      response = CreateResponse.newBuilder().setResult(result(NfsStatus.fromIOException(e))).build();
    } catch (IllegalArgumentException e) {
      try {
        NfsFileHandle.fromBytes(request.getParentHandle().toByteArray());
      } catch (IllegalArgumentException malformed) {
        observer.onNext(CreateResponse.newBuilder().setResult(result(NfsStatus.BADHANDLE)).build());
        observer.onCompleted();
        return;
      }
      observer.onError(Status.INTERNAL.withDescription("NFS backend create failed").asRuntimeException());
      return;
    } catch (RuntimeException e) {
      LOG.warn("NFS create failed unexpectedly", e);
      observer.onError(Status.INTERNAL.withDescription("NFS backend create failed").asRuntimeException());
      return;
    }
    observer.onNext(response);
    observer.onCompleted();
  }

  @Override
  public void mkdir(MkdirRequest request, StreamObserver<MkdirResponse> observer) {
    MkdirResponse response;
    try {
      NfsObjectReader reader = readerFor(request.getContext());
      if (!request.isInitialized()) {
        throw Status.INVALID_ARGUMENT.withDescription("Incomplete NFS mkdir request").asRuntimeException();
      }
      OzoneFileStatus directory = reader.createDirectory(request.getContext(), request.getParentHandle(),
          request.getName());
      response = MkdirResponse.newBuilder().setResult(result(NfsStatus.OK))
          .setAttributes(toAttributes(reader.handleFor(directory), directory)).build();
    } catch (StatusRuntimeException e) {
      observer.onError(e);
      return;
    } catch (OMException e) {
      response = MkdirResponse.newBuilder().setResult(result(NfsStatus.fromOmException(e,
          NfsStatus.LookupContext.HANDLE))).build();
    } catch (IOException e) {
      response = MkdirResponse.newBuilder().setResult(result(NfsStatus.fromIOException(e))).build();
    } catch (IllegalArgumentException e) {
      response = MkdirResponse.newBuilder().setResult(result(NfsStatus.BADHANDLE)).build();
    } catch (RuntimeException e) {
      observer.onError(Status.INTERNAL.withDescription("NFS backend mkdir failed").asRuntimeException());
      return;
    }
    observer.onNext(response);
    observer.onCompleted();
  }

  @Override
  public void remove(RemoveRequest request, StreamObserver<RemoveResponse> observer) {
    RemoveResponse response;
    try {
      NfsObjectReader reader = readerFor(request.getContext());
      if (!request.isInitialized()) {
        throw Status.INVALID_ARGUMENT.withDescription("Incomplete NFS remove request").asRuntimeException();
      }
      reader.remove(request.getContext(), request.getParentHandle(), request.getObjectHandle(), request.getName());
      response = RemoveResponse.newBuilder().setResult(result(NfsStatus.OK)).build();
    } catch (StatusRuntimeException e) {
      observer.onError(e);
      return;
    } catch (OMException e) {
      response = RemoveResponse.newBuilder().setResult(result(NfsStatus.fromOmException(e,
          NfsStatus.LookupContext.HANDLE))).build();
    } catch (IOException e) {
      response = RemoveResponse.newBuilder().setResult(result(NfsStatus.fromIOException(e))).build();
    } catch (IllegalArgumentException e) {
      response = RemoveResponse.newBuilder().setResult(result(NfsStatus.BADHANDLE)).build();
    } catch (RuntimeException e) {
      observer.onError(Status.INTERNAL.withDescription("NFS backend remove failed").asRuntimeException());
      return;
    }
    observer.onNext(response);
    observer.onCompleted();
  }

  @Override
  public void rename(RenameRequest request, StreamObserver<RenameResponse> observer) {
    RenameResponse response;
    try {
      NfsObjectReader reader = readerFor(request.getContext());
      if (!request.isInitialized()) {
        throw Status.INVALID_ARGUMENT.withDescription("Incomplete NFS rename request").asRuntimeException();
      }
      reader.rename(request.getContext(), request.getObjectHandle(), request.getOldParentHandle(),
          request.getOldName(), request.getNewParentHandle(), request.getNewName());
      response = RenameResponse.newBuilder().setResult(result(NfsStatus.OK)).build();
    } catch (StatusRuntimeException e) {
      observer.onError(e);
      return;
    } catch (OMException e) {
      response = RenameResponse.newBuilder().setResult(result(NfsStatus.fromOmException(e,
          NfsStatus.LookupContext.HANDLE))).build();
    } catch (IOException e) {
      response = RenameResponse.newBuilder().setResult(result(NfsStatus.fromIOException(e))).build();
    } catch (IllegalArgumentException e) {
      response = RenameResponse.newBuilder().setResult(result(NfsStatus.BADHANDLE)).build();
    } catch (RuntimeException e) {
      observer.onError(Status.INTERNAL.withDescription("NFS backend rename failed").asRuntimeException());
      return;
    }
    observer.onNext(response);
    observer.onCompleted();
  }

  @Override
  public void readdir(ReaddirRequest request, StreamObserver<ReaddirResponse> observer) {
    ReaddirResponse response;
    try {
      NfsObjectReader reader = readerFor(request.getContext());
      if (!request.isInitialized() || request.getMaxEntries() == 0 || request.getMaxEntries() > MAX_DIRECTORY_ENTRIES) {
        throw Status.INVALID_ARGUMENT.withDescription("Invalid NFS readdir request").asRuntimeException();
      }
      boolean hasCookie = request.hasCookie() && !request.getCookie().isEmpty();
      if ((request.hasVerifier() && (!hasCookie || request.getVerifier().size() != Long.BYTES))
          || (hasCookie && request.getCookie().size() != Long.BYTES)) {
        throw Status.INVALID_ARGUMENT.withDescription("Invalid NFS readdir cookie").asRuntimeException();
      }
      Long startAfterObjectId = hasCookie ? ByteBuffer.wrap(request.getCookie().toByteArray()).getLong() - 3 : null;
      Long verifier = request.hasVerifier()
          ? ByteBuffer.wrap(request.getVerifier().toByteArray()).getLong() : null;
      org.apache.hadoop.ozone.nfs.NfsChildrenPage page = reader.lookupChildren(request.getContext(),
          request.getDirectoryHandle(), null, (int) request.getMaxEntries(), startAfterObjectId, verifier);
      ReaddirResponse.Builder builder = ReaddirResponse.newBuilder().setResult(result(NfsStatus.OK))
          .setVerifier(ByteString.copyFrom(ByteBuffer.allocate(Long.BYTES).putLong(page.getVerifier()).array()))
          .setEof(page.isEof());
      for (OzoneFileStatus child : page.getChildren()) {
        ByteString handle = reader.handleFor(child);
        Attributes attributes = toAttributes(handle, child);
        String name = child.getKeyInfo().getFileName();
        long cookie = child.getKeyInfo().getObjectID() + 3;
        builder.addEntries(DirectoryEntry.newBuilder().setName(name).setAttributes(attributes)
            .setCookie(ByteString.copyFrom(ByteBuffer.allocate(Long.BYTES).putLong(cookie).array())));
      }
      response = builder.build();
    } catch (StatusRuntimeException e) {
      observer.onError(e);
      return;
    } catch (OMException e) {
      response = ReaddirResponse.newBuilder().setResult(result(readdirStatus(e))).build();
    } catch (IOException e) {
      response = ReaddirResponse.newBuilder().setResult(result(NfsStatus.fromIOException(e))).build();
    } catch (IllegalArgumentException e) {
      try {
        NfsFileHandle.fromBytes(request.getDirectoryHandle().toByteArray());
      } catch (IllegalArgumentException malformed) {
        observer.onNext(ReaddirResponse.newBuilder().setResult(result(NfsStatus.BADHANDLE)).build());
        observer.onCompleted();
        return;
      }
      observer.onError(Status.INTERNAL.withDescription("NFS backend readdir failed").asRuntimeException());
      return;
    } catch (RuntimeException e) {
      observer.onError(Status.INTERNAL.withDescription("NFS backend readdir failed").asRuntimeException());
      return;
    }
    observer.onNext(response);
    observer.onCompleted();
  }

  private static Attributes toAttributes(ByteString encodedHandle, OzoneFileStatus status) {
    NfsFileHandle handle = NfsFileHandle.fromBytes(encodedHandle.toByteArray());
    Attributes.Builder attributes = Attributes.newBuilder().setHandle(encodedHandle)
        .setDirectory(status.isDirectory()).setObjectId(handle.getObjectId());
    OmKeyInfo keyInfo = status.getKeyInfo();
    if (keyInfo != null) {
      attributes.setChangeId(keyInfo.getUpdateID()).setSize(keyInfo.getDataSize())
          .setModificationTimeMillis(keyInfo.getModificationTime())
          .setCreationTimeMillis(keyInfo.getCreationTime());
      if (keyInfo.getOwnerName() != null) {
        attributes.setOwner(keyInfo.getOwnerName());
      }
    } else {
      attributes.setChangeId(0).setSize(0).setModificationTimeMillis(0).setCreationTimeMillis(0);
    }
    return attributes.build();
  }

  private static NfsStatus lookupStatus(OMException exception) {
    if (exception.getResult() == OMException.ResultCodes.NOT_A_FILE) {
      return NfsStatus.NOTDIR;
    }
    return NfsStatus.fromOmException(exception,
        exception.getResult() == OMException.ResultCodes.FILE_NOT_FOUND
            ? NfsStatus.LookupContext.HANDLE : NfsStatus.LookupContext.NAME);
  }

  private static NfsStatus readdirStatus(OMException exception) {
    if (exception.getResult() == OMException.ResultCodes.NOT_A_FILE) {
      return NfsStatus.NOTDIR;
    }
    if (exception.getResult() == OMException.ResultCodes.INVALID_REQUEST) {
      return NfsStatus.BAD_COOKIE;
    }
    return NfsStatus.fromOmException(exception, NfsStatus.LookupContext.HANDLE);
  }

  private static ReadResponse failure(NfsStatus status) {
    return ReadResponse.newBuilder().setResult(result(status)).build();
  }

  private static Result result(NfsStatus status) {
    return Result.newBuilder().setNfsStatus(status.getCode()).build();
  }

  private NfsObjectReader readerFor(RequestContext context) {
    return readerFor(context.getExportUuid());
  }

  private NfsObjectReader readerFor(ByteString exportUuid) {
    NfsTlsPeerInterceptor.requireExport(exportUuid);
    NfsObjectReader reader = readers.get(exportUuid);
    if (reader == null) {
      throw Status.PERMISSION_DENIED.withDescription("NFS export is not configured")
          .asRuntimeException();
    }
    if (leaseManager != null && !leaseManager.isActive(exportUuid)) {
      throw Status.UNAVAILABLE.withDescription("NFS export lease is not active").asRuntimeException();
    }
    return reader;
  }
}
