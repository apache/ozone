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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.io.LengthInputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.nfs.NfsChildrenPage;
import org.apache.hadoop.ozone.nfs.NfsExportBinding;
import org.apache.hadoop.ozone.nfs.NfsFileHandle;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.ReadResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.RequestContext;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.Result;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.protocolPB.OmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsTransferRangesRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsTransferRangesResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsExportLeaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsExportLeaseRequest.Operation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsExportLeaseResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileCommitRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileCreateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileLeaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileLeaseResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeResponse;
import org.apache.hadoop.security.UserGroupInformation;

/** Resolve persistent handles through OM under the authorized NFS caller's identity. */
public final class NfsObjectReader {
  public static final int MAX_READ_BYTES = 1024 * 1024;
  private final ConfigurationSource configuration;
  private final String omServiceId;
  private final String connectedClusterId;
  private final NfsExportBinding binding;
  private final NfsCallerAuthorizer authorizer;
  private final OmTransportFactory transportFactory;

  /** The cluster ID must come from trusted backend discovery, never from the export configuration or a caller. */
  public NfsObjectReader(ConfigurationSource configuration, String omServiceId, String connectedClusterId,
      NfsExportBinding binding, NfsCallerAuthorizer authorizer) throws IOException {
    this(configuration, omServiceId, connectedClusterId, binding, authorizer,
        OmTransportFactory.createFactory(configuration));
  }

  NfsObjectReader(ConfigurationSource configuration, String omServiceId, String connectedClusterId,
      NfsExportBinding binding, NfsCallerAuthorizer authorizer, OmTransportFactory transportFactory) {
    this.configuration = Objects.requireNonNull(configuration, "configuration");
    this.omServiceId = omServiceId;
    this.connectedClusterId = Objects.requireNonNull(connectedClusterId, "connectedClusterId");
    this.binding = Objects.requireNonNull(binding, "binding");
    this.authorizer = Objects.requireNonNull(authorizer, "authorizer");
    this.transportFactory = Objects.requireNonNull(transportFactory, "transportFactory");
  }

  /** Head lookups omit block locations; data lookups return only the locations authorized by OM for this identity. */
  public OzoneFileStatus lookup(RequestContext context, ByteString encodedHandle, boolean head) throws IOException {
    UserGroupInformation caller = authorizer.authorize(context);
    NfsFileHandle handle = validateHandle(context, encodedHandle);
    return lookup(caller, handle, head);
  }

  /** Create a new FSO file under a directory handle and atomically acquire its OM writer lease. */
  NfsFileCreateResponse createFile(RequestContext context, ByteString encodedParentHandle, String name,
      String gatewayId, long gatewayGeneration, String transferId, long leaseDurationMillis) throws IOException {
    UserGroupInformation caller = authorizer.authorize(context);
    NfsFileHandle parentHandle = validateHandle(context, encodedParentHandle);
    if (name == null || name.isEmpty() || name.equals(".") || name.equals("..") || name.indexOf('/') >= 0
        || name.indexOf('\0') >= 0 || name.getBytes(StandardCharsets.UTF_8).length > 255) {
      throw new OMException("Invalid NFS file name", OMException.ResultCodes.INVALID_REQUEST);
    }
    if (parentHandle.getKind() != NfsFileHandle.Kind.BUCKET
        && parentHandle.getKind() != NfsFileHandle.Kind.DIRECTORY) {
      throw new OMException("NFS create requires a directory handle", OMException.ResultCodes.NOT_A_FILE);
    }
    String parentPath = "";
    if (parentHandle.getKind() != NfsFileHandle.Kind.BUCKET) {
      OzoneFileStatus parent = lookup(caller, parentHandle, true);
      if (!parent.isDirectory() || parent.getKeyInfo() == null) {
        throw new OMException("NFS create requires a directory handle", OMException.ResultCodes.NOT_A_FILE);
      }
      parentPath = parent.getKeyInfo().getKeyName();
      while (parentPath.endsWith("/")) {
        parentPath = parentPath.substring(0, parentPath.length() - 1);
      }
    }
    String keyName = parentPath.isEmpty() ? name : parentPath + "/" + name;
    NfsFileCreateRequest request = NfsFileCreateRequest.newBuilder().setExportUUID(binding.getExportId().toString())
        .setGatewayID(gatewayId).setGatewayGeneration(gatewayGeneration).setTransferID(transferId)
        .setLeaseDurationMillis(leaseDurationMillis).build();
    try {
      return caller.doAs((PrivilegedExceptionAction<NfsFileCreateResponse>) () -> {
        RpcClient client = new RpcClient(configuration, omServiceId);
        try {
          OzoneBucket bucket = client.getBucketDetails(binding.getVolumeName(), binding.getBucketName());
          RpcClient.NfsFileOutput output = client.createNfsFileWithLease(binding.getVolumeName(),
              binding.getBucketName(), keyName, 0,
              NfsReplicationConfig.forBucket(bucket, configuration), request);
          try (OzoneOutputStream stream = output.getOutputStream()) {
            // Closing publishes the empty file and its object identity before returning the handle.
            stream.flush();
          }
          return output.getLease();
        } finally {
          client.close();
        }
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while creating an NFS file", e);
    }
  }

  public OzoneFileStatus createDirectory(RequestContext context, ByteString encodedParentHandle, String name)
      throws IOException {
    UserGroupInformation caller = authorizer.authorize(context);
    validateName(name);
    NfsFileHandle parentHandle = validateHandle(context, encodedParentHandle);
    OzoneFileStatus parent = lookup(caller, parentHandle, true);
    if (!parent.isDirectory()) {
      throw new OMException("NFS mkdir requires a directory handle", OMException.ResultCodes.NOT_A_FILE);
    }
    String parentKey = parent.getKeyInfo() == null ? "" : parent.getKeyInfo().getKeyName();
    while (parentKey.endsWith("/")) {
      parentKey = parentKey.substring(0, parentKey.length() - 1);
    }
    String directoryKey = parentKey.isEmpty() ? name : parentKey + "/" + name;
    try {
      caller.doAs((PrivilegedExceptionAction<Void>) () -> {
        RpcClient client = new RpcClient(configuration, omServiceId);
        try {
          client.createDirectory(binding.getVolumeName(), binding.getBucketName(), directoryKey);
        } finally {
          client.close();
        }
        return null;
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while creating an NFS directory", e);
    }
    NfsChildrenPage child = lookupChildren(context, encodedParentHandle, name, null, null, null);
    if (child.getChildren().isEmpty()) {
      throw new OMException("NFS directory was not visible after creation", OMException.ResultCodes.FILE_NOT_FOUND);
    }
    return child.getChildren().get(0);
  }

  public void remove(RequestContext context, ByteString encodedParentHandle, ByteString encodedObjectHandle,
      String name) throws IOException {
    UserGroupInformation caller = authorizer.authorize(context);
    validateName(name);
    NfsFileHandle parentHandle = validateHandle(context, encodedParentHandle);
    NfsFileHandle objectHandle = validateHandle(context, encodedObjectHandle);
    OzoneFileStatus parent = lookup(caller, parentHandle, true);
    OzoneFileStatus object = lookup(caller, objectHandle, true);
    if (!parent.isDirectory() || object.getKeyInfo() == null) {
      throw new OMException("NFS remove requires a directory and a named object", OMException.ResultCodes.NOT_A_FILE);
    }
    NfsChildrenPage child = lookupChildren(context, encodedParentHandle, name, null, null, null);
    if (child.getChildren().isEmpty() || child.getChildren().get(0).getKeyInfo().getObjectID()
        != object.getKeyInfo().getObjectID()) {
      throw new OMException("NFS remove target is not the named child", OMException.ResultCodes.FILE_NOT_FOUND);
    }
    String keyName = object.getKeyInfo().getKeyName();
    try {
      caller.doAs((PrivilegedExceptionAction<Void>) () -> {
        RpcClient client = new RpcClient(configuration, omServiceId);
        try {
          client.deleteKey(binding.getVolumeName(), binding.getBucketName(), keyName, false);
        } finally {
          client.close();
        }
        return null;
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while removing an NFS object", e);
    }
  }

  public void rename(RequestContext context, ByteString encodedObjectHandle, ByteString encodedOldParentHandle,
      String oldName, ByteString encodedNewParentHandle, String newName) throws IOException {
    UserGroupInformation caller = authorizer.authorize(context);
    validateName(oldName);
    validateName(newName);
    NfsFileHandle objectHandle = validateHandle(context, encodedObjectHandle);
    NfsFileHandle oldParentHandle = validateHandle(context, encodedOldParentHandle);
    NfsFileHandle newParentHandle = validateHandle(context, encodedNewParentHandle);
    OzoneFileStatus object = lookup(caller, objectHandle, true);
    OzoneFileStatus oldParent = lookup(caller, oldParentHandle, true);
    OzoneFileStatus newParent = lookup(caller, newParentHandle, true);
    if (!oldParent.isDirectory() || !newParent.isDirectory() || object.getKeyInfo() == null) {
      throw new OMException("NFS rename requires a named object and directory handles",
          OMException.ResultCodes.NOT_A_FILE);
    }
    NfsChildrenPage child = lookupChildren(context, encodedOldParentHandle, oldName, null, null, null);
    if (child.getChildren().isEmpty() || child.getChildren().get(0).getKeyInfo().getObjectID()
        != object.getKeyInfo().getObjectID()) {
      throw new OMException("NFS rename source is not the named child", OMException.ResultCodes.FILE_NOT_FOUND);
    }
    String oldKey = object.getKeyInfo().getKeyName();
    String parentKey = newParent.getKeyInfo() == null ? "" : newParent.getKeyInfo().getKeyName();
    while (parentKey.endsWith("/")) {
      parentKey = parentKey.substring(0, parentKey.length() - 1);
    }
    String newKey = parentKey.isEmpty() ? newName : parentKey + "/" + newName;
    try {
      caller.doAs((PrivilegedExceptionAction<Void>) () -> {
        RpcClient client = new RpcClient(configuration, omServiceId);
        try {
          client.renameKey(binding.getVolumeName(), binding.getBucketName(), oldKey, newKey);
        } finally {
          client.close();
        }
        return null;
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while renaming an NFS object", e);
    }
  }

  private static void validateName(String name) throws OMException {
    if (name == null || name.isEmpty() || name.equals(".") || name.equals("..") || name.indexOf('/') >= 0
        || name.indexOf('\0') >= 0 || name.getBytes(StandardCharsets.UTF_8).length > 255) {
      throw new OMException("Invalid NFS object name", OMException.ResultCodes.INVALID_REQUEST);
    }
  }

  void publishStagedFile(RequestContext context, ByteString encodedHandle, NfsFileCommitRequest commitRequest,
      long fileSize, StagedDataReader dataReader) throws IOException {
    UserGroupInformation caller = authorizer.authorize(context);
    NfsFileHandle handle = validateHandle(context, encodedHandle);
    if (handle.getKind() != NfsFileHandle.Kind.FILE || fileSize < 0 || dataReader == null) {
      throw new OMException("Invalid NFS file publication request", OMException.ResultCodes.INVALID_REQUEST);
    }
    OzoneFileStatus file = lookup(caller, handle, true);
    if (file.isDirectory() || file.getKeyInfo() == null) {
      throw new OMException("NFS publication requires a file handle", OMException.ResultCodes.NOT_A_FILE);
    }
    String keyName = file.getKeyInfo().getKeyName();
    try {
      caller.doAs((PrivilegedExceptionAction<Void>) () -> {
        RpcClient client = new RpcClient(configuration, omServiceId);
        try {
          OzoneBucket bucket = client.getBucketDetails(binding.getVolumeName(), binding.getBucketName());
          try (OzoneOutputStream output = client.openNfsFileWithLease(binding.getVolumeName(),
              binding.getBucketName(), keyName, NfsReplicationConfig.forBucket(bucket, configuration),
              commitRequest)) {
            for (long offset = 0; offset < fileSize;) {
              int count = (int) Math.min(MAX_READ_BYTES, fileSize - offset);
              byte[] data = dataReader.read(offset, count);
              if (data == null || data.length != count) {
                throw new IOException("NFS staged file assembly returned an incomplete range");
              }
              output.write(data);
              offset += count;
            }
          }
        } finally {
          client.close();
        }
        return null;
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while publishing an NFS file", e);
    }
  }

  private OzoneFileStatus lookup(UserGroupInformation caller, NfsFileHandle handle, boolean head) throws IOException {
    OmKeyArgs args = new OmKeyArgs.Builder().setVolumeName(binding.getVolumeName())
        .setBucketName(binding.getBucketName()).setKeyName("").setHeadOp(head).setLatestVersionLocation(true).build();
    try {
      return caller.doAs((PrivilegedExceptionAction<OzoneFileStatus>) () -> {
        try (OzoneManagerProtocolClientSideTranslatorPB client = new OzoneManagerProtocolClientSideTranslatorPB(
            transportFactory.createOmTransport(configuration, caller, omServiceId), UUID.randomUUID().toString())) {
          return client.lookupNfsObject(args, handle.getVolumeId(), handle.getBucketId(), handle.getObjectId(),
              handle.getKind());
        }
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while resolving an NFS object", e);
    }
  }

  /** Return a bounded range from the authorized file identity; the unsigned offset may be beyond EOF. */
  public ReadResponse read(RequestContext context, ByteString encodedHandle, long offset, int count)
      throws IOException {
    UserGroupInformation caller = authorizer.authorize(context);
    NfsFileHandle handle = validateHandle(context, encodedHandle);
    if (Integer.compareUnsigned(count, MAX_READ_BYTES) > 0) {
      throw new OMException("NFS read count exceeds the backend limit", OMException.ResultCodes.INVALID_REQUEST);
    }
    if (handle.getKind() != NfsFileHandle.Kind.FILE) {
      lookup(caller, handle, true);
      throw new OMException("NFS read requires a file handle", OMException.ResultCodes.NOT_A_FILE);
    }
    try {
      return caller.doAs((PrivilegedExceptionAction<ReadResponse>) () -> {
        RpcClient client = new RpcClient(configuration, omServiceId);
        try (LengthInputStream input = client.readNfsFile(binding.getVolumeName(), binding.getBucketName(),
            handle.getVolumeId(), handle.getBucketId(), handle.getObjectId())) {
          long length = input.getLength();
          if (Long.compareUnsigned(offset, length) >= 0) {
            return readResponse(ByteString.EMPTY, true);
          }
          int bytesToRead = (int) Math.min(count, length - offset);
          if (bytesToRead == 0) {
            return readResponse(ByteString.EMPTY, false);
          }
          OzoneInputStream stream = (OzoneInputStream) input.getWrappedStream();
          stream.seek(offset);
          byte[] data = new byte[bytesToRead];
          IOUtils.readFully(stream, data, 0, data.length);
          return readResponse(ByteString.copyFrom(data), offset + bytesToRead == length);
        } finally {
          client.close();
        }
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while reading an NFS object", e);
    }
  }

  long validateWrite(RequestContext context, ByteString encodedHandle) throws IOException {
    authorizer.authorize(context);
    NfsFileHandle handle = validateHandle(context, encodedHandle);
    if (handle.getKind() != NfsFileHandle.Kind.FILE) {
      throw new OMException("NFS write requires a file handle", OMException.ResultCodes.NOT_A_FILE);
    }
    return handle.getObjectId();
  }

  String getCallerUserName(RequestContext context) throws IOException {
    return authorizer.authorize(context).getShortUserName();
  }

  /** Resolve a direct child or fetch a bounded page under the caller's OM identity. */
  public NfsChildrenPage lookupChildren(RequestContext context, ByteString encodedHandle, String name,
      Integer maxEntries, Long startAfterObjectId, Long verifier) throws IOException {
    UserGroupInformation caller = authorizer.authorize(context);
    NfsFileHandle handle = validateHandle(context, encodedHandle);
    if (handle.getKind() == NfsFileHandle.Kind.FILE) {
      lookup(caller, handle, true);
      throw new OMException("NFS child lookup requires a directory handle", OMException.ResultCodes.NOT_A_FILE);
    }
    try {
      return caller.doAs((PrivilegedExceptionAction<NfsChildrenPage>) () -> {
        try (OzoneManagerProtocolClientSideTranslatorPB om = new OzoneManagerProtocolClientSideTranslatorPB(
            transportFactory.createOmTransport(configuration, caller, omServiceId), UUID.randomUUID().toString())) {
          OmKeyArgs args = new OmKeyArgs.Builder().setVolumeName(binding.getVolumeName())
              .setBucketName(binding.getBucketName()).setKeyName("").setHeadOp(true).build();
          NfsChildrenPage page = om.lookupNfsChildren(args, handle.getVolumeId(), handle.getBucketId(),
              handle.getObjectId(), handle.getKind(), name, maxEntries, startAfterObjectId, verifier);
          return page;
        }
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while resolving NFS directory children", e);
    }
  }

  /** Build a persistent handle for a live child returned by OM. */
  public ByteString handleFor(OzoneFileStatus status) {
    OmKeyInfo keyInfo = status.getKeyInfo();
    if (keyInfo == null) {
      throw new IllegalArgumentException("NFS child status has no object identity");
    }
    NfsFileHandle.Kind kind = status.isDirectory() ? NfsFileHandle.Kind.DIRECTORY : NfsFileHandle.Kind.FILE;
    return ByteString.copyFrom(new NfsFileHandle(binding.getExportId(), binding.getVolumeId(), binding.getBucketId(),
        keyInfo.getObjectID(), kind).toBytes());
  }

  ByteString handleForObjectId(long objectId) {
    return ByteString.copyFrom(new NfsFileHandle(binding.getExportId(), binding.getVolumeId(), binding.getBucketId(),
        objectId, NfsFileHandle.Kind.FILE).toBytes());
  }

  public ByteString getExportUuid() {
    UUID exportId = binding.getExportId();
    return ByteString.copyFrom(ByteBuffer.allocate(16)
        .putLong(exportId.getMostSignificantBits()).putLong(exportId.getLeastSignificantBits()).array());
  }

  NfsExportLeaseResponse updateExportLease(Operation operation, String gatewayId, long durationMillis,
      Long gatewayGeneration, Long generation) throws IOException {
    NfsExportLeaseRequest.Builder request = NfsExportLeaseRequest.newBuilder().setOperation(operation)
        .setExportUUID(binding.getExportId().toString()).setVolumeName(binding.getVolumeName())
        .setBucketName(binding.getBucketName()).setGatewayID(gatewayId);
    if (operation != Operation.RELEASE) {
      request.setLeaseDurationMillis(durationMillis);
    }
    if (gatewayGeneration != null) {
      request.setGatewayGeneration(gatewayGeneration);
    }
    if (generation != null) {
      request.setGeneration(generation);
    }
    UserGroupInformation gatewayUser = authorizer.getGatewayUser();
    try {
      return gatewayUser.doAs((PrivilegedExceptionAction<NfsExportLeaseResponse>) () -> {
        try (OzoneManagerProtocolClientSideTranslatorPB om = new OzoneManagerProtocolClientSideTranslatorPB(
            transportFactory.createOmTransport(configuration, gatewayUser, omServiceId),
            UUID.randomUUID().toString())) {
          return om.updateNfsExportLease(request.build());
        }
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while updating the NFS export lease", e);
    }
  }

  @SuppressWarnings("checkstyle:parameternumber")
  NfsFileLeaseResponse updateFileLease(NfsFileLeaseRequest.Operation operation, String gatewayId,
      long gatewayGeneration, long objectId, String transferId, long generation, ByteString ownerToken,
      long durationMillis) throws IOException {
    return updateFileLease(operation, gatewayId, gatewayGeneration, objectId, transferId, generation,
        ownerToken, durationMillis, null);
  }

  @SuppressWarnings("checkstyle:parameternumber")
  NfsFileLeaseResponse updateFileLease(NfsFileLeaseRequest.Operation operation, String gatewayId,
      long gatewayGeneration, long objectId, String transferId, long generation, ByteString ownerToken,
      long durationMillis, String requesterPrincipal) throws IOException {
    NfsFileLeaseRequest.Builder request = NfsFileLeaseRequest.newBuilder().setOperation(operation)
        .setExportUUID(binding.getExportId().toString()).setVolumeName(binding.getVolumeName())
        .setBucketName(binding.getBucketName()).setObjectID(objectId).setGatewayID(gatewayId)
        .setGatewayGeneration(gatewayGeneration).setTransferID(transferId).setGeneration(generation)
        .setOwnerToken(ownerToken);
    if (operation != NfsFileLeaseRequest.Operation.RELEASE) {
      request.setLeaseDurationMillis(durationMillis);
    }
    if (requesterPrincipal != null) {
      request.setRequesterPrincipal(requesterPrincipal);
    }
    UserGroupInformation gatewayUser = authorizer.getGatewayUser();
    try {
      return gatewayUser.doAs((PrivilegedExceptionAction<NfsFileLeaseResponse>) () -> {
        try (OzoneManagerProtocolClientSideTranslatorPB om = new OzoneManagerProtocolClientSideTranslatorPB(
            transportFactory.createOmTransport(configuration, gatewayUser, omServiceId),
            UUID.randomUUID().toString())) {
          return om.updateNfsFileLease(request.build());
        }
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while updating the NFS file lease", e);
    }
  }

  @SuppressWarnings("checkstyle:parameternumber")
  NfsTransferRangeResponse updateTransferRange(NfsTransferRangeRequest.Operation operation, String gatewayId,
      long gatewayGeneration, long objectId, String transferId, long leaseGeneration, ByteString ownerToken,
      long offset, long length, String stagingKey, ByteString sha256) throws IOException {
    NfsTransferRangeRequest request = NfsTransferRangeRequest.newBuilder().setOperation(operation)
        .setVolumeName(binding.getVolumeName()).setBucketName(binding.getBucketName())
        .setExportUUID(binding.getExportId().toString()).setObjectID(objectId).setGatewayID(gatewayId)
        .setGatewayGeneration(gatewayGeneration).setTransferID(transferId).setLeaseGeneration(leaseGeneration)
        .setOwnerToken(ownerToken).setOffset(offset).setLength(length).setStagingKey(stagingKey)
        .setSha256(sha256).build();
    UserGroupInformation gatewayUser = authorizer.getGatewayUser();
    try {
      return gatewayUser.doAs((PrivilegedExceptionAction<NfsTransferRangeResponse>) () -> {
        try (OzoneManagerProtocolClientSideTranslatorPB om = new OzoneManagerProtocolClientSideTranslatorPB(
            transportFactory.createOmTransport(configuration, gatewayUser, omServiceId),
            UUID.randomUUID().toString())) {
          return om.updateNfsTransferRange(request);
        }
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while updating an NFS transfer range", e);
    }
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  LookupNfsTransferRangesResponse lookupTransferRanges(String gatewayId, long gatewayGeneration, long objectId,
      String transferId, long leaseGeneration, ByteString ownerToken, String principal, String startAfterKey)
      throws IOException {
    LookupNfsTransferRangesRequest.Builder request = LookupNfsTransferRangesRequest.newBuilder()
        .setVolumeName(binding.getVolumeName()).setBucketName(binding.getBucketName())
        .setVolumeID(binding.getVolumeId()).setBucketID(binding.getBucketId()).setObjectID(objectId)
        .setExportUUID(binding.getExportId().toString()).setGatewayID(gatewayId)
        .setGatewayGeneration(gatewayGeneration).setTransferID(transferId).setLeaseGeneration(leaseGeneration)
        .setOwnerToken(ownerToken).setRequesterPrincipal(principal).setMaxEntries(256);
    if (startAfterKey != null && !startAfterKey.isEmpty()) {
      request.setStartAfterKey(startAfterKey);
    }
    UserGroupInformation gatewayUser = authorizer.getGatewayUser();
    try {
      return gatewayUser.doAs((PrivilegedExceptionAction<LookupNfsTransferRangesResponse>) () -> {
        try (OzoneManagerProtocolClientSideTranslatorPB om = new OzoneManagerProtocolClientSideTranslatorPB(
            transportFactory.createOmTransport(configuration, gatewayUser, omServiceId),
            UUID.randomUUID().toString())) {
          return om.lookupNfsTransferRanges(request.build());
        }
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while reading NFS transfer ranges", e);
    }
  }

  private NfsFileHandle validateHandle(RequestContext context, ByteString encodedHandle) throws OMException {
    UUID exportId = binding.getExportId();
    ByteString expectedExport = ByteString.copyFrom(ByteBuffer.allocate(16)
        .putLong(exportId.getMostSignificantBits()).putLong(exportId.getLeastSignificantBits()).array());
    if (!expectedExport.equals(context.getExportUuid())) {
      throw new OMException("NFS request does not match the export binding", OMException.ResultCodes.FILE_NOT_FOUND);
    }
    NfsFileHandle handle = NfsFileHandle.fromBytes(encodedHandle.toByteArray());
    binding.validateHandle(connectedClusterId, handle);
    return handle;
  }

  private static ReadResponse readResponse(ByteString data, boolean eof) {
    return ReadResponse.newBuilder().setResult(Result.newBuilder().setNfsStatus(0)).setData(data).setEof(eof).build();
  }

  @FunctionalInterface
  interface StagedDataReader {
    byte[] read(long offset, int count) throws IOException;
  }

  NfsStagingStore.StagedRange stageRangeObject(String transferId, long offset, byte[] data) throws IOException {
    String stagingVolume = configuration.getTrimmed(NfsGatewayConfigKeys.STAGING_VOLUME);
    String stagingBucket = configuration.getTrimmed(NfsGatewayConfigKeys.STAGING_BUCKET);
    if (binding.getVolumeName().equals(stagingVolume) && binding.getBucketName().equals(stagingBucket)) {
      throw new IOException("NFS staging bucket must be outside the exported bucket");
    }
    NfsStagingStore store = new NfsStagingStore(stagingVolume, stagingBucket, authorizer.getGatewayUser(),
        configuration, () -> new RpcClient(configuration, omServiceId));
    return store.stage(binding.getExportId().toString(), transferId, offset, data);
  }

  byte[] readStagedRange(String stagingKey) throws IOException {
    String stagingVolume = configuration.getTrimmed(NfsGatewayConfigKeys.STAGING_VOLUME);
    String stagingBucket = configuration.getTrimmed(NfsGatewayConfigKeys.STAGING_BUCKET);
    NfsStagingStore store = new NfsStagingStore(stagingVolume, stagingBucket, authorizer.getGatewayUser(),
        configuration, () -> new RpcClient(configuration, omServiceId));
    return store.read(stagingKey);
  }

  void deleteStagedRange(String stagingKey) throws IOException {
    String stagingVolume = configuration.getTrimmed(NfsGatewayConfigKeys.STAGING_VOLUME);
    String stagingBucket = configuration.getTrimmed(NfsGatewayConfigKeys.STAGING_BUCKET);
    NfsStagingStore store = new NfsStagingStore(stagingVolume, stagingBucket, authorizer.getGatewayUser(),
        configuration, () -> new RpcClient(configuration, omServiceId));
    store.delete(stagingKey);
  }

  void deleteStagedTransfer(String transferId) throws IOException {
    String stagingVolume = configuration.getTrimmed(NfsGatewayConfigKeys.STAGING_VOLUME);
    String stagingBucket = configuration.getTrimmed(NfsGatewayConfigKeys.STAGING_BUCKET);
    NfsStagingStore store = new NfsStagingStore(stagingVolume, stagingBucket, authorizer.getGatewayUser(),
        configuration, () -> new RpcClient(configuration, omServiceId));
    store.deleteTransfer(binding.getExportId().toString(), transferId);
  }
}
