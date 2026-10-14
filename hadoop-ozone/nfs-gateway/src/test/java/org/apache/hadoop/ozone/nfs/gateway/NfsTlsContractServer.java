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
import io.grpc.BindableService;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import java.io.File;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.ozone.nfs.NfsChildrenPage;
import org.apache.hadoop.ozone.nfs.NfsFileHandle;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;

/** Linux transport fixture with mock file data; it never advertises export readiness. */
public final class NfsTlsContractServer {
  private NfsTlsContractServer() {
  }

  @SuppressWarnings("checkstyle:MethodLength")
  public static void main(String[] args) throws Exception {
    if (args.length != 6 && args.length != 7) {
      throw new IllegalArgumentException("Expected server cert/key, CA, allowed client cert, port file, and stop file");
    }
    X509Certificate allowed;
    try (InputStream input = Files.newInputStream(java.nio.file.Paths.get(args[3]))) {
      allowed = (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(input);
    }
    UUID export = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    java.util.concurrent.atomic.AtomicBoolean fileCreated = new java.util.concurrent.atomic.AtomicBoolean();
    AtomicReference<byte[]> stagedFile = new AtomicReference<>(new byte[0]);
    AtomicReference<byte[]> publishedFile = new AtomicReference<>(new byte[0]);
    org.mockito.Mockito.when(reader.getCallerUserName(org.mockito.ArgumentMatchers.any())).thenAnswer(invocation -> {
      org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.RequestContext context =
          invocation.getArgument(0);
      String principal = context.getCaller().getKerberosPrincipal();
      return principal.substring(0, principal.indexOf('@'));
    });
    org.mockito.Mockito.when(reader.read(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(),
        org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyInt())).thenAnswer(invocation -> {
          org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.RequestContext context =
              invocation.getArgument(0);
          String principal = context.getCaller().getKerberosPrincipal();
          if (!"alice@EXAMPLE.COM".equals(principal)
              && !"bob@EXAMPLE.COM".equals(principal)) {
            throw Status.UNAUTHENTICATED.withDescription("Test requires an authenticated Kerberos caller")
                .asRuntimeException();
          }
          if (context.getCaller().getProtection()
              != org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.Caller.Protection.PRIVACY) {
            throw Status.UNAUTHENTICATED.withDescription("Test requires privacy protection")
                .asRuntimeException();
          }
          if ("bob@EXAMPLE.COM".equals(principal)) {
            throw new org.apache.hadoop.ozone.om.exceptions.OMException("Permission denied",
                org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PERMISSION_DENIED);
          }
          ByteString encodedHandle = invocation.getArgument(1);
          NfsFileHandle handle = NfsFileHandle.fromBytes(encodedHandle.toByteArray());
          if (!export.equals(handle.getExportId())) {
            throw new org.apache.hadoop.ozone.om.exceptions.OMException("Wrong export",
                org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND);
          }
          if (handle.getObjectId() != 3 && handle.getObjectId() != 4) {
            throw new org.apache.hadoop.ozone.om.exceptions.OMException("Stale handle",
                org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND);
          }
          long offset = invocation.getArgument(2);
          int count = invocation.getArgument(3);
          byte[] file = handle.getObjectId() == 4 ? publishedFile.get()
              : "native READ interop".getBytes(StandardCharsets.UTF_8);
          if (Long.compareUnsigned(offset, file.length) >= 0) {
            return org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.ReadResponse.newBuilder()
                .setResult(org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.Result.newBuilder()
                    .setNfsStatus(0)).setData(ByteString.EMPTY).setEof(true).build();
          }
          int start = (int) offset;
          int length = Math.min(count, file.length - start);
          return org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.ReadResponse.newBuilder()
              .setResult(org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.Result.newBuilder()
                  .setNfsStatus(0)).setData(ByteString.copyFrom(file, start, length))
              .setEof(start + length == file.length).build();
        });
    org.mockito.Mockito.when(reader.lookup(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(),
        org.mockito.ArgumentMatchers.anyBoolean())).thenAnswer(invocation -> {
          org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.RequestContext context =
              invocation.getArgument(0);
          ByteString encodedHandle = invocation.getArgument(1);
          NfsFileHandle handle = NfsFileHandle.fromBytes(encodedHandle.toByteArray());
          if (!export.equals(handle.getExportId())) {
            throw new org.apache.hadoop.ozone.om.exceptions.OMException("Wrong export",
                org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND);
          }
          if (handle.getObjectId() == 2 && handle.getKind() == NfsFileHandle.Kind.BUCKET) {
            OzoneFileStatus root = org.mockito.Mockito.mock(OzoneFileStatus.class);
            org.mockito.Mockito.when(root.isDirectory()).thenReturn(true);
            if (!"nfs/localhost@EXAMPLE.COM".equals(context.getCaller().getKerberosPrincipal())) {
              validateCaller(context);
            }
            return root;
          }
          validateCaller(context);
          if (handle.getObjectId() != 3 && handle.getObjectId() != 4) {
            throw new org.apache.hadoop.ozone.om.exceptions.OMException("Stale handle",
                org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND);
          }
          return handle.getObjectId() == 3 ? fileStatus() : fileStatus("created", 4, publishedFile.get().length);
        });
    org.mockito.Mockito.when(reader.lookupChildren(
        org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(),
        org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any()))
        .thenAnswer(invocation -> {
          validateCaller(invocation.getArgument(0));
          String name = invocation.getArgument(2);
          if (name != null && !"child".equals(name) && !"café 文件".equals(name)
              && !(name.startsWith("write-contract-") && fileCreated.get())) {
            throw new org.apache.hadoop.ozone.om.exceptions.OMException("File not found",
                org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND);
          }
          Long startAfterObjectId = invocation.getArgument(4);
          Long verifier = invocation.getArgument(5);
          if (startAfterObjectId != null && (!Long.valueOf(3L).equals(startAfterObjectId)
              || (verifier != null && !Long.valueOf(7L).equals(verifier)))) {
            throw new org.apache.hadoop.ozone.om.exceptions.OMException("Stale NFS cookie",
                org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST);
          }
          java.util.List<OzoneFileStatus> children;
          if (name != null && name.startsWith("write-contract-")) {
            children = java.util.Collections.singletonList(fileStatus(name, 4, publishedFile.get().length));
          } else {
            children = startAfterObjectId == null
                ? java.util.Collections.singletonList(fileStatus()) : java.util.Collections.emptyList();
          }
          return new NfsChildrenPage(children, 7L, true);
        });
    NfsFileHandle fileHandle = new NfsFileHandle(export, 1, 2, 3, NfsFileHandle.Kind.FILE);
    org.mockito.Mockito.when(reader.handleFor(org.mockito.ArgumentMatchers.any()))
        .thenAnswer(invocation -> {
          OzoneFileStatus status = invocation.getArgument(0);
          if (status.isDirectory()) {
            return ByteString.copyFrom(new NfsFileHandle(export, 1, 2, 5,
                NfsFileHandle.Kind.DIRECTORY).toBytes());
          }
          long objectId = status.getKeyInfo().getObjectID();
          return objectId == 4 ? ByteString.copyFrom(new NfsFileHandle(export, 1, 2, 4,
              NfsFileHandle.Kind.FILE).toBytes()) : ByteString.copyFrom(fileHandle.toBytes());
        });
    NfsFileHandle createdHandle = new NfsFileHandle(export, 1, 2, 4, NfsFileHandle.Kind.FILE);
    org.mockito.Mockito.when(reader.handleForObjectId(4)).thenReturn(ByteString.copyFrom(createdHandle.toBytes()));
    configureNamespaceMutations(reader, export);
    NfsExportLeaseManager leaseManager = org.mockito.Mockito.mock(NfsExportLeaseManager.class);
    org.mockito.Mockito.when(leaseManager.isActive(ByteString.copyFrom(uuidBytes(export)))).thenReturn(true);
    org.mockito.Mockito.when(leaseManager.recoverFileLease(org.mockito.ArgumentMatchers.any(),
        org.mockito.ArgumentMatchers.eq(4L), org.mockito.ArgumentMatchers.anyString())).thenReturn(
            org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileLeaseResponse.newBuilder()
                .setGeneration(1).setExpiresAtMillis(9999999999999L)
                .setTransferID("00112233-4455-6677-8899-aabbccddeeff").build());
    org.mockito.Mockito.when(leaseManager.fileCommitRequest(org.mockito.ArgumentMatchers.any(),
        org.mockito.ArgumentMatchers.eq(4L), org.mockito.ArgumentMatchers.anyString())).thenReturn(
            org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileCommitRequest.newBuilder()
                .setExportUUID(export.toString()).setGatewayID("gateway").setGatewayGeneration(1)
                .setTransferID("00112233-4455-6677-8899-aabbccddeeff").setFileGeneration(1)
                .setOwnerToken(ByteString.copyFromUtf8("test-owner")).build());
    org.mockito.Mockito.when(leaseManager.stagedFileSize(org.mockito.ArgumentMatchers.any(),
        org.mockito.ArgumentMatchers.eq(4L), org.mockito.ArgumentMatchers.anyString()))
        .thenAnswer(invocation -> (long) stagedFile.get().length);
    org.mockito.Mockito.when(leaseManager.createFile(
        org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(),
        org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyString())).thenAnswer(invocation -> {
          validateCaller(invocation.getArgument(1));
          fileCreated.set(true);
          return org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileCreateResponse.newBuilder()
              .setObjectID(4).setGatewayGeneration(1).setGeneration(1).setExpiresAtMillis(9999999999999L)
              .setOwnerToken(ByteString.copyFromUtf8("test-owner")).build();
        });
    org.mockito.Mockito.when(reader.validateWrite(org.mockito.ArgumentMatchers.any(),
        org.mockito.ArgumentMatchers.any())).thenReturn(4L);
    org.mockito.Mockito.when(leaseManager.stageRange(org.mockito.ArgumentMatchers.any(),
        org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.eq(4L),
        org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyLong(),
        org.mockito.ArgumentMatchers.any())).thenAnswer(invocation -> {
          byte[] data = invocation.getArgument(5);
          long offset = invocation.getArgument(4);
          if (offset > Integer.MAX_VALUE - data.length) {
            throw new IllegalArgumentException("Test write exceeds in-memory fixture bounds");
          }
          synchronized (stagedFile) {
            byte[] current = stagedFile.get();
            int end = (int) offset + data.length;
            byte[] updated = Arrays.copyOf(current, Math.max(current.length, end));
            System.arraycopy(data, 0, updated, (int) offset, data.length);
            stagedFile.set(updated);
          }
          return org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeResponse
              .newBuilder().setRecorded(data.length > 0).build();
        });
    org.mockito.Mockito.doAnswer(invocation -> {
      long fileSize = invocation.getArgument(3);
      if (fileSize > Integer.MAX_VALUE || fileSize > stagedFile.get().length) {
        throw new java.io.IOException("Test publication exceeds staged fixture data");
      }
      publishedFile.set(Arrays.copyOf(stagedFile.get(), (int) fileSize));
      return null;
    }).when(reader).publishStagedFile(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(),
        org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyLong(),
        org.mockito.ArgumentMatchers.any());
    NfsGatewayService service = new NfsGatewayService(Map.of(ByteString.copyFrom(uuidBytes(export)), reader),
        leaseManager);
    BindableService bindableService = service;
    if (args.length == 7) {
      java.nio.file.Path capabilitiesMarker = java.nio.file.Paths.get(args[6]);
      ServerInterceptor markerInterceptor = new ServerInterceptor() {
        @Override
        public <RequestT, ResponseT> ServerCall.Listener<RequestT> interceptCall(
            ServerCall<RequestT, ResponseT> call, Metadata headers,
            ServerCallHandler<RequestT, ResponseT> next) {
          if (call.getMethodDescriptor().getFullMethodName().endsWith("/Capabilities")) {
            try {
              Files.write(capabilitiesMarker, new byte[] {1});
            } catch (java.io.IOException ex) {
              throw new IllegalStateException("Unable to write capabilities marker", ex);
            }
          }
          return next.startCall(call, headers);
        }
      };
      bindableService = new BindableService() {
        @Override
        public ServerServiceDefinition bindService() {
          return ServerInterceptors.intercept(service, markerInterceptor);
        }
      };
    }
    try (NfsGatewayServer server = new NfsGatewayServer(new InetSocketAddress("127.0.0.1", 0),
        new File(args[0]), new File(args[1]), new File(args[2]),
        Map.of(NfsTlsPeerInterceptor.fingerprint(allowed), Set.of(export)), bindableService)) {
      Files.write(java.nio.file.Paths.get(args[4]),
          Integer.toString(server.getPort()).getBytes(StandardCharsets.UTF_8));
      long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(10);
      while (!Files.exists(java.nio.file.Paths.get(args[5])) && System.nanoTime() < deadline) {
        Thread.sleep(100);
      }
    }
  }

  private static byte[] uuidBytes(UUID uuid) {
    return java.nio.ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits())
        .putLong(uuid.getLeastSignificantBits()).array();
  }

  private static void configureNamespaceMutations(NfsObjectReader reader, UUID export) throws Exception {
    NfsFileHandle directoryHandle = new NfsFileHandle(export, 1, 2, 5, NfsFileHandle.Kind.DIRECTORY);
    OzoneFileStatus createdDirectory = org.mockito.Mockito.mock(OzoneFileStatus.class);
    org.mockito.Mockito.when(createdDirectory.isDirectory()).thenReturn(true);
    org.mockito.Mockito.when(reader.createDirectory(org.mockito.ArgumentMatchers.any(),
        org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq("new-directory")))
        .thenAnswer(invocation -> {
          validateCaller(invocation.getArgument(0));
          return createdDirectory;
        });
    org.mockito.Mockito.when(reader.handleFor(createdDirectory))
        .thenReturn(ByteString.copyFrom(directoryHandle.toBytes()));
    org.mockito.Mockito.doAnswer(invocation -> {
      validateCaller(invocation.getArgument(0));
      return null;
    }).when(reader).remove(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(),
        org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyString());
    org.mockito.Mockito.doAnswer(invocation -> {
      validateCaller(invocation.getArgument(0));
      return null;
    }).when(reader).rename(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(),
        org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyString(),
        org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyString());
  }

  private static void validateCaller(
      org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.RequestContext context)
      throws org.apache.hadoop.ozone.om.exceptions.OMException {
    String principal = context.getCaller().getKerberosPrincipal();
    if (!"alice@EXAMPLE.COM".equals(principal) && !"bob@EXAMPLE.COM".equals(principal)) {
      throw new org.apache.hadoop.ozone.om.exceptions.OMException("Test requires an authenticated Kerberos caller",
          org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PERMISSION_DENIED);
    }
    if (context.getCaller().getProtection()
        != org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.Caller.Protection.PRIVACY) {
      throw new org.apache.hadoop.ozone.om.exceptions.OMException("Test requires privacy protection",
          org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PERMISSION_DENIED);
    }
    if ("bob@EXAMPLE.COM".equals(principal)) {
      throw new org.apache.hadoop.ozone.om.exceptions.OMException("Permission denied",
          org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PERMISSION_DENIED);
    }
  }

  private static OzoneFileStatus fileStatus() {
    return fileStatus("child", 3);
  }

  private static OzoneFileStatus fileStatus(String keyName, long objectId) {
    return fileStatus(keyName, objectId, 19);
  }

  private static OzoneFileStatus fileStatus(String keyName, long objectId, long dataSize) {
    OmKeyInfo keyInfo = new OmKeyInfo.Builder().setVolumeName("volume").setBucketName("bucket")
        .setKeyName(keyName).setOwnerName("alice").setObjectID(objectId).setUpdateID(9).setDataSize(dataSize)
        .setCreationTime(11).setModificationTime(13).build();
    return new OzoneFileStatus(keyInfo, 1, false);
  }
}
