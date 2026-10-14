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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.client.io.LengthInputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.nfs.NfsExportBinding;
import org.apache.hadoop.ozone.nfs.NfsFileHandle;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.ReadResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.RequestContext;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OmTransportFactory;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsObjectResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneFileStatusProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

class TestNfsObjectReader {
  private final OzoneConfiguration configuration = new OzoneConfiguration();
  private final NfsExportBinding binding = new NfsExportBinding(UUID.randomUUID(), "cluster", "vol", "bucket", 1, 2);
  private final NfsCallerAuthorizer authorizer = mock(NfsCallerAuthorizer.class);
  private final OmTransportFactory factory = mock(OmTransportFactory.class);
  private final OmTransport transport = mock(OmTransport.class);
  private final UserGroupInformation caller = UserGroupInformation.createProxyUser("alice",
      UserGroupInformation.createRemoteUser("nfs-gateway"));
  private final RequestContext context = context(binding.getExportId());

  @Test
  void createsAndClosesTransportUnderCallerIdentity() throws Exception {
    when(authorizer.authorize(context)).thenReturn(caller);
    when(factory.createOmTransport(configuration, caller, "om-service")).thenAnswer(invocation -> {
      assertCallerIdentity();
      return transport;
    });
    OmKeyInfo key = new OmKeyInfo.Builder().setVolumeName("vol").setBucketName("bucket").setKeyName("renamed/file")
        .setObjectID(3).setReplicationConfig(RatisReplicationConfig.getInstance(ReplicationFactor.ONE)).build();
    when(transport.submitRequest(any(OMRequest.class))).thenAnswer(invocation -> {
      assertCallerIdentity();
      OMRequest request = invocation.getArgument(0);
      assertEquals(Type.LookupNfsObject, request.getCmdType());
      assertEquals(1, request.getLookupNfsObjectRequest().getVolumeID());
      assertEquals(2, request.getLookupNfsObjectRequest().getBucketID());
      assertEquals(3, request.getLookupNfsObjectRequest().getObjectID());
      assertFalse(request.getLookupNfsObjectRequest().getHeadOp());
      return OMResponse.newBuilder().setCmdType(Type.LookupNfsObject)
          .setStatus(org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK)
          .setLookupNfsObjectResponse(LookupNfsObjectResponse.newBuilder()
              .setStatus(new OzoneFileStatus(key, 1024, false).getProtobuf(ClientVersion.CURRENT))).build();
    });
    UserGroupInformation previous = UserGroupInformation.getCurrentUser();
    OzoneFileStatus result = reader("cluster").lookup(context, handle(3), false);
    assertEquals(3, result.getKeyInfo().getObjectID());
    assertEquals("renamed/file", result.getKeyInfo().getKeyName());
    assertEquals(previous.getUserName(), UserGroupInformation.getCurrentUser().getUserName());
    verify(transport).close();
  }

  @Test
  void closesTransportOnOmFailure() throws Exception {
    when(authorizer.authorize(context)).thenReturn(caller);
    when(factory.createOmTransport(configuration, caller, "om-service")).thenReturn(transport);
    OMException denied = new OMException("denied", OMException.ResultCodes.PERMISSION_DENIED);
    when(transport.submitRequest(any(OMRequest.class))).thenThrow(denied);
    assertSame(denied, assertThrows(OMException.class, () -> reader("cluster").lookup(context, handle(3), true)));
    verify(transport).close();
  }

  @Test
  void rejectsAuthorizationFailureBeforeOpeningOmTransport() {
    when(authorizer.authorize(context)).thenThrow(Status.PERMISSION_DENIED.asRuntimeException());
    assertThrows(StatusRuntimeException.class, () -> reader("cluster").lookup(context, handle(3), true));
    verifyNoInteractions(factory, transport);
  }

  @Test
  void rejectsOtherClusterExportAndMalformedHandlesBeforeOpeningOmTransport() throws IOException {
    when(authorizer.authorize(any(RequestContext.class))).thenReturn(caller);
    assertEquals(OMException.ResultCodes.FILE_NOT_FOUND,
        assertThrows(OMException.class, () -> reader("another-cluster").lookup(context, handle(3), true)).getResult());
    assertThrows(OMException.class, () -> reader("cluster").lookup(context(UUID.randomUUID()), handle(3), true));
    ByteString foreign = ByteString.copyFrom(new NfsFileHandle(UUID.randomUUID(), 1, 2, 3,
        NfsFileHandle.Kind.FILE).toBytes());
    assertThrows(OMException.class, () -> reader("cluster").lookup(context, foreign, true));
    assertThrows(IllegalArgumentException.class, () -> reader("cluster").lookup(context, ByteString.EMPTY, true));
    verifyNoInteractions(factory, transport);
  }

  @Test
  void unsignedOffsetsAndZeroCountDoNotReadOrSeek() throws Exception {
    when(authorizer.authorize(context)).thenReturn(caller);
    OzoneInputStream stream = mock(OzoneInputStream.class);
    try (MockedConstruction<RpcClient> clients = mockConstruction(RpcClient.class, (client, construction) -> {
      assertCallerIdentity();
      assertSame(configuration, construction.arguments().get(0));
      assertEquals("om-service", construction.arguments().get(1));
      when(client.readNfsFile("vol", "bucket", 1, 2, 3)).thenReturn(new LengthInputStream(stream, 3));
    })) {
      for (long offset : new long[]{3, Long.MAX_VALUE, Long.MIN_VALUE, -1}) {
        ReadResponse result = reader("cluster").read(context, handle(3), offset, NfsObjectReader.MAX_READ_BYTES);
        assertEquals(0, result.getResult().getNfsStatus());
        assertEquals(ByteString.EMPTY, result.getData());
        assertTrue(result.getEof());
      }
      ReadResponse zeroCount = reader("cluster").read(context, handle(3), 0, 0);
      assertEquals(ByteString.EMPTY, zeroCount.getData());
      assertFalse(zeroCount.getEof());
      assertEquals(5, clients.constructed().size());
      for (RpcClient client : clients.constructed()) {
        verify(client).close();
      }
      verify(stream, times(5)).close();
      verify(stream, never()).seek(anyLong());
      verify(stream, never()).read(any(byte[].class), anyInt(), anyInt());
    }
  }

  @Test
  void readRejectsUnboundedCountsAndAuthorizationBeforeCreatingClient() throws Exception {
    try (MockedConstruction<RpcClient> clients = mockConstruction(RpcClient.class)) {
      when(authorizer.authorize(context)).thenThrow(Status.PERMISSION_DENIED.asRuntimeException()).thenReturn(caller);
      assertThrows(StatusRuntimeException.class, () -> reader("cluster").read(context, handle(3), 0, 1));
      for (int count : new int[]{NfsObjectReader.MAX_READ_BYTES + 1, Integer.MIN_VALUE, -1}) {
        assertEquals(OMException.ResultCodes.INVALID_REQUEST, assertThrows(OMException.class,
            () -> reader("cluster").read(context, handle(3), 0, count)).getResult());
      }
      assertThrows(IllegalArgumentException.class,
          () -> reader("cluster").read(context, ByteString.EMPTY, 0, 1));
      assertEquals(0, clients.constructed().size());
    }
  }

  @Test
  void readClosesClientAfterOmDenial() throws Exception {
    when(authorizer.authorize(context)).thenReturn(caller);
    OMException denied = new OMException("denied", OMException.ResultCodes.PERMISSION_DENIED);
    try (MockedConstruction<RpcClient> clients = mockConstruction(RpcClient.class, (client, construction) -> {
      when(client.readNfsFile("vol", "bucket", 1, 2, 3)).thenThrow(denied);
    })) {
      assertSame(denied, assertThrows(OMException.class, () -> reader("cluster").read(context, handle(3), 0, 1)));
      verify(clients.constructed().get(0)).close();
    }
  }

  @Test
  void shortStorageReadFailsInsteadOfReturningSuccessfulEof() throws Exception {
    when(authorizer.authorize(context)).thenReturn(caller);
    OzoneInputStream stream = mock(OzoneInputStream.class);
    when(stream.read(any(byte[].class), anyInt(), anyInt())).thenReturn(-1);
    try (MockedConstruction<RpcClient> clients = mockConstruction(RpcClient.class, (client, construction) -> {
      when(client.readNfsFile("vol", "bucket", 1, 2, 3)).thenReturn(new LengthInputStream(stream, 3));
    })) {
      assertThrows(IOException.class, () -> reader("cluster").read(context, handle(3), 0, 3));
      verify(stream).close();
      verify(clients.constructed().get(0)).close();
    }
  }

  @Test
  void directoryReadConsultsOmBeforeReturningIsDirectory() throws Exception {
    when(authorizer.authorize(context)).thenReturn(caller);
    when(factory.createOmTransport(configuration, caller, "om-service")).thenReturn(transport);
    when(transport.submitRequest(any(OMRequest.class))).thenReturn(OMResponse.newBuilder()
        .setCmdType(Type.LookupNfsObject)
        .setStatus(org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK)
        .setLookupNfsObjectResponse(LookupNfsObjectResponse.newBuilder()
            .setStatus(OzoneFileStatusProto.newBuilder().setIsDirectory(true))).build());
    ByteString root = ByteString.copyFrom(binding.getRootHandle().toBytes());
    assertEquals(OMException.ResultCodes.NOT_A_FILE, assertThrows(OMException.class,
        () -> reader("cluster").read(context, root, 0, 1)).getResult());
    verify(transport).close();
  }

  @Test
  void staleDirectoryReadPreservesOmIdentityFailure() throws Exception {
    when(authorizer.authorize(context)).thenReturn(caller);
    when(factory.createOmTransport(configuration, caller, "om-service")).thenReturn(transport);
    OMException stale = new OMException("deleted bucket", OMException.ResultCodes.BUCKET_NOT_FOUND);
    when(transport.submitRequest(any(OMRequest.class))).thenThrow(stale);
    ByteString root = ByteString.copyFrom(binding.getRootHandle().toBytes());
    assertSame(stale, assertThrows(OMException.class, () -> reader("cluster").read(context, root, 0, 1)));
    verify(transport).close();
  }

  private void assertCallerIdentity() throws IOException {
    UserGroupInformation current = UserGroupInformation.getCurrentUser();
    assertEquals(caller.getUserName(), current.getUserName());
    assertEquals(UserGroupInformation.AuthenticationMethod.PROXY, current.getAuthenticationMethod());
    assertEquals(caller.getRealUser().getUserName(), current.getRealUser().getUserName());
  }

  private NfsObjectReader reader(String clusterId) {
    return new NfsObjectReader(configuration, "om-service", clusterId, binding, authorizer, factory);
  }

  private ByteString handle(long objectId) {
    return ByteString.copyFrom(
        new NfsFileHandle(binding.getExportId(), 1, 2, objectId, NfsFileHandle.Kind.FILE).toBytes());
  }

  private static RequestContext context(UUID exportId) {
    return RequestContext.newBuilder().setVersion(1)
        .setExportUuid(ByteString.copyFrom(ByteBuffer.allocate(16)
        .putLong(exportId.getMostSignificantBits()).putLong(exportId.getLeastSignificantBits()).array()))
        .buildPartial();
  }
}
