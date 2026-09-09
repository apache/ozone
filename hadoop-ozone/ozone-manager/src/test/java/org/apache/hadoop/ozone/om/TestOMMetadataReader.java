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

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.Context;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Test ozone metadata reader.
 */
public class TestOMMetadataReader {

  @Test
  public void testGetClientAddress() {
    try (
        MockedStatic<Server> ipcServerStaticMock = mockStatic(Server.class);
        MockedStatic<Context> grpcRequestContextStaticMock = mockStatic(Context.class);
    ) {
      // given
      String expectedClientAddressInCaseOfHadoopRpcCall =
          "hadoop.ipc.client.com";
      ipcServerStaticMock.when(Server::getRemoteAddress)
          .thenReturn(null, null, expectedClientAddressInCaseOfHadoopRpcCall);

      String expectedClientAddressInCaseOfGrpcCall = "172.45.23.4";
      Context.Key<String> clientIpAddressKey = mock(Context.Key.class);
      when(clientIpAddressKey.get())
          .thenReturn(expectedClientAddressInCaseOfGrpcCall, null);

      grpcRequestContextStaticMock.when(() -> Context.key("CLIENT_IP_ADDRESS"))
          .thenReturn(clientIpAddressKey);

      // when (GRPC call with defined client address)
      String clientAddress = OmMetadataReader.getClientAddress();
      // then
      assertEquals(expectedClientAddressInCaseOfGrpcCall, clientAddress);

      // and when (GRPC call without client address)
      clientAddress = OmMetadataReader.getClientAddress();
      // then
      assertEquals("", clientAddress);

      // and when (Hadoop RPC client call)
      clientAddress = OmMetadataReader.getClientAddress();
      // then
      assertEquals(expectedClientAddressInCaseOfHadoopRpcCall, clientAddress);
    }
  }

  @Test
  public void getFileStatusRejectsObjectStoreLayout() throws Exception {
    OzoneManager ozoneManager = mock(OzoneManager.class);
    KeyManager keyManager = mock(KeyManager.class);
    when(ozoneManager.getAclsEnabled()).thenReturn(false);
    when(ozoneManager.getBucketManager()).thenReturn(mock(BucketManager.class));
    when(ozoneManager.getVolumeManager()).thenReturn(mock(VolumeManager.class));
    when(ozoneManager.getPerfMetrics()).thenReturn(mock(OMPerformanceMetrics.class));
    when(ozoneManager.resolveBucketLink(any(OmKeyArgs.class)))
        .thenReturn(new ResolvedBucket("vol", "obs-bucket", "vol", "obs-bucket",
            "owner", BucketLayout.OBJECT_STORE));

    OmMetadataReader reader = new OmMetadataReader(keyManager,
        mock(PrefixManager.class), ozoneManager, mock(org.slf4j.Logger.class),
        mock(AuditLogger.class), mock(OmMetadataReaderMetrics.class), null);

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName("vol")
        .setBucketName("obs-bucket")
        .setKeyName("key1")
        .build();

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> reader.getFileStatus(keyArgs));
    assertTrue(exception.getMessage().contains("obs-bucket"));
    assertTrue(exception.getMessage().contains("OBJECT_STORE"));
    verify(keyManager, never()).getFileStatus(any(), anyString());
  }

  @Test
  public void getFileStatusAllowsLegacyLayout() throws Exception {
    OzoneManager ozoneManager = mock(OzoneManager.class);
    KeyManager keyManager = mock(KeyManager.class);
    when(ozoneManager.getAclsEnabled()).thenReturn(false);
    when(ozoneManager.getBucketManager()).thenReturn(mock(BucketManager.class));
    when(ozoneManager.getVolumeManager()).thenReturn(mock(VolumeManager.class));
    when(ozoneManager.getPerfMetrics()).thenReturn(mock(OMPerformanceMetrics.class));
    when(ozoneManager.resolveBucketLink(any(OmKeyArgs.class)))
        .thenReturn(new ResolvedBucket("vol", "legacy-bucket", "vol",
            "legacy-bucket", "owner", BucketLayout.LEGACY));
    OzoneFileStatus expectedStatus = new OzoneFileStatus();
    when(keyManager.getFileStatus(any(OmKeyArgs.class), anyString()))
        .thenReturn(expectedStatus);

    OmMetadataReader reader = new OmMetadataReader(keyManager,
        mock(PrefixManager.class), ozoneManager, mock(org.slf4j.Logger.class),
        mock(AuditLogger.class), mock(OmMetadataReaderMetrics.class), null);

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName("vol")
        .setBucketName("legacy-bucket")
        .setKeyName("key1")
        .build();

    OzoneFileStatus status = reader.getFileStatus(keyArgs);
    assertEquals(expectedStatus, status);
    verify(keyManager).getFileStatus(any(OmKeyArgs.class), anyString());
  }
}
