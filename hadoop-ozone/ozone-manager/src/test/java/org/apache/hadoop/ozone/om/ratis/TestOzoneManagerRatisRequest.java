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

package org.apache.hadoop.ozone.om.ratis;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.INVALID_REQUEST;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.OMExecutionFlow;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.security.ManagedS3AccessKeySecretRetrievalManager;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.security.ManagedS3AccessKeyConfig;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.ratis.protocol.ClientId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test OM Ratis request handling.
 */
public class TestOzoneManagerRatisRequest {
  @TempDir
  private Path folder;

  private OzoneManager ozoneManager;
  private final OzoneConfiguration ozoneConfiguration =
      new OzoneConfiguration();

  @Test
  public void testRequestWithNonExistentBucket() throws Exception {
    ozoneManager = mock(OzoneManager.class);
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.resolve("om").toAbsolutePath().toString());
    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration,
        ozoneManager);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);

    String volumeName = "vol1";
    String bucketName = "invalidBuck";

    // Add entry to Volume Table.
    OmVolumeArgs omVolumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(volumeName)
        .setOwnerName("owner")
        .setAdminName("admin")
        .build();
    omMetadataManager.getVolumeTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getVolumeKey(volumeName)),
        CacheValue.get(100L, omVolumeArgs));

    OzoneManagerProtocolProtos.OMRequest omRequest = OMRequestTestUtils
        .createCompleteMPURequest(volumeName, bucketName, "mpuKey", "mpuKeyID",
            new ArrayList<>());

    OMException omException = assertThrows(OMException.class,
        () -> OzoneManagerRatisUtils.createClientRequest(omRequest,
            ozoneManager));
    assertEquals(OMException.ResultCodes.BUCKET_NOT_FOUND,
        omException.getResult());
  }

  @Test
  public void testUnknownRequestHandling()
      throws IOException, ServiceException {
    // Create an instance of OMRequest with an unknown command type.
    OzoneManagerProtocolProtos.OMRequest omRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.UnknownCommand)
            .setClientId("test-client-id")
            .build();

    ozoneManager = mock(OzoneManager.class);
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.resolve("om").toAbsolutePath().toString());
    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration,
        ozoneManager);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    OMExecutionFlow omExecutionFlow = new OMExecutionFlow(ozoneManager);
    when(ozoneManager.getOmExecutionFlow()).thenReturn(omExecutionFlow);
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);
    final OmConfig omConfig = ozoneConfiguration.getObject(OmConfig.class);
    when(ozoneManager.getConfig()).thenReturn(omConfig);

    OzoneManagerRatisServer ratisServer = mock(OzoneManagerRatisServer.class);
    ProtocolMessageMetrics<OzoneManagerProtocolProtos.Type> protocolMessageMetrics =
        mock(ProtocolMessageMetrics.class);

    OzoneManagerProtocolProtos.OMResponse expectedResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder()
            .setStatus(INVALID_REQUEST)
            .setCmdType(omRequest.getCmdType())
            .setTraceID(omRequest.getTraceID())
            .setSuccess(false)
            .setMessage("Unrecognized write command type request " +
                omRequest.getCmdType())
            .build();

    OzoneManagerProtocolServerSideTranslatorPB serverSideTranslatorPB =
        new OzoneManagerProtocolServerSideTranslatorPB(ozoneManager,
            ratisServer, protocolMessageMetrics);

    OzoneManagerProtocolProtos.OMResponse actualResponse =
        serverSideTranslatorPB.processRequest(omRequest);

    assertEquals(expectedResponse, actualResponse);
  }

  @Test
  public void testAssumeRoleRejectedWhenStsDisabled() {
    ozoneManager = mock(OzoneManager.class, CALLS_REAL_METHODS);
    when(ozoneManager.isS3STSEnabled()).thenReturn(false);

    final OzoneManagerProtocolProtos.OMRequest omRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.AssumeRole)
            .setClientId(ClientId.randomId().toString())
            .build();

    final OMException omException = assertThrows(OMException.class,
        () -> OzoneManagerRatisUtils.createClientRequest(omRequest, ozoneManager));
    assertEquals(OMException.ResultCodes.FEATURE_NOT_ENABLED, omException.getResult());
  }

  @Test
  public void testManagedS3AccessKeyRetrieveBypassesGenericReadSubmit()
      throws Exception {
    OzoneManagerProtocolProtos.OMRequest omRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type
                .RetrieveManagedS3AccessKeySecret)
            .setClientId("test-client-id")
            .setUserInfo(OzoneManagerProtocolProtos.UserInfo.newBuilder()
                .setUserName("om-admin"))
            .setRetrieveManagedS3AccessKeySecretRequest(
                OzoneManagerProtocolProtos
                    .RetrieveManagedS3AccessKeySecretRequest.newBuilder()
                    .setAccessKeyId("access-id")
                    .setRetrievalHandle("handle")
                    .build())
            .build();

    ozoneManager = mock(OzoneManager.class);
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.resolve("om").toAbsolutePath().toString());
    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(
        ozoneConfiguration, ozoneManager);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);
    when(ozoneManager.getConfig()).thenReturn(
        ozoneConfiguration.getObject(OmConfig.class));
    when(ozoneManager.getManagedS3AccessKeyConfig()).thenReturn(
        ManagedS3AccessKeyConfig.newBuilder()
            .setEnabled(true)
            .setEncryptionKeyName("test-key")
            .build());
    when(ozoneManager.getManagedS3AccessKeySecretRetrievalManager())
        .thenReturn(new ManagedS3AccessKeySecretRetrievalManager(
            Duration.ofSeconds(60), 16));
    OMLayoutVersionManager versionManager =
        mock(OMLayoutVersionManager.class);
    when(versionManager.isAllowed(OMLayoutFeature
        .MANAGED_LOCAL_S3_ACCESS_KEYS)).thenReturn(true);
    when(ozoneManager.getVersionManager()).thenReturn(versionManager);

    OzoneManagerRatisServer ratisServer = mock(OzoneManagerRatisServer.class);
    ProtocolMessageMetrics<OzoneManagerProtocolProtos.Type> metrics =
        mock(ProtocolMessageMetrics.class);

    OzoneManagerProtocolServerSideTranslatorPB translator =
        new OzoneManagerProtocolServerSideTranslatorPB(ozoneManager,
            ratisServer, metrics);

    OzoneManagerProtocolProtos.OMResponse response =
        translator.processRequest(omRequest);

    assertEquals(OzoneManagerProtocolProtos.Type.RetrieveManagedS3AccessKeySecret,
        response.getCmdType());
    verify(ozoneManager).checkLeaderStatus();
    verify(ozoneManager, never()).getOmExecutionFlow();
  }

  @Test
  public void testManagedS3AccessKeyListAndInfoBypassGenericReadSubmit()
      throws Exception {
    ozoneManager = mockManagedS3AccessKeyOzoneManager();
    OzoneManagerRatisServer ratisServer = mock(OzoneManagerRatisServer.class);
    ProtocolMessageMetrics<OzoneManagerProtocolProtos.Type> metrics =
        mock(ProtocolMessageMetrics.class);
    OzoneManagerProtocolServerSideTranslatorPB translator =
        new OzoneManagerProtocolServerSideTranslatorPB(ozoneManager,
            ratisServer, metrics);

    translator.processRequest(OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.ListManagedS3AccessKeys)
        .setClientId("list-client")
        .setUserInfo(OzoneManagerProtocolProtos.UserInfo.newBuilder()
            .setUserName("om-admin"))
        .setListManagedS3AccessKeysRequest(OzoneManagerProtocolProtos
            .ListManagedS3AccessKeysRequest.newBuilder())
        .build());
    translator.processRequest(OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.InfoManagedS3AccessKey)
        .setClientId("info-client")
        .setUserInfo(OzoneManagerProtocolProtos.UserInfo.newBuilder()
            .setUserName("om-admin"))
        .setInfoManagedS3AccessKeyRequest(OzoneManagerProtocolProtos
            .InfoManagedS3AccessKeyRequest.newBuilder()
            .setAccessKeyId("missing"))
        .build());

    verify(ozoneManager, times(2)).checkLeaderStatus();
    verify(ozoneManager, never()).getOmExecutionFlow();
  }

  private OzoneManager mockManagedS3AccessKeyOzoneManager() throws Exception {
    OzoneManager manager = mock(OzoneManager.class);
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.resolve("om-managed").toAbsolutePath().toString());
    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(
        ozoneConfiguration, manager);
    when(manager.getMetadataManager()).thenReturn(omMetadataManager);
    when(manager.getConfiguration()).thenReturn(ozoneConfiguration);
    when(manager.getConfig()).thenReturn(
        ozoneConfiguration.getObject(OmConfig.class));
    when(manager.getManagedS3AccessKeyConfig()).thenReturn(
        ManagedS3AccessKeyConfig.newBuilder()
            .setEnabled(true)
            .setEncryptionKeyName("test-key")
            .build());
    when(manager.getManagedS3AccessKeySecretRetrievalManager())
        .thenReturn(new ManagedS3AccessKeySecretRetrievalManager(
            Duration.ofSeconds(60), 16));
    OMLayoutVersionManager versionManager =
        mock(OMLayoutVersionManager.class);
    when(versionManager.isAllowed(OMLayoutFeature
        .MANAGED_LOCAL_S3_ACCESS_KEYS)).thenReturn(true);
    when(manager.getVersionManager()).thenReturn(versionManager);
    return manager;
  }

  @Test
  public void testAssumeRoleRejectedWhenStsEnabledButNativeAuthorizerUsed() {
    ozoneManager = mock(OzoneManager.class, CALLS_REAL_METHODS);
    when(ozoneManager.isS3STSEnabled()).thenReturn(true);

    final IAccessAuthorizer authorizer = mock(IAccessAuthorizer.class);
    when(authorizer.isNative()).thenReturn(true);
    when(ozoneManager.getAccessAuthorizer()).thenReturn(authorizer);

    final OzoneManagerProtocolProtos.OMRequest omRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.AssumeRole)
            .setClientId(ClientId.randomId().toString())
            .build();

    final OMException omException = assertThrows(OMException.class,
        () -> OzoneManagerRatisUtils.createClientRequest(omRequest, ozoneManager));
    assertEquals(OMException.ResultCodes.FEATURE_NOT_ENABLED, omException.getResult());
  }

  @Test
  public void testAssumeRoleAllowedWhenStsEnabledAndNativeAuthorizerNotUsed() {
    ozoneManager = mock(OzoneManager.class, CALLS_REAL_METHODS);
    when(ozoneManager.isS3STSEnabled()).thenReturn(true);

    final IAccessAuthorizer authorizer = mock(IAccessAuthorizer.class);
    when(authorizer.isNative()).thenReturn(false);
    when(ozoneManager.getAccessAuthorizer()).thenReturn(authorizer);

    final OzoneManagerProtocolProtos.OMRequest omRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.AssumeRole)
            .setClientId(ClientId.randomId().toString())
            .build();

    assertDoesNotThrow(() -> OzoneManagerRatisUtils.createClientRequest(omRequest, ozoneManager));
  }
}
