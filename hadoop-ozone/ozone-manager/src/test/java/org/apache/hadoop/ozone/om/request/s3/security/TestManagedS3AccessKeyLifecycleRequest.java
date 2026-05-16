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

package org.apache.hadoop.ozone.om.request.s3.security;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.MANAGED_S3_ACCESS_KEY_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.MANAGED_S3_ACCESS_KEY_OPERATION_NOT_SUPPORTED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.MANAGED_S3_ACCESS_KEY_SECRET_UNAVAILABLE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.framework;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.S3SecretCache;
import org.apache.hadoop.ozone.om.S3SecretFunction;
import org.apache.hadoop.ozone.om.S3SecretLockedManager;
import org.apache.hadoop.ozone.om.S3SecretManager;
import org.apache.hadoop.ozone.om.S3SecretManagerImpl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.S3ManagedAccessKeyInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.s3.S3SecretCacheProvider;
import org.apache.hadoop.ozone.om.security.ManagedS3AccessKeySecretRetrievalManager;
import org.apache.hadoop.ozone.om.security.ManagedS3AccessKeySecretRetrievalManager.Operation;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3SecretRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3ManagedAccessKeyInfoProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetS3SecretRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.security.ManagedS3AccessKeyConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests managed local S3 access-key lifecycle request behavior.
 */
public class TestManagedS3AccessKeyLifecycleRequest {

  private static final String ACCESS_KEY_ID = "managed-access-key";
  private static final String EFFECTIVE_USER = "alice";
  private static final String ADMIN = "om-admin";
  private static final String KEY_NAME = "ozone-s3-managed-access-keys";
  private static final String KEY_VERSION_NAME =
      "ozone-s3-managed-access-keys@0";
  private static final byte[] PLAIN_DEK =
      "01234567890123456789012345678901".getBytes(UTF_8);
  private static final byte[] EDEK =
      "wrapped-edek-sentinel".getBytes(UTF_8);
  private static final byte[] EDEK_IV =
      "edek-iv-sentinel".getBytes(UTF_8);
  private static final String CUSTOM_SECRET =
      "plain-secret-sentinel-1234567890";

  @TempDir
  private Path folder;

  private OzoneManager ozoneManager;
  private OmMetadataManagerImpl metadataManager;
  private ManagedS3AccessKeySecretRetrievalManager retrievalManager;
  private KeyProviderCryptoExtension kmsProvider;
  private OMMetrics omMetrics;

  @BeforeEach
  public void setup() throws Exception {
    ozoneManager = mock(OzoneManager.class);
    OzoneConfiguration conf = new OzoneConfiguration();
    omMetrics = OMMetrics.create(conf);
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.toAbsolutePath().toString());
    metadataManager = new OmMetadataManagerImpl(conf, ozoneManager);
    retrievalManager = new ManagedS3AccessKeySecretRetrievalManager(
        Duration.ofSeconds(60), 16);
    kmsProvider = kmsProviderReturningDek(PLAIN_DEK);

    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(metadataManager);
    when(ozoneManager.getManagedS3AccessKeyConfig())
        .thenReturn(managedConfig());
    when(ozoneManager.getManagedS3AccessKeySecretRetrievalManager())
        .thenReturn(retrievalManager);
    when(ozoneManager.getKmsProvider()).thenReturn(kmsProvider);
    when(ozoneManager.isAdminAuthorizationEnabled()).thenReturn(false);

    OMLayoutVersionManager versionManager =
        mock(OMLayoutVersionManager.class);
    when(versionManager.isAllowed(OMLayoutFeature
        .MANAGED_LOCAL_S3_ACCESS_KEYS)).thenReturn(true);
    when(versionManager.isAllowed(any(String.class))).thenReturn(true);
    when(ozoneManager.getVersionManager()).thenReturn(versionManager);

    S3SecretLockedManager secretManager = new S3SecretLockedManager(
        new S3SecretManagerImpl(metadataManager,
            S3SecretCacheProvider.IN_MEMORY.get(conf)),
        metadataManager.getLock());
    when(ozoneManager.getS3SecretManager()).thenReturn(secretManager);

    AuditLogger auditLogger = mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    doNothing().when(auditLogger).logWrite(any(AuditMessage.class));
  }

  @AfterEach
  public void tearDown() {
    if (omMetrics != null) {
      omMetrics.unRegister();
    }
    framework().clearInlineMocks();
  }

  @Test
  public void createPreExecuteEncryptsAndResponseContainsOnlyHandle()
      throws Exception {
    OMRequest preExecuted =
        new CreateManagedS3AccessKeyRequest(createRequest()).preExecute(
            ozoneManager);

    verify(kmsProvider).getCurrentKey(KEY_NAME);
    verify(kmsProvider).generateEncryptedKey(KEY_NAME);
    verify(kmsProvider).decryptEncryptedKey(any());
    assertTrue(preExecuted.hasUpdateCreateManagedS3AccessKeyRequest());
    assertFalse(preExecuted.hasCreateManagedS3AccessKeyRequest());
    assertNoPlaintext(preExecuted.toByteArray(), CUSTOM_SECRET);
    String handle = responseHandle(preExecuted, Operation.CREATE);
    assertFalse(handle.isEmpty());
    assertNoPlaintext(preExecuted.toByteArray(), handle);
    S3ManagedAccessKeyInfoProto updateInfo =
        preExecuted.getUpdateCreateManagedS3AccessKeyRequest().getInfo();
    assertNoPlaintext(updateInfo.toByteArray(), CUSTOM_SECRET);
    assertNoPlaintext(S3ManagedAccessKeyInfo.getCodec()
        .toPersistedFormat(S3ManagedAccessKeyInfo.fromProtobuf(updateInfo)),
        CUSTOM_SECRET);
    assertEquals(KEY_VERSION_NAME, updateInfo.getSecretKeyId());

    clearInvocations(kmsProvider);
    OMClientResponse clientResponse =
        new CreateManagedS3AccessKeyRequest.Update(preExecuted)
            .validateAndUpdateCache(ozoneManager, 1L);

    verifyNoInteractions(kmsProvider);
    OMResponse response = clientResponse.getOMResponse();
    assertEquals(OK, response.getStatus());
    assertTrue(response.getSuccess());
    assertTrue(response.hasCreateManagedS3AccessKeyResponse());
    assertNoPlaintext(response.toByteArray(), CUSTOM_SECRET);
    assertFalse(response.getCreateManagedS3AccessKeyResponse()
        .getRetrievalHandle().isEmpty());
    assertEquals(handle, response.getCreateManagedS3AccessKeyResponse()
        .getRetrievalHandle());
    assertNotNull(metadataManager.getS3ManagedAccessKeyTable()
        .get(ACCESS_KEY_ID));
  }

  @Test
  public void rotatePreExecuteEncryptsAndKeepsImmutableMetadata()
      throws Exception {
    metadataManager.getS3ManagedAccessKeyTable().put(ACCESS_KEY_ID,
        existingInfo(false));
    OMRequest preExecuted =
        new RotateManagedS3AccessKeyRequest(rotateRequest()).preExecute(
            ozoneManager);

    verify(kmsProvider).getCurrentKey(KEY_NAME);
    verify(kmsProvider).generateEncryptedKey(KEY_NAME);
    verify(kmsProvider).decryptEncryptedKey(any());
    assertTrue(preExecuted.hasUpdateRotateManagedS3AccessKeyRequest());
    assertFalse(preExecuted.hasRotateManagedS3AccessKeyRequest());
    assertNoPlaintext(preExecuted.toByteArray(), CUSTOM_SECRET);
    String handle = responseHandle(preExecuted, Operation.ROTATE);
    assertFalse(handle.isEmpty());
    assertNoPlaintext(preExecuted.toByteArray(), handle);
    S3ManagedAccessKeyInfo rotated = S3ManagedAccessKeyInfo.fromProtobuf(
        preExecuted.getUpdateRotateManagedS3AccessKeyRequest().getInfo());
    assertEquals(ACCESS_KEY_ID, rotated.getAccessKeyId());
    assertEquals(EFFECTIVE_USER, rotated.getEffectiveUser());
    assertEquals(Collections.singletonList("group1"), rotated.getGroups());
    assertEquals("", rotated.getPolicyDocument());
    assertEquals(KEY_VERSION_NAME, rotated.getSecretKeyId());
    assertNotEquals(existingInfo(false).getEncryptedSecretKey(),
        rotated.getEncryptedSecretKey());

    clearInvocations(kmsProvider);
    OMClientResponse clientResponse =
        new RotateManagedS3AccessKeyRequest.Update(preExecuted)
            .validateAndUpdateCache(ozoneManager, 2L);

    verifyNoInteractions(kmsProvider);
    OMResponse response = clientResponse.getOMResponse();
    assertEquals(OK, response.getStatus());
    assertTrue(response.hasRotateManagedS3AccessKeyResponse());
    assertNoPlaintext(response.toByteArray(), CUSTOM_SECRET);
    assertEquals(handle, response.getRotateManagedS3AccessKeyResponse()
        .getRetrievalHandle());
  }

  @Test
  public void directInternalCreateUpdateIsRejectedBeforeRatis()
      throws Exception {
    OMException exception = org.junit.jupiter.api.Assertions.assertThrows(
        OMException.class, () -> new CreateManagedS3AccessKeyRequest.Update(
            createRequest().toBuilder()
                .setUpdateCreateManagedS3AccessKeyRequest(
                    OzoneManagerProtocolProtos
                        .UpdateCreateManagedS3AccessKeyRequest.newBuilder()
                        .setInfo(existingInfo(false).getProtobuf()))
                .build()).preExecute(ozoneManager));

    assertEquals(INVALID_REQUEST, exception.getResult());
  }

  @Test
  public void directInternalRotateUpdateIsRejectedBeforeRatis()
      throws Exception {
    OMException exception = org.junit.jupiter.api.Assertions.assertThrows(
        OMException.class, () -> new RotateManagedS3AccessKeyRequest.Update(
            rotateRequest().toBuilder()
                .setUpdateRotateManagedS3AccessKeyRequest(
                    OzoneManagerProtocolProtos
                        .UpdateRotateManagedS3AccessKeyRequest.newBuilder()
                        .setInfo(existingInfo(false).getProtobuf())
                        .setExpectedPreviousSecretKeyId("old-key@0")
                        .setExpectedPreviousEncryptedSecretKeySha256(
                            ManagedS3AccessKeyRequestHelper.sha256(
                                ByteString.copyFromUtf8("old-envelope"))))
                .build()).preExecute(ozoneManager));

    assertEquals(INVALID_REQUEST, exception.getResult());
  }

  @Test
  public void staleRotateUpdateFailsAndDropsHandle() throws Exception {
    S3ManagedAccessKeyInfo oldInfo = existingInfo(false);
    metadataManager.getS3ManagedAccessKeyTable().put(ACCESS_KEY_ID, oldInfo);
    OMRequest first =
        new RotateManagedS3AccessKeyRequest(rotateRequest()).preExecute(
            ozoneManager);
    String firstHandle = responseHandle(first, Operation.ROTATE);

    OMRequest second =
        new RotateManagedS3AccessKeyRequest(rotateRequest().toBuilder()
            .setClientId("client-rotate-2")
            .build()).preExecute(ozoneManager);
    String secondHandle = responseHandle(second, Operation.ROTATE);

    OMResponse firstResponse = new RotateManagedS3AccessKeyRequest.Update(first)
        .validateAndUpdateCache(ozoneManager, 8L).getOMResponse();
    OMResponse secondResponse = new RotateManagedS3AccessKeyRequest.Update(
        second).validateAndUpdateCache(ozoneManager, 9L).getOMResponse();

    assertEquals(OK, firstResponse.getStatus());
    assertEquals(OzoneManagerProtocolProtos.Status.INVALID_REQUEST,
        secondResponse.getStatus());
    assertFalse(firstHandle.isEmpty());
    assertSecretUnavailable(secondHandle);
  }

  @Test
  public void sameClientConcurrentCreatesKeepIndependentResponseHandles()
      throws Exception {
    String firstSecret = "first-custom-secret-123456789";
    String secondSecret = "second-custom-secret-123456789";
    OMRequest first = new CreateManagedS3AccessKeyRequest(
        createRequest(firstSecret)).preExecute(ozoneManager);
    OMRequest second = new CreateManagedS3AccessKeyRequest(
        createRequest(secondSecret)).preExecute(ozoneManager);
    String firstHandle = responseHandle(first, Operation.CREATE);
    String secondHandle = responseHandle(second, Operation.CREATE);

    OMResponse firstResponse = new CreateManagedS3AccessKeyRequest.Update(
        first).validateAndUpdateCache(ozoneManager, 10L).getOMResponse();
    OMResponse secondResponse = new CreateManagedS3AccessKeyRequest.Update(
        second).validateAndUpdateCache(ozoneManager, 11L).getOMResponse();

    assertEquals(OK, firstResponse.getStatus());
    assertEquals(firstHandle, firstResponse
        .getCreateManagedS3AccessKeyResponse().getRetrievalHandle());
    assertArrayEquals(firstSecret.getBytes(UTF_8),
        retrievalManager.retrieve(ADMIN, ACCESS_KEY_ID, firstHandle));
    assertEquals(OzoneManagerProtocolProtos.Status
            .MANAGED_S3_ACCESS_KEY_ALREADY_EXISTS,
        secondResponse.getStatus());
    assertSecretUnavailable(secondHandle);
  }

  @Test
  public void sameClientConcurrentRotatesKeepIndependentResponseHandles()
      throws Exception {
    String firstSecret = "first-rotate-secret-123456789";
    String secondSecret = "second-rotate-secret-123456789";
    metadataManager.getS3ManagedAccessKeyTable().put(ACCESS_KEY_ID,
        existingInfo(false));
    OMRequest first = new RotateManagedS3AccessKeyRequest(
        rotateRequest(firstSecret)).preExecute(ozoneManager);
    OMRequest second = new RotateManagedS3AccessKeyRequest(
        rotateRequest(secondSecret)).preExecute(ozoneManager);
    String firstHandle = responseHandle(first, Operation.ROTATE);
    String secondHandle = responseHandle(second, Operation.ROTATE);

    OMResponse firstResponse = new RotateManagedS3AccessKeyRequest.Update(
        first).validateAndUpdateCache(ozoneManager, 12L).getOMResponse();
    OMResponse secondResponse = new RotateManagedS3AccessKeyRequest.Update(
        second).validateAndUpdateCache(ozoneManager, 13L).getOMResponse();

    assertEquals(OK, firstResponse.getStatus());
    assertEquals(firstHandle, firstResponse
        .getRotateManagedS3AccessKeyResponse().getRetrievalHandle());
    assertArrayEquals(firstSecret.getBytes(UTF_8),
        retrievalManager.retrieve(ADMIN, ACCESS_KEY_ID, firstHandle));
    assertEquals(OzoneManagerProtocolProtos.Status.INVALID_REQUEST,
        secondResponse.getStatus());
    assertSecretUnavailable(secondHandle);
  }

  @Test
  public void disableAndDeleteMutateManagedMetadata() throws Exception {
    metadataManager.getS3ManagedAccessKeyTable().put(ACCESS_KEY_ID,
        existingInfo(false));

    OMRequest disable = new DisableManagedS3AccessKeyRequest(
        disableRequest()).preExecute(ozoneManager);
    OMResponse disableResponse =
        new DisableManagedS3AccessKeyRequest(disable)
            .validateAndUpdateCache(ozoneManager, 3L).getOMResponse();

    assertEquals(OK, disableResponse.getStatus());
    assertTrue(metadataManager.getS3ManagedAccessKeyTable()
        .get(ACCESS_KEY_ID).isDisabled());

    OMRequest delete = new DeleteManagedS3AccessKeyRequest(
        deleteRequest()).preExecute(ozoneManager);
    OMResponse deleteResponse =
        new DeleteManagedS3AccessKeyRequest(delete)
            .validateAndUpdateCache(ozoneManager, 4L).getOMResponse();

    assertEquals(OK, deleteResponse.getStatus());
    assertNull(metadataManager.getS3ManagedAccessKeyTable()
        .get(ACCESS_KEY_ID));
  }

  @Test
  public void createRejectsLegacySecretCollisionAndDropsHandle()
      throws Exception {
    metadataManager.storeSecret(ACCESS_KEY_ID,
        S3SecretValue.of(ACCESS_KEY_ID, "legacy-secret"));
    OMRequest preExecuted =
        new CreateManagedS3AccessKeyRequest(createRequest()).preExecute(
            ozoneManager);
    String handle = responseHandle(preExecuted, Operation.CREATE);

    OMResponse response = new CreateManagedS3AccessKeyRequest.Update(
        preExecuted).validateAndUpdateCache(ozoneManager, 5L).getOMResponse();

    assertEquals(OzoneManagerProtocolProtos.Status
            .MANAGED_S3_ACCESS_KEY_ALREADY_EXISTS,
        response.getStatus());
    assertSecretUnavailable(handle);
  }

  @Test
  public void createRejectsExternalLegacyProviderWithoutBatchSupport()
      throws Exception {
    OMRequest preExecuted =
        new CreateManagedS3AccessKeyRequest(createRequest()).preExecute(
            ozoneManager);
    String handle = responseHandle(preExecuted, Operation.CREATE);
    S3SecretManager unsupportedProvider = unsupportedProvider();
    when(ozoneManager.getS3SecretManager()).thenReturn(unsupportedProvider);

    OMResponse response = new CreateManagedS3AccessKeyRequest.Update(
        preExecuted).validateAndUpdateCache(ozoneManager, 6L).getOMResponse();

    assertEquals(OzoneManagerProtocolProtos.Status
            .MANAGED_S3_ACCESS_KEY_OPERATION_NOT_SUPPORTED,
        response.getStatus());
    assertSecretUnavailable(handle);
  }

  @Test
  public void legacyGetAndSetSecretRejectManagedAccessKeyCollision()
      throws Exception {
    metadataManager.getS3ManagedAccessKeyTable().put(ACCESS_KEY_ID,
        existingInfo(false));

    OMRequest getPreExecuted =
        new S3GetSecretRequest(legacyGetSecretRequest()).preExecute(
            ozoneManager);
    OMResponse getResponse = new S3GetSecretRequest(getPreExecuted)
        .validateAndUpdateCache(ozoneManager, 7L).getOMResponse();
    assertEquals(OzoneManagerProtocolProtos.Status
            .MANAGED_S3_ACCESS_KEY_ALREADY_EXISTS,
        getResponse.getStatus());

    OMException setPreExecuteException = org.junit.jupiter.api.Assertions
        .assertThrows(OMException.class, () -> new OMSetSecretRequest(
            legacySetSecretRequest()).preExecute(ozoneManager));
    assertEquals(MANAGED_S3_ACCESS_KEY_ALREADY_EXISTS,
        setPreExecuteException.getResult());
  }

  @Test
  public void policyDocumentIsRejectedUntilLocalJsonEvaluatorExists()
      throws Exception {
    OMRequest request = createRequest().toBuilder()
        .setCreateManagedS3AccessKeyRequest(
            createRequest().getCreateManagedS3AccessKeyRequest()
                .toBuilder()
                .setPolicyDocument("{\"Version\":\"2012-10-17\"}"))
        .build();

    OMException exception = org.junit.jupiter.api.Assertions.assertThrows(
        OMException.class, () -> new CreateManagedS3AccessKeyRequest(request)
            .preExecute(ozoneManager));

    assertEquals(MANAGED_S3_ACCESS_KEY_OPERATION_NOT_SUPPORTED,
        exception.getResult());
  }

  private OMRequest createRequest() {
    return createRequest(CUSTOM_SECRET);
  }

  private OMRequest createRequest(String customSecret) {
    return OMRequest.newBuilder()
        .setCmdType(Type.CreateManagedS3AccessKey)
        .setClientId("client-create")
        .setUserInfo(userInfo())
        .setCreateManagedS3AccessKeyRequest(
            OzoneManagerProtocolProtos.CreateManagedS3AccessKeyRequest
                .newBuilder()
                .setAccessKeyId(ACCESS_KEY_ID)
                .setEffectiveUser(EFFECTIVE_USER)
                .setCustomSecret(customSecret)
                .setDescription("test key"))
        .build();
  }

  private OMRequest rotateRequest() {
    return rotateRequest(CUSTOM_SECRET);
  }

  private OMRequest rotateRequest(String customSecret) {
    return OMRequest.newBuilder()
        .setCmdType(Type.RotateManagedS3AccessKey)
        .setClientId("client-rotate")
        .setUserInfo(userInfo())
        .setRotateManagedS3AccessKeyRequest(
            OzoneManagerProtocolProtos.RotateManagedS3AccessKeyRequest
                .newBuilder()
                .setAccessKeyId(ACCESS_KEY_ID)
                .setCustomSecret(customSecret))
        .build();
  }

  private OMRequest disableRequest() {
    return OMRequest.newBuilder()
        .setCmdType(Type.DisableManagedS3AccessKey)
        .setClientId("client-disable")
        .setUserInfo(userInfo())
        .setDisableManagedS3AccessKeyRequest(
            OzoneManagerProtocolProtos.DisableManagedS3AccessKeyRequest
                .newBuilder()
                .setAccessKeyId(ACCESS_KEY_ID))
        .build();
  }

  private OMRequest deleteRequest() {
    return OMRequest.newBuilder()
        .setCmdType(Type.DeleteManagedS3AccessKey)
        .setClientId("client-delete")
        .setUserInfo(userInfo())
        .setDeleteManagedS3AccessKeyRequest(
            OzoneManagerProtocolProtos.DeleteManagedS3AccessKeyRequest
                .newBuilder()
                .setAccessKeyId(ACCESS_KEY_ID))
        .build();
  }

  private OMRequest legacyGetSecretRequest() {
    return OMRequest.newBuilder()
        .setCmdType(Type.GetS3Secret)
        .setClientId("client-getsecret")
        .setUserInfo(userInfo())
        .setGetS3SecretRequest(GetS3SecretRequest.newBuilder()
            .setKerberosID(ACCESS_KEY_ID)
            .setCreateIfNotExist(true))
        .build();
  }

  private OMRequest legacySetSecretRequest() {
    return OMRequest.newBuilder()
        .setCmdType(Type.SetS3Secret)
        .setClientId("client-setsecret")
        .setUserInfo(userInfo())
        .setSetS3SecretRequest(SetS3SecretRequest.newBuilder()
            .setAccessId(ACCESS_KEY_ID)
            .setSecretKey("legacy-secret-sentinel-1234567890"))
        .build();
  }

  private OzoneManagerProtocolProtos.UserInfo userInfo() {
    return OzoneManagerProtocolProtos.UserInfo.newBuilder()
        .setUserName(ADMIN)
        .build();
  }

  private S3ManagedAccessKeyInfo existingInfo(boolean disabled) {
    return S3ManagedAccessKeyInfo.newBuilder()
        .setAccessKeyId(ACCESS_KEY_ID)
        .setEncryptedSecretKey(ByteString.copyFromUtf8("old-envelope"))
        .setSecretKeyId("old-key@0")
        .setEffectiveUser(EFFECTIVE_USER)
        .setGroups(Collections.singletonList("group1"))
        .setDescription("old key")
        .setCreatedAt(100L)
        .setExpiresAt(System.currentTimeMillis() + 100000L)
        .setDisabled(disabled)
        .setCreatedBy(ADMIN)
        .setPolicyDocument("")
        .build();
  }

  private ManagedS3AccessKeyConfig managedConfig() {
    return ManagedS3AccessKeyConfig.newBuilder()
        .setEnabled(true)
        .setAllowCustomSecret(true)
        .setSecretMinLength(8)
        .setEncryptionKeyName(KEY_NAME)
        .build();
  }

  private KeyProviderCryptoExtension kmsProviderReturningDek(byte[] dek)
      throws Exception {
    KeyProviderCryptoExtension provider =
        mock(KeyProviderCryptoExtension.class);
    KeyProvider.KeyVersion currentKey = mock(KeyProvider.KeyVersion.class);
    when(currentKey.getVersionName()).thenReturn(KEY_VERSION_NAME);
    when(provider.getCurrentKey(KEY_NAME)).thenReturn(currentKey);
    when(provider.generateEncryptedKey(KEY_NAME)).thenReturn(edek());
    when(provider.decryptEncryptedKey(any())).thenAnswer(invocation ->
        keyVersion(dek.clone()));
    return provider;
  }

  private KeyProvider.KeyVersion keyVersion(byte[] material) {
    KeyProvider.KeyVersion keyVersion = mock(KeyProvider.KeyVersion.class);
    when(keyVersion.getMaterial()).thenReturn(material);
    return keyVersion;
  }

  private EncryptedKeyVersion edek() {
    return EncryptedKeyVersion.createForDecryption(KEY_NAME,
        KEY_VERSION_NAME, EDEK_IV.clone(), EDEK.clone());
  }

  private S3SecretManager unsupportedProvider() throws IOException {
    S3SecretManager manager = mock(S3SecretManager.class);
    when(manager.doUnderLock(any(), any())).thenAnswer(invocation -> {
      S3SecretFunction<?> action = invocation.getArgument(1,
          S3SecretFunction.class);
      return action.accept(manager);
    });
    when(manager.isBatchSupported()).thenReturn(false);
    when(manager.hasS3Secret(ACCESS_KEY_ID)).thenReturn(false);
    when(manager.batcher()).thenReturn(null);
    S3SecretCache cache = mock(S3SecretCache.class);
    when(manager.cache()).thenReturn(cache);
    return manager;
  }

  private void assertSecretUnavailable(String handle) throws Exception {
    OMException exception = org.junit.jupiter.api.Assertions.assertThrows(
        OMException.class, () -> retrievalManager.retrieve(ADMIN,
            ACCESS_KEY_ID, handle));
    assertEquals(MANAGED_S3_ACCESS_KEY_SECRET_UNAVAILABLE,
        exception.getResult());
  }

  private String responseHandle(OMRequest preExecuted, Operation operation) {
    S3ManagedAccessKeyInfoProto info;
    if (operation == Operation.CREATE) {
      info = preExecuted.getUpdateCreateManagedS3AccessKeyRequest().getInfo();
    } else {
      info = preExecuted.getUpdateRotateManagedS3AccessKeyRequest().getInfo();
    }
    return retrievalManager.responseHandle(preExecuted.getClientId(),
        info.getAccessKeyId(), operation,
        ManagedS3AccessKeyRequestHelper.sha256(info.getEncryptedSecretKey()));
  }

  private void assertNoPlaintext(byte[] haystack, String plaintext) {
    assertFalse(contains(haystack, plaintext.getBytes(UTF_8)),
        "unexpected plaintext: " + plaintext);
  }

  private boolean contains(byte[] haystack, byte[] needle) {
    for (int i = 0; i <= haystack.length - needle.length; i++) {
      if (Arrays.equals(Arrays.copyOfRange(haystack, i, i + needle.length),
          needle)) {
        return true;
      }
    }
    return false;
  }
}
