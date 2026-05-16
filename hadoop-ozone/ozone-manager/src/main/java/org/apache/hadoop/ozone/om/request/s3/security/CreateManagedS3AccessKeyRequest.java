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

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.S3ManagedAccessKeyInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.security.ManagedS3AccessKeyPutResponse;
import org.apache.hadoop.ozone.om.security.ManagedS3AccessKeySecretEnvelopeCodec;
import org.apache.hadoop.ozone.om.security.ManagedS3AccessKeySecretEnvelopeCodec.EncryptedSecret;
import org.apache.hadoop.ozone.om.security.ManagedS3AccessKeySecretRetrievalManager.Operation;
import org.apache.hadoop.ozone.om.upgrade.BelongsToLayoutVersion;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateManagedS3AccessKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateCreateManagedS3AccessKeyRequest;
import org.apache.hadoop.ozone.security.ManagedS3AccessKeyConfig;

/**
 * Handles the external create managed S3 access-key request. This class only
 * runs before Ratis submission.
 */
@BelongsToLayoutVersion(OMLayoutFeature.MANAGED_LOCAL_S3_ACCESS_KEYS)
public class CreateManagedS3AccessKeyRequest extends OMClientRequest {

  private String pendingAccessKeyId;
  private ByteString pendingOperationHash;
  private String pendingRetrievalHandle;

  public CreateManagedS3AccessKeyRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    OMRequest omRequest = super.preExecute(ozoneManager);
    org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
        .CreateManagedS3AccessKeyRequest request =
        omRequest.getCreateManagedS3AccessKeyRequest();
    ManagedS3AccessKeyRequestHelper.checkEnabled(ozoneManager);
    ManagedS3AccessKeyRequestHelper.checkLayoutFinalized(ozoneManager);
    ManagedS3AccessKeyRequestHelper.checkAdmin(ozoneManager,
        omRequest.getUserInfo());
    ManagedS3AccessKeyRequestHelper.requireNoPolicyDocument(
        request.getPolicyDocument());

    ManagedS3AccessKeyConfig config =
        ozoneManager.getManagedS3AccessKeyConfig();
    String caller = ManagedS3AccessKeyRequestHelper.caller(
        omRequest.getUserInfo());
    String accessKeyId =
        ManagedS3AccessKeyRequestHelper.optional(request.getAccessKeyId());
    if (accessKeyId.isEmpty()) {
      accessKeyId = ManagedS3AccessKeyRequestHelper.generatedAccessKeyId();
    }
    String effectiveUser = ManagedS3AccessKeyRequestHelper.required(
        request.getEffectiveUser(), "effectiveUser");
    String description = ManagedS3AccessKeyRequestHelper.optional(
        request.getDescription());
    long createdAt = ManagedS3AccessKeyRequestHelper.now();
    long expiresAt = ManagedS3AccessKeyRequestHelper.effectiveExpiresAt(
        config, createdAt, request.getExpiresAt());
    List<String> groups =
        ManagedS3AccessKeyRequestHelper.snapshotGroups(effectiveUser);

    byte[] plaintextSecret = null;
    String retrievalHandle = null;
    try {
      plaintextSecret = ManagedS3AccessKeyRequestHelper.secretFromRequest(
          request.getCustomSecret(), config);
      EncryptedSecret encryptedSecret =
          ManagedS3AccessKeyRequestHelper.encryptSecret(ozoneManager,
              accessKeyId, effectiveUser, createdAt, plaintextSecret);

      S3ManagedAccessKeyInfo info = S3ManagedAccessKeyInfo.newBuilder()
          .setAccessKeyId(accessKeyId)
          .setEncryptedSecretKey(encryptedSecret.getEnvelope())
          .setSecretKeyId(encryptedSecret.getSecretKeyId())
          .setEffectiveUser(effectiveUser)
          .setGroups(groups)
          .setDescription(description)
          .setCreatedAt(createdAt)
          .setExpiresAt(expiresAt)
          .setDisabled(false)
          .setCreatedBy(caller)
          .setPolicyDocument("")
          .build();
      ByteString operationHash = ManagedS3AccessKeyRequestHelper.sha256(
          info.getEncryptedSecretKey());

      retrievalHandle = ozoneManager
          .getManagedS3AccessKeySecretRetrievalManager()
          .store(caller, accessKeyId, Operation.CREATE, plaintextSecret);
      ozoneManager.getManagedS3AccessKeySecretRetrievalManager()
          .bindResponseHandle(omRequest.getClientId(), accessKeyId,
              Operation.CREATE, operationHash, retrievalHandle);
      pendingAccessKeyId = accessKeyId;
      pendingOperationHash = operationHash;
      pendingRetrievalHandle = retrievalHandle;

      OMRequest.Builder builder = omRequest.toBuilder()
          .clearCreateManagedS3AccessKeyRequest()
          .setUpdateCreateManagedS3AccessKeyRequest(
              UpdateCreateManagedS3AccessKeyRequest.newBuilder()
                  .setInfo(info.getProtobuf()));
      if (getOmRequest().hasTraceID()) {
        builder.setTraceID(getOmRequest().getTraceID());
      }
      return builder.build();
    } catch (IOException | RuntimeException e) {
      if (retrievalHandle != null) {
        ozoneManager.getManagedS3AccessKeySecretRetrievalManager()
            .remove(retrievalHandle);
      }
      throw e;
    } finally {
      ManagedS3AccessKeySecretEnvelopeCodec.clear(plaintextSecret);
    }
  }

  @Override
  public void handleRequestFailure(OzoneManager ozoneManager) {
    if (pendingRetrievalHandle != null) {
      ozoneManager.getManagedS3AccessKeySecretRetrievalManager()
          .removeResponseHandle(getOmRequest().getClientId(),
              pendingAccessKeyId, Operation.CREATE, pendingOperationHash);
      pendingRetrievalHandle = null;
    }
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      ExecutionContext context) {
    throw new UnsupportedOperationException(
        "CreateManagedS3AccessKeyRequest must be rewritten in preExecute");
  }

  /**
   * Ratis-applied update create request.
   */
  public static class Update extends OMClientRequest {

    public Update(OMRequest omRequest) {
      super(omRequest);
    }

    @Override
    public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
      throw new org.apache.hadoop.ozone.om.exceptions.OMException(
          "UpdateCreateManagedS3AccessKeyRequest is an internal " +
              "Ratis-applied request",
          org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes
              .INVALID_REQUEST);
    }

    @Override
    public void handleRequestFailure(OzoneManager ozoneManager) {
      if (getOmRequest().hasUpdateCreateManagedS3AccessKeyRequest()) {
        S3ManagedAccessKeyInfo info = S3ManagedAccessKeyInfo.fromProtobuf(
            getOmRequest().getUpdateCreateManagedS3AccessKeyRequest()
                .getInfo());
        ozoneManager.getManagedS3AccessKeySecretRetrievalManager()
            .removeResponseHandle(getOmRequest().getClientId(),
                info.getAccessKeyId(), Operation.CREATE,
                ManagedS3AccessKeyRequestHelper.sha256(
                    info.getEncryptedSecretKey()));
      }
    }

    @Override
    public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
        ExecutionContext context) {
      OMResponse.Builder omResponse =
          OmResponseUtil.getOMResponseBuilder(getOmRequest());
      Exception exception = null;
      S3ManagedAccessKeyInfo info = S3ManagedAccessKeyInfo.fromProtobuf(
          getOmRequest().getUpdateCreateManagedS3AccessKeyRequest().getInfo());
      String retrievalHandle = ozoneManager
          .getManagedS3AccessKeySecretRetrievalManager()
          .responseHandle(getOmRequest().getClientId(), info.getAccessKeyId(),
              Operation.CREATE, ManagedS3AccessKeyRequestHelper.sha256(
                  info.getEncryptedSecretKey()));
      try {
        ManagedS3AccessKeyRequestHelper.checkEnabled(ozoneManager);
        ManagedS3AccessKeyRequestHelper.checkLayoutFinalized(ozoneManager);
        ozoneManager.getS3SecretManager().doUnderLock(
            info.getAccessKeyId(), s3SecretManager -> {
              if (!s3SecretManager.isBatchSupported()) {
                throw new org.apache.hadoop.ozone.om.exceptions.OMException(
                    "Managed S3 access-key create is not supported with the " +
                        "configured external S3 secret provider",
                    org.apache.hadoop.ozone.om.exceptions.OMException
                        .ResultCodes
                        .MANAGED_S3_ACCESS_KEY_OPERATION_NOT_SUPPORTED);
              }
              if (ozoneManager.getMetadataManager()
                  .getS3ManagedAccessKeyTable()
                  .get(info.getAccessKeyId()) != null ||
                  s3SecretManager.hasS3Secret(info.getAccessKeyId())) {
                throw new org.apache.hadoop.ozone.om.exceptions.OMException(
                    "Managed S3 access key '" + info.getAccessKeyId() +
                        "' already exists",
                    org.apache.hadoop.ozone.om.exceptions.OMException
                        .ResultCodes.MANAGED_S3_ACCESS_KEY_ALREADY_EXISTS);
              }
              ozoneManager.getMetadataManager().getS3ManagedAccessKeyTable()
                  .addCacheEntry(new CacheKey<>(info.getAccessKeyId()),
                      CacheValue.get(context.getIndex(), info));
              return null;
            });

        CreateManagedS3AccessKeyResponse response =
            CreateManagedS3AccessKeyResponse.newBuilder()
                .setMetadata(ManagedS3AccessKeyRequestHelper.toMetadata(info))
                .setRetrievalHandle(retrievalHandle)
                .build();
        markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
            org.apache.hadoop.ozone.audit.OMAction
                .CREATE_MANAGED_S3_ACCESS_KEY,
            ManagedS3AccessKeyRequestHelper.auditMap(info), null,
            getOmRequest().getUserInfo()));
        return new ManagedS3AccessKeyPutResponse(info,
            omResponse.setCreateManagedS3AccessKeyResponse(response).build());
      } catch (Exception ex) {
        exception = ex;
        ozoneManager.getManagedS3AccessKeySecretRetrievalManager()
            .removeResponseHandle(getOmRequest().getClientId(),
                info.getAccessKeyId(), Operation.CREATE,
                ManagedS3AccessKeyRequestHelper.sha256(
                    info.getEncryptedSecretKey()));
        markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
            org.apache.hadoop.ozone.audit.OMAction
                .CREATE_MANAGED_S3_ACCESS_KEY,
            ManagedS3AccessKeyRequestHelper.auditMap(info), exception,
            getOmRequest().getUserInfo()));
        return new ManagedS3AccessKeyPutResponse(null,
            createErrorOMResponse(omResponse, exception));
      }
    }
  }
}
