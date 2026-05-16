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
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RotateManagedS3AccessKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateRotateManagedS3AccessKeyRequest;
import org.apache.hadoop.ozone.security.ManagedS3AccessKeyConfig;

/**
 * Handles the external rotate managed S3 access-key request. This class only
 * runs before Ratis submission.
 */
@BelongsToLayoutVersion(OMLayoutFeature.MANAGED_LOCAL_S3_ACCESS_KEYS)
public class RotateManagedS3AccessKeyRequest extends OMClientRequest {

  private String pendingAccessKeyId;
  private ByteString pendingOperationHash;
  private String pendingRetrievalHandle;

  public RotateManagedS3AccessKeyRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    OMRequest omRequest = super.preExecute(ozoneManager);
    org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
        .RotateManagedS3AccessKeyRequest request =
        omRequest.getRotateManagedS3AccessKeyRequest();
    ManagedS3AccessKeyRequestHelper.checkEnabled(ozoneManager);
    ManagedS3AccessKeyRequestHelper.checkLayoutFinalized(ozoneManager);
    ManagedS3AccessKeyRequestHelper.checkAdmin(ozoneManager,
        omRequest.getUserInfo());

    String accessKeyId = ManagedS3AccessKeyRequestHelper.required(
        request.getAccessKeyId(), "accessKeyId");
    S3ManagedAccessKeyInfo existing = ozoneManager.getMetadataManager()
        .getS3ManagedAccessKeyTable().get(accessKeyId);
    if (existing == null) {
      throw new OMException("Managed S3 access key '" + accessKeyId +
          "' not found", ResultCodes.MANAGED_S3_ACCESS_KEY_NOT_FOUND);
    }
    if (existing.isDisabled()) {
      throw new OMException("Managed S3 access key '" + accessKeyId +
          "' is disabled", ResultCodes.MANAGED_S3_ACCESS_KEY_DISABLED);
    }

    ManagedS3AccessKeyConfig config =
        ozoneManager.getManagedS3AccessKeyConfig();
    long now = ManagedS3AccessKeyRequestHelper.now();
    long expiresAt = ManagedS3AccessKeyRequestHelper.rotateExpiresAt(config,
        existing, now, request.getExpiresAt());
    String caller = ManagedS3AccessKeyRequestHelper.caller(
        omRequest.getUserInfo());

    byte[] plaintextSecret = null;
    String retrievalHandle = null;
    try {
      plaintextSecret = ManagedS3AccessKeyRequestHelper.secretFromRequest(
          request.getCustomSecret(), config);
      EncryptedSecret encryptedSecret =
          ManagedS3AccessKeyRequestHelper.encryptSecret(ozoneManager,
              existing.getAccessKeyId(), existing.getEffectiveUser(),
              existing.getCreatedAt(), plaintextSecret);

      S3ManagedAccessKeyInfo rotated = existing.toBuilder()
          .setEncryptedSecretKey(encryptedSecret.getEnvelope())
          .setSecretKeyId(encryptedSecret.getSecretKeyId())
          .setExpiresAt(expiresAt)
          .build();
      ByteString operationHash = ManagedS3AccessKeyRequestHelper.sha256(
          rotated.getEncryptedSecretKey());

      retrievalHandle = ozoneManager
          .getManagedS3AccessKeySecretRetrievalManager()
          .store(caller, accessKeyId, Operation.ROTATE, plaintextSecret);
      ozoneManager.getManagedS3AccessKeySecretRetrievalManager()
          .bindResponseHandle(omRequest.getClientId(), accessKeyId,
              Operation.ROTATE, operationHash, retrievalHandle);
      pendingAccessKeyId = accessKeyId;
      pendingOperationHash = operationHash;
      pendingRetrievalHandle = retrievalHandle;

      OMRequest.Builder builder = omRequest.toBuilder()
          .clearRotateManagedS3AccessKeyRequest()
          .setUpdateRotateManagedS3AccessKeyRequest(
              UpdateRotateManagedS3AccessKeyRequest.newBuilder()
                  .setInfo(rotated.getProtobuf())
                  .setExpectedPreviousSecretKeyId(existing.getSecretKeyId())
                  .setExpectedPreviousEncryptedSecretKeySha256(
                      ManagedS3AccessKeyRequestHelper.sha256(
                          existing.getEncryptedSecretKey())));
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
              pendingAccessKeyId, Operation.ROTATE, pendingOperationHash);
      pendingRetrievalHandle = null;
    }
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      ExecutionContext context) {
    throw new UnsupportedOperationException(
        "RotateManagedS3AccessKeyRequest must be rewritten in preExecute");
  }

  /**
   * Ratis-applied update rotate request.
   */
  public static class Update extends OMClientRequest {

    public Update(OMRequest omRequest) {
      super(omRequest);
    }

    @Override
    public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
      throw new OMException("UpdateRotateManagedS3AccessKeyRequest is an " +
          "internal Ratis-applied request", ResultCodes.INVALID_REQUEST);
    }

    @Override
    public void handleRequestFailure(OzoneManager ozoneManager) {
      if (getOmRequest().hasUpdateRotateManagedS3AccessKeyRequest()) {
        S3ManagedAccessKeyInfo info = S3ManagedAccessKeyInfo.fromProtobuf(
            getOmRequest().getUpdateRotateManagedS3AccessKeyRequest()
                .getInfo());
        ozoneManager.getManagedS3AccessKeySecretRetrievalManager()
            .removeResponseHandle(getOmRequest().getClientId(),
                info.getAccessKeyId(), Operation.ROTATE,
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
          getOmRequest().getUpdateRotateManagedS3AccessKeyRequest().getInfo());
      String retrievalHandle = ozoneManager
          .getManagedS3AccessKeySecretRetrievalManager()
          .responseHandle(getOmRequest().getClientId(), info.getAccessKeyId(),
              Operation.ROTATE, ManagedS3AccessKeyRequestHelper.sha256(
                  info.getEncryptedSecretKey()));
      try {
        ManagedS3AccessKeyRequestHelper.checkEnabled(ozoneManager);
        ManagedS3AccessKeyRequestHelper.checkLayoutFinalized(ozoneManager);
        ozoneManager.getS3SecretManager().doUnderLock(
            info.getAccessKeyId(), s3SecretManager -> {
              S3ManagedAccessKeyInfo existing =
                  ozoneManager.getMetadataManager()
                      .getS3ManagedAccessKeyTable()
                      .get(info.getAccessKeyId());
              if (existing == null) {
                throw new OMException("Managed S3 access key '" +
                    info.getAccessKeyId() + "' not found",
                    ResultCodes.MANAGED_S3_ACCESS_KEY_NOT_FOUND);
              }
              if (existing.isDisabled()) {
                throw new OMException("Managed S3 access key '" +
                    info.getAccessKeyId() + "' is disabled",
                    ResultCodes.MANAGED_S3_ACCESS_KEY_DISABLED);
              }
              ensureExpectedPreviousSecret(existing, getOmRequest()
                  .getUpdateRotateManagedS3AccessKeyRequest()
                  .getExpectedPreviousSecretKeyId(), getOmRequest()
                  .getUpdateRotateManagedS3AccessKeyRequest()
                  .getExpectedPreviousEncryptedSecretKeySha256());
              ensureImmutableFields(existing, info);
              ozoneManager.getMetadataManager().getS3ManagedAccessKeyTable()
                  .addCacheEntry(new CacheKey<>(info.getAccessKeyId()),
                      CacheValue.get(context.getIndex(), info));
              return null;
            });

        RotateManagedS3AccessKeyResponse response =
            RotateManagedS3AccessKeyResponse.newBuilder()
                .setMetadata(ManagedS3AccessKeyRequestHelper.toMetadata(info))
                .setRetrievalHandle(retrievalHandle)
                .build();
        markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
            org.apache.hadoop.ozone.audit.OMAction
                .ROTATE_MANAGED_S3_ACCESS_KEY,
            ManagedS3AccessKeyRequestHelper.auditMap(info), null,
            getOmRequest().getUserInfo()));
        return new ManagedS3AccessKeyPutResponse(info,
            omResponse.setRotateManagedS3AccessKeyResponse(response).build());
      } catch (Exception ex) {
        exception = ex;
        ozoneManager.getManagedS3AccessKeySecretRetrievalManager()
            .removeResponseHandle(getOmRequest().getClientId(),
                info.getAccessKeyId(), Operation.ROTATE,
                ManagedS3AccessKeyRequestHelper.sha256(
                    info.getEncryptedSecretKey()));
        markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
            org.apache.hadoop.ozone.audit.OMAction
                .ROTATE_MANAGED_S3_ACCESS_KEY,
            ManagedS3AccessKeyRequestHelper.auditMap(info), exception,
            getOmRequest().getUserInfo()));
        return new ManagedS3AccessKeyPutResponse(null,
            createErrorOMResponse(omResponse, exception));
      }
    }

    private static void ensureImmutableFields(S3ManagedAccessKeyInfo existing,
        S3ManagedAccessKeyInfo updated) throws OMException {
      if (!existing.getAccessKeyId().equals(updated.getAccessKeyId()) ||
          !existing.getEffectiveUser().equals(updated.getEffectiveUser()) ||
          !existing.getGroups().equals(updated.getGroups()) ||
          !existing.getDescription().equals(updated.getDescription()) ||
          existing.getCreatedAt() != updated.getCreatedAt() ||
          !existing.getCreatedBy().equals(updated.getCreatedBy()) ||
          !existing.getPolicyDocument().equals(updated.getPolicyDocument())) {
        throw new OMException("Managed S3 access-key rotate attempted to " +
            "change immutable metadata", ResultCodes.INVALID_REQUEST);
      }
    }

    private static void ensureExpectedPreviousSecret(
        S3ManagedAccessKeyInfo existing, String expectedSecretKeyId,
        com.google.protobuf.ByteString expectedEncryptedSecretKeySha256)
        throws OMException {
      if (!existing.getSecretKeyId().equals(expectedSecretKeyId) ||
          !ManagedS3AccessKeyRequestHelper.sha256(
              existing.getEncryptedSecretKey())
              .equals(expectedEncryptedSecretKeySha256)) {
        throw new OMException("Managed S3 access-key rotate was based on " +
            "stale secret metadata", ResultCodes.INVALID_REQUEST);
      }
    }
  }
}
