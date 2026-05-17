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

package org.apache.hadoop.ozone.security;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PERMISSION_DENIED;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.time.Clock;
import java.time.ZoneOffset;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.S3ManagedAccessKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.security.ManagedS3AccessKeySecretEnvelopeCodec;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authenticates OM-managed local S3 access keys for normal S3 requests.
 */
@InterfaceAudience.Private
public final class ManagedAccessKeyAuthenticator {

  private static final Logger LOG =
      LoggerFactory.getLogger(ManagedAccessKeyAuthenticator.class);
  private static final Clock CLOCK = Clock.system(ZoneOffset.UTC);

  private ManagedAccessKeyAuthenticator() {
  }

  /**
   * @return true when a managed key authenticated the request, false when a
   *         secure cluster may continue to the legacy S3SecretManager path.
   */
  public static boolean authenticate(OMRequest omRequest,
      OzoneManager ozoneManager) throws ServiceException, OMException {
    ManagedS3AccessKeyConfig config =
        ozoneManager.getManagedS3AccessKeyConfig();
    if (config == null || !config.isEnabled()) {
      return false;
    }

    OzoneManagerRatisUtils.checkLeaderStatus(ozoneManager);

    if (!ozoneManager.isSecurityEnabled()) {
      throw permissionDenied("Managed S3 access key data path is not " +
          "enabled for non-secure clusters in Phase 5", null);
    }

    checkLayoutFinalized(ozoneManager);

    final S3Authentication s3Authentication = omRequest.getS3Authentication();
    final String accessKeyId = s3Authentication.getAccessId();
    final S3ManagedAccessKeyInfo info = getManagedAccessKeyInfo(
        ozoneManager, accessKeyId);
    if (info == null) {
      return false;
    }

    validateStoredInfo(config, accessKeyId, info);
    validateSignatureAndInstallContext(s3Authentication, ozoneManager, info);
    return true;
  }

  private static void checkLayoutFinalized(OzoneManager ozoneManager)
      throws OMException {
    if (!ozoneManager.getVersionManager()
        .isAllowed(OMLayoutFeature.MANAGED_LOCAL_S3_ACCESS_KEYS)) {
      // This is a layout/server-state failure, not a managed credential
      // data-path auth failure, so keep the existing pre-finalization code.
      throw new OMException("Managed S3 access-key data path is not " +
          "allowed before layout finalization",
          NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION);
    }
  }

  private static S3ManagedAccessKeyInfo getManagedAccessKeyInfo(
      OzoneManager ozoneManager, String accessKeyId) throws OMException {
    try {
      return ozoneManager.getMetadataManager().getS3ManagedAccessKeyTable()
          .getIfExist(accessKeyId);
    } catch (IOException | RuntimeException e) {
      throw permissionDenied("Managed S3 access key lookup failed", e);
    }
  }

  private static void validateStoredInfo(ManagedS3AccessKeyConfig config,
      String accessKeyId, S3ManagedAccessKeyInfo info) throws OMException {
    if (!StringUtils.equals(accessKeyId, info.getAccessKeyId())) {
      throw permissionDenied("Managed S3 access key metadata is inconsistent",
          null);
    }
    if (config.isLocalPolicyEnabled()) {
      throw permissionDenied("Managed S3 access key local policy evaluation " +
          "is not available in Phase 5", null);
    }
    if (StringUtils.isNotBlank(info.getPolicyDocument())) {
      throw permissionDenied("Managed S3 access key policy evaluation is " +
          "not available in Phase 5", null);
    }
    if (info.isDisabled()) {
      throw permissionDenied("Managed S3 access key is disabled", null);
    }
    if (info.getExpiresAt() > 0 && info.getExpiresAt() <= CLOCK.millis()) {
      throw permissionDenied("Managed S3 access key is expired", null);
    }
  }

  private static void validateSignatureAndInstallContext(
      S3Authentication s3Authentication, OzoneManager ozoneManager,
      S3ManagedAccessKeyInfo info) throws OMException {
    byte[] plaintextSecret = null;
    try {
      final String credentialAccessKeyId = s3Authentication.getAccessId();
      final KeyProviderCryptoExtension provider = ozoneManager.getKmsProvider();
      if (provider == null) {
        throw permissionDenied("Managed S3 access key KMS provider is " +
            "unavailable", null);
      }
      plaintextSecret = ManagedS3AccessKeySecretEnvelopeCodec.decrypt(
          info.getAccessKeyId(), info.getEffectiveUser(), info.getCreatedAt(),
          info.getEncryptedSecretKey(), provider);
      if (!AWSV4AuthValidator.validateRequest(
          s3Authentication.getStringToSign(), s3Authentication.getSignature(),
          plaintextSecret)) {
        throw permissionDenied("Managed S3 access key signature mismatch",
            null);
      }
      OzoneManager.setManagedS3AccessKeyAuthContext(
          new ManagedS3AccessKeyAuthContext(
              credentialAccessKeyId,
              credentialAccessKeyId,
              info.getEffectiveUser(),
              info.getGroups(),
              StringUtils.isNotBlank(info.getPolicyDocument())));
    } catch (OMException e) {
      throw e;
    } catch (IOException | RuntimeException e) {
      LOG.debug("Managed S3 access key authentication failed for access key " +
          "{}", info.getAccessKeyId(), e);
      throw permissionDenied("Managed S3 access key authentication failed", e);
    } finally {
      ManagedS3AccessKeySecretEnvelopeCodec.clear(plaintextSecret);
    }
  }

  private static OMException permissionDenied(String message, Throwable cause) {
    return cause == null ? new OMException(message, PERMISSION_DENIED) :
        new OMException(message, cause, PERMISSION_DENIED);
  }
}
