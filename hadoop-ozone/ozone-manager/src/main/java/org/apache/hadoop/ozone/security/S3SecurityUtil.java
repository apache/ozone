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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_TOKEN;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.REVOKED_TOKEN;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto.Type.S3AUTHINFO;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.time.Clock;
import java.time.ZoneOffset;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.security.token.SecretManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class which holds methods required for parse/validation of
 * S3 Authentication Information which is part of OMRequest.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class S3SecurityUtil {

  private static final Clock CLOCK = Clock.system(ZoneOffset.UTC);
  private static final Logger LOG = LoggerFactory.getLogger(S3SecurityUtil.class);

  private S3SecurityUtil() {
  }

  /**
   * Validate S3 Credentials which are part of {@link OMRequest}.
   *
   * If validation is successful returns, else throw exception.
   * @throws OMException         validation failure
   *         ServiceException    Server is not leader or not ready
   */
  public static void validateS3Credential(OMRequest omRequest,
      OzoneManager ozoneManager) throws ServiceException, OMException {
    if (ozoneManager.isSecurityEnabled()) {
      // If STS session token is present, decode, decrypt and validate it once and save in thread-local
      if (omRequest.getS3Authentication().hasSessionToken()) {
        final String token = omRequest.getS3Authentication().getSessionToken();
        if (!token.isEmpty()) {
          final STSTokenIdentifier stsTokenIdentifier = STSSecurityUtil.constructValidateAndDecryptSTSToken(
              token, ozoneManager.getSecretKeyClient(), CLOCK);

          // Ensure the token is not revoked
          if (isRevokedStsToken(token, ozoneManager)) {
            LOG.info("Session token has been revoked: {}, {}", stsTokenIdentifier.getTempAccessKeyId(), token);
            throw new OMException("STS token has been revoked", REVOKED_TOKEN);
          }

          // Ensure the principal that created the STS token (originalAccessKeyId) has not been revoked
          if (isOriginalAccessKeyIdRevoked(stsTokenIdentifier, ozoneManager)) {
            LOG.info("OriginalAccessKeyId for session token has been revoked: {}, {}",
                stsTokenIdentifier.getOriginalAccessKeyId(), stsTokenIdentifier.getTempAccessKeyId());
            throw new OMException("STS token no longer valid: OriginalAccessKeyId principal revoked", REVOKED_TOKEN);
          }

          // HMAC signature and expiration were validated above.  Now validate AWS signature.
          validateSTSTokenAwsSignature(stsTokenIdentifier, omRequest);
          OzoneManager.setStsTokenIdentifier(stsTokenIdentifier);
          return;
        }
      }

      OzoneTokenIdentifier s3Token = constructS3Token(omRequest);
      try {
        // authenticate user with signature verification through
        // delegationTokenMgr validateToken via retrievePassword
        ozoneManager.getDelegationTokenMgr().retrievePassword(s3Token);
      } catch (SecretManager.InvalidToken e) {
        if (e.getCause() != null &&
            (e.getCause().getClass() == OMNotLeaderException.class ||
            e.getCause().getClass() == OMLeaderNotReadyException.class)) {
          throw new ServiceException(e.getCause());
        }

        // TODO: Just check are we okay to log entire token in failure case.
        OzoneManagerProtocolServerSideTranslatorPB.getLog().error(
            "signatures do NOT match for S3 identifier:{}", s3Token, e);
        throw new OMException("User " + s3Token.getAwsAccessId()
            + " request authorization failure: signatures do NOT match",
            INVALID_TOKEN);
      }
    }
  }

  /**
   * Construct and return {@link OzoneTokenIdentifier} from {@link OMRequest}.
   */
  private static OzoneTokenIdentifier constructS3Token(OMRequest omRequest) {
    S3Authentication auth = omRequest.getS3Authentication();
    OzoneTokenIdentifier s3Token = new OzoneTokenIdentifier();
    s3Token.setTokenType(S3AUTHINFO);
    s3Token.setStrToSign(auth.getStringToSign());
    s3Token.setSignature(auth.getSignature());
    s3Token.setAwsAccessId(auth.getAccessId());
    s3Token.setOwner(new Text(auth.getAccessId()));
    return s3Token;
  }

  /**
   * Validates the AWS signature of an STSTokenIdentifier that has already been decrypted.
   * @param stsTokenIdentifier      the decrypted STS token
   * @param omRequest               the OMRequest containing STS token
   * @throws OMException            if the AWS signature validation fails
   */
  private static void validateSTSTokenAwsSignature(STSTokenIdentifier stsTokenIdentifier, OMRequest omRequest)
      throws OMException {
    final String secretAccessKey = stsTokenIdentifier.getSecretAccessKey();
    final S3Authentication s3Authentication = omRequest.getS3Authentication();
    if (AWSV4AuthValidator.validateRequest(
        s3Authentication.getStringToSign(), s3Authentication.getSignature(), secretAccessKey)) {
      return;
    }
    throw new OMException(
        "STS token validation failed for token: " + omRequest.getS3Authentication().getSessionToken(), INVALID_TOKEN);
  }

  /**
   * Returns true if the STS session token is present in the revoked STS token table.
   */
  private static boolean isRevokedStsToken(String sessionToken, OzoneManager ozoneManager)
      throws OMException {
    try {
      final OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
      if (metadataManager == null) {
        final String msg = "Could not determine STS revocation: metadataManager is null";
        LOG.warn(msg);
        throw new OMException(msg, INTERNAL_ERROR);
      }

      final Table<String, Long> revokedStsTokenTable = metadataManager.getS3RevokedStsTokenTable();
      if (revokedStsTokenTable == null) {
        final String msg = "Could not determine STS revocation: revokedStsTokenTable is null";
        LOG.warn(msg);
        throw new OMException(msg, INTERNAL_ERROR);
      }

      return revokedStsTokenTable.getIfExist(sessionToken) != null;
    } catch (Exception e) {
      final String msg = "Could not determine STS revocation because of Exception: " + e.getMessage();
      LOG.warn(msg, e);
      throw new OMException(msg, e, INTERNAL_ERROR);
    }
  }

  /**
   * Returns true if the originalAccessKeyId of the STS token has been revoked.
   */
  private static boolean isOriginalAccessKeyIdRevoked(STSTokenIdentifier stsTokenIdentifier, OzoneManager ozoneManager)
      throws OMException {
    // We already know originalAccessKeyId is not null from STSSecurityUtil.ensureEssentialFieldsArePresentInToken()
    // method called from STSSecurityUtil.constructValidateAndDecryptSTSToken() method above
    final String originalAccessKeyId = stsTokenIdentifier.getOriginalAccessKeyId();
    try {
      // If the secret for the original principal is missing, it means it was revoked
      return !ozoneManager.getS3SecretManager().hasS3Secret(originalAccessKeyId);
    } catch (IOException e) {
      final String msg = "Could not determine if original principal is revoked: " + e.getMessage();
      LOG.warn(msg, e);
      throw new OMException(msg, e, INTERNAL_ERROR);
    }
  }
}
