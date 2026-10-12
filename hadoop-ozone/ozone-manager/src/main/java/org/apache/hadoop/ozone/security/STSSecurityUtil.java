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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_TOKEN;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TOKEN_EXPIRED;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;

/**
 * Utility class with methods to validate and decrypt STS tokens.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class STSSecurityUtil {
  private STSSecurityUtil() {
  }

  /**
   * Constructs, validates and decrypts STS session token.
   *
   * @param sessionToken    the session token from the x-amz-security-token header
   * @param secretKeyClient the Ozone Manager secretKeyClient
   * @param clock           the system clock
   * @return the STSTokenIdentifier with decrypted secretAccessKey
   * @throws OMException if the token is not valid or processing failed otherwise
   */
  public static STSTokenIdentifier constructValidateAndDecryptSTSToken(String sessionToken,
      SecretKeyClient secretKeyClient, Clock clock) throws OMException {
    try {
      final Token<STSTokenIdentifier> token = decodeTokenFromString(sessionToken);
      return verifyAndDecryptToken(token, secretKeyClient, clock);
    } catch (SecretManager.InvalidToken e) {
      throw new OMException("Invalid STS token format: " + e.getMessage(), e, INVALID_TOKEN);
    }
  }

  /**
   * Verifies an STS Token by performing multiple checks.
   *
   * @param token the token to verify
   * @param clock the system clock
   * @return the STSTokenIdentifier with decrypted secretAccessKey
   * @throws SecretManager.InvalidToken if the token is invalid
   */
  private static STSTokenIdentifier verifyAndDecryptToken(Token<STSTokenIdentifier> token,
      SecretKeyClient secretKeyClient, Clock clock) throws SecretManager.InvalidToken, OMException {
    if (!STSTokenIdentifier.KIND_NAME.equals(token.getKind())) {
      throw new SecretManager.InvalidToken("Invalid STS token - kind is incorrect: " + token.getKind());
    }

    if (!STSTokenIdentifier.STS_SERVICE.equals(token.getService().toString())) {
      throw new SecretManager.InvalidToken("Invalid STS token - service is incorrect: " + token.getService());
    }

    final byte[] tokenBytes = token.getIdentifier();
    final OMTokenProto proto;
    try {
      proto = OMTokenProto.parseFrom(tokenBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new SecretManager.InvalidToken("Invalid STS token - could not parse protocol buffer: " + e.getMessage());
    }
    final UUID secretKeyId;
    try {
      secretKeyId = UUID.fromString(proto.getSecretKeyId());
    } catch (IllegalArgumentException e) {
      throw new SecretManager.InvalidToken("Invalid STS token - secretKeyId was not valid: " + proto.getSecretKeyId());
    }

    final ManagedSecretKey secretKey = getValidatedSecretKey(secretKeyId, secretKeyClient);

    // Verify the signature over the identifier bytes before anything is decrypted or parsed out of them, so
    // that only bytes this cluster actually issued are ever fed to the cipher. Keeping this first also means a
    // forged token always fails the same way instead of revealing which field was rejected.
    if (!secretKey.isValidSignature(tokenBytes, token.getPassword())) {
      throw new SecretManager.InvalidToken("Invalid STS token - signature is not correct");
    }

    final STSTokenIdentifier tokenId = new STSTokenIdentifier();
    tokenId.setManagedSecretKey(secretKey);
    try {
      tokenId.readFromByteArray(tokenBytes);
    } catch (OMException e) {
      throw e;
    } catch (IOException e) {
      throw new SecretManager.InvalidToken("Invalid STS token - could not readFromByteArray: " + e.getMessage());
    }

    // Ensure essential fields are present in the token
    ensureEssentialFieldsArePresentInToken(tokenId);

    // Check expiration
    if (tokenId.isExpired(clock.instant())) {
      throw new OMException("Invalid STS token - token expired at " + tokenId.getExpiry(), TOKEN_EXPIRED);
    }

    return tokenId;
  }

  private static ManagedSecretKey getValidatedSecretKey(UUID secretKeyId, SecretKeyClient secretKeyClient)
      throws SecretManager.InvalidToken, OMException {
    if (secretKeyId == null) {
      throw new SecretManager.InvalidToken("STS token missing secret key ID");
    }

    final ManagedSecretKey secretKey;
    try {
      secretKey = secretKeyClient.getSecretKey(secretKeyId);
    } catch (Exception e) {
      throw new SecretManager.InvalidToken("Failed to retrieve secret key: " + e.getMessage());
    }

    if (secretKey == null) {
      throw new SecretManager.InvalidToken("Secret key not found for STS token secretKeyId: " + secretKeyId);
    }

    if (secretKey.isExpired()) {
      throw new OMException(
          "Token cannot be verified due to expired secret key: " + secretKeyId + " Token expired at " +
              secretKey.getExpiryTime(), TOKEN_EXPIRED);
    }

    return secretKey;
  }

  private static Token<STSTokenIdentifier> decodeTokenFromString(String encodedToken)
      throws SecretManager.InvalidToken {
    final Token<STSTokenIdentifier> token = new Token<>();
    // token.decodeFromUrlString() only declares IOException, but deserialization can throw
    // unchecked exceptions (e.g. NegativeArraySizeException) when malformed input decodes to a
    // negative byte-array length. Map those to InvalidToken (via catching RuntimeException)
    // instead of failing the OM request.
    try {
      token.decodeFromUrlString(encodedToken);
      final String canonical = token.encodeToUrlString();
      if (!canonical.equals(encodedToken)) {
        throw new SecretManager.InvalidToken("Failed to decode STS token string: non-canonical token encoding");
      }
      return token;
    } catch (IOException | RuntimeException e) {
      throw new SecretManager.InvalidToken("Failed to decode STS token string: " + e);
    }
  }

  @VisibleForTesting
  static void ensureEssentialFieldsArePresentInToken(STSTokenIdentifier stsTokenIdentifier)
      throws SecretManager.InvalidToken {
    if (StringUtils.isEmpty(stsTokenIdentifier.getTempAccessKeyId())) {
      throw new SecretManager.InvalidToken("Invalid STS token - tempAccessKeyId is null/empty");
    }
    if (stsTokenIdentifier.getExpiry() == null) {
      throw new SecretManager.InvalidToken("Invalid STS token - expiry is null");
    }
    if (StringUtils.isEmpty(stsTokenIdentifier.getRoleArn())) {
      throw new SecretManager.InvalidToken("Invalid STS token - roleArn is null/empty");
    }
    if (StringUtils.isEmpty(stsTokenIdentifier.getOriginalAccessKeyId())) {
      throw new SecretManager.InvalidToken("Invalid STS token - originalAccessKeyId is null/empty");
    }
    if (StringUtils.isEmpty(stsTokenIdentifier.getSecretAccessKey())) {
      throw new SecretManager.InvalidToken("Invalid STS token - secretAccessKey is null/empty");
    }
    if (stsTokenIdentifier.getCreationTime() == null) {
      throw new SecretManager.InvalidToken("Invalid STS token - creationTime is null");
    }
  }

  /**
   * Ensures STS-related {@link S3Authentication} fields are structurally consistent on the Ratis
   * apply path. Cryptographic validation (signature, expiry, secret key lookup) runs on the leader
   * RPC path (e.g. {@code S3SecurityUtil.validateS3Credential}).  This method performs no crypto and does
   * not contact {@link SecretKeyClient}, keeping the apply thread deterministic and lightweight.
   *
   * @param request OM request possibly containing S3 authentication
   * @throws OMException if resolved fields and session token presence are inconsistent
   */
  public static void ensureResolvedStsFieldsInvariants(OMRequest request) throws OMException {
    if (!request.hasS3Authentication()) {
      return;
    }

    final S3Authentication s3Auth = request.getS3Authentication();
    final boolean hasSessionToken = s3Auth.hasSessionToken() && !s3Auth.getSessionToken().isEmpty();

    if (!hasSessionToken) {
      // If sessionToken is missing/empty, resolved fields must be empty.
      if (hasAnyResolvedStsField(s3Auth)) {
        throw new OMException("Resolved STS fields must be empty when sessionToken is not present", INVALID_TOKEN);
      }
      return;
    }

    if (!hasAllResolvedStsFields(s3Auth)) {
      throw new OMException("Resolved STS fields must be present when sessionToken is present", INVALID_TOKEN);
    }

    if (hasPartialResolvedStsAssumedRoleFields(s3Auth)) {
      throw new OMException(
          "Resolved STS assumed-role fields must both be present or both be absent when sessionToken is present",
          INVALID_TOKEN);
    }
  }

  private static boolean hasAnyResolvedStsField(S3Authentication s3Auth) {
    return s3Auth.hasResolvedStsSessionPolicy() || s3Auth.hasResolvedStsRoleArn() ||
        s3Auth.hasResolvedStsOriginalAccessKeyId() || s3Auth.hasResolvedStsTempAccessKeyId() ||
        s3Auth.hasResolvedStsSecretKeyId() || s3Auth.hasResolvedStsAssumedRoleId() ||
        s3Auth.hasResolvedStsAssumedRoleUserArn();
  }

  private static boolean hasAllResolvedStsFields(S3Authentication s3Auth) {
    // Assumed-role id/ARN are optional here: they are used for GetCallerIdentity, not apply-path ACL,
    // and may be absent in Ratis log entries committed by a pre-upgrade OM leader during rolling upgrade.
    return s3Auth.hasResolvedStsSessionPolicy() && s3Auth.hasResolvedStsRoleArn() &&
        s3Auth.hasResolvedStsOriginalAccessKeyId() && s3Auth.hasResolvedStsTempAccessKeyId() &&
        s3Auth.hasResolvedStsSecretKeyId();
  }

  private static boolean hasPartialResolvedStsAssumedRoleFields(S3Authentication s3Auth) {
    return s3Auth.hasResolvedStsAssumedRoleId() != s3Auth.hasResolvedStsAssumedRoleUserArn();
  }

  /**
   * Copies the STS state that {@link S3SecurityUtil#validateS3Credential} put in the {@link OzoneManager}
   * thread local onto {@code s3Auth}, or clears the resolved fields when the request has no session token.
   *
   * <p>The resolved fields are the only STS state visible to the Ratis apply thread, because the thread locals
   * belong to the RPC handler thread and do not follow the request onto a Ratis thread. Resolving here, where
   * the token has just been verified, is what lets the apply thread rebuild the request context without
   * repeating any crypto.</p>
   *
   * <p>Returns {@code s3Auth} itself when there is nothing to resolve and nothing to clear.</p>
   *
   * @throws OMException if a session token is present but the token identifier is not, which means the
   *         request reached this point without passing STS validation
   */
  public static S3Authentication resolveS3Authentication(S3Authentication s3Auth, OzoneManager ozoneManager)
      throws OMException {
    final boolean hasSessionToken = s3Auth.hasSessionToken() && !s3Auth.getSessionToken().isEmpty();
    final STSTokenIdentifier stsTokenIdentifier = OzoneManager.getStsTokenIdentifier();

    // This should not happen, so explicitly throw an error.  An existing sessionToken
    // implies prior STS validation must have populated the ThreadLocal.
    if (ozoneManager.isSecurityEnabled() && hasSessionToken && stsTokenIdentifier == null) {
      throw new OMException(
          "S3Authentication has session token but no STS token identifier in OzoneManager ThreadLocal",
          INVALID_REQUEST);
    }

    if (!hasSessionToken || stsTokenIdentifier == null) {
      return hasAnyResolvedStsField(s3Auth) ? clearResolvedStsFields(s3Auth) : s3Auth;
    }

    final UUID secretKeyId = stsTokenIdentifier.getSecretKeyId();
    return s3Auth.toBuilder()
        .setResolvedStsSessionPolicy(StringUtils.defaultString(stsTokenIdentifier.getSessionPolicy()))
        .setResolvedStsRoleArn(StringUtils.defaultString(stsTokenIdentifier.getRoleArn()))
        .setResolvedStsOriginalAccessKeyId(StringUtils.defaultString(stsTokenIdentifier.getOriginalAccessKeyId()))
        .setResolvedStsTempAccessKeyId(StringUtils.defaultString(stsTokenIdentifier.getTempAccessKeyId()))
        .setResolvedStsSecretKeyId(secretKeyId != null ? secretKeyId.toString() : "")
        .setResolvedStsAssumedRoleId(StringUtils.defaultString(stsTokenIdentifier.getAssumedRoleId()))
        .setResolvedStsAssumedRoleUserArn(StringUtils.defaultString(stsTokenIdentifier.getAssumedRoleUserArn()))
        .build();
  }

  private static S3Authentication clearResolvedStsFields(S3Authentication s3Auth) {
    return s3Auth.toBuilder()
        .clearResolvedStsSessionPolicy()
        .clearResolvedStsRoleArn()
        .clearResolvedStsOriginalAccessKeyId()
        .clearResolvedStsTempAccessKeyId()
        .clearResolvedStsSecretKeyId()
        .clearResolvedStsAssumedRoleId()
        .clearResolvedStsAssumedRoleUserArn()
        .build();
  }

  /**
   * Rebuilds the {@link STSTokenIdentifier} for a request from the resolved fields written by
   * {@link #resolveS3Authentication(S3Authentication, OzoneManager)}, or returns null when the request has
   * no session token.
   *
   * <p>Performs no crypto and does not contact {@link SecretKeyClient}: the token was already verified on
   * the leader RPC path, so only the resolved values are needed for authorization on the apply thread.
   * {@code creationTime} and {@code expiry} are set to {@link Instant#MAX} so that a revocation or expiry
   * check against this identifier is deterministic and never treats it as issued before a stored cutoff.</p>
   *
   * <p>Call {@link #ensureResolvedStsFieldsInvariants(OMRequest)} first: it guarantees the five core resolved
   * fields (session policy, role ARN, original/temp access key IDs, secret key ID) are present. Assumed-role
   * id/ARN may be absent on legacy Ratis log entries; when present, both must be set.</p>
   */
  public static STSTokenIdentifier rehydrateStsTokenIdentifier(S3Authentication s3Auth) {
    if (!s3Auth.hasSessionToken() || s3Auth.getSessionToken().isEmpty()) {
      return null;
    }

    return new STSTokenIdentifier(STSTokenIdentifier.Params.newBuilder()
        .setTempAccessKeyId(s3Auth.getResolvedStsTempAccessKeyId())
        .setOriginalAccessKeyId(s3Auth.getResolvedStsOriginalAccessKeyId())
        .setRoleArn(s3Auth.getResolvedStsRoleArn())
        .setCreationTime(Instant.MAX)
        .setExpiry(Instant.MAX)
        .setSecretAccessKey(null) // no secretAccessKey needed
        .setSessionPolicy(s3Auth.getResolvedStsSessionPolicy())
        .setAssumedRoleId(s3Auth.getResolvedStsAssumedRoleId())
        .setAssumedRoleUserArn(s3Auth.getResolvedStsAssumedRoleUserArn())
        .setManagedSecretKey(null) // no ManagedSecretKey needed
        .build());
  }
}

