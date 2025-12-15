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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_TOKEN;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.time.Clock;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto;
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
      SecretKeyClient secretKeyClient, Clock clock) throws SecretManager.InvalidToken {
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

    final STSTokenIdentifier tokenId = new STSTokenIdentifier();
    final ManagedSecretKey secretKey;
    try {
      secretKey = getValidatedSecretKey(secretKeyId, secretKeyClient);
      tokenId.setEncryptionKey(secretKey.getSecretKey().getEncoded());
      tokenId.readFromByteArray(tokenBytes);
    } catch (IOException e) {
      throw new SecretManager.InvalidToken("Invalid STS token - could not readFromByteArray: " + e.getMessage());
    }

    // Ensure essential fields are present in the token
    ensureEssentialFieldsArePresentInToken(tokenId);

    // Check expiration
    if (tokenId.isExpired(clock.instant())) {
      throw new SecretManager.InvalidToken("Invalid STS token - token expired at " + tokenId.getExpiry());
    }

    // Verify token signature against the original identifier bytes
    if (!secretKey.isValidSignature(tokenBytes, token.getPassword())) {
      throw new SecretManager.InvalidToken("Invalid STS token - signature is not correct for token: " + tokenId);
    }

    return tokenId;
  }

  private static ManagedSecretKey getValidatedSecretKey(UUID secretKeyId, SecretKeyClient secretKeyClient)
      throws SecretManager.InvalidToken {
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
      throw new SecretManager.InvalidToken("Token cannot be verified due to expired secret key " + secretKeyId);
    }

    return secretKey;
  }

  private static Token<STSTokenIdentifier> decodeTokenFromString(String encodedToken)
      throws SecretManager.InvalidToken {
    final Token<STSTokenIdentifier> token = new Token<>();
    try {
      token.decodeFromUrlString(encodedToken);
      return token;
    } catch (IOException e) {
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
  }
}

