/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.security;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto;
import org.apache.hadoop.hdds.security.token.ShortLivedTokenSecretManager;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Set;

/**
 * SecretManager for Ozone Master block tokens.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class OzoneBlockTokenSecretManager extends
    ShortLivedTokenSecretManager<OzoneBlockTokenIdentifier> {

  private static final Logger LOG = LoggerFactory
      .getLogger(OzoneBlockTokenSecretManager.class);

  public OzoneBlockTokenSecretManager(SecurityConfig conf,
      long tokenLifetime, String omCertSerialId) {
    super(conf, tokenLifetime, omCertSerialId, LOG);
  }

  @Override
  public OzoneBlockTokenIdentifier createIdentifier() {
    throw new SecurityException("Ozone block token can't be created "
        + "without owner and access mode information.");
  }

  public OzoneBlockTokenIdentifier createIdentifier(String owner,
      BlockID blockID, Set<AccessModeProto> modes, long maxLength) {
    return new OzoneBlockTokenIdentifier(owner, blockID, modes,
        getTokenExpiryTime().toEpochMilli(), getCertSerialId(), maxLength);
  }

  /**
   * Generate an block token for specified user, blockId. Service field for
   * token is set to blockId.
   */
  public Token<OzoneBlockTokenIdentifier> generateToken(String user,
      BlockID blockId, Set<AccessModeProto> modes, long maxLength) {
    OzoneBlockTokenIdentifier tokenIdentifier = createIdentifier(user,
        blockId, modes, maxLength);
    if (LOG.isDebugEnabled()) {
      long expiryTime = tokenIdentifier.getExpiryDate();
      LOG.info("Issued delegation token -> expiryTime:{}, tokenId:{}",
          Instant.ofEpochMilli(expiryTime), tokenIdentifier);
    }
    return new Token<>(tokenIdentifier.getBytes(),
        createPassword(tokenIdentifier), tokenIdentifier.getKind(),
        new Text(tokenIdentifier.getService()));
  }

  /**
   * Generate an block token for current user.
   */
  public Token<OzoneBlockTokenIdentifier> generateToken(BlockID blockId,
      Set<AccessModeProto> modes, long maxLength) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String userID = (ugi == null ? null : ugi.getShortUserName());
    return generateToken(userID, blockId, modes, maxLength);
  }

  @Override
  public byte[] retrievePassword(OzoneBlockTokenIdentifier identifier)
      throws InvalidToken {
    validateToken(identifier);
    return createPassword(identifier);
  }

  @Override
  public long renewToken(Token<OzoneBlockTokenIdentifier> token,
      String renewer) {
    throw new UnsupportedOperationException("Renew token operation is not " +
        "supported for ozone block tokens.");
  }

  @Override
  public OzoneBlockTokenIdentifier cancelToken(Token<OzoneBlockTokenIdentifier>
      token, String canceller) {
    throw new UnsupportedOperationException("Cancel token operation is not " +
        "supported for ozone block tokens.");
  }

  /**
   * Find the OzoneBlockTokenInfo for the given token id, and verify that if the
   * token is not expired.
   */
  @Override
  public boolean validateToken(OzoneBlockTokenIdentifier identifier)
      throws InvalidToken {
    long now = Time.now();
    if (identifier.getExpiryDate() < now) {
      throw new InvalidToken("token " + formatTokenId(identifier) + " is " +
          "expired, current time: " + Time.formatTime(now) +
          " expiry time: " + identifier.getExpiryDate());
    }

    // FIXME since verifySignature always throws, don't see how this could work
    if (!verifySignature(identifier, createPassword(identifier))) {
      throw new InvalidToken("Tampered/Invalid token.");
    }
    return true;
  }

  /**
   * Validates if given hash is valid.
   */
  public boolean verifySignature(OzoneBlockTokenIdentifier identifier,
      byte[] password) {
    throw new UnsupportedOperationException("This operation is not " +
        "supported for block tokens.");
  }
}
