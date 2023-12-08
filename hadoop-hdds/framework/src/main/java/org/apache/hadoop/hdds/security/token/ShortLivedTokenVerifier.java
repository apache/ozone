/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.security.token;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProtoOrBuilder;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyVerifierClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

/**
 * Verifies short-lived token.
 * @param <T> type of short-lived token
 */
public abstract class
    ShortLivedTokenVerifier<T extends ShortLivedTokenIdentifier>
    implements TokenVerifier {

  private final SecurityConfig conf;
  private final SecretKeyVerifierClient secretKeyClient;

  protected ShortLivedTokenVerifier(SecurityConfig conf,
      SecretKeyVerifierClient secretKeyClient) {
    this.conf = conf;
    this.secretKeyClient = secretKeyClient;
  }

  /** Whether the specific kind of token is required for {@code cmdType}. */
  protected abstract boolean isTokenRequired(ContainerProtos.Type cmdType);

  /** Create new empty token identifier, to be filled from Token. */
  protected abstract T createTokenIdentifier();

  /** Extract info on "service" being accessed by {@code cmd}. */
  protected abstract Object getService(
      ContainerCommandRequestProtoOrBuilder cmd);

  /** Hook for further verification. */
  protected void verify(T tokenId, ContainerCommandRequestProtoOrBuilder cmd)
      throws SCMSecurityException {
    // NOP
  }

  @Override
  public void verify(String user, Token<?> token,
      ContainerCommandRequestProtoOrBuilder cmd) throws SCMSecurityException {

    if (!isTokenRequired(cmd.getCmdType())) {
      return;
    }

    T tokenId = createTokenIdentifier();
    try {
      tokenId.readFields(new DataInputStream(new ByteArrayInputStream(
          token.getIdentifier())));
    } catch (IOException ex) {
      throw new BlockTokenException("Failed to decode token : " + token);
    }

    verifyTokenPassword(tokenId, token.getPassword());

    UserGroupInformation tokenUser = tokenId.getUser();
    // check expiration
    if (tokenId.isExpired(Instant.now())) {
      throw new BlockTokenException("Expired token for user: " + tokenUser);
    }

    // check token service (blockID or containerID)
    String service = String.valueOf(getService(cmd));
    if (!Objects.equals(service, tokenId.getService())) {
      throw new BlockTokenException("ID mismatch. Token for ID: " +
          tokenId.getService() + " can't be used to access: " + service +
          " by user: " + tokenUser);
    }

    verify(tokenId, cmd);
  }

  protected SecurityConfig getConf() {
    return conf;
  }

  private void verifyTokenPassword(
      ShortLivedTokenIdentifier tokenId, byte[] password)
      throws SCMSecurityException {

    ManagedSecretKey secretKey = secretKeyClient.getSecretKey(
        tokenId.getSecretKeyId());
    if (secretKey == null) {
      throw new BlockTokenException("Can't find the signing secret key " +
          tokenId.getSecretKeyId() + " of the token for user: " +
          tokenId.getUser());
    }

    if (secretKey.isExpired()) {
      throw new BlockTokenException("Token can't be verified due to " +
          "expired secret key " + tokenId.getSecretKeyId());
    }

    if (!secretKey.isValidSignature(tokenId, password)) {
      throw new BlockTokenException("Invalid token for user: " +
          tokenId.getUser());
    }
  }
}
