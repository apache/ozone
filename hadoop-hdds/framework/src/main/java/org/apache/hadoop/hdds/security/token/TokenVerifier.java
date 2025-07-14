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

package org.apache.hadoop.hdds.security.token;

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProtoOrBuilder;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyVerifierClient;
import org.apache.hadoop.security.token.Token;

/**
 * Ozone GRPC token header verifier.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface TokenVerifier {

  /**
   * Verify if {@code token} is valid to allow execution of {@code cmd} for
   * {@code user}.
   *
   * @param token the token to verify
   * @param cmd container command
   * @throws SCMSecurityException if token verification fails.
   */
  void verify(Token<?> token,
      ContainerCommandRequestProtoOrBuilder cmd)
      throws SCMSecurityException;

  /** Same as {@link #verify}, but with encoded token. */
  default void verify(ContainerCommandRequestProtoOrBuilder cmd,
      String encodedToken) throws SCMSecurityException {

    if (Strings.isNullOrEmpty(encodedToken)) {
      throw new BlockTokenException("Failed to find any token (empty or " +
          "null.)");
    }

    final Token<?> token = new Token<>();
    try {
      token.decodeFromUrlString(encodedToken);
    } catch (IOException ex) {
      throw new BlockTokenException("Failed to decode token : " + encodedToken);
    }

    verify(token, cmd);
  }

  /** Create appropriate token verifier based on the configuration. */
  static TokenVerifier create(SecurityConfig conf,
      SecretKeyVerifierClient secretKeyClient) throws IOException {
    if (!conf.isBlockTokenEnabled() && !conf.isContainerTokenEnabled()) {
      return new NoopTokenVerifier();
    }

    List<TokenVerifier> list = new LinkedList<>();
    list.add(new BlockTokenVerifier(conf, secretKeyClient));
    list.add(new ContainerTokenVerifier(conf, secretKeyClient));
    return new CompositeTokenVerifier(list);
  }
}
