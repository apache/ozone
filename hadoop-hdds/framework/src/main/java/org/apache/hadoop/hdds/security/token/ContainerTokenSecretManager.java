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

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.security.symmetric.SecretKeySignerClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

/**
 * Secret manager for container tokens.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ContainerTokenSecretManager
    extends ShortLivedTokenSecretManager<ContainerTokenIdentifier>
    implements ContainerTokenGenerator {

  public ContainerTokenSecretManager(long tokenLifetime,
                                     SecretKeySignerClient secretKeyClient) {
    super(tokenLifetime, secretKeyClient);
  }

  public ContainerTokenIdentifier createIdentifier(String user,
      ContainerID containerID) {
    return new ContainerTokenIdentifier(user, containerID,
        getTokenExpiryTime());
  }

  @Override
  public String generateEncodedToken(ContainerID containerID) {
    String user;
    try {
      user = UserGroupInformation.getCurrentUser().getUserName();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to get current user", e);
    }

    try {
      return generateToken(user, containerID).encodeToUrlString();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode token", e);
    }
  }

  @Override
  public Token<ContainerTokenIdentifier> generateToken(String user,
      ContainerID containerID) {
    return generateToken(createIdentifier(user, containerID));
  }
}
