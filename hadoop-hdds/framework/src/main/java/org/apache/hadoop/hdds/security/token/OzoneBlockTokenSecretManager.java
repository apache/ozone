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
import java.time.Instant;
import java.util.Set;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto;
import org.apache.hadoop.hdds.security.symmetric.SecretKeySignerClient;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SecretManager for Ozone Master block tokens.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class OzoneBlockTokenSecretManager extends
    ShortLivedTokenSecretManager<OzoneBlockTokenIdentifier> {
  private static final Logger LOG = LoggerFactory
      .getLogger(OzoneBlockTokenSecretManager.class);

  public OzoneBlockTokenSecretManager(long tokenLifetime,
                                      SecretKeySignerClient passwordManager) {
    super(tokenLifetime, passwordManager);
  }

  public OzoneBlockTokenIdentifier createIdentifier(String owner,
      BlockID blockID, Set<AccessModeProto> modes, long maxLength) {
    return new OzoneBlockTokenIdentifier(owner, blockID, modes,
        getTokenExpiryTime().toEpochMilli(),
        maxLength);
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
    byte[] password = createPassword(tokenIdentifier);
    return new Token<>(tokenIdentifier.getBytes(),
        password, tokenIdentifier.getKind(),
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

}
