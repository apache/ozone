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

package org.apache.hadoop.ozone.container.common.helpers;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.DELETE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.READ;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.WRITE;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.symmetric.SecretKeySignerClient;
import org.apache.hadoop.hdds.security.token.ContainerTokenIdentifier;
import org.apache.hadoop.hdds.security.token.ContainerTokenSecretManager;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenSecretManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

/**
 * Wraps block and container token managers for datanode.
 */
public class TokenHelper {

  private final OzoneBlockTokenSecretManager blockTokenMgr;
  private final ContainerTokenSecretManager containerTokenMgr;
  private final String user;
  private static final Set<AccessModeProto> MODES =
      EnumSet.of(READ, WRITE, DELETE);

  public TokenHelper(SecurityConfig securityConfig,
                     SecretKeySignerClient secretKeyClient) throws IOException {

    boolean blockTokenEnabled = securityConfig.isBlockTokenEnabled();
    boolean containerTokenEnabled = securityConfig.isContainerTokenEnabled();

    // checking certClient != null instead of securityConfig.isSecurityEnabled()
    // to allow integration test without full kerberos etc. setup
    boolean securityEnabled = secretKeyClient != null;

    if (securityEnabled && (blockTokenEnabled || containerTokenEnabled)) {
      user = UserGroupInformation.getCurrentUser().getShortUserName();

      long expiryTime = securityConfig.getBlockTokenExpiryDurationMs();

      if (blockTokenEnabled) {
        blockTokenMgr = new OzoneBlockTokenSecretManager(expiryTime,
            secretKeyClient);
      } else {
        blockTokenMgr = null;
      }

      if (containerTokenEnabled) {
        containerTokenMgr = new ContainerTokenSecretManager(expiryTime,
            secretKeyClient);
      } else {
        containerTokenMgr = null;
      }
    } else {
      user = null;
      blockTokenMgr = null;
      containerTokenMgr = null;
    }
  }

  public Token<OzoneBlockTokenIdentifier> getBlockToken(BlockID blockID, long length) {
    return blockTokenMgr != null
        ? blockTokenMgr.generateToken(user, blockID, MODES, length)
        : null;
  }

  public Token<ContainerTokenIdentifier> getContainerToken(ContainerID containerID) {
    return containerTokenMgr != null
        ? containerTokenMgr.generateToken(user, containerID)
        : null;
  }

  public static String encode(Token<?> token) throws IOException {
    return token != null ? token.encodeToUrlString() : null;
  }

}
