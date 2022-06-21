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
package org.apache.hadoop.ozone.container.ec.reconstruction;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.security.token.ContainerTokenSecretManager;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenSecretManager;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.WRITE;

/**
 * Wraps block and container token managers for datanode.
 */
class TokenHelper {

  private final OzoneBlockTokenSecretManager blockTokenMgr;
  private final ContainerTokenSecretManager containerTokenMgr;
  private final String user;
  private static final Set<AccessModeProto> MODES = EnumSet.of(WRITE);

  TokenHelper(ConfigurationSource conf, CertificateClient certClient)
      throws IOException {

    SecurityConfig securityConfig = new SecurityConfig(conf);

    if (securityConfig.isSecurityEnabled()) {
      user = UserGroupInformation.getCurrentUser().getShortUserName();

      long expiryTime = conf.getTimeDuration(
          HddsConfigKeys.HDDS_BLOCK_TOKEN_EXPIRY_TIME,
          HddsConfigKeys.HDDS_BLOCK_TOKEN_EXPIRY_TIME_DEFAULT,
          TimeUnit.MILLISECONDS);
      String certId = certClient.getCertificate().getSerialNumber().toString();

      blockTokenMgr = securityConfig.isBlockTokenEnabled()
          ? new OzoneBlockTokenSecretManager(securityConfig, expiryTime, certId)
          : null;

      containerTokenMgr = securityConfig.isContainerTokenEnabled()
          ? new ContainerTokenSecretManager(securityConfig, expiryTime, certId)
          : null;
    } else {
      user = null;
      blockTokenMgr = null;
      containerTokenMgr = null;
    }
  }

  Token<OzoneBlockTokenIdentifier> getBlockToken(BlockID blockID, long length) {
    return blockTokenMgr != null
        ? blockTokenMgr.generateToken(user, blockID, MODES, length)
        : null;
  }

  String getEncodedContainerToken(ContainerID containerID) throws IOException {
    return containerTokenMgr != null
        ? containerTokenMgr.generateToken(user, containerID).encodeToUrlString()
        : null;
  }

}
