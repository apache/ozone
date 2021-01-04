/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.security.token;

import com.google.common.base.Strings;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.GetBlock;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.GetSmallFile;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.PutBlock;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.PutSmallFile;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.ReadChunk;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.WriteChunk;


/**
 * Verify token and return a UGI with token if authenticated.
 */
public class BlockTokenVerifier implements TokenVerifier {

  private final CertificateClient caClient;
  private final SecurityConfig conf;
  private static boolean testStub = false;
  private final static Logger LOGGER =
      LoggerFactory.getLogger(BlockTokenVerifier.class);

  public BlockTokenVerifier(SecurityConfig conf, CertificateClient caClient) {
    this.conf = conf;
    this.caClient = caClient;
  }

  private boolean isExpired(long expiryDate) {
    return Time.now() > expiryDate;
  }

  @Override
  public void verify(String user, String tokenStr,
      ContainerProtos.Type cmd, String id) throws SCMSecurityException {
    if (!conf.isBlockTokenEnabled() || !HddsUtils.requireBlockToken(cmd)) {
      return;
    }

    // TODO: add audit logs.
    if (Strings.isNullOrEmpty(tokenStr)) {
      throw new BlockTokenException("Fail to find any token (empty or " +
          "null.)");
    }

    final Token<OzoneBlockTokenIdentifier> token = new Token();
    OzoneBlockTokenIdentifier tokenId = new OzoneBlockTokenIdentifier();
    try {
      token.decodeFromUrlString(tokenStr);
      ByteArrayInputStream buf = new ByteArrayInputStream(
          token.getIdentifier());
      DataInputStream in = new DataInputStream(buf);
      tokenId.readFields(in);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Verifying token:{} for user:{} ", tokenId, user);
      }
    } catch (IOException ex) {
      throw new BlockTokenException("Failed to decode token : " + tokenStr);
    }

    if (caClient == null) {
      throw new SCMSecurityException("Certificate client not available " +
          "to validate token");
    }

    UserGroupInformation tokenUser = tokenId.getUser();
    X509Certificate signerCert;
    signerCert = caClient.getCertificate(tokenId.getOmCertSerialId());

    if (signerCert == null) {
      throw new BlockTokenException("Can't find signer certificate " +
          "(OmCertSerialId: " + tokenId.getOmCertSerialId() +
          ") of the block token for user: " + tokenUser);
    }

    try {
      signerCert.checkValidity();
    } catch (CertificateExpiredException exExp) {
      throw new BlockTokenException("Block token can't be verified due to " +
          "expired certificate " + tokenId.getOmCertSerialId());
    } catch (CertificateNotYetValidException exNyv) {
      throw new BlockTokenException("Block token can't be verified due to " +
          "not yet valid certificate " + tokenId.getOmCertSerialId());
    }

    boolean validToken = caClient.verifySignature(tokenId.getBytes(),
        token.getPassword(), signerCert);
    if (!validToken) {
      throw new BlockTokenException("Invalid block token for user: " +
          tokenId.getUser());
    }
    // check expiration
    if (isExpired(tokenId.getExpiryDate())) {
      throw new BlockTokenException("Expired block token for user: " +
          tokenUser);
    }

    // Token block id mismatch
    if (!tokenId.getBlockId().equals(id)) {
      throw new BlockTokenException("Block id mismatch. Token for block ID: " +
          tokenId.getBlockId() + " can't be used to access block: " + id +
          " by user: " + tokenUser);
    }

    if (cmd == ReadChunk || cmd == GetBlock || cmd == GetSmallFile) {
      if (!tokenId.getAccessModes().contains(
          HddsProtos.BlockTokenSecretProto.AccessModeProto.READ)) {
        throw new BlockTokenException("Block token with " + id
            + " doesn't have READ permission");
      }
    } else if (cmd == WriteChunk || cmd == PutBlock || cmd == PutSmallFile) {
      if (!tokenId.getAccessModes().contains(
          HddsProtos.BlockTokenSecretProto.AccessModeProto.WRITE)) {
        throw new BlockTokenException("Block token with " + id
            + " doesn't have WRITE permission");
      }
    } else {
      throw new BlockTokenException("Block token does not support " + cmd);
    }
  }

  public static boolean isTestStub() {
    return testStub;
  }

  // For testing purpose only.
  public static void setTestStub(boolean isTestStub) {
    BlockTokenVerifier.testStub = isTestStub;
  }
}
