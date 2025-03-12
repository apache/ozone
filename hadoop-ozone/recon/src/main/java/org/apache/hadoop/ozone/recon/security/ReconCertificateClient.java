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

package org.apache.hadoop.ozone.recon.security;

import static org.apache.hadoop.hdds.security.x509.exception.CertificateException.ErrorCode.CSR_ERROR;

import java.io.IOException;
import java.net.InetAddress;
import java.security.KeyPair;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.certificate.client.DefaultCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.exception.CertificateException;
import org.apache.hadoop.ozone.recon.scm.ReconStorageConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Certificate client for Recon.
 */
public class ReconCertificateClient  extends DefaultCertificateClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(ReconCertificateClient.class);

  public static final String COMPONENT_NAME = "recon";
  private final String clusterID;
  private final String reconID;

  public ReconCertificateClient(
      SecurityConfig config,
      SCMSecurityProtocolClientSideTranslatorPB scmSecurityClient,
      ReconStorageConfig storage,
      Consumer<String> saveCertIdCallback,
      Runnable shutdownCallback) {
    super(config, scmSecurityClient, LOG, storage.getReconCertSerialId(),
        COMPONENT_NAME, "", saveCertIdCallback, shutdownCallback);
    this.clusterID = storage.getClusterID();
    this.reconID = storage.getReconId();
  }

  @Override
  public CertificateSignRequest.Builder configureCSRBuilder()
      throws SCMSecurityException {
    LOG.info("Creating CSR for Recon.");
    try {
      CertificateSignRequest.Builder builder = super.configureCSRBuilder();
      String hostname = InetAddress.getLocalHost().getCanonicalHostName();
      String subject = UserGroupInformation.getCurrentUser()
          .getShortUserName() + "@" + hostname;

      builder.setCA(false)
          .setKey(new KeyPair(getPublicKey(), getPrivateKey()))
          .setConfiguration(getSecurityConfig())
          .setSubject(subject);

      return builder;
    } catch (Exception e) {
      LOG.error("Failed to get hostname or current user", e);
      throw new CertificateException("Failed to get hostname or current user",
          e, CSR_ERROR);
    }
  }

  @Override
  protected SCMGetCertResponseProto sign(CertificateSignRequest request) throws IOException {
    SCMGetCertResponseProto response;
    HddsProtos.NodeDetailsProto.Builder reconDetailsProtoBuilder =
        HddsProtos.NodeDetailsProto.newBuilder()
            .setHostName(InetAddress.getLocalHost().getHostName())
            .setClusterId(clusterID)
            .setUuid(reconID)
            .setNodeType(HddsProtos.NodeType.RECON);
    // TODO: For SCM CA we should fetch certificate from multiple SCMs.
    response = getScmSecureClient().getCertificateChain(reconDetailsProtoBuilder.build(), request.toEncodedFormat());
    return response;
  }

  @Override
  public Logger getLogger() {
    return LOG;
  }
}
