/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.security.x509.certificate.client;

import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.exceptions.CertificateException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.FAILURE;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.GETCERT;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.RECOVER;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.SUCCESS;

/**
 * SCM Certificate Client which is used for generating public/private Key pair,
 * generate CSR and finally obtain signed certificate. This Certificate
 * client is used for setting up sub CA by SCM.
 */
public class SCMCertificateClient extends DefaultCertificateClient {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMCertificateClient.class);

  public static final String COMPONENT_NAME =
      Paths.get(OzoneConsts.SCM_CA_CERT_STORAGE_DIR,
          OzoneConsts.SCM_SUB_CA_PATH).toString();

  public SCMCertificateClient(SecurityConfig securityConfig,
      String certSerialId) {
    super(securityConfig, LOG, certSerialId, COMPONENT_NAME);
  }

  public SCMCertificateClient(SecurityConfig securityConfig) {
    super(securityConfig, LOG, null, COMPONENT_NAME);
  }

  @Override
  protected InitResponse handleCase(InitCase init)
      throws CertificateException {
    // This is similar to OM.
    switch (init) {
    case NONE:
      LOG.info("Creating keypair for client as keypair and certificate not " +
          "found.");
      bootstrapClientKeys();
      return GETCERT;
    case CERT:
      LOG.error("Private key not found, while certificate is still present." +
          "Delete keypair and try again.");
      return FAILURE;
    case PUBLIC_KEY:
      LOG.error("Found public key but private key and certificate missing.");
      return FAILURE;
    case PRIVATE_KEY:
      LOG.info("Found private key but public key and certificate is missing.");
      // TODO: Recovering public key from private might be possible in some
      //  cases.
      return FAILURE;
    case PUBLICKEY_CERT:
      LOG.error("Found public key and certificate but private key is " +
          "missing.");
      return FAILURE;
    case PRIVATEKEY_CERT:
      LOG.info("Found private key and certificate but public key missing.");
      if (recoverPublicKey()) {
        return SUCCESS;
      } else {
        LOG.error("Public key recovery failed.");
        return FAILURE;
      }
    case PUBLICKEY_PRIVATEKEY:
      LOG.info("Found private and public key but certificate is missing.");
      if (validateKeyPair(getPublicKey())) {
        return RECOVER;
      } else {
        LOG.error("Keypair validation failed.");
        return FAILURE;
      }
    case ALL:
      LOG.info("Found certificate file along with KeyPair.");
      if (validateKeyPairAndCertificate()) {
        return SUCCESS;
      } else {
        return FAILURE;
      }
    default:
      LOG.error("Unexpected case: {} (private/public/cert)",
          Integer.toBinaryString(init.ordinal()));
      return FAILURE;
    }
  }

  /**
   * Returns a CSR builder that can be used to creates a Certificate signing
   * request.
   *
   * @return CertificateSignRequest.Builder
   */
  @Override
  public CertificateSignRequest.Builder getCSRBuilder()
      throws CertificateException {
    return super.getCSRBuilder()
        .setDigitalEncryption(true)
        .setDigitalSignature(true)
        // Set CA to true, as this will be used to sign certs for OM/DN.
        .setCA(true);
  }


  @Override
  public Logger getLogger() {
    return LOG;
  }

  @Override
  public String getComponentName() {
    return COMPONENT_NAME;
  }
}