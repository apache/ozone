/**
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

package org.apache.hadoop.hdds.security.x509.certificate.client;

import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.exception.CertificateException;
import org.slf4j.Logger;

import java.util.function.Consumer;

import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.FAILURE;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.GETCERT;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.RECOVER;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.REINIT;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.SUCCESS;

/**
 * Common Certificate client.
 */
public abstract class CommonCertificateClient extends DefaultCertificateClient {

  private final Logger log;

  public CommonCertificateClient(SecurityConfig securityConfig, Logger log,
      String certSerialId, String component,
      Consumer<String> saveCertIdCallback, Runnable shutdownCallback) {
    super(securityConfig, log, certSerialId, component, saveCertIdCallback,
        shutdownCallback);
    this.log = log;
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
        .setDigitalSignature(true);
  }

  @Override
  protected InitResponse handleCase(InitCase init)
      throws CertificateException {
    switch (init) {
    case NONE:
      log.info("Creating keypair for client as keypair and certificate not " +
          "found.");
      bootstrapClientKeys();
      return GETCERT;
    case CERT:
      log.error("Private key not found, while certificate is still present." +
          "Delete keypair and try again.");
      return FAILURE;
    case PUBLIC_KEY:
      log.error("Found public key but private key and certificate missing.");
      return FAILURE;
    case PRIVATE_KEY:
      log.info("Found private key but public key and certificate is missing.");
      // TODO: Recovering public key from private might be possible in some
      //  cases.
      return FAILURE;
    case PUBLICKEY_CERT:
      log.error("Found public key and certificate but private key is " +
          "missing.");
      return FAILURE;
    case PRIVATEKEY_CERT:
      log.info("Found private key and certificate but public key missing.");
      if (recoverPublicKey()) {
        return SUCCESS;
      } else {
        log.error("Public key recovery failed.");
        return FAILURE;
      }
    case PUBLICKEY_PRIVATEKEY:
      log.info("Found private and public key but certificate is missing.");
      if (validateKeyPair(getPublicKey())) {
        return RECOVER;
      } else {
        log.error("Keypair validation failed.");
        return FAILURE;
      }
    case ALL:
      log.info("Found certificate file along with KeyPair.");
      if (validateKeyPairAndCertificate()) {
        return SUCCESS;
      } else {
        return FAILURE;
      }
    case EXPIRED_CERT:
      getLogger().info("Component certificate is about to expire. Initiating" +
          "renewal.");
      removeMaterial();
      return REINIT;
    default:
      log.error("Unexpected case: {} (private/public/cert)",
          Integer.toBinaryString(init.ordinal()));
      return FAILURE;
    }
  }

  @Override
  public Logger getLogger() {
    return log;
  }
}
