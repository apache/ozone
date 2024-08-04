/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.security.x509.certificate.authority;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.PKIProfile;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificate.utils.SelfSignedCertificate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.LocalDateTime;
import java.util.function.Consumer;

import static org.apache.hadoop.ozone.OzoneConsts.SCM_CA_CERT_STORAGE_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_CA_PATH;

/**
 * CertificateServer for creating self-signed certificates and thus the trust anchor for pki infrastructure.
 */
public class RootCAServer extends DefaultCAServer {

  public static final String SCM_ROOT_CA_COMPONENT_NAME = Paths.get(SCM_CA_CERT_STORAGE_DIR, SCM_CA_PATH).toString();
  public static final Logger LOG =
      LoggerFactory.getLogger(RootCAServer.class);
  private final BigInteger rootCertId;

  @SuppressWarnings("parameternumber")
  public RootCAServer(String subject, String clusterID, String scmID, CertificateStore certificateStore,
      PKIProfile pkiProfile, BigInteger rootCertId, Consumer<String> saveCert) {
    super(subject, clusterID, scmID, certificateStore, pkiProfile, SCM_ROOT_CA_COMPONENT_NAME, saveCert);
    this.rootCertId = rootCertId;
  }
  
  @SuppressWarnings("parameternumber")
  public RootCAServer(String subject, String clusterID, String scmID, CertificateStore certificateStore,
      PKIProfile pkiProfile, String componentName, BigInteger rootCertId, Consumer<String> saveCert) {
    super(subject, clusterID, scmID, certificateStore, pkiProfile, componentName, saveCert);
    this.rootCertId = rootCertId;
  }

  @Override
  void initKeysAndCa() {
    if (getSecurityConfig().useExternalCACertificate(getComponentName())) {
      initWithExternalRootCa(getSecurityConfig());
    } else {
      try {
        generateSelfSignedCA(getSecurityConfig());
      } catch (NoSuchProviderException | NoSuchAlgorithmException
               | IOException e) {
        LOG.error("Unable to initialize CertificateServer.", e);
      }
    }
    if (!verifySelfSignedCA()) {
      LOG.error("Unable to initialize CertificateServer, failed in verification.");
    }
  }

  private void initWithExternalRootCa(SecurityConfig conf) {
    Path extCertPath = Paths.get(conf.getExternalRootCaCert());

    try {
      CertificateCodec certificateCodec = new CertificateCodec(conf, getComponentName());
      Path extCertParent = extCertPath.getParent();
      Path extCertName = extCertPath.getFileName();
      if (extCertParent == null || extCertName == null) {
        throw new IOException("External cert path is not correct: " +
            extCertPath);
      }
      X509Certificate certificate = certificateCodec.getTargetCert(extCertParent, extCertName.toString());

      certificateCodec.writeCertificate(certificate);
      getSaveCertId().accept(certificate.getSerialNumber().toString());
    } catch (IOException | CertificateException  e) {
      LOG.error("External root CA certificate initialization failed", e);
    }
  }

  /**
   * Generates a Self Signed CertificateServer. These are the steps in
   * generating a Self-Signed CertificateServer.
   * <p>
   * 1. Generate a Private/Public Key Pair. 2. Persist to a protected location.
   * 3. Generate a SelfSigned Root CertificateServer certificate.
   *
   * @param securityConfig - Config.
   */
  private void generateSelfSignedCA(SecurityConfig securityConfig) throws
      NoSuchAlgorithmException, NoSuchProviderException, IOException {
    KeyPair keyPair = generateKeys(securityConfig);
    generateRootCertificate(securityConfig, keyPair);
  }

  /**
   * Generates a self-signed Root Certificate for CA.
   *
   * @param securityConfig - SecurityConfig
   * @param key            - KeyPair.
   * @throws IOException          - on Error.
   * @throws SCMSecurityException - on Error.
   */
  private void generateRootCertificate(
      SecurityConfig securityConfig, KeyPair key)
      throws IOException, SCMSecurityException {
    Preconditions.checkNotNull(getSecurityConfig());
    LocalDateTime beginDate = LocalDateTime.now();
    LocalDateTime endDate =
        beginDate.plus(securityConfig.getMaxCertificateDuration());
    SelfSignedCertificate.Builder builder = SelfSignedCertificate.newBuilder()
        .setSubject(getSubject())
        .setScmID(getScmID())
        .setClusterID(getClusterID())
        .setBeginDate(beginDate)
        .setEndDate(endDate)
        .makeCA(rootCertId)
        .setConfiguration(securityConfig)
        .setKey(key);

    builder.addInetAddresses();
    X509Certificate selfSignedCertificate = builder.build();

    CertificateCodec certCodec =
        new CertificateCodec(getSecurityConfig(), getComponentName());
    certCodec.writeCertificate(selfSignedCertificate);
  }
}
