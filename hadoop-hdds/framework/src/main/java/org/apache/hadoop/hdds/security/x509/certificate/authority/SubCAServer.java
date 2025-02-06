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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.PKIProfile;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertPath;
import java.security.cert.X509Certificate;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType.SCM;
import static org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateApprover.ApprovalType.KERBEROS_TRUSTED;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_SUB_CA_PREFIX;


/**
 * CertificateServer that uses subordinate certificates signed by the RootCAServer.
 */
public class SubCAServer extends DefaultCAServer {
  public static final Logger LOG =
      LoggerFactory.getLogger(SubCAServer.class);
  private RootCAServer rootCAServer;
  public static final String COMPONENT_NAME =
      Paths.get(OzoneConsts.SCM_CA_CERT_STORAGE_DIR,
          OzoneConsts.SCM_SUB_CA_PATH).toString();
  private SCMSecurityProtocolClientSideTranslatorPB clientSideTranslatorPB;
  private String hostName;

  @SuppressWarnings("parameternumber")
  public SubCAServer(String subject, String clusterID, String scmID, CertificateStore certificateStore,
      PKIProfile pkiProfile, Consumer<String> certIdCallBack, String hostName) {
    super(subject, clusterID, scmID, certificateStore, pkiProfile, COMPONENT_NAME, certIdCallBack);
    this.hostName = hostName;
  }

  @SuppressWarnings("parameternumber")
  public SubCAServer(String subject, String clusterID, String scmID, CertificateStore certificateStore,
      PKIProfile pkiProfile, Consumer<String> certIdCallBack, String hostName,
      SCMSecurityProtocolClientSideTranslatorPB translatorPB) {
    super(subject, clusterID, scmID, certificateStore, pkiProfile, COMPONENT_NAME, certIdCallBack);
    this.hostName = hostName;
    this.clientSideTranslatorPB = translatorPB;
  }

  @SuppressWarnings("parameternumber")
  public SubCAServer(String subject, String clusterID, String scmID, CertificateStore certificateStore,
      PKIProfile pkiProfile, Consumer<String> certIdCallBack, String hostName, RootCAServer rootCAServer) {
    super(subject, clusterID, scmID, certificateStore, pkiProfile, COMPONENT_NAME, certIdCallBack);
    this.hostName = hostName;
    this.rootCAServer = rootCAServer;
  }

  @Override
  void initKeysAndCa() {
    try {
      KeyPair keyPair = generateKeys(getSecurityConfig());
      if (clientSideTranslatorPB != null) {
        getRootCASignedSCMCert(keyPair, clientSideTranslatorPB);
        return;
      }
      if (rootCAServer != null) {
        getPrimarySCMSelfSignedCert(keyPair);
        return;
      }
      throw new IllegalStateException("Trying to initialize SubCAServer without creating a root CA first.");
    } catch (NoSuchProviderException | NoSuchAlgorithmException | IOException e) {
      LOG.error("Unable to initialize CertificateServer.", e);
    }
  }

  /**
   * For bootstrapped SCM get sub-ca signed certificate and root CA
   * certificate using scm security client and store it using certificate
   * client.
   */
  private void getRootCASignedSCMCert(KeyPair keyPair, SCMSecurityProtocolClientSideTranslatorPB scmClient) {
    try {
      // Generate CSR.
      CertificateSignRequest csr = configureCSRBuilder(keyPair).build();
      HddsProtos.ScmNodeDetailsProto scmNodeDetailsProto =
          HddsProtos.ScmNodeDetailsProto.newBuilder()
              .setClusterId(getClusterID())
              .setHostName(hostName)
              .setScmNodeId(getScmID()).build();

      // Get SCM sub CA cert.
      SCMSecurityProtocolProtos.SCMGetCertResponseProto response = scmClient.
          getSCMCertChain(scmNodeDetailsProto, csr.toEncodedFormat(), false);
      String pemEncodedCert = response.getX509Certificate();
      if (!response.hasX509CACertificate()) {
        throw new RuntimeException("Unable to retrieve SCM certificate chain");
      }
      // Store SCM sub CA and root CA certificate.
      String pemEncodedRootCert = response.getX509CACertificate();
      storeCertificate(pemEncodedRootCert, CAType.SUBORDINATE);
      storeCertificate(pemEncodedCert, CAType.NONE);
      persistSubCACertificate(pemEncodedCert);

      X509Certificate certificate =
          CertificateCodec.getX509Certificate(pemEncodedCert);
      // Persist scm cert serial ID.
      getSaveCertId().accept(certificate.getSerialNumber().toString());
    } catch (IOException | java.security.cert.CertificateException e) {
      LOG.error("Error while fetching/storing SCM signed certificate.", e);
      throw new RuntimeException(e);
    }
  }

  private void persistSubCACertificate(
      String certificateHolder) throws IOException {
    CertificateCodec certCodec =
        new CertificateCodec(getSecurityConfig(), getComponentName());
    certCodec.writeCertificate(certCodec.getLocation().toAbsolutePath(),
        getSecurityConfig().getCertificateFileName(), certificateHolder);
  }

  private void getPrimarySCMSelfSignedCert(KeyPair keyPair) {
    try {
      CertPath rootCACertificatePath = rootCAServer.getCaCertPath();
      String pemEncodedRootCert =
          CertificateCodec.getPEMEncodedString(rootCACertificatePath);

      CertificateSignRequest csr = configureCSRBuilder(keyPair).build();
      String subCaSerialId = BigInteger.ONE.add(BigInteger.ONE).toString();
      CertPath scmSubCACertPath =
          rootCAServer.requestCertificate(csr.toEncodedFormat(), KERBEROS_TRUSTED, SCM, subCaSerialId).get();
      String pemEncodedCert = CertificateCodec.getPEMEncodedString(scmSubCACertPath);

      storeCertificate(pemEncodedRootCert, CAType.SUBORDINATE);
      storeCertificate(pemEncodedCert, CAType.NONE);
      //note: this does exactly the same as store certificate
      persistSubCACertificate(pemEncodedCert);
      X509Certificate cert = (X509Certificate) scmSubCACertPath.getCertificates().get(0);

      // Persist scm cert serial ID.
      getSaveCertId().accept(cert.getSerialNumber().toString());
    } catch (InterruptedException | ExecutionException | IOException | java.security.cert.CertificateException e) {
      LOG.error("Error while fetching/storing SCM signed certificate.", e);
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns a CSR builder that can be used to creates a Certificate signing
   * request.
   *
   * @return CertificateSignRequest.Builder
   */
  public CertificateSignRequest.Builder configureCSRBuilder(KeyPair keyPair)
      throws SCMSecurityException {
    String subject = SCM_SUB_CA_PREFIX + hostName;

    LOG.info("Creating csr for SCM->hostName:{},scmId:{},clusterId:{}," +
        "subject:{}", hostName, getScmID(), getClusterID(), subject);

    return new CertificateSignRequest.Builder()
        .setConfiguration(getSecurityConfig())
        .addInetAddresses()
        .setDigitalEncryption(true)
        .setDigitalSignature(true)
        .setSubject(subject)
        .setScmID(getScmID())
        .setClusterID(getClusterID())
        // Set CA to true, as this will be used to sign certs for OM/DN.
        .setCA(true)
        .setKey(new KeyPair(keyPair.getPublic(), keyPair.getPrivate()));
  }
}
