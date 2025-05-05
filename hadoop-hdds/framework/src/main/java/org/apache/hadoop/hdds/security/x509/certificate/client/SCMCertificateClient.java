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

package org.apache.hadoop.hdds.security.x509.certificate.client;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType.SCM;
import static org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateApprover.ApprovalType.KERBEROS_TRUSTED;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_ROOT_CA_COMPONENT_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_ROOT_CA_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_SUB_CA_PREFIX;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.cert.CertPath;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CAType;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.security.x509.certificate.authority.DefaultCAServer;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.DefaultCAProfile;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.PKIProfile;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private String scmId;
  private String cId;
  private String scmHostname;
  private ExecutorService executorService;
  private boolean isPrimarySCM = false;
  private Consumer<String> saveCertIdCallback;

  @SuppressWarnings("parameternumber")
  public SCMCertificateClient(SecurityConfig securityConfig,
      SCMSecurityProtocolClientSideTranslatorPB scmClient,
      String scmId, String clusterId, String scmCertId, String hostname,
      boolean isPrimarySCM, Consumer<String> saveCertId) {
    super(securityConfig, scmClient, LOG, scmCertId, COMPONENT_NAME,
        HddsUtils.threadNamePrefix(scmId), saveCertId, null);
    this.scmId = scmId;
    this.cId = clusterId;
    this.scmHostname = hostname;
    this.isPrimarySCM = isPrimarySCM;
    this.saveCertIdCallback = saveCertId;
  }

  public SCMCertificateClient(SecurityConfig securityConfig,
      SCMSecurityProtocolClientSideTranslatorPB scmClient,
      String scmId, String clusterId, String scmCertId, String hostname) {
    this(securityConfig, scmClient, scmId, clusterId, scmCertId, hostname,
        COMPONENT_NAME);
  }

  private SCMCertificateClient(SecurityConfig securityConfig,
      SCMSecurityProtocolClientSideTranslatorPB scmClient,
      String scmId, String clusterId, String scmCertId, String hostname,
      String component) {
    super(securityConfig, scmClient, LOG, scmCertId, component,
        HddsUtils.threadNamePrefix(scmId), null, null);
    this.scmId = scmId;
    this.cId = clusterId;
    this.scmHostname = hostname;
  }

  public SCMCertificateClient(
      SecurityConfig securityConfig,
      SCMSecurityProtocolClientSideTranslatorPB scmClient,
      String certSerialId) {
    this(securityConfig, scmClient, null, null, certSerialId, null,
        COMPONENT_NAME);
  }

  public SCMCertificateClient(
      SecurityConfig securityConfig,
      SCMSecurityProtocolClientSideTranslatorPB scmClient,
      String certSerialId,
      String scmId,
      String component) {
    this(securityConfig, scmClient, scmId, null, certSerialId, null,
        component);
  }

  /**
   * Returns a CSR builder that can be used to creates a Certificate signing
   * request.
   *
   * @return CertificateSignRequest.Builder
   */
  @Override
  public CertificateSignRequest.Builder configureCSRBuilder()
      throws SCMSecurityException {
    String subject = SCM_SUB_CA_PREFIX + scmHostname;

    LOG.info("Creating csr for SCM->hostName:{},scmId:{},clusterId:{}," +
        "subject:{}", scmHostname, scmId, cId, subject);

    return super.configureCSRBuilder()
        .setSubject(subject)
        .setScmID(scmId)
        .setClusterID(cId)
        // Set CA to true, as this will be used to sign certs for OM/DN.
        .setCA(true)
        .setKey(new KeyPair(getPublicKey(), getPrivateKey()));
  }

  @Override
  protected boolean shouldStartCertificateRenewerService() {
    return false;
  }

  @Override
  public Logger getLogger() {
    return LOG;
  }

  @Override
  protected SCMGetCertResponseProto sign(CertificateSignRequest request) {
    throw new UnsupportedOperationException("Invalid call to SCMCertificateClient#sign(CertificateSignRequest. " +
        "SCM certificate client uses a different mechanism to sign the SCMs' certificate.");
  }

  @Override
  public String signAndStoreCertificate(CertificateSignRequest csr, Path certPath, boolean renew) {
    try {
      HddsProtos.ScmNodeDetailsProto scmNodeDetailsProto =
          HddsProtos.ScmNodeDetailsProto.newBuilder()
              .setClusterId(cId)
              .setHostName(scmHostname)
              .setScmNodeId(scmId).build();

      // Get SCM sub CA cert.
      SCMGetCertResponseProto response =
          getScmSecureClient().getSCMCertChain(scmNodeDetailsProto, csr.toEncodedFormat(), true);

      CertificateCodec certCodec = new CertificateCodec(
          getSecurityConfig(), certPath);
      String pemEncodedCert = response.getX509Certificate();

      // Store SCM sub CA and root CA certificate.
      if (response.hasX509CACertificate()) {
        String pemEncodedRootCert = response.getX509CACertificate();
        storeCertificate(pemEncodedRootCert,
            CAType.SUBORDINATE, certCodec, false, !renew);
        storeCertificate(pemEncodedCert, CAType.NONE, certCodec,
            false, !renew);
        //note: this does exactly the same as store certificate
        certCodec.writeCertificate(certCodec.getLocation().toAbsolutePath(),
            getSecurityConfig().getCertificateFileName(), pemEncodedCert);

        X509Certificate certificate =
            CertificateCodec.getX509Certificate(pemEncodedCert);
        // return new scm cert serial ID.
        return certificate.getSerialNumber().toString();
      } else {
        throw new RuntimeException("Unable to retrieve SCM certificate chain");
      }
    } catch (Throwable e) {
      LOG.error("Error while fetching/storing SCM signed certificate.", e);
      throw new RuntimeException(e);
    }
  }

  public void refreshCACertificates() throws IOException {
    if (executorService == null) {
      executorService = Executors.newSingleThreadExecutor(
          new ThreadFactoryBuilder()
              .setNameFormat(threadNamePrefix() + getComponentName()
                  + "-refreshCACertificates")
              .setDaemon(true).build());
    }
    executorService.execute(new RefreshCACertificates(getScmSecureClient()));
  }

  /**
   * Task to refresh root CA certificates for SCM.
   */
  public class RefreshCACertificates implements Runnable {
    private final SCMSecurityProtocolClientSideTranslatorPB scmSecureClient;

    public RefreshCACertificates(
        SCMSecurityProtocolClientSideTranslatorPB client) {
      this.scmSecureClient = client;
    }

    @Override
    public void run() {
      try {
        // In case root CA certificate is rotated during this SCM is offline
        // period, fetch the new root CA list from leader SCM and refresh ratis
        // server's tlsConfig.
        List<String> rootCAPems = scmSecureClient.getAllRootCaCertificates();

        // SCM certificate client currently sets root CA as CA cert
        Set<X509Certificate> certList = getAllRootCaCerts();
        certList = certList.isEmpty() ? getAllCaCerts() : certList;

        List<X509Certificate> rootCAsFromLeaderSCM =
            OzoneSecurityUtil.convertToX509(rootCAPems);
        rootCAsFromLeaderSCM.removeAll(certList);

        if (rootCAsFromLeaderSCM.isEmpty()) {
          LOG.info("CA certificates are not changed.");
          return;
        }

        for (X509Certificate cert : rootCAsFromLeaderSCM) {
          LOG.info("Fetched new root CA certificate {} from leader SCM",
              cert.getSerialNumber().toString());
          storeCertificate(
              CertificateCodec.getPEMEncodedString(cert), CAType.SUBORDINATE);
        }
        String scmCertId = getCertificate().getSerialNumber().toString();
        notifyNotificationReceivers(scmCertId, scmCertId);
      } catch (IOException e) {
        LOG.error("Failed to refresh CA certificates", e);
      }
    }
  }

  @Override
  public synchronized void close() throws IOException {
    super.close();
    if (executorService != null) {
      executorService.shutdownNow();
      executorService = null;
    }
  }

  @Override
  protected void recoverStateIfNeeded(InitResponse state) throws IOException {
    LOG.info("Init response: {}", state);
    switch (state) {
    case SUCCESS:
      LOG.info("Initialization successful.");
      break;
    case GETCERT:
      if (!isPrimarySCM) {
        getRootCASignedSCMCert();
      } else {
        getPrimarySCMSelfSignedCert();
      }
      LOG.info("Successfully stored SCM signed certificate.");
      break;
    case FAILURE:
    default:
      LOG.error("SCM security initialization failed. Init response: {}",
          state);
      throw new RuntimeException("SCM security initialization failed.");
    }
  }

  /**
   * For bootstrapped SCM get sub-ca signed certificate and root CA
   * certificate using scm security client and store it using certificate
   * client.
   */
  private void getRootCASignedSCMCert() {
    try {
      // Generate CSR.
      CertificateSignRequest csr = configureCSRBuilder().build();
      HddsProtos.ScmNodeDetailsProto scmNodeDetailsProto =
          HddsProtos.ScmNodeDetailsProto.newBuilder()
              .setClusterId(cId)
              .setHostName(scmHostname)
              .setScmNodeId(scmId).build();

      // Get SCM sub CA cert.
      SCMGetCertResponseProto response = getScmSecureClient().
          getSCMCertChain(scmNodeDetailsProto, csr.toEncodedFormat(), false);
      String pemEncodedCert = response.getX509Certificate();

      // Store SCM sub CA and root CA certificate.
      if (response.hasX509CACertificate()) {
        String pemEncodedRootCert = response.getX509CACertificate();
        storeCertificate(pemEncodedRootCert, CAType.SUBORDINATE);
        storeCertificate(pemEncodedCert, CAType.NONE);
        //note: this does exactly the same as store certificate
        persistSubCACertificate(pemEncodedCert);

        X509Certificate certificate =
            CertificateCodec.getX509Certificate(pemEncodedCert);
        // Persist scm cert serial ID.
        saveCertIdCallback.accept(certificate.getSerialNumber().toString());
      } else {
        throw new RuntimeException("Unable to retrieve SCM certificate chain");
      }
    } catch (IOException | java.security.cert.CertificateException e) {
      LOG.error("Error while fetching/storing SCM signed certificate.", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * For primary SCM get sub-ca signed certificate and root CA certificate by
   * root CA certificate server and store it using certificate client.
   */
  private void getPrimarySCMSelfSignedCert() {
    try {
      CertificateServer rootCAServer = initializeRootCertificateServer(
          getSecurityConfig(), null, BigInteger.ONE,
          new DefaultCAProfile(), SCM_ROOT_CA_COMPONENT_NAME);
      CertPath rootCACertificatePath = rootCAServer.getCaCertPath();
      String pemEncodedRootCert =
          CertificateCodec.getPEMEncodedString(rootCACertificatePath);

      CertificateSignRequest csr = configureCSRBuilder().build();
      String subCaSerialId = BigInteger.ONE.add(BigInteger.ONE).toString();
      //TODO: do not use generateCSR() here once the server side change is also done.
      CertPath scmSubCACertPath =
          rootCAServer.requestCertificate(csr.generateCSR(), KERBEROS_TRUSTED, SCM, subCaSerialId).get();
      String pemEncodedCert = CertificateCodec.getPEMEncodedString(scmSubCACertPath);

      storeCertificate(pemEncodedRootCert, CAType.SUBORDINATE);
      storeCertificate(pemEncodedCert, CAType.NONE);
      //note: this does exactly the same as store certificate
      persistSubCACertificate(pemEncodedCert);
      X509Certificate cert = (X509Certificate) scmSubCACertPath.getCertificates().get(0);

      // Persist scm cert serial ID.
      saveCertIdCallback.accept(cert.getSerialNumber().toString());
    } catch (InterruptedException | ExecutionException | IOException | java.security.cert.CertificateException e) {
      LOG.error("Error while fetching/storing SCM signed certificate.", e);
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /**
   * This function creates/initializes a certificate server as needed.
   * This function is idempotent, so calling this again and again after the
   * server is initialized is not a problem.
   *
   * @param config
   * @param scmCertStore
   * @param pkiProfile
   * @param component
   */
  public CertificateServer initializeRootCertificateServer(
      SecurityConfig config, CertificateStore scmCertStore,
      BigInteger rootCertId, PKIProfile pkiProfile, String component)
      throws IOException {
    String subject = String.format(SCM_ROOT_CA_PREFIX, rootCertId) +
        InetAddress.getLocalHost().getHostName();

    DefaultCAServer rootCAServer = new DefaultCAServer(subject,
        cId, scmId, scmCertStore, rootCertId, pkiProfile,
        component);

    rootCAServer.init(config, CAType.ROOT);
    return rootCAServer;
  }

  /**
   * Persists the sub SCM signed certificate to the location which can be
   * read by sub CA Certificate server.
   *
   * @param certificateHolder
   * @throws IOException
   */
  private void persistSubCACertificate(
      String certificateHolder) throws IOException {
    CertificateCodec certCodec =
        new CertificateCodec(getSecurityConfig(), getComponentName());

    certCodec.writeCertificate(certCodec.getLocation().toAbsolutePath(),
        getSecurityConfig().getCertificateFileName(), certificateHolder);
  }
}
