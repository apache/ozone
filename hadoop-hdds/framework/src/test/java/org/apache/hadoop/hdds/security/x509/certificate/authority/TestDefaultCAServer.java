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

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.DefaultCAProfile;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.DefaultProfile;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.SCMCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.certificate.utils.SelfSignedCertificate;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyCodec;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;

import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertPath;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType.OM;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType.SCM;
import static org.apache.hadoop.hdds.security.x509.certificate.authority.DefaultCAServer.VerificationStatus;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_CA_CERT_STORAGE_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_CA_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the Default CA Server.
 */
public class TestDefaultCAServer {
  private OzoneConfiguration conf;
  private SecurityConfig securityConfig;
  private MockCAStore caStore;

  @BeforeEach
  public void init(@TempDir Path tempDir) throws IOException {
    conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS, tempDir.toString());
    securityConfig = new SecurityConfig(conf);
    caStore = new MockCAStore();
  }

  @Test
  public void testInit() throws Exception {
    CertificateServer testCA = new DefaultCAServer("testCA",
        RandomStringUtils.randomAlphabetic(4),
        RandomStringUtils.randomAlphabetic(4), caStore,
        new DefaultProfile(),
        Paths.get(SCM_CA_CERT_STORAGE_DIR, SCM_CA_PATH).toString());
    testCA.init(securityConfig, CAType.ROOT);
    X509Certificate first = testCA.getCACertificate();
    assertNotNull(first);
    //Init is idempotent.
    testCA.init(securityConfig, CAType.ROOT);
    X509Certificate second = testCA.getCACertificate();
    assertEquals(first, second);
  }

  @Test
  public void testMissingCertificate() {
    CertificateServer testCA = new DefaultCAServer("testCA",
        RandomStringUtils.randomAlphabetic(4),
        RandomStringUtils.randomAlphabetic(4), caStore,
        new DefaultProfile(),
        Paths.get(SCM_CA_CERT_STORAGE_DIR, SCM_CA_PATH).toString());
    Consumer<SecurityConfig> caInitializer =
        ((DefaultCAServer) testCA).processVerificationStatus(
            DefaultCAServer.VerificationStatus.MISSING_CERTIFICATE,
            CAType.ROOT);
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> caInitializer.accept(securityConfig));
    // This also is a runtime exception. Hence not caught by junit expected
    // exception.
    assertThat(e.toString()).contains("Missing Root Certs");
  }

  @Test
  public void testMissingKey() {
    DefaultCAServer testCA = new DefaultCAServer("testCA",
        RandomStringUtils.randomAlphabetic(4),
        RandomStringUtils.randomAlphabetic(4), caStore,
        new DefaultProfile(),
        Paths.get(SCM_CA_CERT_STORAGE_DIR, SCM_CA_PATH).toString());
    Consumer<SecurityConfig> caInitializer =
        testCA.processVerificationStatus(VerificationStatus.MISSING_KEYS, CAType.ROOT);
    IllegalStateException e = assertThrows(IllegalStateException.class, () -> caInitializer.accept(securityConfig));

    // This also is a runtime exception. Hence, not caught by junit expected exception.
    assertThat(e.toString()).contains("Missing Keys");
  }

  /**
   * The most important test of this test suite. This tests that we are able
   * to create a Test CA, creates it own self-Signed CA and then issue a
   * certificate based on a CSR.
   * @throws SCMSecurityException - on ERROR.
   * @throws ExecutionException - on ERROR.
   * @throws InterruptedException - on ERROR.
   * @throws NoSuchProviderException - on ERROR.
   * @throws NoSuchAlgorithmException - on ERROR.
   */
  @Test
  public void testRequestCertificate() throws Exception {
    String scmId = RandomStringUtils.randomAlphabetic(4);
    String clusterId = RandomStringUtils.randomAlphabetic(4);
    KeyPair keyPair =
        new HDDSKeyGenerator(securityConfig).generateKey();
    //TODO: generateCSR!
    PKCS10CertificationRequest csr = new CertificateSignRequest.Builder()
        .addDnsName("hadoop.apache.org")
        .addIpAddress("8.8.8.8")
        .addServiceName("OzoneMarketingCluster002")
        .setCA(false)
        .setClusterID(clusterId)
        .setScmID(scmId)
        .setSubject("Ozone Cluster")
        .setConfiguration(securityConfig)
        .setKey(keyPair)
        .build()
        .generateCSR();

    CertificateServer testCA = new DefaultCAServer("testCA",
        clusterId, scmId, caStore,
        new DefaultProfile(),
        Paths.get(SCM_CA_CERT_STORAGE_DIR, SCM_CA_PATH).toString());
    testCA.init(securityConfig, CAType.ROOT);

    Future<CertPath> holder = testCA.requestCertificate(
        csr, CertificateApprover.ApprovalType.TESTING_AUTOMATIC, SCM,
        String.valueOf(System.nanoTime()));
    // Right now our calls are synchronous. Eventually this will have to wait.
    assertTrue(holder.isDone());
    //Test that the cert path returned contains the CA certificate in proper
    // place
    List<? extends Certificate> certBundle = holder.get().getCertificates();
    X509Certificate caInReturnedBundle = (X509Certificate) certBundle.get(1);
    assertEquals(caInReturnedBundle, testCA.getCACertificate());
    X509Certificate signedCert =
        CertificateCodec.firstCertificateFrom(holder.get());
    //Test that the ca has signed of the returned certificate
    assertEquals(caInReturnedBundle.getSubjectX500Principal(),
        signedCert.getIssuerX500Principal());

  }

  /**
   * Tests that we are able
   * to create a Test CA, creates it own self-Signed CA and then issue a
   * certificate based on a CSR when scmId and clusterId are not set in
   * csr subject.
   * @throws SCMSecurityException - on ERROR.
   * @throws ExecutionException - on ERROR.
   * @throws InterruptedException - on ERROR.
   * @throws NoSuchProviderException - on ERROR.
   * @throws NoSuchAlgorithmException - on ERROR.
   */
  @Test
  public void testRequestCertificateWithInvalidSubject() throws Exception {
    KeyPair keyPair =
        new HDDSKeyGenerator(securityConfig).generateKey();
    //TODO: generateCSR!
    PKCS10CertificationRequest csr = new CertificateSignRequest.Builder()
        .addDnsName("hadoop.apache.org")
        .addIpAddress("8.8.8.8")
        .setCA(false)
        .setSubject("Ozone Cluster")
        .setConfiguration(securityConfig)
        .setKey(keyPair)
        .build()
        .generateCSR();

    CertificateServer testCA = new DefaultCAServer("testCA",
        RandomStringUtils.randomAlphabetic(4),
        RandomStringUtils.randomAlphabetic(4), caStore,
        new DefaultProfile(),
        Paths.get(SCM_CA_CERT_STORAGE_DIR, SCM_CA_PATH).toString());
    testCA.init(securityConfig, CAType.ROOT);

    Future<CertPath> holder = testCA.requestCertificate(
        csr, CertificateApprover.ApprovalType.TESTING_AUTOMATIC, OM,
        String.valueOf(System.nanoTime()));
    // Right now our calls are synchronous. Eventually this will have to wait.
    assertTrue(holder.isDone());
    assertNotNull(CertificateCodec.firstCertificateFrom(holder.get()));
  }

  @Test
  public void testRequestCertificateWithInvalidSubjectFailure() throws Exception {
    KeyPair keyPair =
        new HDDSKeyGenerator(securityConfig).generateKey();
    //TODO: generateCSR!
    PKCS10CertificationRequest csr = new CertificateSignRequest.Builder()
        .addDnsName("hadoop.apache.org")
        .addIpAddress("8.8.8.8")
        .setCA(false)
        .setScmID("wrong one")
        .setClusterID("223432rf")
        .setSubject("Ozone Cluster")
        .setConfiguration(securityConfig)
        .setKey(keyPair)
        .build()
        .generateCSR();

    CertificateServer testCA = new DefaultCAServer("testCA",
        RandomStringUtils.randomAlphabetic(4),
        RandomStringUtils.randomAlphabetic(4), caStore,
        new DefaultProfile(),
        Paths.get(SCM_CA_CERT_STORAGE_DIR, SCM_CA_PATH).toString());
    testCA.init(securityConfig, CAType.ROOT);

    ExecutionException execution = assertThrows(ExecutionException.class,
        () -> {
          Future<CertPath> holder =
              testCA.requestCertificate(csr,
                  CertificateApprover.ApprovalType.TESTING_AUTOMATIC, OM,
                  String.valueOf(System.nanoTime()));
          holder.get();
        });
    assertThat(execution.getCause().getMessage())
        .contains("ScmId and ClusterId in CSR subject are incorrect");
  }

  @Test
  public void testIntermediaryCAWithEmpty() {

    CertificateServer scmCA = new DefaultCAServer("testCA",
        RandomStringUtils.randomAlphabetic(4),
        RandomStringUtils.randomAlphabetic(4), caStore,
        new DefaultProfile(), Paths.get("scm").toString());

    assertThrows(IllegalStateException.class,
        () -> scmCA.init(securityConfig, CAType.SUBORDINATE));
  }

  @Test
  public void testExternalRootCA(@TempDir Path tempDir) throws Exception {
    //Given an external certificate
    String externalCaCertFileName = "CaCert.pem";
    setExternalPathsInConfig(tempDir, externalCaCertFileName);

    try (SCMCertificateClient scmCertificateClient =
        new SCMCertificateClient(securityConfig, null, null)) {

      KeyPair keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
      KeyCodec keyPEMWriter = new KeyCodec(securityConfig,
          scmCertificateClient.getComponentName());

      keyPEMWriter.writeKey(tempDir, keyPair, true);
      X509Certificate externalCert = generateExternalCert(keyPair);

      CertificateCodec certificateCodec = new CertificateCodec(securityConfig,
          scmCertificateClient.getComponentName());

      certificateCodec.writeCertificate(tempDir, externalCaCertFileName,
          CertificateCodec.getPEMEncodedString(externalCert));

      CertificateServer testCA = new DefaultCAServer("testCA",
          RandomStringUtils.randomAlphabetic(4),
          RandomStringUtils.randomAlphabetic(4), caStore,
          new DefaultProfile(),
          Paths.get(SCM_CA_CERT_STORAGE_DIR, SCM_CA_PATH).toString());
      //When initializing a CA server with external cert
      testCA.init(securityConfig, CAType.ROOT);
      //Then the external cert is set as CA cert for the server.
      assertEquals(externalCert, testCA.getCACertificate());
    }
  }

  private void setExternalPathsInConfig(Path tempDir,
      String externalCaCertFileName) {
    String externalCaCertPart = Paths.get(tempDir.toString(),
        externalCaCertFileName).toString();
    String privateKeyPath = Paths.get(tempDir.toString(),
        HddsConfigKeys.HDDS_PRIVATE_KEY_FILE_NAME_DEFAULT).toString();
    String publicKeyPath = Paths.get(tempDir.toString(),
        HddsConfigKeys.HDDS_PUBLIC_KEY_FILE_NAME_DEFAULT).toString();

    conf.set(HddsConfigKeys.HDDS_X509_ROOTCA_CERTIFICATE_FILE,
        externalCaCertPart);
    conf.set(HddsConfigKeys.HDDS_X509_ROOTCA_PRIVATE_KEY_FILE,
        privateKeyPath);
    conf.set(HddsConfigKeys.HDDS_X509_ROOTCA_PUBLIC_KEY_FILE,
        publicKeyPath);
    securityConfig = new SecurityConfig(conf);
  }

  @Test
  public void testInitWithCertChain(@TempDir Path tempDir) throws Exception {
    String externalCaCertFileName = "CaCert.pem";
    setExternalPathsInConfig(tempDir, externalCaCertFileName);
    CertificateApprover approver = new DefaultApprover(new DefaultCAProfile(),
        securityConfig);
    try (SCMCertificateClient scmCertificateClient =
        new SCMCertificateClient(securityConfig, null, null)) {
      String scmId = RandomStringUtils.randomAlphabetic(4);
      String clusterId = RandomStringUtils.randomAlphabetic(4);
      KeyPair keyPair = new HDDSKeyGenerator(securityConfig).generateKey();
      KeyCodec keyPEMWriter = new KeyCodec(securityConfig,
          scmCertificateClient.getComponentName());

      keyPEMWriter.writeKey(tempDir, keyPair, true);
      LocalDate beginDate = LocalDate.now().atStartOfDay().toLocalDate();
      LocalDate endDate =
          LocalDate.from(LocalDate.now().atStartOfDay().plusDays(10));
      //TODO: generateCSR!
      PKCS10CertificationRequest csr = new CertificateSignRequest.Builder()
          .addDnsName("hadoop.apache.org")
          .addIpAddress("8.8.8.8")
          .addServiceName("OzoneMarketingCluster002")
          .setCA(false)
          .setClusterID(clusterId)
          .setScmID(scmId)
          .setSubject("Ozone Cluster")
          .setConfiguration(securityConfig)
          .setKey(keyPair)
          .build()
          .generateCSR();
      X509Certificate externalCert = generateExternalCert(keyPair);
      X509Certificate signedCert = approver.sign(securityConfig,
          keyPair.getPrivate(), externalCert,
          java.sql.Date.valueOf(beginDate), java.sql.Date.valueOf(endDate), csr,
          scmId, clusterId, String.valueOf(System.nanoTime()));
      CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
      CertificateCodec certificateCodec = new CertificateCodec(securityConfig,
          scmCertificateClient.getComponentName());

      CertPath certPath = certFactory.generateCertPath(ImmutableList.of(signedCert, externalCert));
      certificateCodec.writeCertificate(tempDir, externalCaCertFileName,
          CertificateCodec.getPEMEncodedString(certPath));

      CertificateServer testCA = new DefaultCAServer("testCA",
          RandomStringUtils.randomAlphabetic(4),
          RandomStringUtils.randomAlphabetic(4), caStore,
          new DefaultProfile(),
          Paths.get(SCM_CA_CERT_STORAGE_DIR, SCM_CA_PATH).toString());
      //When initializing a CA server with external cert
      testCA.init(securityConfig, CAType.ROOT);
      //Then the external cert is set as CA cert for the server.
      assertEquals(signedCert, testCA.getCACertificate());
    }
  }

  @Test
  void testIntermediaryCA() throws Exception {

    conf.set(HddsConfigKeys.HDDS_X509_MAX_DURATION, "P3650D");
    securityConfig = new SecurityConfig(conf);

    String clusterId = RandomStringUtils.randomAlphanumeric(4);
    String scmId = RandomStringUtils.randomAlphanumeric(4);

    CertificateServer rootCA = new DefaultCAServer("rootCA",
        clusterId, scmId, caStore, new DefaultProfile(),
        Paths.get("scm", "ca").toString());

    rootCA.init(securityConfig, CAType.ROOT);


    try (SCMCertificateClient scmCertificateClient =
        new SCMCertificateClient(securityConfig, null, null)) {

      CertificateClient.InitResponse response = scmCertificateClient.init();
      assertEquals(CertificateClient.InitResponse.GETCERT, response);

      // Generate cert
      KeyPair keyPair =
          new HDDSKeyGenerator(securityConfig).generateKey();
      //TODO: generateCSR!
      PKCS10CertificationRequest csr = new CertificateSignRequest.Builder()
          .addDnsName("hadoop.apache.org")
          .addIpAddress("8.8.8.8")
          .setCA(false)
          .setSubject("testCA")
          .setConfiguration(securityConfig)
          .setKey(keyPair)
          .build()
          .generateCSR();

      Future<CertPath> holder = rootCA.requestCertificate(csr,
          CertificateApprover.ApprovalType.TESTING_AUTOMATIC, SCM,
          String.valueOf(System.nanoTime()));
      assertTrue(holder.isDone());
      X509Certificate certificate =
          CertificateCodec.firstCertificateFrom(holder.get());

      assertNotNull(certificate);
      LocalDate invalidAfterDate = certificate.getNotAfter().toInstant()
          .atZone(ZoneId.systemDefault())
          .toLocalDate();
      LocalDate now = LocalDate.now();
      assertEquals(0, invalidAfterDate.compareTo(now.plusDays(3650)));

      X509Certificate caCertificate = rootCA.getCACertificate();
      scmCertificateClient.storeCertificate(CertificateCodec.getPEMEncodedString(caCertificate), CAType.SUBORDINATE);

      // Write to the location where Default CA Server reads from.
      scmCertificateClient.storeCertificate(CertificateCodec.getPEMEncodedString(certificate), CAType.NONE);

      CertificateCodec certCodec = new CertificateCodec(securityConfig, scmCertificateClient.getComponentName());
      certCodec.writeCertificate(certificate);

      // The certificate generated by above cert client will be used by scmCA.
      // Now scmCA init should be successful.
      CertificateServer scmCA = new DefaultCAServer(
          "scmCA", clusterId, scmId, caStore, new DefaultProfile(), scmCertificateClient.getComponentName());


      scmCA.init(securityConfig, CAType.SUBORDINATE);
    }
  }

  private X509Certificate generateExternalCert(KeyPair keyPair) throws Exception {
    LocalDateTime notBefore = LocalDateTime.now();
    LocalDateTime notAfter = notBefore.plusYears(1);
    String clusterID = UUID.randomUUID().toString();
    String scmID = UUID.randomUUID().toString();
    String subject = "testRootCert";

    return SelfSignedCertificate.newBuilder()
        .setBeginDate(notBefore)
        .setEndDate(notAfter)
        .setClusterID(clusterID)
        .setScmID(scmID)
        .setSubject(subject)
        .setKey(keyPair)
        .setConfiguration(securityConfig)
        .makeCA()
        .addInetAddresses()
        .build();
  }
}
