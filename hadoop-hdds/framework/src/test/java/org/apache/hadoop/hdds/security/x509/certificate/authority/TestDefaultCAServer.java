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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.PKIProfiles.DefaultProfile;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.SCMCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.ozone.test.LambdaTestUtils;

import org.bouncycastle.asn1.x509.CRLReason;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertTrue;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType.OM;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType.SCM;
import static org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer.CAType.INTERMEDIARY_CA;
import static org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer.CAType.SELF_SIGNED_CA;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_CA_CERT_STORAGE_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_CA_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Tests the Default CA Server.
 */
public class TestDefaultCAServer {
  private static OzoneConfiguration conf = new OzoneConfiguration();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private MockCAStore caStore;

  @Before
  public void init() throws IOException {
    conf.set(OZONE_METADATA_DIRS, temporaryFolder.newFolder().toString());
    caStore = new MockCAStore();
  }

  @Test
  public void testInit() throws SCMSecurityException, CertificateException,
      IOException {
    SecurityConfig securityConfig = new SecurityConfig(conf);
    CertificateServer testCA = new DefaultCAServer("testCA",
        RandomStringUtils.randomAlphabetic(4),
        RandomStringUtils.randomAlphabetic(4), caStore,
        new DefaultProfile(),
        Paths.get(SCM_CA_CERT_STORAGE_DIR, SCM_CA_PATH).toString());
    testCA.init(securityConfig, SELF_SIGNED_CA);
    X509CertificateHolder first = testCA.getCACertificate();
    assertNotNull(first);
    //Init is idempotent.
    testCA.init(securityConfig, SELF_SIGNED_CA);
    X509CertificateHolder second = testCA.getCACertificate();
    assertEquals(first, second);
  }

  @Test
  public void testMissingCertificate() {
    SecurityConfig securityConfig = new SecurityConfig(conf);
    CertificateServer testCA = new DefaultCAServer("testCA",
        RandomStringUtils.randomAlphabetic(4),
        RandomStringUtils.randomAlphabetic(4), caStore,
        new DefaultProfile(),
        Paths.get(SCM_CA_CERT_STORAGE_DIR, SCM_CA_PATH).toString());
    Consumer<SecurityConfig> caInitializer =
        ((DefaultCAServer) testCA).processVerificationStatus(
        DefaultCAServer.VerificationStatus.MISSING_CERTIFICATE, SELF_SIGNED_CA);
    try {

      caInitializer.accept(securityConfig);
      fail("code should not reach here, exception should have been thrown.");
    } catch (IllegalStateException e) {
      // This also is a runtime exception. Hence not caught by junit expected
      // exception.
      assertTrue(e.toString().contains("Missing Root Certs"));
    }
  }

  @Test
  public void testMissingKey() {
    SecurityConfig securityConfig = new SecurityConfig(conf);
    CertificateServer testCA = new DefaultCAServer("testCA",
        RandomStringUtils.randomAlphabetic(4),
        RandomStringUtils.randomAlphabetic(4), caStore,
        new DefaultProfile(),
        Paths.get(SCM_CA_CERT_STORAGE_DIR, SCM_CA_PATH).toString());
    Consumer<SecurityConfig> caInitializer =
        ((DefaultCAServer) testCA).processVerificationStatus(
            DefaultCAServer.VerificationStatus.MISSING_KEYS, SELF_SIGNED_CA);
    try {

      caInitializer.accept(securityConfig);
      fail("code should not reach here, exception should have been thrown.");
    } catch (IllegalStateException e) {
      // This also is a runtime exception. Hence not caught by junit expected
      // exception.
      assertTrue(e.toString().contains("Missing Keys"));
    }
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
  public void testRequestCertificate() throws IOException,
      ExecutionException, InterruptedException,
      NoSuchProviderException, NoSuchAlgorithmException {
    String scmId =  RandomStringUtils.randomAlphabetic(4);
    String clusterId =  RandomStringUtils.randomAlphabetic(4);
    KeyPair keyPair =
        new HDDSKeyGenerator(conf).generateKey();
    PKCS10CertificationRequest csr = new CertificateSignRequest.Builder()
        .addDnsName("hadoop.apache.org")
        .addIpAddress("8.8.8.8")
        .addServiceName("OzoneMarketingCluster002")
        .setCA(false)
        .setClusterID(clusterId)
        .setScmID(scmId)
        .setSubject("Ozone Cluster")
        .setConfiguration(conf)
        .setKey(keyPair)
        .build();

    // Let us convert this to a string to mimic the common use case.
    String csrString = CertificateSignRequest.getEncodedString(csr);

    CertificateServer testCA = new DefaultCAServer("testCA",
        clusterId, scmId, caStore,
        new DefaultProfile(),
        Paths.get(SCM_CA_CERT_STORAGE_DIR, SCM_CA_PATH).toString());
    testCA.init(new SecurityConfig(conf),
        SELF_SIGNED_CA);

    Future<X509CertificateHolder> holder = testCA.requestCertificate(csrString,
        CertificateApprover.ApprovalType.TESTING_AUTOMATIC, SCM);
    // Right now our calls are synchronous. Eventually this will have to wait.
    assertTrue(holder.isDone());
    assertNotNull(holder.get());
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
  public void testRequestCertificateWithInvalidSubject() throws IOException,
      ExecutionException, InterruptedException,
      NoSuchProviderException, NoSuchAlgorithmException {
    KeyPair keyPair =
        new HDDSKeyGenerator(conf).generateKey();
    PKCS10CertificationRequest csr = new CertificateSignRequest.Builder()
        .addDnsName("hadoop.apache.org")
        .addIpAddress("8.8.8.8")
        .setCA(false)
        .setSubject("Ozone Cluster")
        .setConfiguration(conf)
        .setKey(keyPair)
        .build();

    // Let us convert this to a string to mimic the common use case.
    String csrString = CertificateSignRequest.getEncodedString(csr);

    CertificateServer testCA = new DefaultCAServer("testCA",
        RandomStringUtils.randomAlphabetic(4),
        RandomStringUtils.randomAlphabetic(4), caStore,
        new DefaultProfile(),
        Paths.get(SCM_CA_CERT_STORAGE_DIR, SCM_CA_PATH).toString());
    testCA.init(new SecurityConfig(conf),
        SELF_SIGNED_CA);

    Future<X509CertificateHolder> holder = testCA.requestCertificate(csrString,
        CertificateApprover.ApprovalType.TESTING_AUTOMATIC, OM);
    // Right now our calls are synchronous. Eventually this will have to wait.
    assertTrue(holder.isDone());
    assertNotNull(holder.get());
  }

  @Test
  public void testRevokeCertificates() throws Exception {
    String scmId =  RandomStringUtils.randomAlphabetic(4);
    String clusterId =  RandomStringUtils.randomAlphabetic(4);
    Date now = new Date();

    CertificateServer testCA = new DefaultCAServer("testCA",
        clusterId, scmId, caStore,
        new DefaultProfile(),
        Paths.get(SCM_CA_CERT_STORAGE_DIR, SCM_CA_PATH).toString());
    testCA.init(new SecurityConfig(conf),
        SELF_SIGNED_CA);

    KeyPair keyPair =
        new HDDSKeyGenerator(conf).generateKey();
    PKCS10CertificationRequest csr = new CertificateSignRequest.Builder()
        .addDnsName("hadoop.apache.org")
        .addIpAddress("8.8.8.8")
        .setCA(false)
        .setSubject("testCA")
        .setConfiguration(conf)
        .setKey(keyPair)
        .build();

    // Let us convert this to a string to mimic the common use case.
    String csrString = CertificateSignRequest.getEncodedString(csr);

    Future<X509CertificateHolder> holder = testCA.requestCertificate(csrString,
        CertificateApprover.ApprovalType.TESTING_AUTOMATIC, OM);

    X509Certificate certificate =
        new JcaX509CertificateConverter().getCertificate(holder.get());
    List<BigInteger> serialIDs = new ArrayList<>();
    serialIDs.add(certificate.getSerialNumber());
    Future<Optional<Long>> revoked = testCA.revokeCertificates(serialIDs,
        CRLReason.lookup(CRLReason.keyCompromise), now);

    // Revoking a valid certificate complete successfully without errors.
    assertTrue(revoked.isDone());

    // Revoking empty list of certificates should throw an error.
    LambdaTestUtils.intercept(ExecutionException.class, "Certificates " +
        "cannot be null",
        () -> {
          Future<Optional<Long>> result =
              testCA.revokeCertificates(Collections.emptyList(),
              CRLReason.lookup(CRLReason.keyCompromise), now);
          result.isDone();
          result.get();
        });
  }

  @Test
  public void testRequestCertificateWithInvalidSubjectFailure()
      throws Exception {
    KeyPair keyPair =
        new HDDSKeyGenerator(conf).generateKey();
    PKCS10CertificationRequest csr = new CertificateSignRequest.Builder()
        .addDnsName("hadoop.apache.org")
        .addIpAddress("8.8.8.8")
        .setCA(false)
        .setScmID("wrong one")
        .setClusterID("223432rf")
        .setSubject("Ozone Cluster")
        .setConfiguration(conf)
        .setKey(keyPair)
        .build();

    // Let us convert this to a string to mimic the common use case.
    String csrString = CertificateSignRequest.getEncodedString(csr);

    CertificateServer testCA = new DefaultCAServer("testCA",
        RandomStringUtils.randomAlphabetic(4),
        RandomStringUtils.randomAlphabetic(4), caStore,
        new DefaultProfile(),
        Paths.get(SCM_CA_CERT_STORAGE_DIR, SCM_CA_PATH).toString());
    testCA.init(new SecurityConfig(conf),
        SELF_SIGNED_CA);

    LambdaTestUtils.intercept(ExecutionException.class, "ScmId and " +
            "ClusterId in CSR subject are incorrect",
        () -> {
          Future<X509CertificateHolder> holder =
              testCA.requestCertificate(csrString,
                  CertificateApprover.ApprovalType.TESTING_AUTOMATIC, OM);
          holder.isDone();
          holder.get();
        });
  }

  @Test(expected = IllegalStateException.class)
  public void testIntermediaryCAWithEmpty()
      throws Exception {

    CertificateServer scmCA = new DefaultCAServer("testCA",
        RandomStringUtils.randomAlphabetic(4),
        RandomStringUtils.randomAlphabetic(4), caStore,
        new DefaultProfile(), Paths.get("scm").toString());

    scmCA.init(new SecurityConfig(conf), INTERMEDIARY_CA);
  }

  @Test
  public void testIntermediaryCA() throws Exception {

    conf.set(HddsConfigKeys.HDDS_X509_MAX_DURATION, "P3650D");

    String clusterId = RandomStringUtils.randomAlphanumeric(4);
    String scmId = RandomStringUtils.randomAlphanumeric(4);

    CertificateServer rootCA = new DefaultCAServer("rootCA",
        clusterId, scmId, caStore, new DefaultProfile(),
        Paths.get("scm", "ca").toString());

    rootCA.init(new SecurityConfig(conf), SELF_SIGNED_CA);


    SCMCertificateClient scmCertificateClient =
        new SCMCertificateClient(new SecurityConfig(conf));

    CertificateClient.InitResponse response = scmCertificateClient.init();
    Assert.assertEquals(CertificateClient.InitResponse.GETCERT, response);

    // Generate cert
    KeyPair keyPair =
        new HDDSKeyGenerator(conf).generateKey();
    PKCS10CertificationRequest csr = new CertificateSignRequest.Builder()
        .addDnsName("hadoop.apache.org")
        .addIpAddress("8.8.8.8")
        .setCA(false)
        .setSubject("testCA")
        .setConfiguration(conf)
        .setKey(keyPair)
        .build();

    Future<X509CertificateHolder> holder = rootCA.requestCertificate(csr,
        CertificateApprover.ApprovalType.TESTING_AUTOMATIC, SCM);
    Assert.assertTrue(holder.isDone());

    X509CertificateHolder certificateHolder = holder.get();


    Assert.assertNotNull(certificateHolder);
    Assert.assertEquals(10, certificateHolder.getNotAfter().toInstant()
        .atZone(ZoneId.systemDefault())
        .toLocalDate().compareTo(LocalDate.now()));

    X509CertificateHolder rootCertHolder = rootCA.getCACertificate();

    scmCertificateClient.storeCertificate(
        CertificateCodec.getPEMEncodedString(rootCertHolder), true, true);

    // Write to the location where Default CA Server reads from.
    scmCertificateClient.storeCertificate(
        CertificateCodec.getPEMEncodedString(certificateHolder), true);

    CertificateCodec certCodec =
        new CertificateCodec(new SecurityConfig(conf),
            scmCertificateClient.getComponentName());
    certCodec.writeCertificate(certificateHolder);

    // The certificate generated by above cert client will be used by scmCA.
    // Now scmCA init should be successful.
    CertificateServer scmCA = new DefaultCAServer("scmCA",
        clusterId, scmId, caStore, new DefaultProfile(),
        scmCertificateClient.getComponentName());

    try {
      scmCA.init(new SecurityConfig(conf), INTERMEDIARY_CA);
    } catch (Exception e) {
      fail("testIntermediaryCA failed during init");
    }

  }

}
