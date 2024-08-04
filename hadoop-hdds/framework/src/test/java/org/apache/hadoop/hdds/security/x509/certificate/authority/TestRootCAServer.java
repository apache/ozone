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
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.DefaultCAProfile;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.DefaultProfile;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.certificate.utils.SelfSignedCertificate;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyCodec;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.cert.CertPath;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_CA_CERT_STORAGE_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_CA_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests the RootCAServer's additional functionality to DefaultCAServer.
 */
public class TestRootCAServer {

  private OzoneConfiguration conf;
  private SecurityConfig securityConfig;
  private MockCAStore caStore;
  private Path testDir;

  @BeforeEach
  public void init(@TempDir Path dir) throws IOException {
    conf = new OzoneConfiguration();
    testDir = dir;
    conf.set(OZONE_METADATA_DIRS, testDir.toString());
    securityConfig = new SecurityConfig(conf);
    caStore = new MockCAStore();
  }

  @Test
  public void testExternalRootCA(@TempDir Path tempDir) throws Exception {
    //Given an external certificate
    String externalCaCertFileName = "CaCert.pem";
    setExternalPathsInConfig(tempDir, externalCaCertFileName);
    String componentName = Paths.get(SCM_CA_CERT_STORAGE_DIR, SCM_CA_PATH).toString();

    KeyPair keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    KeyCodec codec = securityConfig.keyCodec();
    Files.write(securityConfig.getExternalRootCaPrivateKeyPath(), codec.encodePrivateKey(keyPair.getPrivate()));
    Files.write(securityConfig.getExternalRootCaPublicKeyPath(), codec.encodePublicKey(keyPair.getPublic()));

    X509Certificate externalCert = generateExternalCert(keyPair);

    CertificateCodec certificateCodec = new CertificateCodec(securityConfig,
        componentName);

    certificateCodec.writeCertificate(tempDir, externalCaCertFileName,
        CertificateCodec.getPEMEncodedString(externalCert));

    CertificateServer testCA = new RootCAServer("testCA",
        RandomStringUtils.randomAlphabetic(4),
        RandomStringUtils.randomAlphabetic(4), caStore,
        new DefaultProfile(),
        Paths.get(SCM_CA_CERT_STORAGE_DIR, SCM_CA_PATH).toString(), BigInteger.ONE, t -> {
    });
    //When initializing a CA server with external cert
    testCA.init(securityConfig);
    //Then the external cert is set as CA cert for the server.
    assertEquals(externalCert, testCA.getCACertificate());
  }

  @Test
  public void testInitWithExternalCertChain() throws Exception {
    String externalCaCertFileName = "CaCert.pem";
    setExternalPathsInConfig(testDir, externalCaCertFileName);
    CertificateApprover approver = new DefaultApprover(new DefaultCAProfile(),
        securityConfig);
    String componentName = SCM_CA_CERT_STORAGE_DIR + "/" + SCM_CA_PATH;
    String scmId = RandomStringUtils.randomAlphabetic(4);
    String clusterId = RandomStringUtils.randomAlphabetic(4);
    KeyPair keyPair = new HDDSKeyGenerator(securityConfig).generateKey();
    KeyCodec codec = securityConfig.keyCodec();
    Files.write(securityConfig.getExternalRootCaPrivateKeyPath(), codec.encodePrivateKey(keyPair.getPrivate()));
    Files.write(securityConfig.getExternalRootCaPublicKeyPath(), codec.encodePublicKey(keyPair.getPublic()));

    LocalDate beginDate = LocalDate.now().atStartOfDay().toLocalDate();
    LocalDate endDate =
        LocalDate.from(LocalDate.now().atStartOfDay().plusDays(10));
    CertificateSignRequest csr = new CertificateSignRequest.Builder()
        .addDnsName("hadoop.apache.org")
        .addIpAddress("8.8.8.8")
        .addServiceName("OzoneMarketingCluster002")
        .setCA(false)
        .setClusterID(clusterId)
        .setScmID(scmId)
        .setSubject("Ozone Cluster")
        .setConfiguration(securityConfig)
        .setKey(keyPair)
        .build();
    X509Certificate externalCert = generateExternalCert(keyPair);
    X509Certificate signedCert = approver.sign(securityConfig,
        keyPair.getPrivate(), externalCert,
        java.sql.Date.valueOf(beginDate), java.sql.Date.valueOf(endDate), csr.toEncodedFormat(),
        scmId, clusterId, String.valueOf(System.nanoTime()));
    CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
    CertificateCodec certificateCodec = new CertificateCodec(securityConfig, componentName);

    CertPath certPath = certFactory.generateCertPath(ImmutableList.of(signedCert, externalCert));
    certificateCodec.writeCertificate(testDir, externalCaCertFileName,
        CertificateCodec.getPEMEncodedString(certPath));

    CertificateServer testCA = new RootCAServer("testCA",
        RandomStringUtils.randomAlphabetic(4),
        RandomStringUtils.randomAlphabetic(4), caStore,
        new DefaultProfile(),
        componentName, BigInteger.ONE, t -> {
    });
    //When initializing a CA server with external cert
    testCA.init(securityConfig);
    //Then the external cert is set as CA cert for the server.
    assertEquals(signedCert, testCA.getCACertificate());
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
