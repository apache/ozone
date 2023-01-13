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
package org.apache.hadoop.hdds.security.x509.certificate.client;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.exceptions.CertificateException;
import org.apache.hadoop.hdds.security.x509.keys.KeyCodec;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;


import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.slf4j.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_METADATA_DIR_NAME;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.FAILURE;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.REINIT;
import static org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec.getPEMEncodedString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test class for {@link DefaultCertificateClient}.
 */
public class TestDefaultCertificateClient {

  private String certSerialId;
  private X509Certificate x509Certificate;
  private DNCertificateClient dnCertClient;
  private HDDSKeyGenerator keyGenerator;
  private Path dnMetaDirPath;
  private SecurityConfig dnSecurityConfig;
  private static final String DN_COMPONENT = DNCertificateClient.COMPONENT_NAME;
  private KeyCodec dnKeyCodec;

  @BeforeEach
  public void setUp() throws Exception {
    OzoneConfiguration config = new OzoneConfiguration();
    config.setStrings(OZONE_SCM_NAMES, "localhost");
    config.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 2);
    final String dnPath = GenericTestUtils
        .getTempPath(UUID.randomUUID().toString());

    dnMetaDirPath = Paths.get(dnPath, "test");
    config.set(HDDS_METADATA_DIR_NAME, dnMetaDirPath.toString());
    dnSecurityConfig = new SecurityConfig(config);

    keyGenerator = new HDDSKeyGenerator(dnSecurityConfig);
    dnKeyCodec = new KeyCodec(dnSecurityConfig, DN_COMPONENT);

    Files.createDirectories(dnSecurityConfig.getKeyLocation(DN_COMPONENT));
    x509Certificate = generateX509Cert(null);
    certSerialId = x509Certificate.getSerialNumber().toString();
    getCertClient();
  }

  private void getCertClient() {
    dnCertClient = new DNCertificateClient(dnSecurityConfig,
        MockDatanodeDetails.randomDatanodeDetails(), certSerialId, null,
        () -> System.exit(1));
  }

  @AfterEach
  public void tearDown() {
    dnCertClient = null;
    FileUtils.deleteQuietly(dnMetaDirPath.toFile());
  }

  /**
   * Tests: 1. getPrivateKey 2. getPublicKey 3. storePrivateKey 4.
   * storePublicKey
   */
  @Test
  public void testKeyOperations() throws Exception {
    cleanupOldKeyPair();
    PrivateKey pvtKey = dnCertClient.getPrivateKey();
    PublicKey publicKey = dnCertClient.getPublicKey();
    assertNull(publicKey);
    assertNull(pvtKey);

    KeyPair keyPair = generateKeyPairFiles();
    pvtKey = dnCertClient.getPrivateKey();
    assertNotNull(pvtKey);
    assertEquals(pvtKey, keyPair.getPrivate());

    publicKey = dnCertClient.getPublicKey();
    assertNotNull(publicKey);
    assertEquals(publicKey, keyPair.getPublic());
  }

  private KeyPair generateKeyPairFiles() throws Exception {
    cleanupOldKeyPair();
    KeyPair keyPair = keyGenerator.generateKey();
    dnKeyCodec.writePrivateKey(keyPair.getPrivate());
    dnKeyCodec.writePublicKey(keyPair.getPublic());
    return keyPair;
  }

  private void cleanupOldKeyPair() {
    FileUtils.deleteQuietly(Paths.get(
        dnSecurityConfig.getKeyLocation(DN_COMPONENT).toString(),
        dnSecurityConfig.getPrivateKeyFileName()).toFile());
    FileUtils.deleteQuietly(Paths.get(
        dnSecurityConfig.getKeyLocation(DN_COMPONENT).toString(),
        dnSecurityConfig.getPublicKeyFileName()).toFile());
  }

  /**
   * Tests: 1. storeCertificate 2. getCertificate 3. verifyCertificate
   */
  @Test
  public void testCertificateOps() throws Exception {
    X509Certificate cert = dnCertClient.getCertificate();
    assertNull(cert);
    dnCertClient.storeCertificate(getPEMEncodedString(x509Certificate),
        true);

    cert = dnCertClient.getCertificate(
        x509Certificate.getSerialNumber().toString());
    assertNotNull(cert);
    assertTrue(cert.getEncoded().length > 0);
    assertEquals(x509Certificate, cert);

    // TODO: test verifyCertificate once implemented.
  }

  private X509Certificate generateX509Cert(KeyPair keyPair) throws Exception {
    if (keyPair == null) {
      keyPair = generateKeyPairFiles();
    }
    return KeyStoreTestUtil.generateCertificate("CN=Test", keyPair, 30,
        dnSecurityConfig.getSignatureAlgo());
  }

  @Test
  public void testSignDataStream() throws Exception {
    String data = RandomStringUtils.random(100);
    FileUtils.deleteQuietly(Paths.get(
        dnSecurityConfig.getKeyLocation(DN_COMPONENT).toString(),
        dnSecurityConfig.getPrivateKeyFileName()).toFile());
    FileUtils.deleteQuietly(Paths.get(
        dnSecurityConfig.getKeyLocation(DN_COMPONENT).toString(),
        dnSecurityConfig.getPublicKeyFileName()).toFile());

    // Expect error when there is no private key to sign.
    LambdaTestUtils.intercept(IOException.class, "Error while " +
            "signing the stream",
        () -> dnCertClient.signDataStream(IOUtils.toInputStream(data, UTF_8)));

    generateKeyPairFiles();
    byte[] sign = dnCertClient.signDataStream(IOUtils.toInputStream(data,
        UTF_8));
    validateHash(sign, data.getBytes(UTF_8));
  }

  /**
   * Validate hash using public key of KeyPair.
   */
  private void validateHash(byte[] hash, byte[] data)
      throws Exception {
    Signature rsaSignature =
        Signature.getInstance(dnSecurityConfig.getSignatureAlgo(),
            dnSecurityConfig.getProvider());
    rsaSignature.initVerify(dnCertClient.getPublicKey());
    rsaSignature.update(data);
    assertTrue(rsaSignature.verify(hash));
  }

  /**
   * Tests: 1. verifySignature
   */
  @Test
  public void verifySignatureStream() throws Exception {
    String data = RandomStringUtils.random(500);
    byte[] sign = dnCertClient.signDataStream(IOUtils.toInputStream(data,
        UTF_8));

    // Positive tests.
    assertTrue(dnCertClient.verifySignature(data.getBytes(UTF_8), sign,
        x509Certificate));
    assertTrue(dnCertClient.verifySignature(
        IOUtils.toInputStream(data, UTF_8),
        sign, x509Certificate));

    // Negative tests.
    assertFalse(dnCertClient.verifySignature(data.getBytes(UTF_8),
        "abc".getBytes(UTF_8), x509Certificate));
    assertFalse(dnCertClient.verifySignature(IOUtils.toInputStream(data,
        UTF_8), "abc".getBytes(UTF_8), x509Certificate));

  }

  /**
   * Tests: 1. verifySignature
   */
  @Test
  public void verifySignatureDataArray() throws Exception {
    String data = RandomStringUtils.random(500);
    byte[] sign = dnCertClient.signData(data.getBytes(UTF_8));

    // Positive tests.
    assertTrue(dnCertClient.verifySignature(data.getBytes(UTF_8), sign,
        x509Certificate));
    assertTrue(dnCertClient.verifySignature(
        IOUtils.toInputStream(data, UTF_8),
        sign, x509Certificate));

    // Negative tests.
    assertFalse(dnCertClient.verifySignature(data.getBytes(UTF_8),
        "abc".getBytes(UTF_8), x509Certificate));
    assertFalse(dnCertClient.verifySignature(IOUtils.toInputStream(data,
        UTF_8), "abc".getBytes(UTF_8), x509Certificate));

  }

  @Test
  public void queryCertificate() throws Exception {
    LambdaTestUtils.intercept(UnsupportedOperationException.class,
        "Operation not supported",
        () -> dnCertClient.queryCertificate(""));
  }

  @Test
  public void testCertificateLoadingOnInit() throws Exception {
    KeyPair keyPair = keyGenerator.generateKey();
    X509Certificate cert1 = generateX509Cert(keyPair);
    X509Certificate cert2 = generateX509Cert(keyPair);
    X509Certificate cert3 = generateX509Cert(keyPair);

    Path certPath = dnSecurityConfig.getCertificateLocation(DN_COMPONENT);
    CertificateCodec codec = new CertificateCodec(dnSecurityConfig,
        DN_COMPONENT);

    // Certificate not found.
    LambdaTestUtils.intercept(CertificateException.class, "Error while" +
            " getting certificate",
        () -> dnCertClient.getCertificate(cert1.getSerialNumber()
            .toString()));
    LambdaTestUtils.intercept(CertificateException.class, "Error while" +
            " getting certificate",
        () -> dnCertClient.getCertificate(cert2.getSerialNumber()
            .toString()));
    LambdaTestUtils.intercept(CertificateException.class, "Error while" +
            " getting certificate",
        () -> dnCertClient.getCertificate(cert3.getSerialNumber()
            .toString()));
    codec.writeCertificate(certPath, "1.crt",
        getPEMEncodedString(cert1), true);
    codec.writeCertificate(certPath, "2.crt",
        getPEMEncodedString(cert2), true);
    codec.writeCertificate(certPath, "3.crt",
        getPEMEncodedString(cert3), true);

    // Re instantiate DN client which will load certificates from filesystem.
    dnCertClient = new DNCertificateClient(dnSecurityConfig, null,
        certSerialId, null, null);

    assertNotNull(dnCertClient.getCertificate(cert1.getSerialNumber()
        .toString()));
    assertNotNull(dnCertClient.getCertificate(cert2.getSerialNumber()
        .toString()));
    assertNotNull(dnCertClient.getCertificate(cert3.getSerialNumber()
        .toString()));

  }

  @Test
  public void testStoreCertificate() throws Exception {
    KeyPair keyPair = keyGenerator.generateKey();
    X509Certificate cert1 = generateX509Cert(keyPair);
    X509Certificate cert2 = generateX509Cert(keyPair);
    X509Certificate cert3 = generateX509Cert(keyPair);

    dnCertClient.storeCertificate(getPEMEncodedString(cert1), true);
    dnCertClient.storeCertificate(getPEMEncodedString(cert2), true);
    dnCertClient.storeCertificate(getPEMEncodedString(cert3), true);

    assertNotNull(dnCertClient.getCertificate(cert1.getSerialNumber()
        .toString()));
    assertNotNull(dnCertClient.getCertificate(cert2.getSerialNumber()
        .toString()));
    assertNotNull(dnCertClient.getCertificate(cert3.getSerialNumber()
        .toString()));
  }

  @Test
  public void testInitCertAndKeypairValidationFailures() throws Exception {
    GenericTestUtils.LogCapturer dnClientLog = GenericTestUtils.LogCapturer
        .captureLogs(dnCertClient.getLogger());
    KeyPair keyPair = keyGenerator.generateKey();
    KeyPair keyPair1 = keyGenerator.generateKey();
    dnClientLog.clearOutput();

    // Case 1. Expect failure when keypair validation fails.
    FileUtils.deleteQuietly(Paths.get(
        dnSecurityConfig.getKeyLocation(DN_COMPONENT).toString(),
        dnSecurityConfig.getPrivateKeyFileName()).toFile());
    FileUtils.deleteQuietly(Paths.get(
        dnSecurityConfig.getKeyLocation(DN_COMPONENT).toString(),
        dnSecurityConfig.getPublicKeyFileName()).toFile());
    dnKeyCodec.writePrivateKey(keyPair.getPrivate());
    dnKeyCodec.writePublicKey(keyPair1.getPublic());

    // Check for DN.
    assertEquals(FAILURE, dnCertClient.init());
    assertTrue(dnClientLog.getOutput().contains("Keypair validation failed"));
    dnClientLog.clearOutput();

    // Case 2. Expect failure when certificate is generated from different
    // private key and keypair validation fails.
    getCertClient();
    FileUtils.deleteQuietly(Paths.get(
        dnSecurityConfig.getKeyLocation(DN_COMPONENT).toString(),
        dnSecurityConfig.getCertificateFileName()).toFile());

    CertificateCodec dnCertCodec = new CertificateCodec(dnSecurityConfig,
        DN_COMPONENT);
    dnCertCodec.writeCertificate(new X509CertificateHolder(
        x509Certificate.getEncoded()));
    // Check for DN.
    assertEquals(FAILURE, dnCertClient.init());
    assertTrue(dnClientLog.getOutput().contains("Keypair validation failed"));
    dnClientLog.clearOutput();

    // Case 3. Expect failure when certificate is generated from different
    // private key and certificate validation fails.

    // Re-write the correct public key.
    FileUtils.deleteQuietly(Paths.get(
        dnSecurityConfig.getKeyLocation(DN_COMPONENT).toString(),
        dnSecurityConfig.getPublicKeyFileName()).toFile());
    getCertClient();
    dnKeyCodec.writePublicKey(keyPair.getPublic());

    // Check for DN.
    assertEquals(FAILURE, dnCertClient.init());
    assertTrue(dnClientLog.getOutput()
        .contains("Stored certificate is generated with different"));
    dnClientLog.clearOutput();

    // Case 4. Failure when public key recovery fails.
    getCertClient();
    FileUtils.deleteQuietly(Paths.get(
        dnSecurityConfig.getKeyLocation(DN_COMPONENT).toString(),
        dnSecurityConfig.getPublicKeyFileName()).toFile());

    // Check for DN.
    assertEquals(FAILURE, dnCertClient.init());
    assertTrue(dnClientLog.getOutput().contains("Can't recover public key"));
  }

  @Test
  public void testCertificateExpirationHandlingInInit() throws Exception {
    String certId = "1L";
    String compName = "TEST";

    Logger mockLogger = mock(Logger.class);

    SecurityConfig config = mock(SecurityConfig.class);
    Path nonexistent = Paths.get("nonexistent");
    when(config.getCertificateLocation(anyString())).thenReturn(nonexistent);
    when(config.getKeyLocation(anyString())).thenReturn(nonexistent);
    when(config.getRenewalGracePeriod()).thenReturn(Duration.ofDays(28));

    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DAY_OF_YEAR, 2);
    Date expiration = cal.getTime();
    X509Certificate mockCert = mock(X509Certificate.class);
    when(mockCert.getNotAfter()).thenReturn(expiration);

    DefaultCertificateClient client =
        new DefaultCertificateClient(config, mockLogger, certId, compName,
            null, null) {
          @Override
          public PrivateKey getPrivateKey() {
            return mock(PrivateKey.class);
          }

          @Override
          public PublicKey getPublicKey() {
            return mock(PublicKey.class);
          }

          @Override
          public X509Certificate getCertificate() {
            return mockCert;
          }

          @Override
          public String signAndStoreCertificate(
              PKCS10CertificationRequest request, Path certPath)
              throws CertificateException {
            return null;
          }

          @Override
          public CertificateSignRequest.Builder getCSRBuilder(KeyPair keyPair)
              throws CertificateException {
            return null;
          }
        };

    InitResponse resp = client.init();
    verify(mockLogger, atLeastOnce()).info(anyString());
    assertEquals(resp, REINIT);
  }

  @Test
  public void testTimeBeforeExpiryGracePeriod() throws Exception {
    KeyPair keyPair = keyGenerator.generateKey();
    Duration gracePeriod = dnSecurityConfig.getRenewalGracePeriod();

    X509Certificate cert = KeyStoreTestUtil.generateCertificate("CN=Test",
        keyPair, (int)(gracePeriod.toDays()),
        dnSecurityConfig.getSignatureAlgo());
    dnCertClient.storeCertificate(getPEMEncodedString(cert), true);
    Duration duration = dnCertClient.timeBeforeExpiryGracePeriod(cert);
    Assert.assertTrue(duration.isZero());

    cert = KeyStoreTestUtil.generateCertificate("CN=Test",
        keyPair, (int)(gracePeriod.toDays() + 1),
        dnSecurityConfig.getSignatureAlgo());
    dnCertClient.storeCertificate(getPEMEncodedString(cert), true);
    duration = dnCertClient.timeBeforeExpiryGracePeriod(cert);
    Assert.assertTrue(duration.toMillis() < Duration.ofDays(1).toMillis() &&
        duration.toMillis() > Duration.ofHours(23).plusMinutes(59).toMillis());
  }

  @Test
  public void testRenewAndStoreKeyAndCertificate() throws Exception {
    // save the certificate on dn
    CertificateCodec certCodec = new CertificateCodec(dnSecurityConfig,
        dnSecurityConfig.getCertificateLocation(DN_COMPONENT));
    certCodec.writeCertificate(
        new X509CertificateHolder(x509Certificate.getEncoded()));

    SCMSecurityProtocolClientSideTranslatorPB scmClient =
        mock(SCMSecurityProtocolClientSideTranslatorPB.class);
    X509Certificate newCert = generateX509Cert(null);
    dnCertClient.setSecureScmClient(scmClient);
    String pemCert = CertificateCodec.getPEMEncodedString(newCert);
    SCMSecurityProtocolProtos.SCMGetCertResponseProto responseProto =
        SCMSecurityProtocolProtos.SCMGetCertResponseProto
            .newBuilder().setResponseCode(SCMSecurityProtocolProtos
                .SCMGetCertResponseProto.ResponseCode.success)
            .setX509Certificate(pemCert)
            .setX509CACertificate(pemCert)
            .build();
    when(scmClient.getDataNodeCertificateChain(anyObject(), anyString()))
        .thenReturn(responseProto);

    String certID = dnCertClient.getCertificate().getSerialNumber().toString();
    // a success renew
    String newCertId = dnCertClient.renewAndStoreKeyAndCertificate(true);
    Assert.assertFalse(certID.equals(newCertId));
    Assert.assertTrue(dnCertClient.getCertificate().getSerialNumber()
        .toString().equals(certID));

    File newKeyDir = new File(dnSecurityConfig.getKeyLocation(
        dnCertClient.getComponentName()).toString() +
            HddsConfigKeys.HDDS_NEW_KEY_CERT_DIR_NAME_SUFFIX);
    File newCertDir = new File(dnSecurityConfig.getCertificateLocation(
        dnCertClient.getComponentName()).toString() +
            HddsConfigKeys.HDDS_NEW_KEY_CERT_DIR_NAME_SUFFIX);
    File backupKeyDir = new File(dnSecurityConfig.getKeyLocation(
        dnCertClient.getComponentName()).toString() +
            HddsConfigKeys.HDDS_BACKUP_KEY_CERT_DIR_NAME_SUFFIX);
    File backupCertDir = new File(dnSecurityConfig.getCertificateLocation(
        dnCertClient.getComponentName()).toString() +
            HddsConfigKeys.HDDS_BACKUP_KEY_CERT_DIR_NAME_SUFFIX);

    // backup directories exist
    Assert.assertTrue(backupKeyDir.exists());
    Assert.assertTrue(backupCertDir.exists());
    // new directories should not exist
    Assert.assertFalse(newKeyDir.exists());
    Assert.assertFalse(newCertDir.exists());

    // cleanup backup key and cert dir
    dnCertClient.cleanBackupDir();

    Files.createDirectories(newKeyDir.toPath());
    Files.createDirectories(newCertDir.toPath());
    KeyPair keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    KeyCodec newKeyCodec = new KeyCodec(dnSecurityConfig, newKeyDir.toPath());
    newKeyCodec.writeKey(keyPair);

    X509Certificate cert = KeyStoreTestUtil.generateCertificate(
        "CN=OzoneMaster", keyPair, 30, "SHA256withRSA");
    certCodec = new CertificateCodec(dnSecurityConfig,
        newCertDir.toPath());
    dnCertClient.storeCertificate(getPEMEncodedString(cert), true, false, false,
        certCodec, false);
    // a success renew after auto cleanup new key and cert dir
    dnCertClient.renewAndStoreKeyAndCertificate(true);
  }
}