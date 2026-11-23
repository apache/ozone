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

package org.apache.hadoop.ozone;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_ACK_TIMEOUT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_CHECK_INTERNAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_GRACE_DURATION_TOKEN_CHECKS_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_RENEW_GRACE_DURATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.CertificateExpiredException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.DNCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificate.utils.SelfSignedCertificate;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyStorage;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test class for {@link HddsDatanodeService}.
 */
public class TestHddsSecureDatanodeInit {
  @TempDir
  private static File testDir;
  private static HddsDatanodeService service;
  private static String[] args = new String[]{};
  private static PrivateKey privateKey;
  private static PublicKey publicKey;
  private static LogCapturer dnLogs;
  private static SecurityConfig securityConfig;
  private static KeyStorage keyStorage;
  private static CertificateCodec certCodec;
  private static X509Certificate cert;
  private static final String DN_COMPONENT = DNCertificateClient.COMPONENT_NAME;
  private static final int CERT_LIFETIME = 15; // seconds
  // Addresses for the SAN extension and subject of the CSRs to avoid real resolving.
  private static final List<InetAddress> CSR_INET_ADDRESSES =
      Collections.singletonList(mockInetAddress("192.0.2.10", "dn1.example.com"));

  private DNCertificateClient client;
  private static DatanodeDetails datanodeDetails;
  private static SCMSecurityProtocolClientSideTranslatorPB scmClient;

  @BeforeAll
  public static void setUp() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getPath());
    //conf.set(ScmConfigKeys.OZONE_SCM_NAMES, "localhost");
    String volumeDir = testDir + "/disk1";
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, volumeDir);

    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    conf.setClass(OzoneConfigKeys.HDDS_DATANODE_PLUGINS_KEY,
        TestHddsDatanodeService.MockService.class,
        ServicePlugin.class);
    conf.set(HDDS_X509_RENEW_GRACE_DURATION, "PT5S"); // 5s
    conf.set(HDDS_X509_CA_ROTATION_CHECK_INTERNAL, "PT1S"); // 1s
    conf.setBoolean(HDDS_X509_GRACE_DURATION_TOKEN_CHECKS_ENABLED, false);
    conf.set(HDDS_X509_CA_ROTATION_ACK_TIMEOUT, "PT1S"); // 1s
    securityConfig = new SecurityConfig(conf);

    service = new HddsDatanodeService(args) {
      @Override
      SCMSecurityProtocolClientSideTranslatorPB createScmSecurityClient()
          throws IOException {
        return mock(SCMSecurityProtocolClientSideTranslatorPB.class);
      }
    };
    dnLogs = LogCapturer.captureLogs(DNCertificateClient.class);
    certCodec = new CertificateCodec(securityConfig, DN_COMPONENT);
    keyStorage = new KeyStorage(securityConfig, DN_COMPONENT);
    dnLogs.clearOutput();
    KeyPair keyPair = new HDDSKeyGenerator(securityConfig).generateKey();
    privateKey = keyPair.getPrivate();
    publicKey = keyPair.getPublic();

    cert = generateX509Cert(new KeyPair(publicKey, privateKey),
        null, Duration.ofSeconds(CERT_LIFETIME));
    datanodeDetails = MockDatanodeDetails.randomDatanodeDetails();

    scmClient = mock(SCMSecurityProtocolClientSideTranslatorPB.class);
  }

  @BeforeEach
  public void setUpDNCertClient() throws IOException {

    FileUtils.deleteQuietly(Paths.get(
        securityConfig.getKeyLocation(DN_COMPONENT).toString(),
        securityConfig.getPrivateKeyFileName()).toFile());
    FileUtils.deleteQuietly(Paths.get(
        securityConfig.getKeyLocation(DN_COMPONENT).toString(),
        securityConfig.getPublicKeyFileName()).toFile());
    FileUtils.deleteQuietly(Paths.get(securityConfig
        .getCertificateLocation(DN_COMPONENT).toString(),
        securityConfig.getCertificateFileName()).toFile());
    dnLogs.clearOutput();
    client = newDnCertificateClient(cert.getSerialNumber().toString());
  }

  @AfterEach
  public void tearDownClient() throws IOException {
    client.close();
  }

  /**
   * A client with the scheduled renewer off, so it cannot renew while the test re-stubs the SCM mock.
   * <p>
   * This is what HDDS-8873 fixes: the rotation tests used to start the scheduled CertificateRenewerService twice
   * (lazily from getCertificate() -> loadAllCertificates(), then explicitly) on a background thread that called the
   * shared Mockito SCM client while the test thread was still re-stubbing it, and then waited with Thread.sleep and
   * waitFor ceilings derived from the {@value #CERT_LIFETIME}-second certificate minted in {@code @BeforeAll}.
   * The tests now call {@link #renewCertificateNow()} on the test thread instead. Because renewal only proceeds for a
   * certificate that is already inside its grace period, each test first installs an in-grace (or expired)
   * certificate with {@link #useCertificate(X509Certificate)} and rebuilds the client with that serial id.
   */
  private static DNCertificateClient newDnCertificateClient(String certSerialId) {
    return new DNCertificateClient(securityConfig, scmClient, datanodeDetails,
        certSerialId, id -> { }, null) {
      @Override
      protected boolean shouldStartCertificateRenewerService() {
        return false;
      }

      @Override
      protected List<InetAddress> getLocalInetAddresses() {
        return CSR_INET_ADDRESSES;
      }

      @Override
      protected String getLocalHostName() {
        return CSR_INET_ADDRESSES.get(0).getCanonicalHostName();
      }
    };
  }

  private static InetAddress mockInetAddress(String ip, String canonicalHostName) {
    InetAddress address = mock(InetAddress.class);
    when(address.getHostAddress()).thenReturn(ip);
    when(address.getCanonicalHostName()).thenReturn(canonicalHostName);
    return address;
  }

  /**
   * Replacement for the scheduled renewer.
   */
  private void renewCertificateNow() {
    client.new CertificateRenewerService(false, () -> { }).run();
  }

  /**
   * Still valid, but expires halfway into the grace period, so renewal is due and stays due.
   */
  private static X509Certificate generateCertInGracePeriod(KeyPair keyPair) throws Exception {
    Duration remainingLifetime = securityConfig.getRenewalGracePeriod().dividedBy(2);
    return generateX509Cert(keyPair, ZonedDateTime.now().plus(remainingLifetime).minusSeconds(CERT_LIFETIME),
        Duration.ofSeconds(CERT_LIFETIME));
  }

  /**
   * Already expired, as the datanode's own is once renewal keeps failing.
   */
  private static X509Certificate generateExpiredCert(KeyPair keyPair) throws Exception {
    return generateX509Cert(keyPair, ZonedDateTime.now().minusSeconds(CERT_LIFETIME),
        Duration.ofSeconds(CERT_LIFETIME));
  }

  /**
   * Makes {@code certificate} the certificate the client currently holds.
   */
  private void useCertificate(X509Certificate certificate) throws IOException {
    certCodec.writeCertificate(certificate);
    client.close();
    client = newDnCertificateClient(certificate.getSerialNumber().toString());
  }

  @Test
  public void testSecureDnStartupCase0() {

    // Case 0: When keypair as well as certificate is missing. Initial keypair
    // boot-up. Get certificate will fail as no SCM is not running.
    assertThrows(Exception.class,
        () -> service.initializeCertificateClient(client));

    assertNotNull(client.getPrivateKey());
    assertNotNull(client.getPublicKey());
    assertNull(client.getCertificate());
    assertThat(dnLogs.getOutput()).contains("Init response: GETCERT");
  }

  @Test
  public void testSecureDnStartupCase1() throws Exception {
    // Case 1: When only certificate is present.
    certCodec.writeCertificate(cert);
    RuntimeException rteException = assertThrows(
        RuntimeException.class,
        () -> service.initializeCertificateClient(client));
    assertThat(rteException.getMessage())
        .contains("DN security initialization failed");
    assertNull(client.getPrivateKey());
    assertNull(client.getPublicKey());
    assertNotNull(client.getCertificate());
    assertThat(dnLogs.getOutput())
        .contains("Init response: FAILURE");
  }

  @Test
  public void testSecureDnStartupCase2() throws Exception {
    // Case 2: When private key and certificate is missing.
    keyStorage.storePublicKey(publicKey);
    RuntimeException rteException = assertThrows(
        RuntimeException.class,
        () -> service.initializeCertificateClient(client));
    assertThat(rteException.getMessage())
        .contains("DN security initialization failed");
    assertNull(client.getPrivateKey());
    assertNotNull(client.getPublicKey());
    assertNull(client.getCertificate());
    assertThat(dnLogs.getOutput())
        .contains("Init response: FAILURE");
  }

  @Test
  public void testSecureDnStartupCase3() throws Exception {
    // Case 3: When only public key and certificate is present.
    keyStorage.storePublicKey(publicKey);
    certCodec.writeCertificate(cert);
    RuntimeException rteException = assertThrows(
        RuntimeException.class,
        () -> service.initializeCertificateClient(client));
    assertThat(rteException.getMessage())
        .contains("DN security initialization failed");
    assertNull(client.getPrivateKey());
    assertNotNull(client.getPublicKey());
    assertNotNull(client.getCertificate());
    assertThat(dnLogs.getOutput())
        .contains("Init response: FAILURE");
  }

  @Test
  public void testSecureDnStartupCase4() throws Exception {
    // Case 4: When public key as well as certificate is missing.
    keyStorage.storePrivateKey(privateKey);
    // provide a new valid SCMGetCertResponseProto
    X509Certificate newCert = generateX509Cert(null, null, Duration.ofSeconds(CERT_LIFETIME));
    String pemCert = CertificateCodec.getPEMEncodedString(newCert);
    // provide an invalid SCMGetCertResponseProto. Without
    // setX509CACertificate(pemCert), signAndStoreCert will throw exception.
    SCMSecurityProtocolProtos.SCMGetCertResponseProto responseProto =
        SCMSecurityProtocolProtos.SCMGetCertResponseProto
            .newBuilder().setResponseCode(SCMSecurityProtocolProtos
                .SCMGetCertResponseProto.ResponseCode.success)
            .setX509Certificate(pemCert)
            .setX509CACertificate(pemCert)
            .build();
    when(scmClient.getDataNodeCertificateChain(any(), anyString()))
        .thenReturn(responseProto);
    service.initializeCertificateClient(client);
    assertNotNull(client.getPrivateKey());
    assertNotNull(client.getPublicKey());
    assertNotNull(client.getCertificate());
    assertThat(dnLogs.getOutput())
        .contains("Init response: GETCERT");
    dnLogs.clearOutput();
    // reset scmClient behavior
    when(scmClient.getDataNodeCertificateChain(any(), anyString()))
        .thenReturn(null);
  }

  @Test
  public void testSecureDnStartupCase5() throws Exception {
    // Case 5: If private key and certificate is present.
    certCodec.writeCertificate(cert);
    keyStorage.storePrivateKey(privateKey);
    service.initializeCertificateClient(client);
    assertNotNull(client.getPrivateKey());
    assertNotNull(client.getPublicKey());
    assertNotNull(client.getCertificate());
    assertThat(dnLogs.getOutput())
        .contains("Init response: SUCCESS");
  }

  @Test
  public void testSecureDnStartupCase6() throws Exception {
    // Case 6: If key pair already exist than response should be GETCERT.
    keyStorage.storePublicKey(publicKey);
    keyStorage.storePrivateKey(privateKey);
    assertThrows(Exception.class,
        () -> service.initializeCertificateClient(client));
    assertNotNull(client.getPrivateKey());
    assertNotNull(client.getPublicKey());
    assertNull(client.getCertificate());
    assertThat(dnLogs.getOutput())
        .contains("Init response: GETCERT");
  }

  @Test
  public void testSecureDnStartupCase7() throws Exception {
    // Case 7 When keypair and certificate is present.
    keyStorage.storePublicKey(publicKey);
    keyStorage.storePrivateKey(privateKey);
    certCodec.writeCertificate(cert);

    service.initializeCertificateClient(client);
    assertNotNull(client.getPrivateKey());
    assertNotNull(client.getPublicKey());
    assertNotNull(client.getCertificate());
    assertThat(dnLogs.getOutput())
        .contains("Init response: SUCCESS");
  }

  @Test
  public void testCertificateRotation() throws Exception {
    // the dn holds a valid cert that entered its grace period
    useCertificate(generateCertInGracePeriod(new KeyPair(publicKey, privateKey)));

    // in its grace period too, so the second rotation below is due
    X509Certificate newCert = generateCertInGracePeriod(null);
    String pemCert = CertificateCodec.getPEMEncodedString(newCert);
    SCMSecurityProtocolProtos.SCMGetCertResponseProto responseProto =
        SCMSecurityProtocolProtos.SCMGetCertResponseProto
            .newBuilder().setResponseCode(SCMSecurityProtocolProtos
                .SCMGetCertResponseProto.ResponseCode.success)
            .setX509Certificate(pemCert)
            .setX509CACertificate(pemCert)
            .setX509RootCACertificate(pemCert)
            .build();
    when(scmClient.getDataNodeCertificateChain(any(), anyString()))
        .thenReturn(responseProto);

    List<String> rootCaList = new ArrayList<>();
    rootCaList.add(pemCert);
    when(scmClient.getAllRootCaCertificates()).thenReturn(rootCaList);
    // check that new cert ID should not equal to current cert ID
    String certId = newCert.getSerialNumber().toString();
    assertNotEquals(certId, client.getCertificate().getSerialNumber().toString());

    // renew key and cert
    renewCertificateNow();

    // after renew, client has the new cert ID
    assertEquals(certId, client.getCertificate().getSerialNumber().toString());
    PrivateKey privateKey1 = client.getPrivateKey();
    PublicKey publicKey1 = client.getPublicKey();
    String caCertId1 = client.getCACertificate().getSerialNumber().toString();
    String rootCaCertId1 =
        client.getRootCACertificate().getSerialNumber().toString();

    // test the second time certificate rotation. Nothing renews this one, so it gets a full lifetime
    newCert = generateX509Cert(null, null, Duration.ofSeconds(CERT_LIFETIME));
    rootCaList.remove(pemCert);
    pemCert = CertificateCodec.getPEMEncodedString(newCert);
    responseProto = SCMSecurityProtocolProtos.SCMGetCertResponseProto
        .newBuilder().setResponseCode(SCMSecurityProtocolProtos
            .SCMGetCertResponseProto.ResponseCode.success)
        .setX509Certificate(pemCert)
        .setX509CACertificate(pemCert)
        .setX509RootCACertificate(pemCert)
        .build();
    when(scmClient.getDataNodeCertificateChain(any(), anyString()))
        .thenReturn(responseProto);
    rootCaList.add(pemCert);
    when(scmClient.getAllRootCaCertificates()).thenReturn(rootCaList);
    String certId2 = newCert.getSerialNumber().toString();

    renewCertificateNow();

    // after renew, client has the new cert ID
    assertEquals(certId2, client.getCertificate().getSerialNumber().toString());
    assertNotEquals(privateKey1, client.getPrivateKey());
    assertNotEquals(publicKey1, client.getPublicKey());
    assertNotEquals(caCertId1, client.getCACertificate().getSerialNumber().toString());
    assertNotEquals(rootCaCertId1, client.getRootCACertificate().getSerialNumber().toString());
  }

  /**
   * Test unexpected SCMGetCertResponseProto returned from SCM.
   */
  @Test
  public void testCertificateRotationRecoverableFailure() throws Exception {
    // the dn holds an expired cert: renewal has been failing past its expiry
    X509Certificate currentCert = generateExpiredCert(new KeyPair(publicKey, privateKey));
    useCertificate(currentCert);
    String currentCertId = currentCert.getSerialNumber().toString();

    X509Certificate newCert = generateX509Cert(null, null, Duration.ofSeconds(CERT_LIFETIME));
    String pemCert = CertificateCodec.getPEMEncodedString(newCert);
    // provide an invalid SCMGetCertResponseProto. Without
    // setX509CACertificate(pemCert), signAndStoreCert will throw exception.
    SCMSecurityProtocolProtos.SCMGetCertResponseProto responseProto =
        SCMSecurityProtocolProtos.SCMGetCertResponseProto
            .newBuilder().setResponseCode(SCMSecurityProtocolProtos
                .SCMGetCertResponseProto.ResponseCode.success)
            .setX509Certificate(pemCert)
            .build();
    when(scmClient.getDataNodeCertificateChain(any(), anyString()))
        .thenReturn(responseProto);

    // check that new cert ID should not equal to current cert ID
    String certId = newCert.getSerialNumber().toString();
    assertNotEquals(certId, client.getCertificate().getSerialNumber().toString());

    // renewal fails, client still holds the old expired cert.
    renewCertificateNow();
    assertEquals(currentCertId, client.getCertificate().getSerialNumber().toString());
    assertThrows(CertificateExpiredException.class,
        () -> client.getCertificate().checkValidity());

    // provide a new valid SCMGetCertResponseProto
    newCert = generateX509Cert(null, null, Duration.ofSeconds(CERT_LIFETIME));
    pemCert = CertificateCodec.getPEMEncodedString(newCert);
    responseProto = SCMSecurityProtocolProtos.SCMGetCertResponseProto
        .newBuilder().setResponseCode(SCMSecurityProtocolProtos
            .SCMGetCertResponseProto.ResponseCode.success)
        .setX509Certificate(pemCert)
        .setX509CACertificate(pemCert)
        .build();
    when(scmClient.getDataNodeCertificateChain(any(), anyString()))
        .thenReturn(responseProto);
    String certId2 = newCert.getSerialNumber().toString();

    renewCertificateNow();

    // after renew, client has the new cert ID
    assertEquals(certId2, client.getCertificate().getSerialNumber().toString());
  }

  private static X509Certificate generateX509Cert(KeyPair keyPair,
      ZonedDateTime startDate, Duration certLifetime) throws Exception {
    if (keyPair == null) {
      keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    }
    ZonedDateTime start = startDate == null ? ZonedDateTime.now() : startDate;
    ZonedDateTime end = start.plus(certLifetime);
    return SelfSignedCertificate.newBuilder()
        .setBeginDate(start)
        .setEndDate(end)
        .setClusterID("cluster")
        .setKey(keyPair)
        .setSubject("localhost")
        .setConfiguration(securityConfig)
        .setScmID("test")
        .build();
  }
}
