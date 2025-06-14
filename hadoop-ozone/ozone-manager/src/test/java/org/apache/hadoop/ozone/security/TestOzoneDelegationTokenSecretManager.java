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

package org.apache.hadoop.ozone.security;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto.Type.S3AUTHINFO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.CertPath;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.S3SecretCache;
import org.apache.hadoop.ozone.om.S3SecretLockedManager;
import org.apache.hadoop.ozone.om.S3SecretManager;
import org.apache.hadoop.ozone.om.S3SecretManagerImpl;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.event.Level;

/**
 * Test class for {@link OzoneDelegationTokenSecretManager}.
 */
public class TestOzoneDelegationTokenSecretManager {

  private static final long TOKEN_MAX_LIFETIME = 1000 * 20;
  private static final long TOKEN_REMOVER_SCAN_INTERVAL = 1000 * 20;

  private OzoneManager om;
  private OzoneDelegationTokenSecretManager secretManager;
  private SecurityConfig securityConfig;
  private OMCertificateClient certificateClient;
  private SecretKeyClient secretKeyClient;
  private long expiryTime;
  private Text serviceRpcAdd;
  private OzoneConfiguration conf;
  private static final Text TEST_USER = new Text("testUser");
  private S3SecretManager s3SecretManager;

  @TempDir
  private Path folder;

  @BeforeEach
  public void setUp() throws Exception {
    conf = createNewTestPath();
    securityConfig = new SecurityConfig(conf);
    certificateClient = setupCertificateClient();
    certificateClient.init();
    secretKeyClient = new SecretKeyTestClient();
    expiryTime = Time.monotonicNow() + 60 * 60 * 24;
    serviceRpcAdd = new Text("localhost");
    final Map<String, S3SecretValue> s3Secrets = new HashMap<>();
    s3Secrets.put("testuser1",
        S3SecretValue.of("testuser1", "dbaksbzljandlkandlsd"));
    s3Secrets.put("abc",
        S3SecretValue.of("abc", "djakjahkd"));
    om = mock(OzoneManager.class);
    OMMetadataManager metadataManager = new OmMetadataManagerImpl(conf, om);
    when(om.getMetadataManager()).thenReturn(metadataManager);
    OMLayoutVersionManager versionManager = mock(OMLayoutVersionManager.class);
    when(versionManager.isAllowed(any(LayoutFeature.class))).thenReturn(true);
    when(om.getVersionManager()).thenReturn(versionManager);
    s3SecretManager = new S3SecretLockedManager(
        new S3SecretManagerImpl(new S3SecretStoreMap(s3Secrets),
            mock(S3SecretCache.class)),
        metadataManager.getLock()
    );
  }

  private OzoneConfiguration createNewTestPath() throws IOException {
    OzoneConfiguration config = new OzoneConfiguration();
    File newFolder = folder.toFile();
    if (!newFolder.exists()) {
      assertTrue(newFolder.mkdirs());
    }
    ServerUtils.setOzoneMetaDirPath(config, newFolder.toString());
    return config;
  }

  /**
   * Helper function to create certificate client.
   * */
  private OMCertificateClient setupCertificateClient() throws Exception {
    KeyPair keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    CertificateFactory fact = CertificateCodec.getCertFactory();
    X509Certificate singleCert = KeyStoreTestUtil.generateCertificate("CN=OzoneMaster", keyPair, 30, "SHA256withRSA");
    CertPath certPath = fact.generateCertPath(ImmutableList.of(singleCert));

    OMStorage omStorage = mock(OMStorage.class);
    when(omStorage.getOmCertSerialId()).thenReturn(null);
    when(omStorage.getClusterID()).thenReturn("test");
    when(omStorage.getOmId()).thenReturn(UUID.randomUUID().toString());
    return new OMCertificateClient(
        securityConfig, null, omStorage, null, "", null, null, null) {
      @Override
      public CertPath getCertPath() {
        return certPath;
      }

      @Override
      public PrivateKey getPrivateKey() {
        return keyPair.getPrivate();
      }

      @Override
      public PublicKey getPublicKey() {
        return keyPair.getPublic();
      }

      @Override
      public X509Certificate getCertificate(String serialId) {
        return CertificateCodec.firstCertificateFrom(certPath);
      }
    };
  }

  @AfterEach
  public void tearDown() throws IOException {
    secretManager.stop();
    certificateClient.close();
  }

  @Test
  public void testLeadershipCheckinRetrievePassword() throws Exception {
    secretManager = createSecretManager(conf, TOKEN_MAX_LIFETIME,
        expiryTime, TOKEN_REMOVER_SCAN_INTERVAL);
    doThrow(new OMNotLeaderException(RaftPeerId.valueOf("om")))
        .when(om).checkLeaderStatus();
    OzoneTokenIdentifier identifier = new OzoneTokenIdentifier();
    try {
      secretManager.retrievePassword(identifier);
    } catch (Exception e) {
      assertEquals(SecretManager.InvalidToken.class, e.getClass());
      assertEquals(OMNotLeaderException.class,
          e.getCause().getClass());
    }

    doThrow(new OMLeaderNotReadyException("Leader not ready"))
        .when(om).checkLeaderStatus();
    try {
      secretManager.retrievePassword(identifier);
    } catch (Exception e) {
      assertEquals(SecretManager.InvalidToken.class, e.getClass());
      assertEquals(OMLeaderNotReadyException.class,
          e.getCause().getClass());
    }

    doNothing().when(om).checkLeaderStatus();
    try {
      secretManager.retrievePassword(identifier);
    } catch (Exception e) {
      assertEquals(SecretManager.InvalidToken.class, e.getClass());
      assertNull(e.getCause());
    }
  }

  @Test
  public void testCreateToken() throws Exception {
    secretManager = createSecretManager(conf, TOKEN_MAX_LIFETIME,
        expiryTime, TOKEN_REMOVER_SCAN_INTERVAL);
    secretManager.start(certificateClient);
    Token<OzoneTokenIdentifier> token = secretManager.createToken(TEST_USER,
        TEST_USER, TEST_USER);
    OzoneTokenIdentifier identifier =
        OzoneTokenIdentifier.readProtoBuf(token.getIdentifier());
    // Check basic details.
    assertEquals(identifier.getRealUser(), TEST_USER);
    assertEquals(identifier.getRenewer(), TEST_USER);
    assertEquals(identifier.getOwner(), TEST_USER);

    validateHash(token.getPassword(), token.getIdentifier());
  }

  @Test
  public void testExpiredSecretKey() throws Exception {
    SecretKeyClient old = secretKeyClient;
    secretKeyClient = spy(secretKeyClient);
    doReturn(null).when(secretKeyClient).getSecretKey(any());

    Text tester = new Text("tester");
    OzoneTokenIdentifier identifier =
        new OzoneTokenIdentifier(tester, tester, tester);
    identifier.setSecretKeyId(UUID.randomUUID().toString());
    identifier.setOmServiceId(OzoneConsts.OM_SERVICE_ID_DEFAULT);

    // case 1: Secret key not found, and delegation token is valid.
    om.getMetadataManager().getDelegationTokenTable().put(identifier, Long.MAX_VALUE);
    try {
      secretManager = createSecretManager(conf, TOKEN_MAX_LIFETIME,
          expiryTime, TOKEN_REMOVER_SCAN_INTERVAL);
      om.getMetadataManager().getDelegationTokenTable().delete(identifier);

      // case 2: Secret key not found, and delegation token is expired.
      OzoneTokenIdentifier identifier2 =
          new OzoneTokenIdentifier(tester, tester, tester);
      identifier2.setSecretKeyId(UUID.randomUUID().toString());
      identifier2.setOmServiceId(OzoneConsts.OM_SERVICE_ID_DEFAULT);

      om.getMetadataManager().getDelegationTokenTable().put(identifier2, Time.now() - 1);
      secretManager = createSecretManager(conf, TOKEN_MAX_LIFETIME,
          expiryTime, TOKEN_REMOVER_SCAN_INTERVAL);
      // expired token should be removed from the table.
      assertFalse(om.getMetadataManager().getDelegationTokenTable().isExist(identifier2),
          "Expired token " + identifier2 + " should be removed from the table");
    } finally {
      verify(secretKeyClient, times(2)).getSecretKey(any());
      secretKeyClient = old;
    }
  }

  private void restartSecretManager() throws IOException {
    secretManager.stop();
    secretManager = null;
    secretManager = createSecretManager(conf, TOKEN_MAX_LIFETIME,
        expiryTime, TOKEN_REMOVER_SCAN_INTERVAL);
  }

  private void testRenewTokenSuccessHelper(boolean restartSecretManager)
      throws Exception {
    secretManager = createSecretManager(conf, TOKEN_MAX_LIFETIME,
        expiryTime, TOKEN_REMOVER_SCAN_INTERVAL);
    secretManager.start(certificateClient);
    Token<OzoneTokenIdentifier> token = secretManager.createToken(TEST_USER,
        TEST_USER,
        TEST_USER);
    addToTokenStore(token);
    Thread.sleep(10 * 5);

    if (restartSecretManager) {
      restartSecretManager();
    }

    long renewalTime = secretManager.renewToken(token, TEST_USER.toString());
    addToTokenStore(token);
    assertThat(renewalTime).isGreaterThan(0);
  }

  @Test
  public void testReloadAndRenewToken() throws Exception {
    testRenewTokenSuccessHelper(true);
  }

  @Test
  public void testRenewTokenSuccess() throws Exception {
    testRenewTokenSuccessHelper(false);
  }

  /**
   * Tests failure for mismatch in renewer.
   */
  @Test
  public void testRenewTokenFailure() throws Exception {
    secretManager = createSecretManager(conf, TOKEN_MAX_LIFETIME,
        expiryTime, TOKEN_REMOVER_SCAN_INTERVAL);
    secretManager.start(certificateClient);
    Token<OzoneTokenIdentifier> token = secretManager.createToken(TEST_USER,
        TEST_USER, TEST_USER);
    addToTokenStore(token);
    AccessControlException exception =
        assertThrows(AccessControlException.class,
            () -> secretManager.renewToken(token, "rougeUser"));
    assertTrue(exception.getMessage()
        .startsWith("rougeUser tries to renew a token"));
  }

  /**
   * Tests token renew failure due to max time.
   */
  @Test
  public void testRenewTokenFailureMaxTime() throws Exception {
    secretManager = createSecretManager(conf, 100,
        100, TOKEN_REMOVER_SCAN_INTERVAL);
    secretManager.start(certificateClient);
    Token<OzoneTokenIdentifier> token = secretManager.createToken(TEST_USER,
        TEST_USER,
        TEST_USER);
    Thread.sleep(101);

    IOException ioException =
        assertThrows(IOException.class,
            () -> secretManager.renewToken(token, TEST_USER.toString()));
    assertTrue(ioException.getMessage()
        .startsWith("testUser tried to renew an expired token"));
  }

  /**
   * Tests token renew failure due to renewal time.
   */
  @Test
  public void testRenewTokenFailureRenewalTime() throws Exception {
    secretManager = createSecretManager(conf, 1000 * 10,
        10, TOKEN_REMOVER_SCAN_INTERVAL);
    secretManager.start(certificateClient);
    Token<OzoneTokenIdentifier> token = secretManager.createToken(TEST_USER,
        TEST_USER,
        TEST_USER);
    Thread.sleep(15);

    IOException ioException =
        assertThrows(IOException.class,
            () -> secretManager.renewToken(token, TEST_USER.toString()));
    String errorMessage = ioException.getMessage();
    assertTrue(errorMessage.contains("is expired") || errorMessage.contains("can't be found in cache"),
        "\nExpecting:\n" + errorMessage + "\n to contain \"is expired\" or \"can't be found in cache\"");
  }

  @Test
  public void testCreateIdentifier() throws Exception {
    secretManager = createSecretManager(conf, TOKEN_MAX_LIFETIME,
        expiryTime, TOKEN_REMOVER_SCAN_INTERVAL);
    secretManager.start(certificateClient);
    OzoneTokenIdentifier identifier = secretManager.createIdentifier();
    // Check basic details.
    assertEquals(identifier.getOwner(), new Text(""));
    assertEquals(identifier.getRealUser(), new Text(""));
    assertEquals(identifier.getRenewer(), new Text(""));
  }

  @Test
  public void testCancelTokenSuccess() throws Exception {
    secretManager = createSecretManager(conf, TOKEN_MAX_LIFETIME,
        expiryTime, TOKEN_REMOVER_SCAN_INTERVAL);
    secretManager.start(certificateClient);
    Token<OzoneTokenIdentifier> token = secretManager.createToken(TEST_USER,
        TEST_USER, TEST_USER);
    addToTokenStore(token);
    secretManager.cancelToken(token, TEST_USER.toString());
  }

  @Test
  public void testCancelTokenFailure() throws Exception {
    secretManager = createSecretManager(conf, TOKEN_MAX_LIFETIME,
        expiryTime, TOKEN_REMOVER_SCAN_INTERVAL);
    secretManager.start(certificateClient);
    Token<OzoneTokenIdentifier> token = secretManager.createToken(TEST_USER,
        TEST_USER,
        TEST_USER);

    AccessControlException exception =
        assertThrows(AccessControlException.class,
            () -> secretManager.cancelToken(token, "rougeUser"));
    assertTrue(exception.getMessage()
        .startsWith("rougeUser is not authorized to cancel the token"));
  }

  @Test
  public void testVerifySignatureSuccess() throws Exception {
    secretManager = createSecretManager(conf, TOKEN_MAX_LIFETIME,
        expiryTime, TOKEN_REMOVER_SCAN_INTERVAL);
    secretManager.start(certificateClient);
    OzoneTokenIdentifier id = new OzoneTokenIdentifier();
    id.setMaxDate(Time.now() + 60 * 60 * 24);
    id.setOwner(new Text("test"));
    id.setSecretKeyId(secretKeyClient.getCurrentSecretKey().getId().toString());
    assertTrue(secretManager.verifySignature(id, secretKeyClient.getCurrentSecretKey().sign(id.getBytes())));
  }

  @Test
  public void testVerifyAsymmetricSignatureSuccess() throws Exception {
    GenericTestUtils.setLogLevel(OzoneDelegationTokenSecretManager.class, Level.DEBUG);
    LogCapturer logCapturer = LogCapturer.captureLogs(OzoneDelegationTokenSecretManager.class);
    secretManager = createSecretManager(conf, TOKEN_MAX_LIFETIME,
        expiryTime, TOKEN_REMOVER_SCAN_INTERVAL);
    secretManager.start(certificateClient);
    OzoneTokenIdentifier id = new OzoneTokenIdentifier();
    id.setOmCertSerialId(certificateClient.getCertificate()
        .getSerialNumber().toString());
    id.setMaxDate(Time.now() + 60 * 60 * 24);
    id.setOwner(new Text("test"));
    assertTrue(secretManager.verifySignature(id, certificateClient.signData(id.getBytes())));
    assertTrue(logCapturer.getOutput().contains("Verify an asymmetric key signed Token"));
    logCapturer.stopCapturing();
  }

  @Test
  public void testVerifySignatureFailure() throws Exception {
    secretManager = createSecretManager(conf, TOKEN_MAX_LIFETIME,
        expiryTime, TOKEN_REMOVER_SCAN_INTERVAL);
    secretManager.start(certificateClient);
    OzoneTokenIdentifier id = new OzoneTokenIdentifier();
    // set invalid om cert serial id
    id.setOmCertSerialId("1927393");
    id.setMaxDate(Time.now() + 60 * 60 * 24);
    id.setOwner(new Text("test"));
    assertFalse(secretManager.verifySignature(id, id.getBytes()));
  }

  @Test
  public void testValidateS3AUTHINFOSuccess() throws Exception {
    secretManager = createSecretManager(conf, TOKEN_MAX_LIFETIME,
        expiryTime, TOKEN_REMOVER_SCAN_INTERVAL);
    secretManager.start(certificateClient);

    OzoneTokenIdentifier identifier = new OzoneTokenIdentifier();
    identifier.setTokenType(S3AUTHINFO);
    identifier.setSignature("56ec73ba1974f8feda8365c3caef89c5d4a688d" +
        "5f9baccf4765f46a14cd745ad");
    identifier.setStrToSign("AWS4-HMAC-SHA256\n" +
        "20190221T002037Z\n" +
        "20190221/us-west-1/s3/aws4_request\n" +
        "c297c080cce4e0927779823d3fd1f5cae71481a8f7dfc7e18d91851294efc47d");
    identifier.setAwsAccessId("testuser1");
    identifier.setOwner(new Text("testuser1"));
    secretManager.retrievePassword(identifier);
  }

  @Test
  public void testValidateS3AUTHINFOFailure() throws Exception {
    secretManager = createSecretManager(conf, TOKEN_MAX_LIFETIME,
        expiryTime, TOKEN_REMOVER_SCAN_INTERVAL);
    secretManager.start(certificateClient);

    OzoneTokenIdentifier identifier = new OzoneTokenIdentifier();
    identifier.setTokenType(S3AUTHINFO);
    identifier.setSignature("56ec73ba1974f8feda8365c3caef89c5d4a688d" +
        "5f9baccf4765f46a14cd745ad");
    identifier.setStrToSign("AWS4-HMAC-SHA256\n" +
        "20190221T002037Z\n" +
        "20190221/us-west-1/s3/aws4_request\n" +
        "c297c080cce4e0927779823d3fd1f5cae71481a8f7dfc7e18d91851294efc47d");
    identifier.setAwsAccessId("testuser2");
    identifier.setOwner(new Text("testuser2"));
    // Case 1: User don't have aws secret set.
    SecretManager.InvalidToken invalidToken =
        assertThrows(SecretManager.InvalidToken.class,
            () -> secretManager.retrievePassword(identifier));
    assertTrue(invalidToken.getMessage()
        .startsWith("No S3 secret found for S3 identifier"));

    // Case 2: Invalid hash in string to sign.
    identifier.setStrToSign("AWS4-HMAC-SHA256\n" +
        "20190221T002037Z\n" +
        "20190221/us-west-1/s3/aws4_request\n" +
        "c297c080cce4e0927779823d3fd1f5cae71481a8f7dfc7e18d91851294efc47d" +
        "+invalidhash");

    invalidToken = assertThrows(SecretManager.InvalidToken.class,
        () -> secretManager.retrievePassword(identifier));
    assertTrue(invalidToken.getMessage()
        .startsWith("No S3 secret found for S3 identifier"));

    // Case 3: Invalid hash in authorization hmac.
    identifier.setSignature("56ec73ba1974f8feda8365c3caef89c5d4a688d" +
        "+invalidhash" + "5f9baccf4765f46a14cd745ad");
    identifier.setStrToSign("AWS4-HMAC-SHA256\n" +
        "20190221T002037Z\n" +
        "20190221/us-west-1/s3/aws4_request\n" +
        "c297c080cce4e0927779823d3fd1f5cae71481a8f7dfc7e18d91851294efc47d");

    invalidToken = assertThrows(SecretManager.InvalidToken.class,
        () -> secretManager.retrievePassword(identifier));
    assertTrue(invalidToken.getMessage()
        .startsWith("No S3 secret found for S3 identifier"));
  }

  /**
   * Validate hash using public key of KeyPair.
   */
  private void validateHash(byte[] hash, byte[] identifier) throws Exception {
    OzoneTokenIdentifier ozoneTokenIdentifier = OzoneTokenIdentifier.readProtoBuf(identifier);
    ManagedSecretKey verifyKey = secretKeyClient.getSecretKey(UUID.fromString(ozoneTokenIdentifier.getSecretKeyId()));
    verifyKey.isValidSignature(identifier, hash);
  }

  /**
   * Create instance of {@link OzoneDelegationTokenSecretManager}.
   */
  private OzoneDelegationTokenSecretManager
      createSecretManager(OzoneConfiguration config, long tokenMaxLife,
      long expiry, long tokenRemoverScanTime) throws IOException {
    return new OzoneDelegationTokenSecretManager.Builder()
        .setConf(config)
        .setTokenMaxLifetime(tokenMaxLife)
        .setTokenRenewInterval(expiry)
        .setTokenRemoverScanInterval(tokenRemoverScanTime)
        .setService(serviceRpcAdd)
        .setOzoneManager(om)
        .setS3SecretManager(s3SecretManager)
        .setCertificateClient(certificateClient)
        .setOmServiceId(OzoneConsts.OM_SERVICE_ID_DEFAULT)
        .setSecretKeyClient(secretKeyClient)
        .build();
  }

  private void addToTokenStore(Token<OzoneTokenIdentifier> token) throws IOException {
    OzoneTokenIdentifier ozoneTokenIdentifier = OzoneTokenIdentifier.
        readProtoBuf(token.getIdentifier());
    long renewDate = secretManager.updateToken(token, ozoneTokenIdentifier, expiryTime);
    om.getMetadataManager().getDelegationTokenTable().put(ozoneTokenIdentifier, renewDate);
  }
}
