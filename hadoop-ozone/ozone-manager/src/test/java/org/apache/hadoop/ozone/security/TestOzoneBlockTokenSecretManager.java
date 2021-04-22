/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.security;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.OMCertificateClient;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.security.KeyPair;
import java.security.Signature;
import java.security.cert.X509Certificate;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

/**
 * Test class for {@link OzoneBlockTokenSecretManager}.
 */
public class TestOzoneBlockTokenSecretManager {

  private OzoneBlockTokenSecretManager secretManager;
  private String omCertSerialId;
  private CertificateClient client;
  private static final String BASEDIR = GenericTestUtils
      .getTempPath(TestOzoneBlockTokenSecretManager.class.getSimpleName());
  private static final String ALGORITHM = "SHA256withRSA";

  @Before
  public void setUp() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, BASEDIR);
    conf.setBoolean(HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED, true);
    // Create Ozone Master key pair.
    KeyPair keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    // Create Ozone Master certificate (SCM CA issued cert) and key store.
    SecurityConfig securityConfig = new SecurityConfig(conf);
    X509Certificate x509Certificate = KeyStoreTestUtil
        .generateCertificate("CN=OzoneMaster", keyPair, 30, ALGORITHM);
    omCertSerialId = x509Certificate.getSerialNumber().toString();
    secretManager = new OzoneBlockTokenSecretManager(securityConfig,
        TimeUnit.HOURS.toMillis(1), omCertSerialId);
    client = Mockito.mock(OMCertificateClient.class);
    Mockito.when(client.getCertificate()).thenReturn(x509Certificate);
    Mockito.when(client.getCertificate(Mockito.anyString())).
        thenReturn(x509Certificate);
    Mockito.when(client.getPublicKey()).thenReturn(keyPair.getPublic());
    Mockito.when(client.getPrivateKey()).thenReturn(keyPair.getPrivate());
    Mockito.when(client.getSignatureAlgorithm()).thenReturn(
        securityConfig.getSignatureAlgo());
    Mockito.when(client.getSecurityProvider()).thenReturn(
        securityConfig.getProvider());
    Mockito.when(client.verifySignature((byte[]) Mockito.any(),
        Mockito.any(), Mockito.any())).thenCallRealMethod();

    secretManager.start(client);
  }

  @After
  public void tearDown() {
    secretManager = null;
  }

  @Test
  public void testGenerateToken() throws Exception {
    Token<OzoneBlockTokenIdentifier> token = secretManager.generateToken(
        "101", EnumSet.allOf(AccessModeProto.class), 100);
    OzoneBlockTokenIdentifier identifier =
        OzoneBlockTokenIdentifier.readFieldsProtobuf(new DataInputStream(
            new ByteArrayInputStream(token.getIdentifier())));
    // Check basic details.
    Assert.assertEquals("101", identifier.getService());
    Assert.assertEquals(EnumSet.allOf(AccessModeProto.class),
        identifier.getAccessModes());
    Assert.assertEquals(omCertSerialId, identifier.getCertSerialId());

    validateHash(token.getPassword(), token.getIdentifier());
  }

  @Test
  public void testCreateIdentifierSuccess() throws Exception {
    OzoneBlockTokenIdentifier btIdentifier = secretManager.createIdentifier(
        "testUser", "101", EnumSet.allOf(AccessModeProto.class), 100);

    // Check basic details.
    Assert.assertEquals("testUser", btIdentifier.getOwnerId());
    Assert.assertEquals("101", btIdentifier.getService());
    Assert.assertEquals(EnumSet.allOf(AccessModeProto.class),
        btIdentifier.getAccessModes());
    Assert.assertEquals(omCertSerialId, btIdentifier.getCertSerialId());

    byte[] hash = secretManager.createPassword(btIdentifier);
    validateHash(hash, btIdentifier.getBytes());
  }

  /**
   * Validate hash using public key of KeyPair.
   * */
  private void validateHash(byte[] hash, byte[] identifier) throws Exception {
    Signature rsaSignature =
        Signature.getInstance(secretManager.getDefaultSignatureAlgorithm());
    rsaSignature.initVerify(client.getPublicKey());
    rsaSignature.update(identifier);
    Assert.assertTrue(rsaSignature.verify(hash));
  }

  @Test
  @SuppressWarnings("java:S2699")
  public void testCreateIdentifierFailure() throws Exception {
    LambdaTestUtils.intercept(SecurityException.class,
        "Ozone block token can't be created without owner and access mode "
            + "information.", () -> {
          secretManager.createIdentifier();
        });
  }

  @Test
  @SuppressWarnings("java:S2699")
  public void testRenewToken() throws Exception {
    LambdaTestUtils.intercept(UnsupportedOperationException.class,
        "Renew token operation is not supported for ozone block" +
            " tokens.", () -> {
          secretManager.renewToken(null, null);
        });
  }

  @Test
  @SuppressWarnings("java:S2699")
  public void testCancelToken() throws Exception {
    LambdaTestUtils.intercept(UnsupportedOperationException.class,
        "Cancel token operation is not supported for ozone block" +
            " tokens.", () -> {
          secretManager.cancelToken(null, null);
        });
  }

  @Test
  @SuppressWarnings("java:S2699")
  public void testVerifySignatureFailure() throws Exception {
    OzoneBlockTokenIdentifier id = new OzoneBlockTokenIdentifier(
        "testUser", "4234", EnumSet.allOf(AccessModeProto.class),
        Time.now() + 60 * 60 * 24, "123444", 1024);
    LambdaTestUtils.intercept(UnsupportedOperationException.class, "operation" +
            " is not supported for block tokens",
        () -> secretManager.verifySignature(id,
            client.signData(id.getBytes())));
  }
}