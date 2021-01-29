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
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto;
import org.apache.hadoop.hdds.security.token.BlockTokenException;
import org.apache.hadoop.hdds.security.token.BlockTokenVerifier;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.OMCertificateClient;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v1CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Date;
import java.util.EnumSet;

/**
 * Test class for {@link OzoneBlockTokenSecretManager}.
 */
public class TestOzoneBlockTokenSecretManager {

  private OzoneBlockTokenSecretManager secretManager;
  private KeyPair keyPair;
  private X509Certificate x509Certificate;
  private long expiryTime;
  private String omCertSerialId;
  private CertificateClient client;
  private static final String BASEDIR = GenericTestUtils
      .getTempPath(TestOzoneBlockTokenSecretManager.class.getSimpleName());
  private BlockTokenVerifier tokenVerifier;
  private final static String ALGORITHM = "SHA256withRSA";

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, BASEDIR);
    conf.setBoolean(HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED, true);
    // Create Ozone Master key pair.
    keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    expiryTime = Time.monotonicNow() + 60 * 60 * 24;
    // Create Ozone Master certificate (SCM CA issued cert) and key store.
    SecurityConfig securityConfig = new SecurityConfig(conf);
    x509Certificate = KeyStoreTestUtil
        .generateCertificate("CN=OzoneMaster", keyPair, 30, ALGORITHM);
    omCertSerialId = x509Certificate.getSerialNumber().toString();
    secretManager = new OzoneBlockTokenSecretManager(securityConfig,
        expiryTime, omCertSerialId);
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
    tokenVerifier = new BlockTokenVerifier(securityConfig, client);
  }

  @After
  public void tearDown() throws Exception {
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
    Assert.assertTrue(identifier.getBlockId().equals("101"));
    Assert.assertTrue(identifier.getAccessModes().equals(EnumSet
        .allOf(AccessModeProto.class)));
    Assert.assertTrue(identifier.getOmCertSerialId().equals(omCertSerialId));

    validateHash(token.getPassword(), token.getIdentifier());
  }

  @Test
  public void testCreateIdentifierSuccess() throws Exception {
    OzoneBlockTokenIdentifier btIdentifier = secretManager.createIdentifier(
        "testUser", "101", EnumSet.allOf(AccessModeProto.class), 100);

    // Check basic details.
    Assert.assertTrue(btIdentifier.getOwnerId().equals("testUser"));
    Assert.assertTrue(btIdentifier.getBlockId().equals("101"));
    Assert.assertTrue(btIdentifier.getAccessModes().equals(EnumSet
        .allOf(AccessModeProto.class)));
    Assert.assertTrue(btIdentifier.getOmCertSerialId().equals(omCertSerialId));

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
  public void testCreateIdentifierFailure() throws Exception {
    LambdaTestUtils.intercept(SecurityException.class,
        "Ozone block token can't be created without owner and access mode "
            + "information.", () -> {
          secretManager.createIdentifier();
        });
  }

  @Test
  public void testRenewToken() throws Exception {
    LambdaTestUtils.intercept(UnsupportedOperationException.class,
        "Renew token operation is not supported for ozone block" +
            " tokens.", () -> {
          secretManager.renewToken(null, null);
        });
  }

  @Test
  public void testCancelToken() throws Exception {
    LambdaTestUtils.intercept(UnsupportedOperationException.class,
        "Cancel token operation is not supported for ozone block" +
            " tokens.", () -> {
          secretManager.cancelToken(null, null);
        });
  }

  @Test
  public void testVerifySignatureFailure() throws Exception {
    OzoneBlockTokenIdentifier id = new OzoneBlockTokenIdentifier(
        "testUser", "4234", EnumSet.allOf(AccessModeProto.class),
        Time.now() + 60 * 60 * 24, "123444", 1024);
    LambdaTestUtils.intercept(UnsupportedOperationException.class, "operation" +
            " is not supported for block tokens",
        () -> secretManager.verifySignature(id,
            client.signData(id.getBytes())));
  }

  @Test
  public void testBlockTokenVerifier() throws Exception {
    String tokenBlockID = "101";
    Token<OzoneBlockTokenIdentifier> token =
        secretManager.generateToken("testUser", tokenBlockID,
            EnumSet.allOf(AccessModeProto.class), 100);
    OzoneBlockTokenIdentifier btIdentifier =
        OzoneBlockTokenIdentifier.readFieldsProtobuf(new DataInputStream(
            new ByteArrayInputStream(token.getIdentifier())));

    // Check basic details.
    Assert.assertTrue(btIdentifier.getOwnerId().equals("testUser"));
    Assert.assertTrue(btIdentifier.getBlockId().equals("101"));
    Assert.assertTrue(btIdentifier.getAccessModes().equals(EnumSet
        .allOf(AccessModeProto.class)));
    Assert.assertTrue(btIdentifier.getOmCertSerialId().equals(omCertSerialId));

    validateHash(token.getPassword(), btIdentifier.getBytes());

    tokenVerifier.verify("testUser", token.encodeToUrlString(),
        ContainerProtos.Type.PutBlock, "101");

    String notAllledBlockID = "NotAllowedBlockID";
    LambdaTestUtils.intercept(BlockTokenException.class,
        "Token for block ID: " + tokenBlockID +
        " can't be used to access block: " + notAllledBlockID,
        () -> tokenVerifier.verify("testUser", token.encodeToUrlString(),
            ContainerProtos.Type.PutBlock, notAllledBlockID));

    // Non block operations are not checked by block token verifier
    tokenVerifier.verify(null, null,
        ContainerProtos.Type.CloseContainer, null);
  }

  @Test
  public void testBlockTokenReadAccessMode() throws Exception {
    final String testUser1 = "testUser1";
    final String testBlockId1 = "101";
    Token<OzoneBlockTokenIdentifier> readToken =
        secretManager.generateToken(testUser1, testBlockId1,
            EnumSet.of(AccessModeProto.READ), 100);

    exception.expect(BlockTokenException.class);
    exception.expectMessage("doesn't have WRITE permission");
    tokenVerifier.verify(testUser1, readToken.encodeToUrlString(),
        ContainerProtos.Type.PutBlock, testBlockId1);

    tokenVerifier.verify(testUser1, readToken.encodeToUrlString(),
        ContainerProtos.Type.GetBlock, testBlockId1);
  }

  @Test
  public void testBlockTokenWriteAccessMode() throws Exception {
    final String testUser2 = "testUser2";
    final String testBlockId2 = "102";
    Token<OzoneBlockTokenIdentifier> writeToken =
        secretManager.generateToken("testUser2", testBlockId2,
            EnumSet.of(AccessModeProto.WRITE), 100);

    tokenVerifier.verify(testUser2, writeToken.encodeToUrlString(),
        ContainerProtos.Type.WriteChunk, testBlockId2);

    exception.expect(BlockTokenException.class);
    exception.expectMessage("doesn't have READ permission");
    tokenVerifier.verify(testUser2, writeToken.encodeToUrlString(),
        ContainerProtos.Type.ReadChunk, testBlockId2);
  }

  @Test
  public void testExpiredCertificate() throws Exception {
    String tokenBlockID = "102";
    Token<OzoneBlockTokenIdentifier> blockToken =
        secretManager.generateToken("testUser2", tokenBlockID,
            EnumSet.allOf(AccessModeProto.class), 100);

    // Mock client with an expired cert
    X509Certificate expiredCert = generateExpiredCert(
        "CN=OzoneMaster", keyPair, ALGORITHM);
    Mockito.when(client.getCertificate(Mockito.anyString())).
        thenReturn(expiredCert);

    exception.expect(BlockTokenException.class);
    exception.expectMessage("Block token can't be verified due to expired" +
        " certificate");
    tokenVerifier.verify("testUser", blockToken.encodeToUrlString(),
        ContainerProtos.Type.PutBlock, "102");

    // Reset to original valid certificate
    Mockito.when(client.getCertificate(Mockito.anyString())).
        thenReturn(x509Certificate);

    tokenVerifier.verify("testUser", blockToken.encodeToUrlString(),
        ContainerProtos.Type.PutBlock, "102");
  }

  @Test
  public void testNetYetValidCertificate() throws Exception {
    String tokenBlockID = "103";
    Token<OzoneBlockTokenIdentifier> blockToken =
        secretManager.generateToken("testUser3", tokenBlockID,
            EnumSet.allOf(AccessModeProto.class), 100);

    // Mock client with an expired cert
    X509Certificate netYetValidCert = generateNotValidYetCert(
        "CN=OzoneMaster", keyPair, ALGORITHM);
    Mockito.when(client.getCertificate(Mockito.anyString())).
        thenReturn(netYetValidCert);

    exception.expect(BlockTokenException.class);
    exception.expectMessage("Block token can't be verified due to not" +
        " yet valid certificate");
    tokenVerifier.verify("testUser", blockToken.encodeToUrlString(),
        ContainerProtos.Type.PutBlock, "103");

    // Reset to original valid certificate
    Mockito.when(client.getCertificate(Mockito.anyString())).
        thenReturn(x509Certificate);

    tokenVerifier.verify("testUser", blockToken.encodeToUrlString(),
        ContainerProtos.Type.PutBlock, "103");
  }

  private X509Certificate generateExpiredCert(String dn,
      KeyPair pair, String algorithm) throws CertificateException,
      IllegalStateException, IOException, OperatorCreationException {
    Date from = new Date();
    // Set end date same as start date to make sure the cert is expired.
    return generateTestCert(dn, pair, algorithm, from, from);
  }

  private X509Certificate generateNotValidYetCert(String dn,
      KeyPair pair, String algorithm) throws CertificateException,
      IllegalStateException, IOException, OperatorCreationException {
    Date from = new Date(Instant.now().toEpochMilli() + 100000L);
    Date to = new Date(from.getTime() + 200000L);
    return generateTestCert(dn, pair, algorithm, from, to);
  }

  private X509Certificate generateTestCert(String dn,
      KeyPair pair, String algorithm, Date from, Date to)
      throws CertificateException, IllegalStateException,
      IOException, OperatorCreationException {
    BigInteger sn = new BigInteger(64, new SecureRandom());
    SubjectPublicKeyInfo subPubKeyInfo = SubjectPublicKeyInfo.getInstance(
        pair.getPublic().getEncoded());
    X500Name subjectDN = new X500Name(dn);
    X509v1CertificateBuilder builder = new X509v1CertificateBuilder(
        subjectDN, sn, from, to, subjectDN, subPubKeyInfo);

    AlgorithmIdentifier sigAlgId =
        new DefaultSignatureAlgorithmIdentifierFinder().find(algorithm);
    AlgorithmIdentifier digAlgId =
        new DefaultDigestAlgorithmIdentifierFinder().find(sigAlgId);
    ContentSigner signer =
        new BcRSAContentSignerBuilder(sigAlgId, digAlgId)
            .build(PrivateKeyFactory.createKey(pair.getPrivate().getEncoded()));
    X509CertificateHolder holder = builder.build(signer);
    return new JcaX509CertificateConverter().getCertificate(holder);
  }
}