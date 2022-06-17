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

import static org.apache.hadoop.ozone.container.ContainerTestHelper.getBlockRequest;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getPutBlockRequest;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getReadChunkRequest;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getWriteChunkRequest;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.token.BlockTokenException;
import org.apache.hadoop.hdds.security.token.BlockTokenVerifier;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.hdds.security.token.TokenVerifier;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.OMCertificateClient;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
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
import org.junit.Test;
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
import java.util.concurrent.TimeUnit;

/**
 * Test class for {@link OzoneBlockTokenSecretManager}.
 */
public class TestOzoneBlockTokenSecretManager {

  private static final String BASEDIR = GenericTestUtils
      .getTempPath(TestOzoneBlockTokenSecretManager.class.getSimpleName());
  private static final String ALGORITHM = "SHA256withRSA";

  private OzoneBlockTokenSecretManager secretManager;
  private KeyPair keyPair;
  private String omCertSerialId;
  private CertificateClient client;
  private TokenVerifier tokenVerifier;
  private Pipeline pipeline;

  @Before
  public void setUp() throws Exception {
    pipeline = MockPipeline.createPipeline(3);

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, BASEDIR);
    conf.setBoolean(HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED, true);
    // Create Ozone Master key pair.
    keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    // Create Ozone Master certificate (SCM CA issued cert) and key store.
    SecurityConfig securityConfig = new SecurityConfig(conf);
    X509Certificate x509Certificate = KeyStoreTestUtil
        .generateCertificate("CN=OzoneMaster", keyPair, 30, ALGORITHM);
    omCertSerialId = x509Certificate.getSerialNumber().toString();
    secretManager = new OzoneBlockTokenSecretManager(securityConfig,
        TimeUnit.HOURS.toMillis(1), omCertSerialId);
    client = Mockito.mock(OMCertificateClient.class);
    when(client.getCertificate()).thenReturn(x509Certificate);
    when(client.getCertificate(anyString())).
        thenReturn(x509Certificate);
    when(client.getPublicKey()).thenReturn(keyPair.getPublic());
    when(client.getPrivateKey()).thenReturn(keyPair.getPrivate());
    when(client.getSignatureAlgorithm()).thenReturn(
        securityConfig.getSignatureAlgo());
    when(client.getSecurityProvider()).thenReturn(
        securityConfig.getProvider());
    when(client.verifySignature((byte[]) Mockito.any(),
        Mockito.any(), Mockito.any())).thenCallRealMethod();

    secretManager.start(client);
    tokenVerifier = new BlockTokenVerifier(securityConfig, client);
  }

  @After
  public void tearDown() {
    secretManager = null;
  }

  @Test
  public void testGenerateToken() throws Exception {
    BlockID blockID = new BlockID(101, 0);

    Token<OzoneBlockTokenIdentifier> token = secretManager.generateToken(
        blockID, EnumSet.allOf(AccessModeProto.class), 100);
    OzoneBlockTokenIdentifier identifier =
        OzoneBlockTokenIdentifier.readFieldsProtobuf(new DataInputStream(
            new ByteArrayInputStream(token.getIdentifier())));
    // Check basic details.
    Assert.assertEquals(OzoneBlockTokenIdentifier.getTokenService(blockID),
        identifier.getService());
    Assert.assertEquals(EnumSet.allOf(AccessModeProto.class),
        identifier.getAccessModes());
    Assert.assertEquals(omCertSerialId, identifier.getCertSerialId());

    validateHash(token.getPassword(), token.getIdentifier());
  }

  @Test
  public void testCreateIdentifierSuccess() throws Exception {
    BlockID blockID = new BlockID(101, 0);
    OzoneBlockTokenIdentifier btIdentifier = secretManager.createIdentifier(
        "testUser", blockID, EnumSet.allOf(AccessModeProto.class), 100);

    // Check basic details.
    Assert.assertEquals("testUser", btIdentifier.getOwnerId());
    Assert.assertEquals(BlockTokenVerifier.getTokenService(blockID),
        btIdentifier.getService());
    Assert.assertEquals(EnumSet.allOf(AccessModeProto.class),
        btIdentifier.getAccessModes());
    Assert.assertEquals(omCertSerialId, btIdentifier.getCertSerialId());

    byte[] hash = secretManager.createPassword(btIdentifier);
    validateHash(hash, btIdentifier.getBytes());
  }

  @Test
  public void tokenCanBeUsedForSpecificBlock() throws Exception {
    // GIVEN
    BlockID blockID = new BlockID(101, 0);

    // WHEN
    Token<OzoneBlockTokenIdentifier> token =
        secretManager.generateToken("testUser", blockID,
            EnumSet.allOf(AccessModeProto.class), 100);
    String encodedToken = token.encodeToUrlString();
    ContainerCommandRequestProto writeChunkRequest = getWriteChunkRequest(
        pipeline, blockID, 100, encodedToken);
    ContainerCommandRequestProto putBlockCommand = getPutBlockRequest(
        pipeline, encodedToken, writeChunkRequest.getWriteChunk());

    // THEN
    tokenVerifier.verify("testUser", token, putBlockCommand);
  }

  @Test
  public void tokenCannotBeUsedForOtherBlock() throws Exception {
    // GIVEN
    BlockID blockID = new BlockID(101, 0);
    BlockID otherBlockID = new BlockID(102, 0);

    // WHEN
    Token<OzoneBlockTokenIdentifier> token =
        secretManager.generateToken("testUser", blockID,
            EnumSet.allOf(AccessModeProto.class), 100);
    String encodedToken = token.encodeToUrlString();
    ContainerCommandRequestProto writeChunkRequest = getWriteChunkRequest(
        pipeline, otherBlockID, 100, encodedToken);

    // THEN
    BlockTokenException e = assertThrows(BlockTokenException.class,
        () -> tokenVerifier.verify("testUser", token, writeChunkRequest));
    String msg = e.getMessage();
    assertTrue(msg, msg.contains("Token for ID: " +
        OzoneBlockTokenIdentifier.getTokenService(blockID) +
        " can't be used to access: " +
        OzoneBlockTokenIdentifier.getTokenService(otherBlockID)));
  }

  /**
   * Validate hash using public key of KeyPair.
   * */
  private void validateHash(byte[] hash, byte[] identifier) throws Exception {
    Signature rsaSignature =
        Signature.getInstance(secretManager.getDefaultSignatureAlgorithm());
    rsaSignature.initVerify(client.getPublicKey());
    rsaSignature.update(identifier);
    assertTrue(rsaSignature.verify(hash));
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
        "testUser", "123", EnumSet.allOf(AccessModeProto.class),
        Time.now() + 60 * 60 * 24, "123444", 1024);
    LambdaTestUtils.intercept(UnsupportedOperationException.class, "operation" +
            " is not supported for block tokens",
        () -> secretManager.verifySignature(id,
            client.signData(id.getBytes())));
  }

  @Test
  public void testBlockTokenReadAccessMode() throws Exception {
    final String testUser1 = "testUser1";
    BlockID blockID = new BlockID(101, 0);
    Token<OzoneBlockTokenIdentifier> token =
        secretManager.generateToken(testUser1, blockID,
            EnumSet.of(AccessModeProto.READ), 100);
    String encodedToken = token.encodeToUrlString();
    ContainerCommandRequestProto writeChunkRequest = getWriteChunkRequest(
        pipeline, blockID, 100, encodedToken);
    ContainerCommandRequestProto putBlockCommand = getPutBlockRequest(
        pipeline, encodedToken, writeChunkRequest.getWriteChunk());
    ContainerCommandRequestProto getBlockCommand = getBlockRequest(
        pipeline, putBlockCommand.getPutBlock());

    BlockTokenException e = assertThrows(BlockTokenException.class,
        () -> tokenVerifier.verify(testUser1, token, putBlockCommand));
    String msg = e.getMessage();
    assertTrue(msg, msg.contains("doesn't have WRITE permission"));

    tokenVerifier.verify(testUser1, token, getBlockCommand);
  }

  @Test
  public void testBlockTokenWriteAccessMode() throws Exception {
    final String testUser2 = "testUser2";
    BlockID blockID = new BlockID(102, 0);
    Token<OzoneBlockTokenIdentifier> token =
        secretManager.generateToken(testUser2, blockID,
            EnumSet.of(AccessModeProto.WRITE), 100);
    String encodedToken = token.encodeToUrlString();
    ContainerCommandRequestProto writeChunkRequest = getWriteChunkRequest(
        pipeline, blockID, 100, encodedToken);
    ContainerCommandRequestProto readChunkRequest =
        getReadChunkRequest(pipeline, writeChunkRequest.getWriteChunk());

    tokenVerifier.verify(testUser2, token, writeChunkRequest);

    BlockTokenException e = assertThrows(BlockTokenException.class,
        () -> tokenVerifier.verify(testUser2, token, readChunkRequest));
    String msg = e.getMessage();
    assertTrue(msg, msg.contains("doesn't have READ permission"));
  }

  @Test
  public void testExpiredCertificate() throws Exception {
    String user = "testUser2";
    BlockID blockID = new BlockID(102, 0);
    Token<OzoneBlockTokenIdentifier> token =
        secretManager.generateToken(user, blockID,
            EnumSet.allOf(AccessModeProto.class), 100);
    ContainerCommandRequestProto writeChunkRequest = getWriteChunkRequest(
        pipeline, blockID, 100, token.encodeToUrlString());

    tokenVerifier.verify("testUser", token, writeChunkRequest);

    // Mock client with an expired cert
    X509Certificate expiredCert = generateExpiredCert(
        "CN=OzoneMaster", keyPair, ALGORITHM);
    when(client.getCertificate(anyString())).thenReturn(expiredCert);

    BlockTokenException e = assertThrows(BlockTokenException.class,
        () -> tokenVerifier.verify(user, token, writeChunkRequest));
    String msg = e.getMessage();
    assertTrue(msg, msg.contains("Token can't be verified due to" +
        " expired certificate"));
  }

  @Test
  public void testNetYetValidCertificate() throws Exception {
    String user = "testUser2";
    BlockID blockID = new BlockID(102, 0);
    Token<OzoneBlockTokenIdentifier> token =
        secretManager.generateToken(user, blockID,
            EnumSet.allOf(AccessModeProto.class), 100);
    ContainerCommandRequestProto writeChunkRequest = getWriteChunkRequest(
        pipeline, blockID, 100, token.encodeToUrlString());

    tokenVerifier.verify(user, token, writeChunkRequest);

    // Mock client with an expired cert
    X509Certificate netYetValidCert = generateNotValidYetCert(
        "CN=OzoneMaster", keyPair, ALGORITHM);
    when(client.getCertificate(anyString())).
        thenReturn(netYetValidCert);

    BlockTokenException e = assertThrows(BlockTokenException.class,
        () -> tokenVerifier.verify(user, token, writeChunkRequest));
    String msg = e.getMessage();
    assertTrue(msg, msg.contains("Token can't be verified due to not" +
        " yet valid certificate"));
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
