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

package org.apache.hadoop.hdds.security.token;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.symmetric.SecretKeySignerClient;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyVerifierClient;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyTestUtil;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.security.token.Token;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.security.NoSuchAlgorithmException;
import java.util.EnumSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.time.Duration.ofDays;
import static java.time.Instant.now;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getBlockRequest;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getReadChunkRequest;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.newPutBlockRequestBuilder;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.newWriteChunkRequestBuilder;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Test class for {@link OzoneBlockTokenSecretManager}.
 */
public class TestOzoneBlockTokenSecretManager {

  private static final String BASEDIR = GenericTestUtils
      .getTempPath(TestOzoneBlockTokenSecretManager.class.getSimpleName());
  private static final String ALGORITHM = "HmacSHA256";

  private OzoneBlockTokenSecretManager secretManager;
  private UUID secretKeyId;
  private SecretKeyVerifierClient secretKeyClient;
  private SecretKeySignerClient secretKeySignerClient;
  private TokenVerifier tokenVerifier;
  private Pipeline pipeline;
  private ManagedSecretKey secretKey;

  @Before
  public void setUp() throws Exception {
    pipeline = MockPipeline.createPipeline(3);

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, BASEDIR);
    conf.setBoolean(HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED, true);
    SecurityConfig securityConfig = new SecurityConfig(conf);

    secretKey = generateValidSecretKey();
    secretKeyId = secretKey.getId();

    secretKeyClient = Mockito.mock(SecretKeyVerifierClient.class);
    secretKeySignerClient = Mockito.mock(SecretKeySignerClient.class);
    when(secretKeySignerClient.getCurrentSecretKey()).thenReturn(secretKey);
    when(secretKeyClient.getSecretKey(secretKeyId)).thenReturn(secretKey);

    secretManager = new OzoneBlockTokenSecretManager(
        TimeUnit.HOURS.toMillis(1), secretKeySignerClient);
    tokenVerifier = new BlockTokenVerifier(securityConfig, secretKeyClient);
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
    Assert.assertEquals(secretKeyId, identifier.getSecretKeyId());

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
    byte[] hash = secretManager.createPassword(btIdentifier);
    Assert.assertEquals(secretKeyId, btIdentifier.getSecretKeyId());
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
    ContainerCommandRequestProto writeChunkRequest =
        newWriteChunkRequestBuilder(pipeline, blockID, 100)
        .setEncodedToken(encodedToken)
        .build();
    ContainerCommandRequestProto putBlockCommand = newPutBlockRequestBuilder(
        pipeline, writeChunkRequest.getWriteChunk())
        .setEncodedToken(encodedToken)
        .build();

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
    ContainerCommandRequestProto writeChunkRequest =
        newWriteChunkRequestBuilder(pipeline, otherBlockID, 100)
            .setEncodedToken(encodedToken).build();

    // THEN
    BlockTokenException e = assertThrows(BlockTokenException.class,
        () -> tokenVerifier.verify("testUser", token, writeChunkRequest));
    String msg = e.getMessage();
    assertTrue(msg, msg.contains("Token for ID: " +
        OzoneBlockTokenIdentifier.getTokenService(blockID) +
        " can't be used to access: " +
        OzoneBlockTokenIdentifier.getTokenService(otherBlockID)));
  }

  private void validateHash(byte[] hash, byte[] identifier) throws Exception {
    assertTrue(secretKey.isValidSignature(identifier, hash));
  }

  @Test
  public void testBlockTokenReadAccessMode() throws Exception {
    final String testUser1 = "testUser1";
    BlockID blockID = new BlockID(101, 0);
    Token<OzoneBlockTokenIdentifier> token =
        secretManager.generateToken(testUser1, blockID,
            EnumSet.of(AccessModeProto.READ), 100);
    String encodedToken = token.encodeToUrlString();
    ContainerCommandRequestProto writeChunkRequest =
        newWriteChunkRequestBuilder(pipeline, blockID, 100)
        .setEncodedToken(encodedToken)
        .build();
    ContainerCommandRequestProto putBlockCommand = newPutBlockRequestBuilder(
        pipeline, writeChunkRequest.getWriteChunk())
        .setEncodedToken(encodedToken)
        .build();
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
    ContainerCommandRequestProto writeChunkRequest =
        newWriteChunkRequestBuilder(pipeline, blockID, 100)
        .setEncodedToken(encodedToken)
        .build();
    ContainerCommandRequestProto readChunkRequest =
        getReadChunkRequest(pipeline, writeChunkRequest.getWriteChunk());

    tokenVerifier.verify(testUser2, token, writeChunkRequest);

    BlockTokenException e = assertThrows(BlockTokenException.class,
        () -> tokenVerifier.verify(testUser2, token, readChunkRequest));
    String msg = e.getMessage();
    assertTrue(msg, msg.contains("doesn't have READ permission"));
  }

  @Test
  public void testExpiredSecretKey() throws Exception {
    String user = "testUser2";
    BlockID blockID = new BlockID(102, 0);
    Token<OzoneBlockTokenIdentifier> token =
        secretManager.generateToken(user, blockID,
            EnumSet.allOf(AccessModeProto.class), 100);
    ContainerCommandRequestProto writeChunkRequest =
        newWriteChunkRequestBuilder(pipeline, blockID, 100)
        .setEncodedToken(token.encodeToUrlString())
        .build();

    tokenVerifier.verify("testUser", token, writeChunkRequest);

    // Mock client with an expired cert
    ManagedSecretKey expiredSecretKey = generateExpiredSecretKey();
    when(secretKeyClient.getSecretKey(any())).thenReturn(expiredSecretKey);

    BlockTokenException e = assertThrows(BlockTokenException.class,
        () -> tokenVerifier.verify(user, token, writeChunkRequest));
    String msg = e.getMessage();
    assertTrue(msg, msg.contains("Token can't be verified due to" +
        " expired secret key"));
  }

  private ManagedSecretKey generateValidSecretKey()
      throws NoSuchAlgorithmException {
    return SecretKeyTestUtil.generateKey(ALGORITHM, now(), ofDays(1));
  }

  private ManagedSecretKey generateExpiredSecretKey() throws Exception {
    return SecretKeyTestUtil.generateKey(ALGORITHM,
        now().minus(ofDays(2)), ofDays(1));
  }
}
