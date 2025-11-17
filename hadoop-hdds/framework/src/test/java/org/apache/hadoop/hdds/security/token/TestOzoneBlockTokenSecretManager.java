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

package org.apache.hadoop.hdds.security.token;

import static java.time.Duration.ofDays;
import static java.time.Instant.now;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getBlockRequest;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getReadChunkRequest;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.newPutBlockRequestBuilder;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.newWriteChunkRequestBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.security.NoSuchAlgorithmException;
import java.util.EnumSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeySignerClient;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyTestUtil;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyVerifierClient;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test class for {@link OzoneBlockTokenSecretManager}.
 */
public class TestOzoneBlockTokenSecretManager {

  @TempDir
  private File baseDir;
  private static final String ALGORITHM = "HmacSHA256";

  private OzoneBlockTokenSecretManager secretManager;
  private UUID secretKeyId;
  private SecretKeyVerifierClient secretKeyClient;
  private TokenVerifier tokenVerifier;
  private Pipeline pipeline;
  private ManagedSecretKey secretKey;

  @BeforeEach
  public void setUp() throws Exception {
    pipeline = MockPipeline.createPipeline(3);

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, baseDir.getPath());
    conf.setBoolean(HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED, true);
    SecurityConfig securityConfig = new SecurityConfig(conf);

    secretKey = generateValidSecretKey();
    secretKeyId = secretKey.getId();

    secretKeyClient = mock(SecretKeyVerifierClient.class);
    SecretKeySignerClient secretKeySignerClient =
        mock(SecretKeySignerClient.class);
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
    assertEquals(OzoneBlockTokenIdentifier.getTokenService(blockID),
        identifier.getService());
    assertEquals(EnumSet.allOf(AccessModeProto.class),
        identifier.getAccessModes());
    assertEquals(secretKeyId, identifier.getSecretKeyId());
    assertTrue(secretKey.isValidSignature(token.getIdentifier(),
        token.getPassword()));
  }

  @Test
  public void testCreateIdentifierSuccess() {
    BlockID blockID = new BlockID(101, 0);
    OzoneBlockTokenIdentifier btIdentifier = secretManager.createIdentifier(
        "testUser", blockID, EnumSet.allOf(AccessModeProto.class), 100);

    // Check basic details.
    assertEquals("testUser", btIdentifier.getOwnerId());
    assertEquals(BlockTokenVerifier.getTokenService(blockID),
        btIdentifier.getService());
    assertEquals(EnumSet.allOf(AccessModeProto.class),
        btIdentifier.getAccessModes());
    byte[] hash = secretManager.createPassword(btIdentifier);
    assertEquals(secretKeyId, btIdentifier.getSecretKeyId());
    assertTrue(secretKey.isValidSignature(btIdentifier.getBytes(), hash));
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
    tokenVerifier.verify(token, putBlockCommand);
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
        () -> tokenVerifier.verify(token, writeChunkRequest));

    assertThat(e.getMessage()).contains("Token for ID: " +
        OzoneBlockTokenIdentifier.getTokenService(blockID) +
        " can't be used to access: " +
        OzoneBlockTokenIdentifier.getTokenService(otherBlockID));
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
        () -> tokenVerifier.verify(token, putBlockCommand));

    assertThat(e.getMessage())
        .contains("doesn't have WRITE permission");

    tokenVerifier.verify(token, getBlockCommand);
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

    tokenVerifier.verify(token, writeChunkRequest);

    BlockTokenException e = assertThrows(BlockTokenException.class,
        () -> tokenVerifier.verify(token, readChunkRequest));
    assertThat(e.getMessage())
        .contains("doesn't have READ permission");
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

    tokenVerifier.verify(token, writeChunkRequest);

    // Mock client with an expired cert
    ManagedSecretKey expiredSecretKey = generateExpiredSecretKey();
    when(secretKeyClient.getSecretKey(any())).thenReturn(expiredSecretKey);

    BlockTokenException e = assertThrows(BlockTokenException.class,
        () -> tokenVerifier.verify(token, writeChunkRequest));
    assertThat(e.getMessage())
        .contains("Token can't be verified due to expired secret key");
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
