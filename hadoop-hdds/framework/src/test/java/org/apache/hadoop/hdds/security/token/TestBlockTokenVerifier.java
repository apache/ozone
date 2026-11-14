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

import static org.apache.hadoop.ozone.container.ContainerTestHelper.getWriteChunkRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.time.Instant;
import java.util.EnumSet;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyVerifierClient;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests for {@link BlockTokenVerifier}.
 */
public class TestBlockTokenVerifier
    extends TokenVerifierTests<OzoneBlockTokenIdentifier> {

  @Override
  protected String tokenEnabledConfigKey() {
    return HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
  }

  @Override
  protected TokenVerifier newTestSubject(SecurityConfig secConf,
      SecretKeyVerifierClient secretKeyClient) {
    return new BlockTokenVerifier(secConf, secretKeyClient);
  }

  @Override
  protected ContainerCommandRequestProto unverifiedRequest()
      throws IOException {
    Pipeline pipeline = MockPipeline.createPipeline(1);
    return ContainerTestHelper.getCloseContainer(pipeline, 1);
  }

  @Override
  protected ContainerCommandRequestProto verifiedRequest(
      OzoneBlockTokenIdentifier tokenId) throws IOException {
    Pipeline pipeline = MockPipeline.createPipeline(1);
    return getWriteChunkRequest(
        pipeline, new BlockID(1, 0), 1024);
  }

  @Override
  protected OzoneBlockTokenIdentifier newTokenId() {
    OzoneBlockTokenIdentifier tokenId =
        new OzoneBlockTokenIdentifier("any user",
            new BlockID(1, 0),
            EnumSet.allOf(AccessModeProto.class),
            Instant.now().plusSeconds(3600).toEpochMilli(), 100);
    tokenId.setSecretKeyId(SECRET_KEY_ID);
    return tokenId;
  }

  @ParameterizedTest
  @EnumSource(value = ContainerProtos.Type.class, names = {"StreamInit", "StreamInitWithPutBlock"})
  public void testVerifiesStreamInitLikeWriteChunk(ContainerProtos.Type streamInitType) throws Exception {
    final SecretKeyVerifierClient secretKeyClient = mockSecretKeyClient(true);
    final ShortLivedTokenSecretManager<OzoneBlockTokenIdentifier> secretManager = new MockTokenManager();
    final OzoneBlockTokenIdentifier tokenId = valid(newTokenId());
    final Token<OzoneBlockTokenIdentifier> token = secretManager.generateToken(tokenId);
    final TokenVerifier subject = newTestSubject(tokenEnabled(), secretKeyClient);

    assertDoesNotThrow(() -> subject.verify(token, newStreamInit(streamInitType, new BlockID(1, 0))));

    final BlockTokenException idMismatch = assertThrows(
        BlockTokenException.class, () -> subject.verify(token, newStreamInit(streamInitType, new BlockID(1, 1))));
    assertThat(idMismatch.getMessage()).contains("ID mismatch");

    final OzoneBlockTokenIdentifier readOnly = valid(
        new OzoneBlockTokenIdentifier(
            "any user", new BlockID(1, 0), EnumSet.of(AccessModeProto.READ),
            Instant.now().plusSeconds(3600).toEpochMilli(), 100));
    readOnly.setSecretKeyId(SECRET_KEY_ID);
    final Token<OzoneBlockTokenIdentifier> readToken = secretManager.generateToken(readOnly);
    final BlockTokenException noWrite = assertThrows(
        BlockTokenException.class, () -> subject.verify(readToken, newStreamInit(streamInitType, new BlockID(1, 0))));
    assertThat(noWrite.getMessage()).contains("doesn't have WRITE permission");
  }

  @ParameterizedTest
  @EnumSource(value = ContainerProtos.Type.class, names = {"StreamInit", "StreamInitWithPutBlock"})
  public void testStreamInitRejectsInvalidSignature(ContainerProtos.Type streamInitType) throws Exception {
    // secretKeyClient is mocked to return an invalid signature
    final SecretKeyVerifierClient secretKeyClient = mockSecretKeyClient(false);
    final ShortLivedTokenSecretManager<OzoneBlockTokenIdentifier> secretManager = new MockTokenManager();
    final OzoneBlockTokenIdentifier tokenId = valid(newTokenId());
    final Token<?> invalidToken = secretManager.generateToken(tokenId);
    final TokenVerifier subject = newTestSubject(tokenEnabled(), secretKeyClient);

    final BlockTokenException ex = assertThrows(
        BlockTokenException.class,
        () -> subject.verify(invalidToken, newStreamInit(streamInitType, new BlockID(1, 0))));
    assertThat(ex.getMessage()).contains("Invalid token for user");
  }

  private static ContainerCommandRequestProto newStreamInit(
      ContainerProtos.Type streamInitType, BlockID blockId) throws IOException {
    final Pipeline pipeline = MockPipeline.createPipeline(1);
    final WriteChunkRequestProto writeChunk = WriteChunkRequestProto.newBuilder()
        .setBlockID(blockId.getDatanodeBlockIDProtobuf())
        .build();
    return ContainerCommandRequestProto.newBuilder()
        .setCmdType(streamInitType)
        .setContainerID(blockId.getContainerID())
        .setDatanodeUuid(pipeline.getFirstNode().getUuidString())
        .setWriteChunk(writeChunk)
        .build();
  }
}
