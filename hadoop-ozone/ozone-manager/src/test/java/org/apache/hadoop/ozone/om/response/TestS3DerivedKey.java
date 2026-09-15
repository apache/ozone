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

package org.apache.hadoop.ozone.om.response;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import org.apache.commons.codec.digest.HmacUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.hdds.utils.db.InMemoryTestTable;
import org.apache.hadoop.ozone.om.AWSV4AuthValidator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.S3SecretManager;
import org.apache.hadoop.ozone.om.execution.OMExecutionFlow;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.security.OzoneDelegationTokenSecretManager;
import org.apache.hadoop.ozone.security.STSTokenSecretManager;
import org.apache.hadoop.ozone.security.SecretKeyTestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

/** Tests derived-key responses after S3 authentication, outside Ratis apply. */
class TestS3DerivedKey {
  private static final String ACCESS_ID = "permanent-access-id";
  private static final String TEMP_ACCESS_ID = "temporary-access-id";
  private static final String SECRET = "permanent-secret";
  private static final String TEMP_SECRET = "temporary-secret";
  private static final String STRING_TO_SIGN = "AWS4-HMAC-SHA256\n20260912T010203Z\n"
      + "20260912/us-east-1/s3/aws4_request\n"
      + "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

  private final OzoneManager om = mock(OzoneManager.class, CALLS_REAL_METHODS);
  private final S3SecretManager secrets = mock(S3SecretManager.class);
  private final OMExecutionFlow execution = mock(OMExecutionFlow.class);
  private final OzoneManagerRatisServer ratis = mock(OzoneManagerRatisServer.class);
  private final SecretKeyTestClient secretKeyClient = new SecretKeyTestClient();
  private final InMemoryTestTable<String, Long> revocations = new InMemoryTestTable<>();
  private OzoneManagerProtocolServerSideTranslatorPB translator;

  @BeforeEach
  void setup() throws Exception {
    when(om.getConfiguration()).thenReturn(new OzoneConfiguration());
    when(om.isSecurityEnabled()).thenReturn(true);
    when(om.getS3SecretManager()).thenReturn(secrets);
    when(om.getSecretKeyClient()).thenReturn(secretKeyClient);
    when(om.getDelegationTokenMgr()).thenReturn(mock(OzoneDelegationTokenSecretManager.class));
    when(om.getOmExecutionFlow()).thenReturn(execution);
    OMMetadataManager metadata = mock(OMMetadataManager.class);
    when(om.getMetadataManager()).thenReturn(metadata);
    when(metadata.getS3RevokedStsTokenTable()).thenReturn(revocations);
    when(secrets.hasS3Secret(ACCESS_ID)).thenReturn(true);
    when(secrets.getSecretString(ACCESS_ID)).thenReturn(SECRET);
    translator = new OzoneManagerProtocolServerSideTranslatorPB(om, ratis, mock(ProtocolMessageMetrics.class));
    when(execution.submit(any(), anyBoolean())).thenReturn(response(Status.OK));
  }

  @AfterEach
  void cleanup() {
    assertThat(OzoneManager.getS3Auth()).isNull();
    assertThat(OzoneManager.getStsTokenIdentifier()).isNull();
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void returnsDerivedKey(boolean sts, boolean cacheHit) throws Exception {
    OMRequest request = request(sts, 3600);
    OMResponse original = response(Status.OK);
    when(ratis.checkRetryCache()).thenReturn(cacheHit ? original : null);
    when(execution.submit(any(), anyBoolean())).thenReturn(original);

    OMResponse result = translator.processRequest(request);

    assertThat(result.getStatus()).isEqualTo(Status.OK);
    assertThat(result.getCreateKeyResponse().getDerivedKey().toByteArray())
        .isEqualTo(AWSV4AuthValidator.getSigningKey(sts ? TEMP_SECRET : SECRET, STRING_TO_SIGN));
    assertThat(original.getCreateKeyResponse().hasDerivedKey()).isFalse();
    if (cacheHit) {
      verify(execution, never()).submit(any(), anyBoolean());
    } else {
      ArgumentCaptor<OMRequest> submitted = ArgumentCaptor.forClass(OMRequest.class);
      verify(execution).submit(submitted.capture(), anyBoolean());
      // The response-only hint must not make older followers resolve credentials during apply.
      assertThat(submitted.getValue().getCreateKeyRequest().getDerivedKeyPiggyBacking()).isFalse();
      assertThat(request.getCreateKeyRequest().getDerivedKeyPiggyBacking()).isTrue();
    }
    if (sts) {
      verify(secrets, never()).getSecretString(anyString());
    }
  }

  @Test
  void doesNotAttachKeyToFailedResponse() throws Exception {
    when(execution.submit(any(), anyBoolean())).thenReturn(response(Status.ACCESS_DENIED));

    OMResponse result = translator.processRequest(request(true, 3600));

    assertThat(result.getStatus()).isEqualTo(Status.ACCESS_DENIED);
    assertThat(result.getCreateKeyResponse().hasDerivedKey()).isFalse();
  }

  @Test
  void doesNotDeriveKeyUnlessRequested() throws Exception {
    OMRequest request = request(true, 3600);
    request = request.toBuilder().setCreateKeyRequest(request.getCreateKeyRequest().toBuilder()
        .clearDerivedKeyPiggyBacking()).build();

    OMResponse result = translator.processRequest(request);

    assertThat(result.getStatus()).isEqualTo(Status.OK);
    assertThat(result.getCreateKeyResponse().hasDerivedKey()).isFalse();
    verify(secrets, never()).getSecretString(anyString());
  }

  @Test
  void doesNotDeriveKeyInInsecureMode() throws Exception {
    when(om.isSecurityEnabled()).thenReturn(false);

    OMResponse result = translator.processRequest(request(false, 3600));

    assertThat(result.getStatus()).isEqualTo(Status.OK);
    assertThat(result.getCreateKeyResponse().hasDerivedKey()).isFalse();
    verify(secrets, never()).getSecretString(anyString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rejectsExpiredTokenBeforeSubmissionOrCacheLookup(boolean cacheHit) throws Exception {
    when(ratis.checkRetryCache()).thenReturn(cacheHit ? response(Status.OK) : null);

    OMResponse result = translator.processRequest(request(true, -1));

    assertThat(result.getStatus()).isEqualTo(Status.TOKEN_EXPIRED);
    verify(execution, never()).submit(any(), anyBoolean());
    verify(ratis, never()).checkRetryCache();
  }

  @Test
  void rejectsRevokedTokenBeforeSubmission() throws Exception {
    revocations.put(ACCESS_ID, Instant.now().plusSeconds(60).toEpochMilli());

    OMResponse result = translator.processRequest(request(true, 3600));

    assertThat(result.getStatus()).isEqualTo(Status.REVOKED_TOKEN);
    verify(execution, never()).submit(any(), anyBoolean());
  }

  @Test
  void rejectsInvalidSignatureBeforeSubmission() throws Exception {
    OMRequest request = request(true, 3600);
    request = request.toBuilder().setS3Authentication(request.getS3Authentication().toBuilder()
        .setSignature("invalid")).build();

    OMResponse result = translator.processRequest(request);

    assertThat(result.getStatus()).isEqualTo(Status.INVALID_TOKEN);
    verify(execution, never()).submit(any(), anyBoolean());
  }

  @Test
  void handlesSecretLookupFailureBeforeSubmission() throws Exception {
    when(secrets.getSecretString(ACCESS_ID)).thenThrow(new IOException("secret unavailable"));

    OMResponse result = translator.processRequest(request(false, 3600));

    assertThat(result.getSuccess()).isFalse();
    assertThat(result.getCreateKeyResponse().hasDerivedKey()).isFalse();
    verify(execution, never()).submit(any(), anyBoolean());
  }

  private OMRequest request(boolean sts, int durationSeconds) throws Exception {
    byte[] signingKey = AWSV4AuthValidator.getSigningKey(sts ? TEMP_SECRET : SECRET, STRING_TO_SIGN);
    S3Authentication.Builder auth = S3Authentication.newBuilder()
        .setAccessId(sts ? TEMP_ACCESS_ID : ACCESS_ID)
        .setStringToSign(STRING_TO_SIGN)
        .setSignature(new HmacUtils("HmacSHA256", signingKey).hmacHex(STRING_TO_SIGN));
    if (sts) {
      auth.setSessionToken(new STSTokenSecretManager(secretKeyClient).createSTSTokenString(
          STSTokenSecretManager.CreateSTSTokenParams.newBuilder()
              .setTempAccessKeyId(TEMP_ACCESS_ID)
              .setOriginalAccessKeyId(ACCESS_ID)
              .setSecretAccessKey(TEMP_SECRET)
              .setRoleArn("arn:aws:iam::123456789012:role/test")
              .setCreationTime(Instant.now())
              .setDurationSeconds(durationSeconds)
              .setSessionPolicy("")
              .build()));
    }
    return OMRequest.newBuilder().setCmdType(Type.CreateKey).setClientId("client")
        .setS3Authentication(auth)
        .setCreateKeyRequest(CreateKeyRequest.newBuilder().setDerivedKeyPiggyBacking(true)
            .setKeyArgs(KeyArgs.newBuilder().setVolumeName("volume").setBucketName("bucket").setKeyName("key")))
        .build();
  }

  private OMResponse response(Status status) {
    return OMResponse.newBuilder().setCmdType(Type.CreateKey).setStatus(status)
        .setSuccess(status == Status.OK).setCreateKeyResponse(CreateKeyResponse.newBuilder()).build();
  }
}
