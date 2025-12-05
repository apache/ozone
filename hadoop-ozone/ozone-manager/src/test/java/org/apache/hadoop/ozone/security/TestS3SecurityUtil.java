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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.REVOKED_TOKEN;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.hdds.utils.db.InMemoryTestTable;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Tests for STS revocation handling in {@link S3SecurityUtil}.
 */
public class TestS3SecurityUtil {
  private static final byte[] ENCRYPTION_KEY = new byte[5];

  {
    ThreadLocalRandom.current().nextBytes(ENCRYPTION_KEY);
  }

  @Test
  public void testValidateS3CredentialFailsWhenTokenRevoked() throws Exception {
    // If the revoked STS token table contains an entry for the temporary access key id extracted from the session
    // token, validateS3Credential should reject the request with REVOKED_TOKEN
    final String sessionToken = "session-token-a";
    final String tempAccessKeyId = "ASIA123456789";

    try (OzoneManager ozoneManager = mock(OzoneManager.class)) {
      when(ozoneManager.isSecurityEnabled()).thenReturn(true);
      when(ozoneManager.getSecretKeyClient()).thenReturn(mock(SecretKeyClient.class));

      final OMMetadataManager metadataManager = mock(OMMetadataManager.class);
      when(ozoneManager.getMetadataManager()).thenReturn(metadataManager);

      final Table<String, String> revokedSTSTokenTable = new InMemoryTestTable<>();
      when(metadataManager.getS3RevokedStsTokenTable()).thenReturn(revokedSTSTokenTable);

      // Mock STSSecurityUtil to return a token whose tempAccessKeyId matches the one that's revoked.
      final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(
          tempAccessKeyId, "original-access-key-id", "arn:aws:iam::123456789012:role/test-role",
          Instant.now().plusSeconds(3600), "secret-access-key", "session-policy",
          ENCRYPTION_KEY);

      try (MockedStatic<STSSecurityUtil> stsSecurityUtilMock = mockStatic(STSSecurityUtil.class, CALLS_REAL_METHODS)) {
        stsSecurityUtilMock.when(
            () -> STSSecurityUtil.constructValidateAndDecryptSTSToken(
                eq(sessionToken), any(SecretKeyClient.class), any(Clock.class)))
            .thenReturn(stsTokenIdentifier);

        // Revoke the tempAccessKeyId
        revokedSTSTokenTable.put(tempAccessKeyId, sessionToken);

        final OMRequest omRequest = createRequestWithSessionToken(sessionToken);
        final OMException ex = assertThrows(
            OMException.class, () -> S3SecurityUtil.validateS3Credential(omRequest, ozoneManager));
        assertEquals(REVOKED_TOKEN, ex.getResult());
      }
    }
  }

  @Test
  public void testValidateS3CredentialWhenMetadataUnavailable() {
    // If the metadata manager is not available, the revocation check should not cause the request to be rejected.
    final String sessionToken = "session-token-b";

    try (OzoneManager ozoneManager = mock(OzoneManager.class)) {
      when(ozoneManager.isSecurityEnabled()).thenReturn(true);
      when(ozoneManager.getMetadataManager()).thenReturn(null);
      when(ozoneManager.getSecretKeyClient()).thenReturn(mock(SecretKeyClient.class));

      final OMRequest omRequest = createRequestWithSessionToken(sessionToken);

      try (MockedStatic<STSSecurityUtil> stsSecurityUtilMock = mockStatic(STSSecurityUtil.class, CALLS_REAL_METHODS);
           MockedStatic<AWSV4AuthValidator> awsV4AuthValidatorMock = mockStatic(
               AWSV4AuthValidator.class, CALLS_REAL_METHODS)) {

        final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(
            "temp-access-key-id", "original-access-key-id", "arn:aws:iam::123456789012:role/test-role",
            Instant.now().plusSeconds(3600), "secret-access-key", "session-policy",
            ENCRYPTION_KEY);

        stsSecurityUtilMock.when(
            () -> STSSecurityUtil.constructValidateAndDecryptSTSToken(
                eq(sessionToken), any(SecretKeyClient.class), any(Clock.class)))
            .thenReturn(stsTokenIdentifier);

        // Mock AWS V4 signature validation
        awsV4AuthValidatorMock.when(() -> AWSV4AuthValidator.validateRequest(anyString(), anyString(), anyString()))
            .thenReturn(true);

        assertDoesNotThrow(() -> S3SecurityUtil.validateS3Credential(omRequest, ozoneManager));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testValidateS3CredentialSuccessWhenNotRevoked() {
    // Normal case: token is NOT revoked and request is accepted
    final String sessionToken = "session-token-c";

    try (OzoneManager ozoneManager = mock(OzoneManager.class)) {
      when(ozoneManager.isSecurityEnabled()).thenReturn(true);
      when(ozoneManager.getSecretKeyClient()).thenReturn(mock(SecretKeyClient.class));

      final OMMetadataManager metadataManager = mock(OMMetadataManager.class);
      when(ozoneManager.getMetadataManager()).thenReturn(metadataManager);

      final Table<String, String> revokedSTSTokenTable = new InMemoryTestTable<>();
      when(metadataManager.getS3RevokedStsTokenTable()).thenReturn(revokedSTSTokenTable);

      // Not revoked -> getIfExist returns null by default in InMemoryTestTable
      final OMRequest omRequest = createRequestWithSessionToken(sessionToken);
      final STSTokenIdentifier stsTokenIdentifier = new STSTokenIdentifier(
          "temp-access-key-id", "original-access-key-id", "arn:aws:iam::123456789012:role/test-role",
          Instant.now().plusSeconds(3600), "secret-access-key", "session-policy",
          ENCRYPTION_KEY);

      try (MockedStatic<STSSecurityUtil> stsSecurityUtilMock = mockStatic(STSSecurityUtil.class, CALLS_REAL_METHODS);
           MockedStatic<AWSV4AuthValidator> awsV4AuthValidatorMock = mockStatic(
               AWSV4AuthValidator.class, CALLS_REAL_METHODS)) {

        stsSecurityUtilMock.when(
                () -> STSSecurityUtil.constructValidateAndDecryptSTSToken(
                    eq(sessionToken), any(SecretKeyClient.class), any(Clock.class)))
            .thenReturn(stsTokenIdentifier);
        awsV4AuthValidatorMock.when(() -> AWSV4AuthValidator.validateRequest(anyString(), anyString(), anyString()))
            .thenReturn(true);

        assertDoesNotThrow(() -> S3SecurityUtil.validateS3Credential(omRequest, ozoneManager));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static OMRequest createRequestWithSessionToken(String sessionToken) {
    final S3Authentication s3Authentication = S3Authentication.newBuilder()
        .setAccessId("accessKeyId")
        .setStringToSign("string-to-sign")
        .setSignature("signature")
        .setSessionToken(sessionToken)
        .build();

    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(Type.CreateVolume)
        .setS3Authentication(s3Authentication)
        .build();
  }
}
