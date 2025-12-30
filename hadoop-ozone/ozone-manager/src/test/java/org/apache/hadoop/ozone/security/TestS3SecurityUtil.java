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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.REVOKED_TOKEN;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Clock;
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
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Tests for STS revocation handling in {@link S3SecurityUtil}.
 */
public class TestS3SecurityUtil {
  private static final byte[] ENCRYPTION_KEY = new byte[5];
  private static final TestClock CLOCK = TestClock.newInstance();

  {
    ThreadLocalRandom.current().nextBytes(ENCRYPTION_KEY);
  }

  @Test
  public void testValidateS3CredentialFailsWhenTokenRevoked() throws Exception {
    // If the revoked STS token table contains an entry for the temporary access key id extracted from the session
    // token, validateS3Credential should reject the request with REVOKED_TOKEN
    final OMMetadataManager metadataManager = mock(OMMetadataManager.class);
    final Table<String, Long> revokedSTSTokenTable = new InMemoryTestTable<>();
    validateS3CredentialHelper(
        "session-token-a", metadataManager, revokedSTSTokenTable, true, createSTSTokenIdentifier(),
        REVOKED_TOKEN, "STS token has been revoked");
  }

  @Test
  public void testValidateS3CredentialWhenMetadataUnavailable() throws Exception {
    // If the metadata manager is not available, throws INTERNAL_ERROR
    validateS3CredentialHelper(
        "session-token-b", null, null, false, createSTSTokenIdentifier(),
        INTERNAL_ERROR, "Could not determine STS revocation: metadataManager is null");
  }

  @Test
  public void testValidateS3CredentialSuccessWhenNotRevoked() throws Exception {
    // Normal case: token is NOT revoked and request is accepted
    final OMMetadataManager metadataManager = mock(OMMetadataManager.class);
    final Table<String, Long> revokedSTSTokenTable = new InMemoryTestTable<>();
    validateS3CredentialHelper(
        "session-token-c", metadataManager, revokedSTSTokenTable, false, createSTSTokenIdentifier(),
        null, null);
  }

  @Test
  public void testValidateS3CredentialWhenMetadataManagerAvailableButRevokedTableNull() throws Exception {
    // If the revoked STS token table is not available, throws INTERNAL_ERROR
    final OMMetadataManager metadataManager = mock(OMMetadataManager.class);
    validateS3CredentialHelper(
        "session-token-d", metadataManager, null, false, createSTSTokenIdentifier(),
        INTERNAL_ERROR, "Could not determine STS revocation: revokedStsTokenTable is null");
  }

  @Test
  public void testValidateS3CredentialWhenTableThrowsException() throws Exception {
    // If the revoked STS token table lookup throws, throws INTERNAL_ERROR (wrapped)
    final OMMetadataManager metadataManager = mock(OMMetadataManager.class);
    final Table<String, Long> revokedSTSTokenTable = spy(new InMemoryTestTable<>());
    doThrow(new RuntimeException("lookup failed")).when(revokedSTSTokenTable).getIfExist(anyString());
    validateS3CredentialHelper(
        "session-token-g", metadataManager, revokedSTSTokenTable, false, createSTSTokenIdentifier(),
        INTERNAL_ERROR, "Could not determine STS revocation because of Exception: lookup failed");
  }

  private void validateS3CredentialHelper(String sessionToken, OMMetadataManager metadataManager,
      Table<String, Long> revokedSTSTokenTable, boolean isRevoked, STSTokenIdentifier stsTokenIdentifier,
      OMException.ResultCodes expectedResult, String expectedMessageContents) throws Exception {

    try (OzoneManager ozoneManager = mock(OzoneManager.class)) {
      when(ozoneManager.isSecurityEnabled()).thenReturn(true);
      when(ozoneManager.getSecretKeyClient()).thenReturn(mock(SecretKeyClient.class));

      when(ozoneManager.getMetadataManager()).thenReturn(metadataManager);
      if (metadataManager != null) {
        when(metadataManager.getS3RevokedStsTokenTable()).thenReturn(revokedSTSTokenTable);
      }

      final String tempAccessKeyId = "temp-access-key-id";
      if (isRevoked) {
        final long insertionTimeMillis = CLOCK.millis();
        revokedSTSTokenTable.put(sessionToken, insertionTimeMillis);
      }

      try (MockedStatic<STSSecurityUtil> stsSecurityUtilMock = mockStatic(STSSecurityUtil.class, CALLS_REAL_METHODS);
           MockedStatic<AWSV4AuthValidator> awsV4AuthValidatorMock = mockStatic(
               AWSV4AuthValidator.class, CALLS_REAL_METHODS)) {

        stsSecurityUtilMock.when(
            () -> STSSecurityUtil.constructValidateAndDecryptSTSToken(
                eq(sessionToken), any(SecretKeyClient.class), any(Clock.class)))
            .thenReturn(stsTokenIdentifier);

        // Mock AWS V4 signature validation
        awsV4AuthValidatorMock.when(() -> AWSV4AuthValidator.validateRequest(anyString(), anyString(), anyString()))
            .thenReturn(true);

        final OMRequest omRequest = createRequestWithSessionToken(sessionToken);

        if (expectedResult != null) {
          final OMException omException = assertThrows(
              OMException.class, () -> S3SecurityUtil.validateS3Credential(omRequest, ozoneManager));
          assertEquals(expectedResult, omException.getResult());
          if (expectedMessageContents != null) {
            assertTrue(
                omException.getMessage().contains(expectedMessageContents),
                "Expected exception message to contain: '" + expectedMessageContents + "' but was: '" +
                omException.getMessage() + "'");
          }
        } else {
          assertDoesNotThrow(() -> S3SecurityUtil.validateS3Credential(omRequest, ozoneManager));
        }
      }
    }
  }

  private STSTokenIdentifier createSTSTokenIdentifier() {
    return new STSTokenIdentifier(
        "temp-access-key-id", "original-access-key-id", "arn:aws:iam::123456789012:role/test-role",
        CLOCK.instant().plusSeconds(3600), "secret-access-key", "session-policy",
        ENCRYPTION_KEY);
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
