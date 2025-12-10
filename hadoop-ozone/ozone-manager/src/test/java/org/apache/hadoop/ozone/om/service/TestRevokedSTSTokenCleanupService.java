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

package org.apache.hadoop.ozone.om.service;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.protobuf.ServiceException;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.utils.db.StringInMemoryTestTable;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CleanupRevokedSTSTokensRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.security.STSSecurityUtil;
import org.apache.hadoop.ozone.security.STSTokenIdentifier;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Unit tests for {@link RevokedSTSTokenCleanupService}.
 */
public class TestRevokedSTSTokenCleanupService {
  private static final byte[] ENCRYPTION_KEY = new byte[5];

  {
    ThreadLocalRandom.current().nextBytes(ENCRYPTION_KEY);
  }

  private TestClock testClock;
  private OzoneManager ozoneManager;
  private StringInMemoryTestTable<String> revokedStsTokenTable;

  @BeforeEach
  public void setUp() {
    testClock = TestClock.newInstance();
    ozoneManager = mock(OzoneManager.class);
    final OMMetadataManager omMetadataManager = mock(OMMetadataManager.class);
    revokedStsTokenTable = new StringInMemoryTestTable<>();

    when(ozoneManager.isLeaderReady()).thenReturn(true);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getThreadNamePrefix()).thenReturn("om-");
    when(omMetadataManager.getS3RevokedStsTokenTable()).thenReturn(revokedStsTokenTable);
  }

  @Test
  public void submitsCleanupRequestForOnlyExpiredTokens() throws Exception {
    // If there are two revoked entries, one expired and one not expired, only the expired access key id should be
    // submitted for cleanup.
    revokedStsTokenTable.put("ASIA1234567890", "expired-session-token");
    revokedStsTokenTable.put("ASIA4567890123", "valid-session-token");

    final Instant now = testClock.instant();
    final STSTokenIdentifier expiredSTSTokenIdentifier = createToken("ASIA1234567890", now.minusSeconds(3600));
    final STSTokenIdentifier validSTSTokenIdentifier = createToken("ASIA4567890123", now.plusSeconds(3600));

    final AtomicReference<OMRequest> capturedRequest = new AtomicReference<>();

    try (MockedStatic<STSSecurityUtil> stsSecurityUtilMock = mockStatic(STSSecurityUtil.class);
         MockedStatic<OzoneManagerRatisUtils> ozoneManagerRatisUtilsMock = mockStatic(OzoneManagerRatisUtils.class)) {
      configureStsSecurityUtilMock(stsSecurityUtilMock, "expired-session-token", expiredSTSTokenIdentifier);
      configureStsSecurityUtilMock(stsSecurityUtilMock, "valid-session-token", validSTSTokenIdentifier);

      mockRatisSubmitAndCapture(ozoneManagerRatisUtilsMock, capturedRequest);

      // Run the cleanup service
      final RevokedSTSTokenCleanupService revokedSTSTokenCleanupService = createAndRunCleanupService();

      assertThat(revokedSTSTokenCleanupService.getRunCount()).isEqualTo(1);
      assertThat(revokedSTSTokenCleanupService.getSubmittedDeletedEntryCount()).isEqualTo(1);

      final OMRequest omRequest = capturedRequest.get();
      assertThat(omRequest).isNotNull();
      assertThat(omRequest.getCmdType()).isEqualTo(Type.CleanupRevokedSTSTokens);

      final CleanupRevokedSTSTokensRequest cleanupRevokedSTSTokensRequest =
          omRequest.getCleanupRevokedSTSTokensRequest();
      assertThat(cleanupRevokedSTSTokensRequest.getAccessKeyIdList()).containsExactly("ASIA1234567890");
    }
  }

  @Test
  public void doesNotSubmitRequestWhenThereAreNoExpiredTokens() throws Exception {
    // If only non-expired entries exist in the revoked sts token table, no cleanup request should be submitted and
    // no metrics should be updated.
    revokedStsTokenTable.put("ASIA1234567890", "valid-session-token-1");
    revokedStsTokenTable.put("ASIA0123456789", "valid-session-token-2");

    final Instant now = testClock.instant();
    final STSTokenIdentifier validSTSTokenIdentifier1 = createToken("ASIA1234567890", now.plusSeconds(3600));
    final STSTokenIdentifier validSTSTokenIdentifier2 = createToken("ASIA0123456789", now.plusSeconds(7200));

    final AtomicReference<OMRequest> capturedRequest = new AtomicReference<>();

    try (MockedStatic<STSSecurityUtil> stsSecurityUtilMock = mockStatic(STSSecurityUtil.class);
         MockedStatic<OzoneManagerRatisUtils> ozoneManagerRatisUtilsMock = mockStatic(OzoneManagerRatisUtils.class)) {

      configureStsSecurityUtilMock(stsSecurityUtilMock, "valid-session-token-1", validSTSTokenIdentifier1);
      configureStsSecurityUtilMock(stsSecurityUtilMock, "valid-session-token-2", validSTSTokenIdentifier2);

      mockRatisSubmitAndCapture(ozoneManagerRatisUtilsMock, capturedRequest);

      // Run the cleanup service
      final RevokedSTSTokenCleanupService revokedSTSTokenCleanupService = createAndRunCleanupService();

      assertThat(revokedSTSTokenCleanupService.getRunCount()).isEqualTo(1);
      assertThat(revokedSTSTokenCleanupService.getSubmittedDeletedEntryCount()).isZero();
      assertThat(capturedRequest.get()).isNull();
    }
  }

  @Test
  public void handlesNoEntriesInRevokedSTSTokenTable() throws Exception {
    // If the table is empty (which most of the time it will be), no cleanup requests should be submitted and no metrics
    // should be updated.
    final AtomicReference<OMRequest> capturedRequest = new AtomicReference<>();

    try (MockedStatic<OzoneManagerRatisUtils> ozoneManagerRatisUtilsMock = mockStatic(OzoneManagerRatisUtils.class)) {
      mockRatisSubmitAndCapture(ozoneManagerRatisUtilsMock, capturedRequest);

      // Run the cleanup service
      final RevokedSTSTokenCleanupService revokedSTSTokenCleanupService = createAndRunCleanupService();

      assertThat(revokedSTSTokenCleanupService.getRunCount()).isEqualTo(1);
      assertThat(revokedSTSTokenCleanupService.getSubmittedDeletedEntryCount()).isZero();
      assertThat(capturedRequest.get()).isNull();
    }
  }

  @Test
  public void doesNotUpdateMetricsOnRatisSubmissionServiceExceptionFailure() throws Exception {
    // If there are expired tokens in the table but the OM request submission to clean up the entries fails with a
    // service exception, the metrics should not be updated
    revokedStsTokenTable.put("ASIA1234567890", "expired-session-token-1");
    revokedStsTokenTable.put("ASIA0987654321", "expired-session-token-2");

    final Instant now = testClock.instant();
    final STSTokenIdentifier expiredSTSTokenIdentifier1 = createToken("ASIA1234567890", now.minusSeconds(3600));
    final STSTokenIdentifier expiredSTSTokenIdentifier2 = createToken("ASIA0987654321", now.minusSeconds(7200));

    final AtomicInteger submitAttempts = new AtomicInteger(0);

    try (MockedStatic<STSSecurityUtil> stsSecurityUtilMock = mockStatic(STSSecurityUtil.class);
         MockedStatic<OzoneManagerRatisUtils> ozoneManagerRatisUtilsMock = mockStatic(OzoneManagerRatisUtils.class)) {

      configureStsSecurityUtilMock(stsSecurityUtilMock, "expired-session-token-1", expiredSTSTokenIdentifier1);
      configureStsSecurityUtilMock(stsSecurityUtilMock, "expired-session-token-2", expiredSTSTokenIdentifier2);

      // Simulate Ratis submission failure
      mockRatisSubmitToFail(ozoneManagerRatisUtilsMock, submitAttempts);

      // Run the cleanup service
      final RevokedSTSTokenCleanupService revokedSTSTokenCleanupService = createAndRunCleanupService();

      assertThat(revokedSTSTokenCleanupService.getRunCount()).isEqualTo(1);
      assertThat(submitAttempts.get()).isEqualTo(1);
      assertThat(revokedSTSTokenCleanupService.getSubmittedDeletedEntryCount()).isZero();
    }
  }

  @Test
  public void doesNotUpdateMetricsOnNonSuccessfulResponse() throws Exception {
    // If there is an expired token in the table but the OM request submission to clean up the entries gets a
    // non-successful response, the metrics should not be updated
    revokedStsTokenTable.put("ASIA1234567890", "expired-session-token");

    final Instant now = testClock.instant();
    final STSTokenIdentifier expiredSTSTokenIdentifier = createToken("ASIA1234567890", now.minusSeconds(3600));

    try (MockedStatic<STSSecurityUtil> stsSecurityUtilMock = mockStatic(STSSecurityUtil.class);
         MockedStatic<OzoneManagerRatisUtils> ozoneManagerRatisUtilsMock = mockStatic(OzoneManagerRatisUtils.class)) {

      configureStsSecurityUtilMock(stsSecurityUtilMock, "expired-session-token", expiredSTSTokenIdentifier);

      // Return a non-successful response
      mockRatisSubmitWithInternalErrorResponse(ozoneManagerRatisUtilsMock);

      // Run the cleanup service
      final RevokedSTSTokenCleanupService revokedSTSTokenCleanupService = createAndRunCleanupService();

      assertThat(revokedSTSTokenCleanupService.getRunCount()).isEqualTo(1);
      assertThat(revokedSTSTokenCleanupService.getSubmittedDeletedEntryCount()).isZero();
    }
  }

  @Test
  public void handlesAllExpiredTokens() throws Exception {
    // If all the tokens in the table are expired on a particular run, ensure the metrics are updated appropriately
    revokedStsTokenTable.put("ASIA1234567890", "expired-session-token-1");
    revokedStsTokenTable.put("ASIA0123456789", "expired-session-token-2");
    revokedStsTokenTable.put("ASIA9876543210", "expired-session-token-3");

    final Instant now = testClock.instant();
    final STSTokenIdentifier expiredSTSTokenIdentifier1 = createToken("ASIA1234567890", now.minusSeconds(3600));
    final STSTokenIdentifier expiredSTSTokenIdentifier2 = createToken("ASIA0123456789", now.minusSeconds(7200));
    final STSTokenIdentifier expiredSTSTokenIdentifier3 = createToken("ASIA9876543210", now.minusSeconds(10800));

    final AtomicReference<OMRequest> capturedRequest = new AtomicReference<>();

    try (MockedStatic<STSSecurityUtil> stsSecurityUtilMock = mockStatic(STSSecurityUtil.class);
         MockedStatic<OzoneManagerRatisUtils> ozoneManagerRatisUtilsMock = mockStatic(OzoneManagerRatisUtils.class)) {

      configureStsSecurityUtilMock(stsSecurityUtilMock, "expired-session-token-1", expiredSTSTokenIdentifier1);
      configureStsSecurityUtilMock(stsSecurityUtilMock, "expired-session-token-2", expiredSTSTokenIdentifier2);
      configureStsSecurityUtilMock(stsSecurityUtilMock, "expired-session-token-3", expiredSTSTokenIdentifier3);

      mockRatisSubmitAndCapture(ozoneManagerRatisUtilsMock, capturedRequest);

      // Run the cleanup service
      final RevokedSTSTokenCleanupService revokedSTSTokenCleanupService = createAndRunCleanupService();

      assertThat(revokedSTSTokenCleanupService.getRunCount()).isEqualTo(1);
      assertThat(revokedSTSTokenCleanupService.getSubmittedDeletedEntryCount()).isEqualTo(3);

      final OMRequest omRequest = capturedRequest.get();
      assertThat(omRequest).isNotNull();
      assertThat(omRequest.getCmdType()).isEqualTo(Type.CleanupRevokedSTSTokens);

      final CleanupRevokedSTSTokensRequest cleanupRevokedSTSTokensRequest =
          omRequest.getCleanupRevokedSTSTokensRequest();
      assertThat(cleanupRevokedSTSTokensRequest.getAccessKeyIdList())
          .containsExactlyInAnyOrder("ASIA1234567890", "ASIA0123456789", "ASIA9876543210");
    }
  }

  private RevokedSTSTokenCleanupService createAndRunCleanupService() throws Exception {
    final RevokedSTSTokenCleanupService revokedSTSTokenCleanupService =
        new RevokedSTSTokenCleanupService(1, TimeUnit.HOURS, 1_000, ozoneManager);
    revokedSTSTokenCleanupService.runPeriodicalTaskNow();
    return revokedSTSTokenCleanupService;
  }

  private STSTokenIdentifier createToken(String accessKeyId, Instant expiry) {
    return new STSTokenIdentifier(
        accessKeyId, "AKIA999333000111", "arn:aws:iam::123456789012:role/test-role",
        expiry, "secretAccessKey", null, ENCRYPTION_KEY);
  }

  private void configureStsSecurityUtilMock(MockedStatic<STSSecurityUtil> stsSecurityUtilMock, String sessionToken,
      STSTokenIdentifier identifier) {
    stsSecurityUtilMock.when(
        () -> STSSecurityUtil.constructValidateAndDecryptSTSToken(eq(sessionToken), any(), any()))
        .thenReturn(identifier);
  }

  private void mockRatisSubmitAndCapture(MockedStatic<OzoneManagerRatisUtils> ozoneManagerRatisUtilsMock,
      AtomicReference<OMRequest> capturedRequest) {
    ozoneManagerRatisUtilsMock.when(
        () -> OzoneManagerRatisUtils.submitRequest(any(), any(), any(), anyLong()))
        .thenAnswer(invocation -> {
          final OMRequest omRequest = invocation.getArgument(1);
          capturedRequest.set(omRequest);
          return buildOkResponse(omRequest);
        });
  }

  private void mockRatisSubmitToFail(MockedStatic<OzoneManagerRatisUtils> ozoneManagerRatisUtilsMock,
      AtomicInteger submitAttempts) {
    ozoneManagerRatisUtilsMock.when(
        () -> OzoneManagerRatisUtils.submitRequest(any(), any(), any(), anyLong()))
        .thenAnswer(invocation -> {
          submitAttempts.incrementAndGet();
          throw new ServiceException("Simulated Ratis failure");
        });
  }

  private void mockRatisSubmitWithInternalErrorResponse(MockedStatic<OzoneManagerRatisUtils> omRatisUtilsMock) {
    omRatisUtilsMock.when(
        () -> OzoneManagerRatisUtils.submitRequest(any(), any(), any(), anyLong()))
        .thenReturn(OMResponse.newBuilder()
            .setCmdType(Type.CleanupRevokedSTSTokens)
            .setStatus(Status.INTERNAL_ERROR)
            .setSuccess(false)
            .build());
  }

  private static OMResponse buildOkResponse(OMRequest omRequest) {
    return OMResponse.newBuilder()
        .setCmdType(omRequest.getCmdType())
        .setStatus(OK)
        .setSuccess(true)
        .build();
  }
}


