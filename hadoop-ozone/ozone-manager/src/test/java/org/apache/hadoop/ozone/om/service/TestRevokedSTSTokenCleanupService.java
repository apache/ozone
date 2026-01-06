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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.protobuf.ServiceException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.utils.db.StringInMemoryTestTable;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteRevokedSTSTokensRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Unit tests for {@link RevokedSTSTokenCleanupService}.
 */
public class TestRevokedSTSTokenCleanupService {
  private OzoneManager ozoneManager;
  private StringInMemoryTestTable<Long> revokedStsTokenTable;
  private TestClock testClock;

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
    // If there are two revoked entries, one expired and one not expired, only the expired session token should be
    // submitted for cleanup.
    final long nowMillis = testClock.millis();
    final long expiredCreationTimeMillis = nowMillis - TimeUnit.HOURS.toMillis(13); // older than 12h threshold
    final long validCreationTimeMillis = nowMillis - TimeUnit.HOURS.toMillis(1);
    revokedStsTokenTable.put("session-token-a", expiredCreationTimeMillis);
    revokedStsTokenTable.put("session-token-b", validCreationTimeMillis);

    final AtomicReference<OMRequest> capturedRequest = new AtomicReference<>();

    try (MockedStatic<OzoneManagerRatisUtils> ozoneManagerRatisUtilsMock = mockStatic(OzoneManagerRatisUtils.class)) {
      mockRatisSubmitAndCapture(ozoneManagerRatisUtilsMock, capturedRequest);

      // Run the cleanup service
      final RevokedSTSTokenCleanupService revokedSTSTokenCleanupService = createAndRunCleanupService();

      assertThat(revokedSTSTokenCleanupService.getRunCount()).isEqualTo(1);
      assertThat(revokedSTSTokenCleanupService.getSubmittedDeletedEntryCount()).isEqualTo(1);

      final OMRequest omRequest = capturedRequest.get();
      assertThat(omRequest).isNotNull();
      assertThat(omRequest.getCmdType()).isEqualTo(Type.DeleteRevokedSTSTokens);

      final DeleteRevokedSTSTokensRequest deleteRevokedSTSTokensRequest =
          omRequest.getDeleteRevokedSTSTokensRequest();
      assertThat(deleteRevokedSTSTokensRequest.getSessionTokenList()).containsExactly("session-token-a");
    }
  }

  @Test
  public void doesNotSubmitRequestWhenThereAreNoExpiredTokens() throws Exception {
    // If only non-expired entries exist in the revoked sts token table, no cleanup request should be submitted and
    // no metrics should be updated.
    final long nowMillis = testClock.millis();
    revokedStsTokenTable.put("session-token-c", nowMillis - TimeUnit.HOURS.toMillis(1));
    revokedStsTokenTable.put("session-token-d", nowMillis - TimeUnit.HOURS.toMillis(2));

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
    final long nowMillis = testClock.millis();
    revokedStsTokenTable.put("session-token-e", nowMillis - TimeUnit.HOURS.toMillis(13));
    revokedStsTokenTable.put("session-token-f", nowMillis - TimeUnit.HOURS.toMillis(14));

    final AtomicInteger submitAttempts = new AtomicInteger(0);

    try (MockedStatic<OzoneManagerRatisUtils> ozoneManagerRatisUtilsMock = mockStatic(OzoneManagerRatisUtils.class)) {
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
    final long nowMillis = testClock.millis();
    revokedStsTokenTable.put("session-token-f", nowMillis - TimeUnit.HOURS.toMillis(20));

    try (MockedStatic<OzoneManagerRatisUtils> ozoneManagerRatisUtilsMock = mockStatic(OzoneManagerRatisUtils.class)) {
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
    final long nowMillis = testClock.millis();
    revokedStsTokenTable.put("session-token-g", nowMillis - TimeUnit.HOURS.toMillis(13));
    revokedStsTokenTable.put("session-token-h", nowMillis - TimeUnit.HOURS.toMillis(14));
    revokedStsTokenTable.put("session-token-i", nowMillis - TimeUnit.HOURS.toMillis(15));

    final AtomicReference<OMRequest> capturedRequest = new AtomicReference<>();

    try (MockedStatic<OzoneManagerRatisUtils> ozoneManagerRatisUtilsMock = mockStatic(OzoneManagerRatisUtils.class)) {
      mockRatisSubmitAndCapture(ozoneManagerRatisUtilsMock, capturedRequest);

      // Run the cleanup service
      final RevokedSTSTokenCleanupService revokedSTSTokenCleanupService = createAndRunCleanupService();

      assertThat(revokedSTSTokenCleanupService.getRunCount()).isEqualTo(1);
      assertThat(revokedSTSTokenCleanupService.getSubmittedDeletedEntryCount()).isEqualTo(3);

      final OMRequest omRequest = capturedRequest.get();
      assertThat(omRequest).isNotNull();
      assertThat(omRequest.getCmdType()).isEqualTo(Type.DeleteRevokedSTSTokens);

      final DeleteRevokedSTSTokensRequest deleteRevokedSTSTokensRequest =
          omRequest.getDeleteRevokedSTSTokensRequest();
      assertThat(deleteRevokedSTSTokensRequest.getSessionTokenList())
          .containsExactlyInAnyOrder("session-token-g", "session-token-h", "session-token-i");
    }
  }

  private RevokedSTSTokenCleanupService createAndRunCleanupService() throws Exception {
    final RevokedSTSTokenCleanupService revokedSTSTokenCleanupService =
        new RevokedSTSTokenCleanupService(1, TimeUnit.HOURS, 1_000, ozoneManager);
    revokedSTSTokenCleanupService.runPeriodicalTaskNow();
    return revokedSTSTokenCleanupService;
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
            .setCmdType(Type.DeleteRevokedSTSTokens)
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


