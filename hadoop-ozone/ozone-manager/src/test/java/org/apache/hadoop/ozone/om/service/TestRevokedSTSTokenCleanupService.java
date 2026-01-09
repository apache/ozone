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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.db.StringInMemoryTestTable;
import org.apache.hadoop.ozone.om.OMConfigKeys;
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
  private OzoneConfiguration ozoneConfiguration;

  @BeforeEach
  public void setUp() {
    testClock = TestClock.newInstance();
    ozoneManager = mock(OzoneManager.class);
    ozoneConfiguration = new OzoneConfiguration();
    final OMMetadataManager omMetadataManager = mock(OMMetadataManager.class);
    revokedStsTokenTable = new StringInMemoryTestTable<>();

    when(ozoneManager.isLeaderReady()).thenReturn(true);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);
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

  @Test
  public void submitsMultipleRequestsWhenBatchSizeIsExceeded() throws Exception {
    // If the tokens exceed the configured batch size, multiple requests should be submitted
    final long nowMillis = testClock.millis();

    // Create 10 expired tokens
    for (int i = 0; i < 10; i++) {
      revokedStsTokenTable.put("session-token-" + i, nowMillis - TimeUnit.HOURS.toMillis(13));
    }

    // Set a very small ratisByteLimit (100 bytes) to force batching. A single token request will be small, but 10
    // will exceed this. The effective limit will be 90 bytes (90% of 100).
    ozoneConfiguration.setStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT, 100, StorageUnit.BYTES);

    final List<OMRequest> capturedRequests = new ArrayList<>();

    try (MockedStatic<OzoneManagerRatisUtils> ozoneManagerRatisUtilsMock = mockStatic(OzoneManagerRatisUtils.class)) {
      mockRatisSubmitAndCaptureRequests(ozoneManagerRatisUtilsMock, capturedRequests);

      // Run the cleanup service
      final RevokedSTSTokenCleanupService revokedSTSTokenCleanupService = createAndRunCleanupService();

      assertThat(revokedSTSTokenCleanupService.getRunCount()).isEqualTo(1);
      // There should be multiple requests
      assertThat(capturedRequests.size()).isEqualTo(2);

      // Verify all tokens were included across the requests
      final int totalTokens = capturedRequests.stream()
          .mapToInt(r -> r.getDeleteRevokedSTSTokensRequest().getSessionTokenList().size())
          .sum();
      assertThat(totalTokens).isEqualTo(10);
      assertThat(revokedSTSTokenCleanupService.getSubmittedDeletedEntryCount()).isEqualTo(10);
    }
  }

  @Test
  public void testSingleOversizedExpiredTokenAndItIsTheOnlyExpiredToken() throws Exception {
    // One sessionToken is larger than the ratisByteLimit, and it is the only expired token
    final long nowMillis = testClock.millis();
    // Serialized size for largeToken is 102 > 90 (the effective ratisByteLimit) .
    final String largeToken = new String(new char[100]).replace('\0', 'a');
    revokedStsTokenTable.put(largeToken, nowMillis - TimeUnit.HOURS.toMillis(13));

    ozoneConfiguration.setStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT, 100, StorageUnit.BYTES);

    final List<OMRequest> capturedRequests = new ArrayList<>();

    try (MockedStatic<OzoneManagerRatisUtils> ozoneManagerRatisUtilsMock = mockStatic(OzoneManagerRatisUtils.class)) {
      mockRatisSubmitAndCaptureRequests(ozoneManagerRatisUtilsMock, capturedRequests);

      final RevokedSTSTokenCleanupService revokedSTSTokenCleanupService = createAndRunCleanupService();

      assertThat(revokedSTSTokenCleanupService.getRunCount()).isEqualTo(1);
      // Single token exceeding ratisByteLimit is skipped
      assertThat(capturedRequests).isEmpty();
      assertThat(revokedSTSTokenCleanupService.getSubmittedDeletedEntryCount()).isZero();
    }
  }

  @Test
  public void testSingleOversizedExpiredTokenAndThereAreMultipleExpiredTokens() throws Exception {
    // One sessionToken is larger than the ratisByteLimit, and it is not the only expired token
    final long nowMillis = testClock.millis();
    final String smallToken = "session-token-j";
    final String largeToken = "session-token-k-" + new String(new char[90]).replace('\0', 'a'); // > 90 bytes

    revokedStsTokenTable.put(smallToken, nowMillis - TimeUnit.HOURS.toMillis(13));
    revokedStsTokenTable.put(largeToken, nowMillis - TimeUnit.HOURS.toMillis(13));

    ozoneConfiguration.setStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT, 100, StorageUnit.BYTES);

    final List<OMRequest> capturedRequests = new ArrayList<>();

    try (MockedStatic<OzoneManagerRatisUtils> ozoneManagerRatisUtilsMock = mockStatic(OzoneManagerRatisUtils.class)) {
      mockRatisSubmitAndCaptureRequests(ozoneManagerRatisUtilsMock, capturedRequests);

      final RevokedSTSTokenCleanupService revokedSTSTokenCleanupService = createAndRunCleanupService();

      assertThat(revokedSTSTokenCleanupService.getRunCount()).isEqualTo(1);
      assertThat(capturedRequests).hasSize(1);
      assertThat(revokedSTSTokenCleanupService.getSubmittedDeletedEntryCount()).isEqualTo(1);
    }
  }

  @Test
  public void testExpiredAndNonExpiredTokensWithSmallRatisByteLimit() throws Exception {
    // Expired and non-expired entries with ratisByteLimit of 100
    final long nowMillis = testClock.millis();

    revokedStsTokenTable.put("session-token-l", nowMillis - TimeUnit.HOURS.toMillis(13));
    revokedStsTokenTable.put("session-token-m", nowMillis - TimeUnit.HOURS.toMillis(1)); // Should be skipped
    revokedStsTokenTable.put("session-token-n", nowMillis - TimeUnit.HOURS.toMillis(13));

    ozoneConfiguration.setStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT, 100, StorageUnit.BYTES);

    final List<OMRequest> capturedRequests = new ArrayList<>();

    try (MockedStatic<OzoneManagerRatisUtils> ozoneManagerRatisUtilsMock = mockStatic(OzoneManagerRatisUtils.class)) {
      mockRatisSubmitAndCaptureRequests(ozoneManagerRatisUtilsMock, capturedRequests);

      final RevokedSTSTokenCleanupService revokedSTSTokenCleanupService = createAndRunCleanupService();

      assertThat(revokedSTSTokenCleanupService.getRunCount()).isEqualTo(1);
      // session-token-l and session-token-n fit in one batch.  session-token-m is ignored because it is not expired.
      assertThat(capturedRequests).hasSize(1);
      assertThat(capturedRequests.get(0).getDeleteRevokedSTSTokensRequest().getSessionTokenList())
          .containsExactly("session-token-l", "session-token-n");
      assertThat(revokedSTSTokenCleanupService.getSubmittedDeletedEntryCount()).isEqualTo(2);
    }
  }

  @Test
  public void testExpiredTokenMatchesRatisByteLimitExactly() throws Exception {
    // Force small batch of 100 bytes and test when the batch size is exactly ratisByteLimit
    final long nowMillis = testClock.millis();
    final String tokenMatchingRatisByteLimitWhenSerialized = new String(new char[88]).replace('\0', 'a');

    revokedStsTokenTable.put(tokenMatchingRatisByteLimitWhenSerialized, nowMillis - TimeUnit.HOURS.toMillis(13));

    ozoneConfiguration.setStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT, 100, StorageUnit.BYTES);

    final List<OMRequest> capturedRequests = new ArrayList<>();

    try (MockedStatic<OzoneManagerRatisUtils> ozoneManagerRatisUtilsMock = mockStatic(OzoneManagerRatisUtils.class)) {
      mockRatisSubmitAndCaptureRequests(ozoneManagerRatisUtilsMock, capturedRequests);

      final RevokedSTSTokenCleanupService revokedSTSTokenCleanupService = createAndRunCleanupService();

      assertThat(revokedSTSTokenCleanupService.getRunCount()).isEqualTo(1);
      assertThat(capturedRequests).hasSize(1);
      assertThat(revokedSTSTokenCleanupService.getSubmittedDeletedEntryCount()).isEqualTo(1);
    }
  }

  @Test
  public void testCallIdCountIncreasesAcrossBatches() throws Exception {
    // Force small batch of 40 bytea (which should trigger multiple calls to OzoneManagerRatisUtils.submitRequest)
    // and ensure the callIdCount increases across each batch
    // session-token-1 and session-token-2 are in first batch, and session-token-3 is in second batch.
    final long nowMillis = testClock.millis();

    revokedStsTokenTable.put("session-token-1", nowMillis - TimeUnit.HOURS.toMillis(13));
    revokedStsTokenTable.put("session-token-2", nowMillis - TimeUnit.HOURS.toMillis(13));
    revokedStsTokenTable.put("session-token-3", nowMillis - TimeUnit.HOURS.toMillis(13));

    ozoneConfiguration.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT, 40, StorageUnit.BYTES);

    final List<Long> capturedCallIdCounts = new ArrayList<>();

    try (MockedStatic<OzoneManagerRatisUtils> ozoneManagerRatisUtilsMock = mockStatic(OzoneManagerRatisUtils.class)) {
      // Capture the callIdCount (4th argument)
      ozoneManagerRatisUtilsMock.when(
          () -> OzoneManagerRatisUtils.submitRequest(any(), any(), any(), anyLong()))
          .thenAnswer(invocation -> {
            capturedCallIdCounts.add(invocation.getArgument(3));
            return buildOkResponse(invocation.getArgument(1));
          });

      final RevokedSTSTokenCleanupService revokedSTSTokenCleanupService = createAndRunCleanupService();

      assertThat(revokedSTSTokenCleanupService.getRunCount()).isEqualTo(1);
      assertThat(revokedSTSTokenCleanupService.getSubmittedDeletedEntryCount()).isEqualTo(3);
      assertThat(capturedCallIdCounts).hasSize(2);
      assertThat(capturedCallIdCounts.get(1)).isGreaterThan(capturedCallIdCounts.get(0));
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
    mockRatisSubmit(ozoneManagerRatisUtilsMock, capturedRequest::set);
  }

  private void mockRatisSubmitAndCaptureRequests(MockedStatic<OzoneManagerRatisUtils> ozoneManagerRatisUtilsMock,
      List<OMRequest> capturedRequests) {
    mockRatisSubmit(ozoneManagerRatisUtilsMock, capturedRequests::add);
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

  private void mockRatisSubmit(MockedStatic<OzoneManagerRatisUtils> ozoneManagerRatisUtilsMock,
      Consumer<OMRequest> requestConsumer) {
    ozoneManagerRatisUtilsMock.when(
            () -> OzoneManagerRatisUtils.submitRequest(any(), any(), any(), anyLong()))
        .thenAnswer(invocation -> {
          final OMRequest omRequest = invocation.getArgument(1);
          requestConsumer.accept(omRequest);
          return buildOkResponse(omRequest);
        });
  }
}


