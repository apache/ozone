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

package org.apache.hadoop.hdds.scm.server;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.ozone.test.MetricsAsserts.assertGauge;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.util.MetricUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestSCMFullContainerReportLeaseManager {

  @Test
  void shouldRejectInvalidConfiguration() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SCMFullContainerReportLeaseManager(
            0, 5_000L, true, () -> 0L, null));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SCMFullContainerReportLeaseManager(
            1, 0L, true, () -> 0L, null));
  }

  @Test
  void shouldLimitOutstandingLeasesAndReleaseAfterReport() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, true, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();

    long firstLease = requireLease(leaseManager, firstDn);
    OptionalLong secondLease = leaseManager.requestLease(secondDn);

    assertThat(firstLease).isNotZero();
    assertThat(secondLease).isEmpty();
    SCMFullContainerReportLeaseManager.LeaseClaim claim =
        requireClaim(leaseManager, firstDn, firstLease);
    assertThat(claim).isNotNull();
    assertThat(leaseManager.claimLease(firstDn, firstLease)).isEmpty();
    assertThat(leaseManager.requestLease(secondDn)).isEmpty();

    claim.complete(true);

    long secondLeaseAfterRelease = requireLease(leaseManager, secondDn);
    assertThat(secondLeaseAfterRelease).isNotZero();
  }

  @Test
  void shouldPreserveOutstandingReportsWhenRateLimitingChanges() {
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, true, () -> 0L, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();
    DatanodeDetails thirdDn = randomDatanodeDetails();
    DatanodeDetails nextDn = randomDatanodeDetails();
    long firstLease = requireLease(leaseManager, firstDn);
    SCMFullContainerReportLeaseManager.LeaseClaim firstClaim = requireClaim(leaseManager, firstDn, firstLease);
    assertThat(firstClaim.startProcessing()).isTrue();
    assertThat(leaseManager.requestLease(secondDn)).isEmpty();

    leaseManager.setRateLimitEnabled(false);
    long secondLease = requireLease(leaseManager, secondDn);
    long thirdLease = requireLease(leaseManager, thirdDn);
    leaseManager.setRateLimitEnabled(true);

    assertThat(leaseManager.requestLease(nextDn)).isEmpty();
    SCMFullContainerReportLeaseManager.LeaseClaim secondClaim = requireClaim(leaseManager, secondDn, secondLease);
    SCMFullContainerReportLeaseManager.LeaseClaim thirdClaim = requireClaim(leaseManager, thirdDn, thirdLease);
    firstClaim.complete(true);
    assertThat(leaseManager.requestLease(nextDn)).isEmpty();
    secondClaim.complete(false);
    assertThat(leaseManager.requestLease(nextDn)).isEmpty();
    thirdClaim.complete(true);
    assertThat(leaseManager.requestLease(nextDn)).isPresent();
  }

  @Test
  void shouldRejectDuplicateReportsWhenRateLimitingIsDisabled() {
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, false, () -> 0L, null);
    DatanodeDetails datanode = randomDatanodeDetails();
    long lease = requireLease(leaseManager, datanode);
    SCMFullContainerReportLeaseManager.LeaseClaim claim = requireClaim(leaseManager, datanode, lease);

    assertThat(leaseManager.requestLease(datanode)).isEmpty();
    assertThat(leaseManager.claimLease(datanode, lease)).isEmpty();
    assertThat(claim.startProcessing()).isTrue();
    assertThat(leaseManager.requestLease(datanode)).isEmpty();
    claim.complete(true);
    assertThat(leaseManager.requestLease(datanode)).isPresent();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void shouldValidateLeaseIdsAndExpiry(boolean rateLimitEnabled) {
    AtomicLong now = new AtomicLong();
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 100L, rateLimitEnabled, now::get, null);
    DatanodeDetails datanode = randomDatanodeDetails();
    long lease = requireLease(leaseManager, datanode);

    assertThat(leaseManager.claimLease(datanode, 0L)).isEmpty();
    assertThat(leaseManager.claimLease(datanode, lease == 1L ? 2L : 1L)).isEmpty();
    long replacementLease = requireLease(leaseManager, datanode);
    assertThat(leaseManager.claimLease(datanode, lease)).isEmpty();
    now.set(100L);
    assertThat(leaseManager.claimLease(datanode, replacementLease)).isEmpty();

    long newLease = requireLease(leaseManager, datanode);
    assertThat(leaseManager.claimLease(datanode, newLease)).isPresent();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void shouldReportRateLimitModeAndOutstandingLeases(boolean rateLimitEnabled) {
    SCMFullContainerReportLeaseMetrics metrics = SCMFullContainerReportLeaseMetrics.create("scm");
    String source = MetricUtil.getMetricsSourceName(SCMFullContainerReportLeaseMetrics.class.getSimpleName(), "scm");
    try {
      SCMFullContainerReportLeaseManager leaseManager =
          new SCMFullContainerReportLeaseManager(1, 5_000L, rateLimitEnabled, () -> 0L, metrics);
      assertGauge("FCRRateLimitEnabled", rateLimitEnabled ? 1 : 0, getMetrics(source));
      DatanodeDetails firstDn = randomDatanodeDetails();
      DatanodeDetails secondDn = randomDatanodeDetails();
      long firstLease = requireLease(leaseManager, firstDn);
      leaseManager.setRateLimitEnabled(false);
      long secondLease = requireLease(leaseManager, secondDn);

      assertGauge("FCRRateLimitEnabled", 0, getMetrics(source));
      assertGauge("NumFCRLeasesOutstanding", 2L, getMetrics(source));
      leaseManager.setRateLimitEnabled(true);
      assertGauge("FCRRateLimitEnabled", 1, getMetrics(source));
      assertGauge("NumFCRLeasesOutstanding", 2L, getMetrics(source));
      requireClaim(leaseManager, firstDn, firstLease).complete(true);
      requireClaim(leaseManager, secondDn, secondLease).complete(false);
      assertGauge("NumFCRLeasesOutstanding", 0L, getMetrics(source));
      assertThat(metrics.getNumFCRLeaseRequests()).isEqualTo(2L);
      assertThat(metrics.getNumFCRLeasesGranted()).isEqualTo(2L);
      assertThat(metrics.getNumFCRReportsProcessedWithLease()).isEqualTo(1L);
    } finally {
      metrics.unregister();
    }
  }

  @Test
  void shouldUnregisterOnlyOwnMetricsSource() {
    boolean miniClusterMode = DefaultMetricsSystem.inMiniClusterMode();
    DefaultMetricsSystem.setMiniClusterMode(true);
    MetricsSystem metricsSystem = DefaultMetricsSystem.initialize("FCRLeaseMetricsTest");
    String source = SCMFullContainerReportLeaseMetrics.class.getSimpleName();
    String firstSource = MetricUtil.getMetricsSourceName(source, "scm1");
    String secondSource = MetricUtil.getMetricsSourceName(source, "scm2");
    try {
      SCMFullContainerReportLeaseMetrics first = SCMFullContainerReportLeaseMetrics.create("scm1");
      SCMFullContainerReportLeaseMetrics second = SCMFullContainerReportLeaseMetrics.create("scm2");

      second.unregister();
      assertThat(metricsSystem.getSource(secondSource)).isNull();
      assertThat(metricsSystem.getSource(firstSource)).isNotNull();

      first.unregister();
      assertThat(metricsSystem.getSource(firstSource)).isNull();
    } finally {
      metricsSystem.unregisterSource(firstSource);
      metricsSystem.unregisterSource(secondSource);
      DefaultMetricsSystem.shutdown();
      DefaultMetricsSystem.setMiniClusterMode(miniClusterMode);
    }
  }

  @Test
  void shouldRejectLeasesForOtherDatanodes() {
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(2, 5_000L, true, () -> 0L, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();

    long firstLease = requireLease(leaseManager, firstDn);
    long secondLease = requireLease(leaseManager, secondDn);

    assertThat(leaseManager.claimLease(randomDatanodeDetails(), firstLease)).isEmpty();
    assertThat(leaseManager.claimLease(secondDn, firstLease)).isEmpty();
    assertThat(leaseManager.claimLease(firstDn, secondLease)).isEmpty();

    requireClaim(leaseManager, firstDn, firstLease).complete(true);
    requireClaim(leaseManager, secondDn, secondLease).complete(true);
  }

  @Test
  void shouldKeepLeasesLocalToManager() {
    SCMFullContainerReportLeaseManager firstManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, true, () -> 0L, null);
    SCMFullContainerReportLeaseManager secondManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, true, () -> 0L, null);
    DatanodeDetails datanode = randomDatanodeDetails();

    long firstLease = requireLease(firstManager, datanode);
    assertThat(secondManager.claimLease(datanode, firstLease)).isEmpty();
    long secondLease = requireLease(secondManager, datanode);

    assertThat(firstManager.claimLease(datanode, secondLease)).isEmpty();
    assertThat(secondManager.claimLease(datanode, firstLease)).isEmpty();
    requireClaim(firstManager, datanode, firstLease).complete(true);
    requireClaim(secondManager, datanode, secondLease).complete(true);
  }

  @Test
  void shouldPreserveDeferredRegistrationUntilReportIsProcessed() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, true, now::get, null);
    DatanodeDetails datanode = randomDatanodeDetails();

    leaseManager.markFullContainerReportDeferred(datanode);
    long firstLease = requireLease(leaseManager, datanode);
    SCMFullContainerReportLeaseManager.LeaseClaim firstClaim =
        requireClaim(leaseManager, datanode, firstLease);

    assertThat(firstClaim).isNotNull();
    assertThat(firstClaim.isRegistrationReport()).isTrue();

    firstClaim.complete(false);
    long retryLease = requireLease(leaseManager, datanode);
    SCMFullContainerReportLeaseManager.LeaseClaim retryClaim =
        requireClaim(leaseManager, datanode, retryLease);

    assertThat(retryClaim).isNotNull();
    assertThat(retryClaim.isRegistrationReport()).isTrue();

    retryClaim.complete(true);
    long laterLease = requireLease(leaseManager, datanode);
    SCMFullContainerReportLeaseManager.LeaseClaim laterClaim =
        requireClaim(leaseManager, datanode, laterLease);

    assertThat(laterClaim).isNotNull();
    assertThat(laterClaim.isRegistrationReport()).isFalse();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void shouldRetainLeaseAfterGrantExpiry(boolean processingStarted) {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 100L, true, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();

    long firstLease = requireLease(leaseManager, firstDn);
    now.addAndGet(99L);
    SCMFullContainerReportLeaseManager.LeaseClaim claim =
        requireClaim(leaseManager, firstDn, firstLease);
    assertThat(claim).isNotNull();
    assertThat(leaseManager.requestLease(secondDn)).isEmpty();
    if (processingStarted) {
      assertThat(claim.startProcessing()).isTrue();
    }

    now.addAndGet(100L);

    assertThat(leaseManager.requestLease(secondDn)).isEmpty();
    if (!processingStarted) {
      assertThat(claim.startProcessing()).isTrue();
    }
    claim.complete(true);
    assertThat(leaseManager.requestLease(secondDn)).isPresent();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void shouldKeepNewRegistrationPendingWhenOlderReportCompletes(boolean processingStarted) {
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, true, () -> 0L, null);
    DatanodeDetails datanode = randomDatanodeDetails();
    leaseManager.markFullContainerReportDeferred(datanode);
    long oldLease = requireLease(leaseManager, datanode);
    SCMFullContainerReportLeaseManager.LeaseClaim oldClaim = requireClaim(leaseManager, datanode, oldLease);
    if (processingStarted) {
      assertThat(oldClaim.startProcessing()).isTrue();
    }

    // Re-registration needs a new FCR, even if an earlier registration's report is still outstanding.
    leaseManager.markFullContainerReportDeferred(datanode);
    assertThat(leaseManager.requestLease(datanode)).isEmpty();
    if (!processingStarted) {
      assertThat(oldClaim.startProcessing()).isTrue();
    }
    oldClaim.complete(true);

    long newLease = requireLease(leaseManager, datanode);
    SCMFullContainerReportLeaseManager.LeaseClaim newClaim = requireClaim(leaseManager, datanode, newLease);
    assertThat(newClaim.isRegistrationReport()).isTrue();
    newClaim.complete(true);

    long periodicLease = requireLease(leaseManager, datanode);
    assertThat(requireClaim(leaseManager, datanode, periodicLease).isRegistrationReport()).isFalse();
  }

  @Test
  void shouldInvalidateUnclaimedGrantOnReregistration() {
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, true, () -> 0L, null);
    DatanodeDetails datanode = randomDatanodeDetails();
    leaseManager.markFullContainerReportDeferred(datanode);
    long oldLease = requireLease(leaseManager, datanode);

    leaseManager.markFullContainerReportDeferred(datanode);

    assertThat(leaseManager.claimLease(datanode, oldLease)).isEmpty();
    long newLease = requireLease(leaseManager, datanode);
    assertThat(requireClaim(leaseManager, datanode, newLease).isRegistrationReport()).isTrue();
  }

  @Test
  void shouldStartProcessingClaimOnlyOnce() {
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, true, () -> 0L, null);
    DatanodeDetails datanode = randomDatanodeDetails();
    long lease = requireLease(leaseManager, datanode);
    SCMFullContainerReportLeaseManager.LeaseClaim claim = requireClaim(leaseManager, datanode, lease);

    assertThat(claim.startProcessing()).isTrue();
    assertThat(claim.startProcessing()).isFalse();
    assertThat(leaseManager.requestLease(randomDatanodeDetails())).isEmpty();
    claim.complete(false);
    assertThat(leaseManager.requestLease(datanode)).isPresent();
  }

  @Test
  void shouldRetainQueuedRegistrationReportAfterGrantExpiry() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 100L, true, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();
    ContainerReportQueue queue = new ContainerReportQueue();
    leaseManager.markFullContainerReportDeferred(firstDn);
    long lease = requireLease(leaseManager, firstDn);
    SCMFullContainerReportLeaseManager.LeaseClaim claim = requireClaim(leaseManager, firstDn, lease);
    ContainerReportsProto report = ContainerReportsProto.newBuilder()
        .addReports(ContainerReplicaProto.newBuilder().setContainerID(1L).setState(ContainerReplicaProto.State.CLOSED))
        .build();
    queue.add(new ContainerReportFromDatanode(firstDn, report, claim));

    now.addAndGet(1_000L);

    assertThat(leaseManager.requestLease(secondDn)).isEmpty();
    ContainerReportFromDatanode queuedReport = (ContainerReportFromDatanode) queue.remove();
    assertThat(queuedReport.isRegistrationReport()).isTrue();
    assertThat(queuedReport.getReport().getReportsCount()).isEqualTo(1);
    assertThat(queuedReport.startProcessing()).isTrue();
    assertThat(leaseManager.requestLease(secondDn)).isEmpty();
    queuedReport.complete(true);
    long secondLease = requireLease(leaseManager, secondDn);
    requireClaim(leaseManager, secondDn, secondLease).complete(true);

    long nextLease = requireLease(leaseManager, firstDn);
    assertThat(requireClaim(leaseManager, firstDn, nextLease).isRegistrationReport()).isFalse();
  }

  @Test
  void shouldNotReleaseProcessingLeaseForRepeatedExpiredReport() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 100L, true, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();
    long lease = requireLease(leaseManager, firstDn);
    SCMFullContainerReportLeaseManager.LeaseClaim claim = requireClaim(leaseManager, firstDn, lease);
    assertThat(claim.startProcessing()).isTrue();

    now.addAndGet(1_000L);

    assertThat(leaseManager.claimLease(firstDn, lease)).isEmpty();
    assertThat(leaseManager.requestLease(secondDn)).isEmpty();
    claim.complete(true);
    assertThat(leaseManager.requestLease(secondDn)).isPresent();
  }

  @Test
  void shouldRetainClaimedLeaseUntilRemovedReportIsReleased() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 100L, true, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();
    long lease = requireLease(leaseManager, firstDn);
    SCMFullContainerReportLeaseManager.LeaseClaim claim = requireClaim(leaseManager, firstDn, lease);
    ContainerReportQueue queue = new ContainerReportQueue();
    queue.add(new ContainerReportFromDatanode(firstDn, ContainerReportsProto.getDefaultInstance(), claim));

    leaseManager.removeDatanode(firstDn);

    assertThat(leaseManager.requestLease(secondDn)).isEmpty();
    queue.clear();
    assertThat(leaseManager.requestLease(secondDn)).isPresent();
  }

  @Test
  void shouldNotReserveCapacityForDeniedRequests() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, true, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();
    DatanodeDetails thirdDn = randomDatanodeDetails();
    DatanodeDetails newDn = randomDatanodeDetails();

    long firstLease = requireLease(leaseManager, firstDn);
    assertThat(leaseManager.requestLease(secondDn)).isEmpty();
    assertThat(leaseManager.requestLease(thirdDn)).isEmpty();

    requireClaim(leaseManager, firstDn, firstLease).complete(true);

    // The earlier denied datanode need not retry before another datanode can use the free slot.
    now.incrementAndGet();
    long newLease = requireLease(leaseManager, newDn);
    requireClaim(leaseManager, newDn, newLease).complete(true);

    long thirdLease = requireLease(leaseManager, thirdDn);
    assertThat(leaseManager.requestLease(secondDn)).isEmpty();
    requireClaim(leaseManager, thirdDn, thirdLease).complete(true);

    long secondLease = requireLease(leaseManager, secondDn);
    requireClaim(leaseManager, secondDn, secondLease).complete(true);
  }

  @ParameterizedTest
  @ValueSource(ints = {20, 23})
  void shouldFillEveryStartupBatchWithReverseRetryOrder(int datanodeCount) {
    int maxOutstandingLeases = 10;
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(maxOutstandingLeases, 5_000L, true, now::get, null);
    List<DatanodeDetails> pendingDatanodes = new ArrayList<>(datanodeCount);
    for (int i = 0; i < datanodeCount; i++) {
      pendingDatanodes.add(randomDatanodeDetails());
    }

    int completedReports = 0;
    int batches = 0;
    while (!pendingDatanodes.isEmpty()) {
      int expectedBatchSize = Math.min(maxOutstandingLeases, pendingDatanodes.size());
      List<SCMFullContainerReportLeaseManager.LeaseClaim> claims = new ArrayList<>(maxOutstandingLeases);
      Iterator<DatanodeDetails> iterator = pendingDatanodes.iterator();
      while (iterator.hasNext()) {
        DatanodeDetails datanode = iterator.next();
        OptionalLong lease = leaseManager.requestLease(datanode);
        if (claims.size() < maxOutstandingLeases) {
          assertThat(lease).as("free slot in batch %s", batches + 1).isPresent();
          claims.add(requireClaim(leaseManager, datanode, lease.getAsLong()));
          iterator.remove();
        } else {
          assertThat(lease).as("all %s slots occupied in batch %s", maxOutstandingLeases, batches + 1).isEmpty();
        }
      }
      assertThat(claims).as("claimed leases in batch %s", batches + 1).hasSize(expectedBatchSize);

      for (SCMFullContainerReportLeaseManager.LeaseClaim claim : claims) {
        assertThat(claim.startProcessing()).isTrue();
      }
      now.incrementAndGet();
      for (SCMFullContainerReportLeaseManager.LeaseClaim claim : claims) {
        claim.complete(true);
        completedReports++;
      }
      batches++;
      if (batches == 1) {
        // Retry in reverse of the initial request order, before any denied request can expire.
        Collections.reverse(pendingDatanodes);
      }
    }

    assertThat(completedReports).isEqualTo(datanodeCount);
    assertThat(batches).isEqualTo((datanodeCount + maxOutstandingLeases - 1) / maxOutstandingLeases);
  }

  @Test
  void shouldRemoveGrantAndDeferredRegistration() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, true, now::get, null);
    DatanodeDetails removedDn = randomDatanodeDetails();
    DatanodeDetails nextDn = randomDatanodeDetails();

    leaseManager.markFullContainerReportDeferred(removedDn);
    assertThat(leaseManager.requestLease(removedDn)).isPresent();

    leaseManager.removeDatanode(removedDn);

    long nextLease = requireLease(leaseManager, nextDn);
    assertThat(nextLease).isNotZero();
    requireClaim(leaseManager, nextDn, nextLease).complete(true);

    long newLease = requireLease(leaseManager, removedDn);
    SCMFullContainerReportLeaseManager.LeaseClaim claim =
        requireClaim(leaseManager, removedDn, newLease);
    assertThat(claim).isNotNull();
    assertThat(claim.isRegistrationReport()).isFalse();
  }

  private static long requireLease(
      SCMFullContainerReportLeaseManager leaseManager,
      DatanodeDetails datanode) {
    OptionalLong lease = leaseManager.requestLease(datanode);
    assertThat(lease).isPresent();
    return lease.getAsLong();
  }

  private static SCMFullContainerReportLeaseManager.LeaseClaim requireClaim(
      SCMFullContainerReportLeaseManager leaseManager,
      DatanodeDetails datanode, long leaseId) {
    Optional<SCMFullContainerReportLeaseManager.LeaseClaim> claim =
        leaseManager.claimLease(datanode, leaseId);
    assertThat(claim).isPresent();
    return claim.get();
  }
}
