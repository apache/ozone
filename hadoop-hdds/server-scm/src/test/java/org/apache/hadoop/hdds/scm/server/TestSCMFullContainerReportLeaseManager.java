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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestSCMFullContainerReportLeaseManager {

  @Test
  void shouldRejectInvalidConfiguration() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SCMFullContainerReportLeaseManager(
            0, 5_000L, () -> 0L, null));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SCMFullContainerReportLeaseManager(
            1, 0L, () -> 0L, null));
  }

  @Test
  void shouldLimitOutstandingLeasesAndReleaseAfterReport() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();

    long firstLease = requireLease(leaseManager, firstDn, 7L);
    OptionalLong secondLease = leaseManager.requestLease(secondDn, 7L);

    assertThat(firstLease).isNotZero();
    assertThat(secondLease).isEmpty();
    SCMFullContainerReportLeaseManager.LeaseClaim claim =
        requireClaim(leaseManager, firstDn, 7L, firstLease);
    assertThat(claim).isNotNull();
    assertThat(leaseManager.claimLease(firstDn, OptionalLong.of(7L), 7L,
        firstLease)).isEmpty();
    assertThat(leaseManager.requestLease(secondDn, 7L)).isEmpty();

    claim.complete(true);

    long secondLeaseAfterRelease = requireLease(leaseManager, secondDn, 7L);
    assertThat(secondLeaseAfterRelease).isNotZero();
  }

  @Test
  void shouldPreserveOutstandingReportsWhenRateLimitingChanges() {
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, () -> 0L, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();
    DatanodeDetails thirdDn = randomDatanodeDetails();
    DatanodeDetails nextDn = randomDatanodeDetails();
    long firstLease = requireLease(leaseManager, firstDn, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim firstClaim = requireClaim(leaseManager, firstDn, 7L, firstLease);
    assertThat(firstClaim.startProcessing()).isTrue();
    assertThat(leaseManager.requestLease(secondDn, 7L)).isEmpty();

    leaseManager.setRateLimitEnabled(false);
    long secondLease = requireLease(leaseManager, secondDn, 7L);
    long thirdLease = requireLease(leaseManager, thirdDn, 7L);
    leaseManager.setRateLimitEnabled(true);

    assertThat(leaseManager.requestLease(nextDn, 7L)).isEmpty();
    SCMFullContainerReportLeaseManager.LeaseClaim secondClaim = requireClaim(leaseManager, secondDn, 7L, secondLease);
    SCMFullContainerReportLeaseManager.LeaseClaim thirdClaim = requireClaim(leaseManager, thirdDn, 7L, thirdLease);
    firstClaim.complete(true);
    assertThat(leaseManager.requestLease(nextDn, 7L)).isEmpty();
    secondClaim.complete(false);
    assertThat(leaseManager.requestLease(nextDn, 7L)).isEmpty();
    thirdClaim.complete(true);
    assertThat(leaseManager.requestLease(nextDn, 7L)).isPresent();
  }

  @Test
  void shouldRejectDuplicateReportsWhenRateLimitingIsDisabled() {
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, () -> 0L, null);
    leaseManager.setRateLimitEnabled(false);
    DatanodeDetails datanode = randomDatanodeDetails();
    long lease = requireLease(leaseManager, datanode, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim claim = requireClaim(leaseManager, datanode, 7L, lease);

    assertThat(leaseManager.requestLease(datanode, 7L)).isEmpty();
    assertThat(leaseManager.claimLease(datanode, OptionalLong.of(7L), 7L, lease)).isEmpty();
    assertThat(claim.startProcessing()).isTrue();
    assertThat(leaseManager.requestLease(datanode, 7L)).isEmpty();
    claim.complete(true);
    assertThat(leaseManager.requestLease(datanode, 7L)).isPresent();
  }

  @Test
  void shouldValidateLeasesWhenRateLimitingIsDisabled() {
    AtomicLong now = new AtomicLong();
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 100L, now::get, null);
    leaseManager.setRateLimitEnabled(false);
    DatanodeDetails datanode = randomDatanodeDetails();
    long lease = requireLease(leaseManager, datanode, 7L);

    assertThat(leaseManager.claimLease(datanode, OptionalLong.of(7L), 7L, 0L)).isEmpty();
    assertThat(leaseManager.claimLease(datanode, OptionalLong.of(7L), 7L, lease == 1L ? 2L : 1L)).isEmpty();
    assertThat(leaseManager.claimLease(datanode, OptionalLong.of(8L), 7L, lease)).isEmpty();
    assertThat(leaseManager.claimLease(datanode, OptionalLong.empty(), 7L, lease)).isEmpty();
    now.set(100L);
    assertThat(leaseManager.claimLease(datanode, OptionalLong.of(7L), 7L, lease)).isEmpty();

    long newLease = requireLease(leaseManager, datanode, 8L);
    assertThat(leaseManager.claimLease(datanode, OptionalLong.of(8L), 8L, newLease)).isPresent();
  }

  @Test
  void shouldReportRateLimitModeAndOutstandingLeases() {
    SCMFullContainerReportLeaseMetrics metrics = SCMFullContainerReportLeaseMetrics.create();
    String source = SCMFullContainerReportLeaseMetrics.class.getSimpleName();
    try {
      SCMFullContainerReportLeaseManager leaseManager =
          new SCMFullContainerReportLeaseManager(1, 5_000L, () -> 0L, metrics);
      assertGauge("FCRRateLimitEnabled", 1, getMetrics(source));
      DatanodeDetails firstDn = randomDatanodeDetails();
      DatanodeDetails secondDn = randomDatanodeDetails();
      long firstLease = requireLease(leaseManager, firstDn, 7L);
      leaseManager.setRateLimitEnabled(false);
      long secondLease = requireLease(leaseManager, secondDn, 7L);

      assertGauge("FCRRateLimitEnabled", 0, getMetrics(source));
      assertGauge("NumFCRLeasesOutstanding", 2L, getMetrics(source));
      leaseManager.setRateLimitEnabled(true);
      assertGauge("FCRRateLimitEnabled", 1, getMetrics(source));
      assertGauge("NumFCRLeasesOutstanding", 2L, getMetrics(source));
      requireClaim(leaseManager, firstDn, 7L, firstLease).complete(true);
      requireClaim(leaseManager, secondDn, 7L, secondLease).complete(false);
      assertGauge("NumFCRLeasesOutstanding", 0L, getMetrics(source));
      assertThat(metrics.getNumFCRLeaseRequests()).isEqualTo(2L);
      assertThat(metrics.getNumFCRLeasesGranted()).isEqualTo(2L);
      assertThat(metrics.getNumFCRReportsProcessedWithLease()).isEqualTo(1L);
    } finally {
      metrics.unregister();
    }
  }

  @Test
  void shouldRejectExpiredAndWrongTermLeases() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(2, 100L, now::get, null);
    DatanodeDetails wrongTermDatanode = randomDatanodeDetails();
    DatanodeDetails expiredDatanode = randomDatanodeDetails();

    long wrongTermLease = requireLease(leaseManager, wrongTermDatanode, 3L);
    long expiredLease = requireLease(leaseManager, expiredDatanode, 3L);

    assertThat(leaseManager.claimLease(wrongTermDatanode,
        OptionalLong.of(4L), 3L, wrongTermLease)).isEmpty();

    now.addAndGet(101L);
    assertThat(leaseManager.claimLease(expiredDatanode,
        OptionalLong.of(3L), 3L, expiredLease)).isEmpty();
    assertThat(leaseManager.requestLease(wrongTermDatanode, 3L)).isPresent();
  }

  @Test
  void shouldInvalidateOldTermLeasesWhenGrantingNewTerm() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();

    long oldTermLease = requireLease(leaseManager, firstDn, 3L);
    long newTermLease = requireLease(leaseManager, secondDn, 4L);

    assertThat(oldTermLease).isNotZero();
    assertThat(newTermLease).isNotZero();
    assertThat(leaseManager.claimLease(firstDn, OptionalLong.of(3L), 3L,
        oldTermLease)).isEmpty();
    assertThat(leaseManager.claimLease(secondDn, OptionalLong.of(4L), 4L,
        newTermLease)).isPresent();
  }

  @Test
  void shouldPreserveDeferredRegistrationUntilReportIsProcessed() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, now::get, null);
    DatanodeDetails datanode = randomDatanodeDetails();

    leaseManager.markFullContainerReportDeferred(datanode);
    long firstLease = requireLease(leaseManager, datanode, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim firstClaim =
        requireClaim(leaseManager, datanode, 7L, firstLease);

    assertThat(firstClaim).isNotNull();
    assertThat(firstClaim.isRegistrationReport()).isTrue();

    firstClaim.complete(false);
    long retryLease = requireLease(leaseManager, datanode, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim retryClaim =
        requireClaim(leaseManager, datanode, 7L, retryLease);

    assertThat(retryClaim).isNotNull();
    assertThat(retryClaim.isRegistrationReport()).isTrue();

    retryClaim.complete(true);
    long laterLease = requireLease(leaseManager, datanode, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim laterClaim =
        requireClaim(leaseManager, datanode, 7L, laterLease);

    assertThat(laterClaim).isNotNull();
    assertThat(laterClaim.isRegistrationReport()).isFalse();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void shouldRetainLeaseAfterGrantExpiry(boolean processingStarted) {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 100L, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();

    long firstLease = requireLease(leaseManager, firstDn, 7L);
    now.addAndGet(99L);
    SCMFullContainerReportLeaseManager.LeaseClaim claim =
        requireClaim(leaseManager, firstDn, 7L, firstLease);
    assertThat(claim).isNotNull();
    assertThat(leaseManager.requestLease(secondDn, 7L)).isEmpty();
    if (processingStarted) {
      assertThat(claim.startProcessing()).isTrue();
    }

    now.addAndGet(100L);

    assertThat(leaseManager.requestLease(secondDn, 7L)).isEmpty();
    if (!processingStarted) {
      assertThat(claim.startProcessing()).isTrue();
    }
    claim.complete(true);
    assertThat(leaseManager.requestLease(secondDn, 7L)).isPresent();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void shouldKeepNewRegistrationPendingWhenOlderReportCompletes(boolean processingStarted) {
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, () -> 0L, null);
    DatanodeDetails datanode = randomDatanodeDetails();
    leaseManager.markFullContainerReportDeferred(datanode);
    long oldLease = requireLease(leaseManager, datanode, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim oldClaim = requireClaim(leaseManager, datanode, 7L, oldLease);
    if (processingStarted) {
      assertThat(oldClaim.startProcessing()).isTrue();
    }

    // Re-registration needs a new FCR, even if an earlier registration's report is still outstanding.
    leaseManager.markFullContainerReportDeferred(datanode);
    assertThat(leaseManager.requestLease(datanode, 7L)).isEmpty();
    if (!processingStarted) {
      assertThat(oldClaim.startProcessing()).isTrue();
    }
    oldClaim.complete(true);

    long newLease = requireLease(leaseManager, datanode, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim newClaim = requireClaim(leaseManager, datanode, 7L, newLease);
    assertThat(newClaim.isRegistrationReport()).isTrue();
    newClaim.complete(true);

    long periodicLease = requireLease(leaseManager, datanode, 7L);
    assertThat(requireClaim(leaseManager, datanode, 7L, periodicLease).isRegistrationReport()).isFalse();
  }

  @Test
  void shouldInvalidateUnclaimedGrantOnReregistration() {
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, () -> 0L, null);
    DatanodeDetails datanode = randomDatanodeDetails();
    leaseManager.markFullContainerReportDeferred(datanode);
    long oldLease = requireLease(leaseManager, datanode, 7L);

    leaseManager.markFullContainerReportDeferred(datanode);

    assertThat(leaseManager.claimLease(datanode, OptionalLong.of(7L), 7L, oldLease)).isEmpty();
    long newLease = requireLease(leaseManager, datanode, 7L);
    assertThat(requireClaim(leaseManager, datanode, 7L, newLease).isRegistrationReport()).isTrue();
  }

  @Test
  void shouldStartProcessingClaimOnlyOnce() {
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, () -> 0L, null);
    DatanodeDetails datanode = randomDatanodeDetails();
    long lease = requireLease(leaseManager, datanode, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim claim = requireClaim(leaseManager, datanode, 7L, lease);

    assertThat(claim.startProcessing()).isTrue();
    assertThat(claim.startProcessing()).isFalse();
    assertThat(leaseManager.requestLease(randomDatanodeDetails(), 7L)).isEmpty();
    claim.complete(false);
    assertThat(leaseManager.requestLease(datanode, 7L)).isPresent();
  }

  @Test
  void shouldRetainQueuedRegistrationReportAfterGrantExpiry() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 100L, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();
    ContainerReportQueue queue = new ContainerReportQueue();
    leaseManager.markFullContainerReportDeferred(firstDn);
    long lease = requireLease(leaseManager, firstDn, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim claim = requireClaim(leaseManager, firstDn, 7L, lease);
    ContainerReportsProto report = ContainerReportsProto.newBuilder()
        .addReports(ContainerReplicaProto.newBuilder().setContainerID(1L).setState(ContainerReplicaProto.State.CLOSED))
        .build();
    queue.add(new ContainerReportFromDatanode(firstDn, report, claim));

    now.addAndGet(1_000L);

    assertThat(leaseManager.requestLease(secondDn, 7L)).isEmpty();
    ContainerReportFromDatanode queuedReport = (ContainerReportFromDatanode) queue.remove();
    assertThat(queuedReport.isRegistrationReport()).isTrue();
    assertThat(queuedReport.getReport().getReportsCount()).isEqualTo(1);
    assertThat(queuedReport.startProcessing()).isTrue();
    assertThat(leaseManager.requestLease(secondDn, 7L)).isEmpty();
    queuedReport.complete(true);
    long secondLease = requireLease(leaseManager, secondDn, 7L);
    requireClaim(leaseManager, secondDn, 7L, secondLease).complete(true);

    long nextLease = requireLease(leaseManager, firstDn, 7L);
    assertThat(requireClaim(leaseManager, firstDn, 7L, nextLease).isRegistrationReport()).isFalse();
  }

  @Test
  void shouldNotReleaseProcessingLeaseForRepeatedExpiredReport() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 100L, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();
    long lease = requireLease(leaseManager, firstDn, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim claim = requireClaim(leaseManager, firstDn, 7L, lease);
    assertThat(claim.startProcessing()).isTrue();

    now.addAndGet(1_000L);

    assertThat(leaseManager.claimLease(firstDn, OptionalLong.of(7L), 7L, lease)).isEmpty();
    assertThat(leaseManager.requestLease(secondDn, 7L)).isEmpty();
    claim.complete(true);
    assertThat(leaseManager.requestLease(secondDn, 7L)).isPresent();
  }

  @Test
  void shouldRetainClaimedLeaseUntilRemovedReportIsReleased() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 100L, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();
    long lease = requireLease(leaseManager, firstDn, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim claim = requireClaim(leaseManager, firstDn, 7L, lease);
    ContainerReportQueue queue = new ContainerReportQueue();
    queue.add(new ContainerReportFromDatanode(firstDn, ContainerReportsProto.getDefaultInstance(), claim));

    leaseManager.removeDatanode(firstDn);

    assertThat(leaseManager.requestLease(secondDn, 7L)).isEmpty();
    queue.clear();
    assertThat(leaseManager.requestLease(secondDn, 7L)).isPresent();
  }

  @Test
  void shouldRetainClaimedLeaseAcrossTermChange() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 100L, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();
    long lease = requireLease(leaseManager, firstDn, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim claim = requireClaim(leaseManager, firstDn, 7L, lease);

    now.addAndGet(1_000L);

    assertThat(leaseManager.requestLease(secondDn, 8L)).isEmpty();
    assertThat(claim.startProcessing()).isTrue();
    claim.complete(true);
    assertThat(leaseManager.requestLease(secondDn, 8L)).isPresent();
  }

  @Test
  void shouldNotReserveCapacityForDeniedRequests() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();
    DatanodeDetails thirdDn = randomDatanodeDetails();
    DatanodeDetails newDn = randomDatanodeDetails();

    long firstLease = requireLease(leaseManager, firstDn, 7L);
    assertThat(leaseManager.requestLease(secondDn, 7L)).isEmpty();
    assertThat(leaseManager.requestLease(thirdDn, 7L)).isEmpty();

    requireClaim(leaseManager, firstDn, 7L, firstLease).complete(true);

    // The earlier denied datanode need not retry before another datanode can use the free slot.
    now.incrementAndGet();
    long newLease = requireLease(leaseManager, newDn, 7L);
    requireClaim(leaseManager, newDn, 7L, newLease).complete(true);

    long thirdLease = requireLease(leaseManager, thirdDn, 7L);
    assertThat(leaseManager.requestLease(secondDn, 7L)).isEmpty();
    requireClaim(leaseManager, thirdDn, 7L, thirdLease).complete(true);

    long secondLease = requireLease(leaseManager, secondDn, 7L);
    requireClaim(leaseManager, secondDn, 7L, secondLease).complete(true);
  }

  @ParameterizedTest
  @ValueSource(ints = {20, 23})
  void shouldFillEveryStartupBatchWithReverseRetryOrder(int datanodeCount) {
    int maxOutstandingLeases = 10;
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(maxOutstandingLeases, 5_000L, now::get, null);
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
        OptionalLong lease = leaseManager.requestLease(datanode, 7L);
        if (claims.size() < maxOutstandingLeases) {
          assertThat(lease).as("free slot in batch %s", batches + 1).isPresent();
          claims.add(requireClaim(leaseManager, datanode, 7L, lease.getAsLong()));
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
        new SCMFullContainerReportLeaseManager(1, 5_000L, now::get, null);
    DatanodeDetails removedDn = randomDatanodeDetails();
    DatanodeDetails nextDn = randomDatanodeDetails();

    leaseManager.markFullContainerReportDeferred(removedDn);
    assertThat(leaseManager.requestLease(removedDn, 7L)).isPresent();

    leaseManager.removeDatanode(removedDn);

    long nextLease = requireLease(leaseManager, nextDn, 7L);
    assertThat(nextLease).isNotZero();
    requireClaim(leaseManager, nextDn, 7L, nextLease).complete(true);

    long newLease = requireLease(leaseManager, removedDn, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim claim =
        requireClaim(leaseManager, removedDn, 7L, newLease);
    assertThat(claim).isNotNull();
    assertThat(claim.isRegistrationReport()).isFalse();
  }

  private static long requireLease(
      SCMFullContainerReportLeaseManager leaseManager,
      DatanodeDetails datanode, long term) {
    OptionalLong lease = leaseManager.requestLease(datanode, term);
    assertThat(lease).isPresent();
    return lease.getAsLong();
  }

  private static SCMFullContainerReportLeaseManager.LeaseClaim requireClaim(
      SCMFullContainerReportLeaseManager leaseManager,
      DatanodeDetails datanode, long term, long leaseId) {
    Optional<SCMFullContainerReportLeaseManager.LeaseClaim> claim =
        leaseManager.claimLease(datanode, OptionalLong.of(term), term, leaseId);
    assertThat(claim).isPresent();
    return claim.get();
  }
}
