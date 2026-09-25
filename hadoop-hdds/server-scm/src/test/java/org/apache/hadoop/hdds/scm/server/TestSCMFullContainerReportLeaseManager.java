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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.junit.jupiter.api.Test;

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
    assertThat(leaseManager.getOutstandingLeaseCount()).isEqualTo(1);

    SCMFullContainerReportLeaseManager.LeaseClaim claim =
        requireClaim(leaseManager, firstDn, 7L, firstLease);
    assertThat(claim).isNotNull();
    assertThat(leaseManager.claimLease(firstDn, OptionalLong.of(7L), 7L,
        firstLease)).isEmpty();
    assertThat(leaseManager.getOutstandingLeaseCount()).isEqualTo(1);
    assertThat(leaseManager.requestLease(secondDn, 7L)).isEmpty();

    claim.complete(true);

    long secondLeaseAfterRelease = requireLease(leaseManager, secondDn, 7L);
    assertThat(secondLeaseAfterRelease).isNotZero();
    assertThat(leaseManager.getOutstandingLeaseCount()).isEqualTo(1);
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
    assertThat(leaseManager.getOutstandingLeaseCount()).isZero();
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

  @Test
  void shouldExpireClaimedLeaseBeforeProcessingStarts() {
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

    now.addAndGet(100L);

    assertThat(leaseManager.requestLease(secondDn, 7L)).isPresent();
    assertThat(claim.startProcessing()).isFalse();
    assertThat(leaseManager.getOutstandingLeaseCount()).isEqualTo(1);
  }

  @Test
  void shouldRetainLeaseWhileReportIsProcessing() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 100L, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();

    long firstLease = requireLease(leaseManager, firstDn, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim claim =
        requireClaim(leaseManager, firstDn, 7L, firstLease);
    assertThat(claim).isNotNull();
    assertThat(claim.startProcessing()).isTrue();

    now.addAndGet(100L);

    assertThat(leaseManager.requestLease(secondDn, 7L)).isEmpty();
    claim.complete(true);
    assertThat(leaseManager.requestLease(secondDn, 7L)).isPresent();
  }

  @Test
  void shouldGrantLeaseRequestsInOrder() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();
    DatanodeDetails thirdDn = randomDatanodeDetails();

    long firstLease = requireLease(leaseManager, firstDn, 7L);
    assertThat(leaseManager.requestLease(secondDn, 7L)).isEmpty();
    assertThat(leaseManager.requestLease(thirdDn, 7L)).isEmpty();

    requireClaim(leaseManager, firstDn, 7L, firstLease).complete(true);

    assertThat(leaseManager.requestLease(thirdDn, 7L)).isEmpty();
    assertThat(leaseManager.requestLease(secondDn, 7L)).isPresent();
  }

  @Test
  void shouldRemoveStaleLeaseRequest() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 100L, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails staleDn = randomDatanodeDetails();
    DatanodeDetails activeDn = randomDatanodeDetails();

    long firstLease = requireLease(leaseManager, firstDn, 7L);
    assertThat(leaseManager.requestLease(staleDn, 7L)).isEmpty();
    assertThat(leaseManager.requestLease(activeDn, 7L)).isEmpty();
    now.addAndGet(99L);
    assertThat(leaseManager.requestLease(activeDn, 7L)).isEmpty();
    requireClaim(leaseManager, firstDn, 7L, firstLease).complete(true);
    now.incrementAndGet();

    assertThat(leaseManager.requestLease(activeDn, 7L)).isPresent();
  }

  @Test
  void shouldRequeueReturningStaleRequester() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 100L, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails staleDn = randomDatanodeDetails();
    DatanodeDetails activeDn = randomDatanodeDetails();

    long firstLease = requireLease(leaseManager, firstDn, 7L);
    assertThat(leaseManager.requestLease(staleDn, 7L)).isEmpty();
    assertThat(leaseManager.requestLease(activeDn, 7L)).isEmpty();
    now.addAndGet(99L);
    assertThat(leaseManager.requestLease(activeDn, 7L)).isEmpty();
    requireClaim(leaseManager, firstDn, 7L, firstLease).complete(true);
    now.incrementAndGet();

    assertThat(leaseManager.requestLease(staleDn, 7L)).isEmpty();
    assertThat(leaseManager.requestLease(activeDn, 7L)).isPresent();
  }

  @Test
  void shouldRemoveAllDatanodeState() {
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
