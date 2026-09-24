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

    long firstLease = leaseManager.requestLease(firstDn, 7L);
    long secondLease = leaseManager.requestLease(secondDn, 7L);

    assertThat(firstLease).isNotZero();
    assertThat(secondLease).isZero();
    assertThat(leaseManager.getOutstandingLeaseCount()).isEqualTo(1);

    SCMFullContainerReportLeaseManager.LeaseClaim claim =
        leaseManager.claimLease(firstDn, 7L, firstLease);
    assertThat(claim).isNotNull();
    assertThat(leaseManager.claimLease(firstDn, 7L, firstLease)).isNull();
    assertThat(leaseManager.getOutstandingLeaseCount()).isEqualTo(1);
    assertThat(leaseManager.requestLease(secondDn, 7L)).isZero();

    leaseManager.completeLease(firstDn, firstLease, true);

    long secondLeaseAfterRelease = leaseManager.requestLease(secondDn, 7L);
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

    long wrongTermLease = leaseManager.requestLease(wrongTermDatanode, 3L);
    long expiredLease = leaseManager.requestLease(expiredDatanode, 3L);

    assertThat(leaseManager.claimLease(wrongTermDatanode, 4L,
        wrongTermLease)).isNull();

    now.addAndGet(101L);
    assertThat(leaseManager.claimLease(expiredDatanode, 3L,
        expiredLease)).isNull();
    assertThat(leaseManager.getOutstandingLeaseCount()).isZero();
  }

  @Test
  void shouldInvalidateOldTermLeasesWhenGrantingNewTerm() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();

    long oldTermLease = leaseManager.requestLease(firstDn, 3L);
    long newTermLease = leaseManager.requestLease(secondDn, 4L);

    assertThat(oldTermLease).isNotZero();
    assertThat(newTermLease).isNotZero();
    assertThat(leaseManager.claimLease(firstDn, 3L, oldTermLease)).isNull();
    assertThat(leaseManager.claimLease(secondDn, 4L, newTermLease)).isNotNull();
  }

  @Test
  void shouldPreserveDeferredRegistrationUntilReportIsProcessed() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, now::get, null);
    DatanodeDetails datanode = randomDatanodeDetails();

    leaseManager.markFullContainerReportDeferred(datanode);
    long firstLease = leaseManager.requestLease(datanode, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim firstClaim =
        leaseManager.claimLease(datanode, 7L, firstLease);

    assertThat(firstClaim).isNotNull();
    assertThat(firstClaim.isRegistrationReport()).isTrue();

    leaseManager.completeLease(datanode, firstLease, false);
    long retryLease = leaseManager.requestLease(datanode, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim retryClaim =
        leaseManager.claimLease(datanode, 7L, retryLease);

    assertThat(retryClaim).isNotNull();
    assertThat(retryClaim.isRegistrationReport()).isTrue();

    leaseManager.completeLease(datanode, retryLease, true);
    long laterLease = leaseManager.requestLease(datanode, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim laterClaim =
        leaseManager.claimLease(datanode, 7L, laterLease);

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

    long firstLease = leaseManager.requestLease(firstDn, 7L);
    now.addAndGet(99L);
    SCMFullContainerReportLeaseManager.LeaseClaim claim =
        leaseManager.claimLease(firstDn, 7L, firstLease);
    assertThat(claim).isNotNull();
    assertThat(leaseManager.requestLease(secondDn, 7L)).isZero();

    now.addAndGet(100L);

    assertThat(leaseManager.requestLease(secondDn, 7L)).isNotZero();
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

    long firstLease = leaseManager.requestLease(firstDn, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim claim =
        leaseManager.claimLease(firstDn, 7L, firstLease);
    assertThat(claim).isNotNull();
    assertThat(claim.startProcessing()).isTrue();

    now.addAndGet(100L);

    assertThat(leaseManager.requestLease(secondDn, 7L)).isZero();
    leaseManager.completeLease(firstDn, firstLease, true);
    assertThat(leaseManager.requestLease(secondDn, 7L)).isNotZero();
  }

  @Test
  void shouldGrantLeaseRequestsInOrder() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();
    DatanodeDetails thirdDn = randomDatanodeDetails();

    long firstLease = leaseManager.requestLease(firstDn, 7L);
    assertThat(leaseManager.requestLease(secondDn, 7L)).isZero();
    assertThat(leaseManager.requestLease(thirdDn, 7L)).isZero();

    leaseManager.completeLease(firstDn, firstLease, true);

    assertThat(leaseManager.requestLease(thirdDn, 7L)).isZero();
    assertThat(leaseManager.requestLease(secondDn, 7L)).isNotZero();
  }

  @Test
  void shouldRemoveStaleLeaseRequest() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 100L, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails staleDn = randomDatanodeDetails();
    DatanodeDetails activeDn = randomDatanodeDetails();

    long firstLease = leaseManager.requestLease(firstDn, 7L);
    assertThat(leaseManager.requestLease(staleDn, 7L)).isZero();
    assertThat(leaseManager.requestLease(activeDn, 7L)).isZero();
    now.addAndGet(99L);
    assertThat(leaseManager.requestLease(activeDn, 7L)).isZero();
    leaseManager.completeLease(firstDn, firstLease, true);
    now.incrementAndGet();

    assertThat(leaseManager.requestLease(activeDn, 7L)).isNotZero();
  }

  @Test
  void shouldRequeueReturningStaleRequester() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 100L, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails staleDn = randomDatanodeDetails();
    DatanodeDetails activeDn = randomDatanodeDetails();

    long firstLease = leaseManager.requestLease(firstDn, 7L);
    assertThat(leaseManager.requestLease(staleDn, 7L)).isZero();
    assertThat(leaseManager.requestLease(activeDn, 7L)).isZero();
    now.addAndGet(99L);
    assertThat(leaseManager.requestLease(activeDn, 7L)).isZero();
    leaseManager.completeLease(firstDn, firstLease, true);
    now.incrementAndGet();

    assertThat(leaseManager.requestLease(staleDn, 7L)).isZero();
    assertThat(leaseManager.requestLease(activeDn, 7L)).isNotZero();
  }

  @Test
  void shouldRemoveAllDatanodeState() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, now::get, null);
    DatanodeDetails removedDn = randomDatanodeDetails();
    DatanodeDetails nextDn = randomDatanodeDetails();

    leaseManager.markFullContainerReportDeferred(removedDn);
    assertThat(leaseManager.requestLease(removedDn, 7L)).isNotZero();

    leaseManager.removeDatanode(removedDn);

    long nextLease = leaseManager.requestLease(nextDn, 7L);
    assertThat(nextLease).isNotZero();
    leaseManager.completeLease(nextDn, nextLease, true);

    long newLease = leaseManager.requestLease(removedDn, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim claim =
        leaseManager.claimLease(removedDn, 7L, newLease);
    assertThat(claim).isNotNull();
    assertThat(claim.isRegistrationReport()).isFalse();
  }
}
